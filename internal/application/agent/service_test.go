package agentapp

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/reposense/reposense/internal/adapters/memory"
	"github.com/reposense/reposense/internal/domain/agent"
	"github.com/reposense/reposense/internal/domain/common"
	"github.com/reposense/reposense/internal/domain/graph"
	"github.com/reposense/reposense/internal/domain/repository"
	"github.com/reposense/reposense/internal/ports"
)

type fakeGraph struct {
	result graph.Result
	err    error
	calls  int
}

func (f *fakeGraph) Build(context.Context, graph.BuildCommand) (graph.Revision, error) {
	return graph.Revision{}, errors.New("not implemented")
}
func (f *fakeGraph) Query(context.Context, graph.Query) (graph.Result, error) {
	f.calls++
	return f.result, f.err
}

type fakeRetriever struct {
	bundles []ports.EvidenceBundle
	err     error
	calls   int
}

func (f *fakeRetriever) Index(context.Context, common.Scope, []repository.CodeArtifact) (ports.IndexRevision, error) {
	return ports.IndexRevision{}, nil
}
func (f *fakeRetriever) Search(context.Context, ports.RetrievalRequest) (ports.EvidenceBundle, error) {
	i := f.calls
	f.calls++
	if f.err != nil {
		return ports.EvidenceBundle{}, f.err
	}
	if len(f.bundles) == 0 {
		return ports.EvidenceBundle{}, nil
	}
	if i >= len(f.bundles) {
		i = len(f.bundles) - 1
	}
	return f.bundles[i], nil
}

type fakePublisher struct {
	events []common.EventEnvelope
	err    error
}

func (f *fakePublisher) Publish(_ context.Context, event common.EventEnvelope) error {
	f.events = append(f.events, event)
	return f.err
}

type sequenceIDs struct{ value int }

func (s *sequenceIDs) New(prefix string) string {
	s.value++
	return fmt.Sprintf("%s_%d", prefix, s.value)
}

type fixedClock struct{ now time.Time }

func (c fixedClock) Now() time.Time { return c.now }

type invalidGenerator struct{}

func (invalidGenerator) GenerateAnswer(context.Context, ports.AnswerGenerationContext) (ports.AnswerGenerationResult, error) {
	return ports.AnswerGenerationResult{AnswerMarkdown: "claim", CitationIndexes: []int{99}}, nil
}

type staticRetriever struct{ bundle ports.EvidenceBundle }

func (f staticRetriever) Index(context.Context, common.Scope, []repository.CodeArtifact) (ports.IndexRevision, error) {
	return ports.IndexRevision{}, nil
}
func (f staticRetriever) Search(context.Context, ports.RetrievalRequest) (ports.EvidenceBundle, error) {
	return f.bundle, nil
}

func command() agent.QuestionCommand {
	return agent.QuestionCommand{Scope: common.Scope{TenantID: "tenant", RepositoryID: "repo", SnapshotID: "snap", TraceID: "trace"}, ConversationID: "conv", UserID: "user", Question: "What calls Handle?", Permissions: []string{agent.ReadPermission}, Locale: "en-US"}
}
func source(commit, path string) common.SourceRef {
	return common.SourceRef{CommitSHA: commit, Path: path, SymbolID: "sym", StartLine: 1, EndLine: 4, ContentHash: "sha256:x"}
}
func collect(ch <-chan ports.AgentEvent) []ports.AgentEvent {
	var events []ports.AgentEvent
	for event := range ch {
		events = append(events, event)
	}
	return events
}

func TestServiceAnswersWithValidatedSnapshotEvidence(t *testing.T) {
	ref := source("sha", "internal/handler.go")
	graphStore := &fakeGraph{result: graph.Result{Nodes: []graph.Entity{{NodeID: "handle", EntityType: graph.EntityFunction, Name: "Handle", SourceRef: &ref}}, Diagnostics: graph.Diagnostics{RevisionID: "gr"}}}
	retriever := &fakeRetriever{bundles: []ports.EvidenceBundle{{Sources: []common.SourceRef{ref, source("other", "wrong.go")}, ArtifactIDs: []string{"a1"}}}}
	repo, publisher := memory.NewAgentRepository(), &fakePublisher{}
	service, err := New(graphStore, retriever, repo, nil, nil, publisher, nil, &sequenceIDs{}, fixedClock{time.Unix(100, 0)}, Config{})
	if err != nil {
		t.Fatal(err)
	}
	stream, err := service.Ask(context.Background(), command())
	if err != nil {
		t.Fatal(err)
	}
	events := collect(stream)
	if len(events) < 5 || events[0].Type != agent.EventRunStarted || events[len(events)-1].Type != agent.EventCompleted {
		t.Fatalf("unexpected events: %#v", events)
	}
	run, err := repo.GetRun(context.Background(), events[0].RunID)
	if err != nil {
		t.Fatal(err)
	}
	if run.Status != agent.RunCompleted || run.Answer == nil || run.Answer.InsufficientEvidence {
		t.Fatalf("unexpected run: %#v", run)
	}
	if len(run.Answer.Citations) != 1 || run.Answer.Citations[0].CommitSHA != "sha" {
		t.Fatalf("cross-version evidence leaked: %#v", run.Answer.Citations)
	}
	if len(run.ToolCalls) != 2 || run.ToolCalls[0].Arguments["query"] != "" {
		t.Fatalf("tool calls should be bounded and redacted: %#v", run.ToolCalls)
	}
	if len(publisher.events) != 1 || publisher.events[0].EventType != "qa.run.completed.v1" {
		t.Fatalf("event not published: %#v", publisher.events)
	}
}

func TestServiceRetriesThenProducesAnswer(t *testing.T) {
	retriever := &fakeRetriever{bundles: []ports.EvidenceBundle{{}, {Sources: []common.SourceRef{source("sha", "main.go")}}}}
	repo := memory.NewAgentRepository()
	service, _ := New(nil, retriever, repo, nil, nil, nil, nil, &sequenceIDs{}, fixedClock{time.Unix(100, 0)}, Config{MaxRetrievalRounds: 2})
	stream, err := service.Ask(context.Background(), command())
	if err != nil {
		t.Fatal(err)
	}
	events := collect(stream)
	if retriever.calls != 2 || events[len(events)-1].Type != agent.EventCompleted {
		t.Fatalf("calls=%d events=%#v", retriever.calls, events)
	}
}

func TestServiceReturnsExplicitInsufficientEvidence(t *testing.T) {
	repo := memory.NewAgentRepository()
	service, _ := New(&fakeGraph{}, &fakeRetriever{}, repo, nil, nil, nil, nil, &sequenceIDs{}, fixedClock{time.Unix(100, 0)}, Config{})
	stream, _ := service.Ask(context.Background(), command())
	events := collect(stream)
	run, _ := repo.GetRun(context.Background(), events[0].RunID)
	if run.Status != agent.RunCompleted || run.Answer == nil || !run.Answer.InsufficientEvidence || len(run.Answer.Citations) != 0 {
		t.Fatalf("unexpected run: %#v", run)
	}
}

func TestServiceFailsWhenEveryKnowledgeSourceFails(t *testing.T) {
	repo := memory.NewAgentRepository()
	service, _ := New(&fakeGraph{err: errors.New("graph down")}, &fakeRetriever{err: errors.New("index down")}, repo, nil, nil, nil, nil, &sequenceIDs{}, fixedClock{time.Unix(100, 0)}, Config{})
	stream, _ := service.Ask(context.Background(), command())
	events := collect(stream)
	if events[len(events)-1].Type != agent.EventFailed || events[len(events)-1].Payload["code"] != agent.ErrKnowledgeUnavailable {
		t.Fatalf("unexpected events: %#v", events)
	}
	run, _ := repo.GetRun(context.Background(), events[0].RunID)
	if run.Status != agent.RunFailed {
		t.Fatalf("run status=%s", run.Status)
	}
}

func TestServiceRejectsGeneratorCitationFabrication(t *testing.T) {
	repo := memory.NewAgentRepository()
	retriever := &fakeRetriever{bundles: []ports.EvidenceBundle{{Sources: []common.SourceRef{source("sha", "main.go")}}}}
	service, _ := New(nil, retriever, repo, nil, invalidGenerator{}, nil, nil, &sequenceIDs{}, fixedClock{time.Unix(100, 0)}, Config{})
	stream, _ := service.Ask(context.Background(), command())
	events := collect(stream)
	if events[len(events)-1].Type != agent.EventFailed || events[len(events)-1].Payload["code"] != agent.ErrGenerationFailure {
		t.Fatalf("unexpected events: %#v", events)
	}
}

func TestServiceGuardRejectsUnauthorizedQuestionSynchronously(t *testing.T) {
	service, _ := New(nil, &fakeRetriever{}, memory.NewAgentRepository(), nil, nil, nil, nil, nil, nil, Config{})
	cmd := command()
	cmd.Permissions = nil
	stream, err := service.Ask(context.Background(), cmd)
	if stream != nil || !agent.IsCode(err, agent.ErrPermissionDenied) {
		t.Fatalf("stream=%v err=%v", stream, err)
	}
}

func TestServiceHandlesConcurrentQuestions(t *testing.T) {
	repo := memory.NewAgentRepository()
	service, err := New(nil, staticRetriever{bundle: ports.EvidenceBundle{Sources: []common.SourceRef{source("sha", "main.go")}}}, repo, nil, nil, nil, nil, nil, nil, Config{})
	if err != nil {
		t.Fatal(err)
	}
	const count = 32
	var wg sync.WaitGroup
	errorsFound := make(chan error, count)
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			cmd := command()
			cmd.ConversationID = fmt.Sprintf("conv-%d", i)
			stream, askErr := service.Ask(context.Background(), cmd)
			if askErr != nil {
				errorsFound <- askErr
				return
			}
			events := collect(stream)
			if len(events) == 0 || events[len(events)-1].Type != agent.EventCompleted {
				errorsFound <- fmt.Errorf("question %d did not complete", i)
			}
		}(i)
	}
	wg.Wait()
	close(errorsFound)
	for err := range errorsFound {
		t.Error(err)
	}
}
