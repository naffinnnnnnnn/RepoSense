package agentapp

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/reposense/reposense/internal/domain/agent"
	"github.com/reposense/reposense/internal/domain/common"
	"github.com/reposense/reposense/internal/domain/graph"
	"github.com/reposense/reposense/internal/ports"
)

type Config struct {
	RetrievalTopK      int
	MaxRetrievalRounds int
	MaxCitations       int
	GraphLimit         int
}

func DefaultConfig() Config {
	return Config{RetrievalTopK: 20, MaxRetrievalRounds: 2, MaxCitations: 30, GraphLimit: 1_000}
}

type Service struct {
	graph     ports.GraphStore
	retriever ports.Retriever
	repo      ports.AgentRepository
	planner   Planner
	generator ports.AnswerGenerator
	events    ports.EventPublisher
	observer  ports.Observer
	ids       ports.IDGenerator
	clock     ports.Clock
	config    Config
}

func New(graphStore ports.GraphStore, retriever ports.Retriever, repo ports.AgentRepository, planner Planner, generator ports.AnswerGenerator, events ports.EventPublisher, observer ports.Observer, ids ports.IDGenerator, clock ports.Clock, config Config) (*Service, error) {
	if graphStore == nil && retriever == nil {
		return nil, errors.New("at least one knowledge source must not be nil")
	}
	if repo == nil {
		return nil, errors.New("agent repository must not be nil")
	}
	if planner == nil {
		planner = KeywordPlanner{}
	}
	if generator == nil {
		generator = NewStructuredGenerator(StructuredGeneratorConfig{})
	}
	if events == nil {
		events = noopPublisher{}
	}
	if observer == nil {
		observer = noopObserver{}
	}
	if ids == nil {
		ids = randomIDs{}
	}
	if clock == nil {
		clock = systemClock{}
	}
	defaults := DefaultConfig()
	if config.RetrievalTopK <= 0 {
		config.RetrievalTopK = defaults.RetrievalTopK
	}
	if config.RetrievalTopK > 100 {
		config.RetrievalTopK = 100
	}
	if config.MaxRetrievalRounds <= 0 {
		config.MaxRetrievalRounds = defaults.MaxRetrievalRounds
	}
	if config.MaxRetrievalRounds > 5 {
		config.MaxRetrievalRounds = 5
	}
	if config.MaxCitations <= 0 {
		config.MaxCitations = defaults.MaxCitations
	}
	if config.MaxCitations > 100 {
		config.MaxCitations = 100
	}
	if config.GraphLimit <= 0 {
		config.GraphLimit = defaults.GraphLimit
	}
	if config.GraphLimit > 10_000 {
		config.GraphLimit = 10_000
	}
	return &Service{graph: graphStore, retriever: retriever, repo: repo, planner: planner, generator: generator,
		events: events, observer: observer, ids: ids, clock: clock, config: config}, nil
}

func (s *Service) Ask(ctx context.Context, cmd ports.AskQuestion) (<-chan ports.AgentEvent, error) {
	if err := cmd.Validate(); err != nil {
		code := agent.ErrInvalidInput
		if strings.Contains(err.Error(), agent.ReadPermission) {
			code = agent.ErrPermissionDenied
		}
		return nil, domainError(code, "guard", err.Error(), false, err)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	now, runID := s.clock.Now().UTC(), s.ids.New("run")
	conversation := agent.Conversation{EntityMeta: agent.NewMeta(cmd.ConversationID, cmd.Scope, "ACTIVE", principal(cmd.UserID), now),
		ConversationID: cmd.ConversationID, UserID: cmd.UserID, Scope: cmd.Scope, Title: title(cmd.Question)}
	run := agent.Run{EntityMeta: agent.NewMeta(runID, cmd.Scope, string(agent.RunRunning), principal(cmd.UserID), now),
		RunID: runID, ConversationID: cmd.ConversationID, SnapshotID: cmd.Scope.SnapshotID, Question: strings.TrimSpace(cmd.Question), Status: agent.RunRunning}
	if err := s.repo.CreateRun(ctx, conversation, run); err != nil {
		return nil, domainError(agent.ErrPersistence, "create_run", "failed to create agent run", true, err)
	}
	// The event count is bounded: start, plan, one retrieve per round,
	// evaluate, and one terminal event. Buffering prevents abandoned clients
	// from blocking final persistence.
	events := make(chan ports.AgentEvent, s.config.MaxRetrievalRounds+5)
	go s.execute(ctx, cmd, run, events)
	return events, nil
}

func (s *Service) Resume(ctx context.Context, runID string, _ ports.ResumeInput) (<-chan ports.AgentEvent, error) {
	if strings.TrimSpace(runID) == "" {
		return nil, domainError(agent.ErrInvalidInput, "resume", "run_id must not be empty", false, nil)
	}
	run, err := s.repo.GetRun(ctx, runID)
	if err != nil {
		return nil, domainError(agent.ErrRunNotFound, "resume", "agent run not found", false, err)
	}
	if run.Status != agent.RunInterrupted {
		return nil, domainError(agent.ErrRunNotResumable, "resume", "read-only question runs do not require approval and cannot be resumed", false, nil)
	}
	return nil, domainError(agent.ErrRunNotResumable, "resume", "no resumable checkpoint is available", false, nil)
}

func (s *Service) execute(ctx context.Context, cmd agent.QuestionCommand, run agent.Run, out chan<- ports.AgentEvent) {
	defer close(out)
	started, sequence := s.clock.Now().UTC(), 0
	emit := func(kind agent.EventType, payload map[string]any) {
		sequence++
		out <- agent.Event{RunID: run.RunID, Type: kind, Sequence: sequence, OccurredAt: s.clock.Now().UTC(), Payload: payload}
	}
	emit(agent.EventRunStarted, map[string]any{"snapshot_id": cmd.Scope.SnapshotID})
	var finalErr error
	finish := s.observer.Stage(ctx, "agent_run", labels(cmd.Scope))
	defer func() { finish(finalErr) }()

	plan, err := s.planner.Plan(ctx, cmd)
	if err != nil {
		finalErr = s.fail(ctx, &run, started, agent.ErrGenerationFailure, "plan", "failed to plan repository question", err, emit)
		return
	}
	run.Plan = plan
	emit(agent.EventPlanned, map[string]any{"intent": plan.Intent, "steps": len(plan.Steps), "graph_depth": plan.GraphDepth})

	evidence, graphResult, warnings, available, err := s.retrieve(ctx, cmd, &run, emit)
	if err != nil {
		code := agent.ErrKnowledgeUnavailable
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			code = agent.ErrRunCancelled
		}
		finalErr = s.fail(ctx, &run, started, code, "retrieve", "repository knowledge retrieval failed", err, emit)
		return
	}
	if !available {
		finalErr = s.fail(ctx, &run, started, agent.ErrKnowledgeUnavailable, "retrieve", "all configured knowledge sources failed", nil, emit)
		return
	}

	refs := append([]common.SourceRef(nil), evidence.Sources...)
	refs = append(refs, graphSources(graphResult)...)
	expectedCommit := canonicalCommit(graphSources(graphResult))
	if expectedCommit == "" {
		expectedCommit = canonicalCommit(refs)
	}
	citations, discarded := agent.NormalizeCitations(refs, expectedCommit, s.config.MaxCitations)
	if discarded > 0 {
		warnings = append(warnings, fmt.Sprintf("discarded %d invalid, cross-version, or excess citations", discarded))
	}
	if graphResult.Diagnostics.Truncated {
		warnings = append(warnings, "knowledge graph result was truncated")
	}
	warnings = uniqueStrings(warnings)
	if len(run.Plan.Steps) > 1 {
		run.Plan.Steps[1].Status = agent.StepCompleted
	}
	emit(agent.EventEvaluated, map[string]any{"citations": len(citations), "discarded": discarded, "sufficient": len(citations) > 0})

	var answer agent.Answer
	if len(citations) == 0 {
		answer = insufficientAnswer(cmd.Locale, warnings)
	} else {
		if len(run.Plan.Steps) > 2 {
			run.Plan.Steps[2].Status = agent.StepRunning
		}
		generated, generateErr := s.generator.GenerateAnswer(ctx, ports.AnswerGenerationContext{Scope: cmd.Scope, Question: cmd.Question,
			Intent: plan.Intent, Graph: graphResult, Evidence: evidence, Citations: citations, Locale: cmd.Locale})
		if generateErr != nil {
			code := agent.ErrGenerationFailure
			if errors.Is(generateErr, context.Canceled) || errors.Is(generateErr, context.DeadlineExceeded) {
				code = agent.ErrRunCancelled
			}
			finalErr = s.fail(ctx, &run, started, code, "synthesize", "failed to generate evidence-backed answer", generateErr, emit)
			return
		}
		usedCitations, selectErr := selectCitations(citations, generated.CitationIndexes)
		if selectErr != nil {
			finalErr = s.fail(ctx, &run, started, agent.ErrGenerationFailure, "validate_answer", "generator returned invalid citation indexes", selectErr, emit)
			return
		}
		answer = agent.Answer{AnswerMarkdown: generated.AnswerMarkdown, Citations: usedCitations, Warnings: warnings,
			Model: generated.Model, PromptVersion: generated.PromptVersion}
		run.TokenUsage = generated.TokenUsage
		if len(run.Plan.Steps) > 2 {
			run.Plan.Steps[2].Status = agent.StepCompleted
		}
	}
	if err := answer.Validate(); err != nil {
		finalErr = s.fail(ctx, &run, started, agent.ErrGenerationFailure, "validate_answer", "answer failed evidence validation", err, emit)
		return
	}
	run.Answer, run.Status = &answer, agent.RunCompleted
	run.EntityMeta.Status, run.UpdatedAt = string(agent.RunCompleted), s.clock.Now().UTC()
	run.LatencyMS = s.clock.Now().UTC().Sub(started).Milliseconds()
	run.PublishedEvent = common.EventEnvelope{EventID: s.ids.New("evt"), EventType: "qa.run.completed.v1", AggregateID: run.RunID,
		OccurredAt: s.clock.Now().UTC(), Producer: "repository-agent", PayloadVersion: 1, TraceID: cmd.Scope.TraceID,
		Payload: map[string]any{"run_id": run.RunID, "conversation_id": run.ConversationID, "snapshot_id": run.SnapshotID,
			"status": run.Status, "citation_count": len(answer.Citations), "insufficient_evidence": answer.InsufficientEvidence,
			"token_usage": run.TokenUsage, "latency_ms": run.LatencyMS, "model": answer.Model, "prompt_version": answer.PromptVersion}}
	if err := s.repo.UpdateRun(ctx, run); err != nil {
		finalErr = s.fail(ctx, &run, started, agent.ErrPersistence, "save_run", "failed to save completed agent run", err, emit)
		return
	}
	if err := s.events.Publish(ctx, run.PublishedEvent); err != nil {
		finalErr = s.fail(ctx, &run, started, agent.ErrPersistence, "publish_event", "agent run completed but event publication failed", err, emit)
		return
	}
	s.observer.Count("agent_runs_completed_total", 1, labels(cmd.Scope))
	s.observer.Count("agent_citations_total", int64(len(answer.Citations)), labels(cmd.Scope))
	if run.TokenUsage > 0 {
		s.observer.Count("agent_tokens_total", int64(run.TokenUsage), labels(cmd.Scope))
	}
	emit(agent.EventCompleted, map[string]any{"answer": answer, "latency_ms": run.LatencyMS, "token_usage": run.TokenUsage})
}

func (s *Service) retrieve(ctx context.Context, cmd agent.QuestionCommand, run *agent.Run, emit func(agent.EventType, map[string]any)) (ports.EvidenceBundle, graph.Result, []string, bool, error) {
	var evidence ports.EvidenceBundle
	var graphResult graph.Result
	warnings, successes := []string{}, 0
	if len(run.Plan.Steps) > 0 {
		run.Plan.Steps[0].Status = agent.StepRunning
	}
	for round := 1; round <= s.config.MaxRetrievalRounds; round++ {
		if err := ctx.Err(); err != nil {
			return evidence, graphResult, warnings, successes > 0, err
		}
		roundHits := 0
		if s.retriever != nil {
			started := s.clock.Now().UTC()
			bundle, err := s.retriever.Search(ctx, ports.RetrievalRequest{Scope: cmd.Scope, Query: expandedQuery(cmd.Question, run.Plan.Intent, round), Strategies: run.Plan.Strategies, TopK: s.config.RetrievalTopK})
			call := agent.ToolCall{CallID: s.ids.New("tc"), Tool: "search_code", Round: round,
				Arguments: map[string]string{"strategies": strings.Join(run.Plan.Strategies, ","), "top_k": fmt.Sprint(s.config.RetrievalTopK)}, LatencyMS: s.clock.Now().UTC().Sub(started).Milliseconds()}
			if err != nil {
				if ctx.Err() != nil {
					return evidence, graphResult, warnings, successes > 0, ctx.Err()
				}
				call.Status, call.ErrorCode = agent.StepFailed, "RETRIEVER_ERROR"
				warnings = append(warnings, "code retrieval was partially unavailable")
			} else {
				call.Status, call.ResultCount = agent.StepCompleted, len(bundle.Sources)+len(bundle.ArtifactIDs)
				evidence.ArtifactIDs = append(evidence.ArtifactIDs, bundle.ArtifactIDs...)
				evidence.Sources = append(evidence.Sources, bundle.Sources...)
				roundHits, successes = call.ResultCount, successes+1
			}
			run.ToolCalls = append(run.ToolCalls, call)
		}
		if round == 1 && s.graph != nil {
			started := s.clock.Now().UTC()
			result, err := s.graph.Query(ctx, graph.Query{Scope: cmd.Scope, Direction: graph.DirectionBoth, Depth: run.Plan.GraphDepth, Limit: s.config.GraphLimit})
			call := agent.ToolCall{CallID: s.ids.New("tc"), Tool: "query_graph", Round: round,
				Arguments: map[string]string{"depth": fmt.Sprint(run.Plan.GraphDepth), "limit": fmt.Sprint(s.config.GraphLimit)}, LatencyMS: s.clock.Now().UTC().Sub(started).Milliseconds()}
			if err != nil {
				if ctx.Err() != nil {
					return evidence, graphResult, warnings, successes > 0, ctx.Err()
				}
				call.Status, call.ErrorCode = agent.StepFailed, "GRAPH_ERROR"
				warnings = append(warnings, "knowledge graph was partially unavailable")
			} else {
				call.Status, call.ResultCount = agent.StepCompleted, len(result.Nodes)+len(result.Edges)
				graphResult, roundHits, successes = result, roundHits+call.ResultCount, successes+1
			}
			run.ToolCalls = append(run.ToolCalls, call)
		}
		emit(agent.EventRetrieved, map[string]any{"round": round, "hits": roundHits, "tool_calls": len(run.ToolCalls)})
		if len(evidence.Sources)+len(graphSources(graphResult)) > 0 {
			break
		}
	}
	evidence.ArtifactIDs = uniqueStrings(evidence.ArtifactIDs)
	if len(run.Plan.Steps) > 0 {
		run.Plan.Steps[0].Status = agent.StepCompleted
	}
	return evidence, graphResult, warnings, successes > 0, nil
}

func (s *Service) fail(ctx context.Context, run *agent.Run, started time.Time, code agent.ErrorCode, operation, message string, cause error, emit func(agent.EventType, map[string]any)) error {
	err := domainError(code, operation, message, code == agent.ErrKnowledgeUnavailable || code == agent.ErrPersistence, cause)
	run.Status, run.EntityMeta.Status, run.FailureCode, run.FailureMessage = agent.RunFailed, string(agent.RunFailed), string(code), message
	run.UpdatedAt, run.LatencyMS = s.clock.Now().UTC(), s.clock.Now().UTC().Sub(started).Milliseconds()
	persistCtx := ctx
	if ctx.Err() != nil {
		persistCtx = context.WithoutCancel(ctx)
	}
	if saveErr := s.repo.UpdateRun(persistCtx, *run); saveErr != nil && code != agent.ErrPersistence {
		err = domainError(agent.ErrPersistence, "save_failed_run", "failed to save failed agent run", true, saveErr)
	}
	s.observer.Count("agent_runs_failed_total", 1, labels(common.Scope{TenantID: run.TenantID, RepositoryID: run.RepositoryID, SnapshotID: run.SnapshotID, TraceID: run.TraceID}))
	emit(agent.EventFailed, map[string]any{"code": err.Code, "message": err.Message, "retryable": err.Retryable})
	return err
}

func graphSources(result graph.Result) []common.SourceRef {
	refs := make([]common.SourceRef, 0, len(result.Nodes)+len(result.Edges))
	for _, node := range result.Nodes {
		if node.SourceRef != nil {
			refs = append(refs, *node.SourceRef)
		}
	}
	for _, edge := range result.Edges {
		refs = append(refs, edge.Evidence)
	}
	return refs
}

func canonicalCommit(refs []common.SourceRef) string {
	counts := map[string]int{}
	for _, ref := range refs {
		if ref.Validate() == nil {
			counts[ref.CommitSHA]++
		}
	}
	commits := make([]string, 0, len(counts))
	for commit := range counts {
		commits = append(commits, commit)
	}
	sort.Strings(commits)
	best, count := "", 0
	for _, commit := range commits {
		if counts[commit] > count {
			best, count = commit, counts[commit]
		}
	}
	return best
}

func insufficientAnswer(locale string, warnings []string) agent.Answer {
	message := "当前固定快照中没有足够的有效源码证据来可靠回答该问题。请补充更具体的符号、文件路径或错误信息，或确认该快照的索引与图谱已经就绪。"
	if locale == "en-US" {
		message = "There is not enough valid source evidence in the pinned snapshot to answer reliably. Provide a symbol, file path, or error detail, or verify that the snapshot index and graph are ready."
	}
	return agent.Answer{AnswerMarkdown: message, InsufficientEvidence: true, Warnings: warnings, Model: "evidence-gate", PromptVersion: "qa-insufficient-v1"}
}

func selectCitations(citations []common.SourceRef, indexes []int) ([]common.SourceRef, error) {
	if indexes == nil {
		return append([]common.SourceRef(nil), citations...), nil
	}
	if len(indexes) == 0 {
		return nil, errors.New("generator did not cite any evidence")
	}
	seen, selected := map[int]bool{}, make([]common.SourceRef, 0, len(indexes))
	for _, index := range indexes {
		if index < 0 || index >= len(citations) {
			return nil, fmt.Errorf("citation index %d is out of range", index)
		}
		if seen[index] {
			continue
		}
		seen[index] = true
		selected = append(selected, citations[index])
	}
	return selected, nil
}

func expandedQuery(question string, intent agent.Intent, round int) string {
	if round <= 1 {
		return strings.TrimSpace(question)
	}
	return fmt.Sprintf("%s\nintent:%s related symbols dependencies callers implementations", strings.TrimSpace(question), intent)
}
func title(question string) string {
	r := []rune(strings.TrimSpace(question))
	if len(r) > 80 {
		r = r[:80]
	}
	return string(r)
}
func principal(userID string) string {
	if strings.TrimSpace(userID) == "" {
		return "anonymous"
	}
	return strings.TrimSpace(userID)
}
func uniqueStrings(values []string) []string {
	seen := map[string]bool{}
	out := []string{}
	for _, v := range values {
		if v = strings.TrimSpace(v); v != "" && !seen[v] {
			seen[v] = true
			out = append(out, v)
		}
	}
	sort.Strings(out)
	return out
}
func labels(scope common.Scope) map[string]string {
	return map[string]string{"tenant_id": scope.TenantID, "repository_id": scope.RepositoryID, "snapshot_id": scope.SnapshotID, "trace_id": scope.TraceID}
}
func domainError(code agent.ErrorCode, operation, message string, retryable bool, cause error) *agent.DomainError {
	return &agent.DomainError{Code: code, Operation: operation, Message: message, Retryable: retryable, Cause: cause}
}

type noopPublisher struct{}

func (noopPublisher) Publish(context.Context, common.EventEnvelope) error { return nil }

type noopObserver struct{}

func (noopObserver) Stage(context.Context, string, map[string]string) func(error) {
	return func(error) {}
}
func (noopObserver) Count(string, int64, map[string]string) {}

type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now().UTC() }

type randomIDs struct{}

func (randomIDs) New(prefix string) string {
	var b [12]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("%s_%d", prefix, time.Now().UnixNano())
	}
	return prefix + "_" + hex.EncodeToString(b[:])
}

var _ ports.RepositoryAgent = (*Service)(nil)
