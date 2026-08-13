package ports

import (
	"context"

	"github.com/reposense/reposense/internal/domain/agent"
	"github.com/reposense/reposense/internal/domain/assistant"
	"github.com/reposense/reposense/internal/domain/common"
	"github.com/reposense/reposense/internal/domain/graph"
	"github.com/reposense/reposense/internal/domain/rag"
	"github.com/reposense/reposense/internal/domain/repository"
	"github.com/reposense/reposense/internal/domain/visualization"
	"github.com/reposense/reposense/internal/domain/wiki"
)

// 以下 DTO 在编译期明确后续模块的边界，具体实现不属于当前里程碑。
type GraphSource interface {
	GraphInput(context.Context, common.Scope) (graph.BuildInput, error)
}

type GraphRepository interface {
	FindByIdempotencyKey(context.Context, common.Scope, string) (graph.Revision, bool, error)
	RevisionBySnapshot(context.Context, common.Scope) (graph.Revision, error)
	Save(context.Context, string, graph.Revision) error
	Query(context.Context, graph.Query) (graph.Result, error)
}

type GraphStore interface {
	Build(context.Context, graph.BuildCommand) (graph.Revision, error)
	Query(context.Context, graph.Query) (graph.Result, error)
}

type RetrievalRequest = rag.RetrievalRequest
type EvidenceBundle = rag.EvidenceBundle
type IndexRevision = rag.IndexRevision

type RAGRepository interface {
	RevisionBySnapshot(context.Context, common.Scope) (rag.IndexRevision, error)
	Save(context.Context, rag.IndexRevision) error
	Documents(context.Context, common.Scope, string) ([]rag.IndexDocument, error)
}

// Vectorizer and Reranker are provider-neutral algorithm seams. Production
// adapters may use embedding/reranking models while deterministic local
// implementations keep tests and offline development reproducible.
type Vectorizer interface {
	Vectorize(context.Context, []string) ([][]float64, error)
	Version() string
}

type Reranker interface {
	Rerank(context.Context, string, []rag.Hit) ([]rag.Hit, error)
	Version() string
}

type Retriever interface {
	Index(context.Context, common.Scope, []repository.CodeArtifact) (IndexRevision, error)
	Search(context.Context, RetrievalRequest) (EvidenceBundle, error)
}

type WikiJob = wiki.Job
type WikiPageRevision = wiki.PageRevision
type GenerateWiki = wiki.GenerateCommand

// WikiGenerationContext is provider-neutral on purpose. An Eino/LLM adapter,
// a deterministic test fake, or a future algorithm can implement the same
// contract without leaking framework types into the domain.
type WikiGenerationContext struct {
	Scope     common.Scope
	Locale    string
	PageSlugs []string
	Graph     graph.Result
	Evidence  EvidenceBundle
}

type WikiContentGenerator interface {
	Generate(context.Context, WikiGenerationContext) (wiki.GenerationResult, error)
}

type WikiRepository interface {
	FindJobByIdempotencyKey(context.Context, common.Scope, string) (wiki.Job, bool, error)
	LatestPage(context.Context, common.Scope, string, string) (wiki.PageRevision, bool, error)
	SavePublication(context.Context, string, wiki.Space, wiki.Job, []wiki.PageRevision) error
	GetPage(context.Context, common.Scope, string) (wiki.PageRevision, error)
}

type WikiService interface {
	Generate(context.Context, GenerateWiki) (WikiJob, error)
	GetPage(context.Context, common.Scope, string) (WikiPageRevision, error)
}

type AgentEvent = agent.Event
type AskQuestion = agent.QuestionCommand
type ResumeInput struct{ Value any }
type RepositoryAgent interface {
	Ask(context.Context, AskQuestion) (<-chan AgentEvent, error)
	Resume(context.Context, string, ResumeInput) (<-chan AgentEvent, error)
}

type AgentRepository interface {
	CreateRun(context.Context, agent.Conversation, agent.Run) error
	UpdateRun(context.Context, agent.Run) error
	GetRun(context.Context, string) (agent.Run, error)
}

type AnswerGenerationContext struct {
	Scope     common.Scope
	Question  string
	Intent    agent.Intent
	Graph     graph.Result
	Evidence  EvidenceBundle
	Citations []common.SourceRef
	Locale    string
}

type AnswerGenerationResult struct {
	AnswerMarkdown string
	Model          string
	PromptVersion  string
	TokenUsage     int
	// CitationIndexes identifies the validated input citations actually used.
	// Nil means all input citations for backward-compatible generators.
	CitationIndexes []int
}

// AnswerGenerator is provider-neutral so Eino/LLM and deterministic local
// implementations can be evaluated without changing the application service.
type AnswerGenerator interface {
	GenerateAnswer(context.Context, AnswerGenerationContext) (AnswerGenerationResult, error)
}

type VisualizationQuery = visualization.Query
type GraphProjection = visualization.Projection

type VisualizationRepository interface {
	Get(context.Context, common.Scope, string) (visualization.Projection, bool, error)
	Save(context.Context, common.Scope, string, visualization.Projection) error
}

// LayoutEngine keeps layout and future clustering/edge-routing algorithms
// replaceable without changing the visualization use case or API contract.
type LayoutEngine interface {
	Layout(context.Context, visualization.LayoutType, []visualization.Node, []visualization.Edge) (visualization.Layout, error)
}

type VisualizationService interface {
	Project(context.Context, VisualizationQuery) (GraphProjection, error)
}

type CodingCommand = assistant.CodingCommand
type ChangeProposal = assistant.ChangeProposal
type Approval = assistant.Approval
type ApplyResult = assistant.ApplyResult

type AssistantRepository interface {
	FindByIdempotencyKey(context.Context, common.Scope, string) (assistant.ChangeProposal, bool, error)
	CreateProposal(context.Context, string, assistant.CodingSession, assistant.ChangeProposal) error
	GetProposal(context.Context, common.Scope, string) (assistant.ChangeProposal, error)
	UpdateProposal(context.Context, assistant.ChangeProposal, int64) error
}

type ProposalGenerationContext struct {
	Command  assistant.CodingCommand
	Evidence EvidenceBundle
}

// ProposalGenerator isolates model/prompt selection from proposal lifecycle,
// validation, authorization and persistence.
type ProposalGenerator interface {
	GenerateProposal(context.Context, ProposalGenerationContext) (assistant.ProposalDraft, error)
}

type PatchApplyRequest struct {
	Scope         common.Scope
	ProposalID    string
	BaseCommitSHA string
	FileChanges   []assistant.FileChange
}

type PatchApplyResult struct {
	CommitSHA  string
	Validation []assistant.ValidationResult
}

// PatchApplier must verify every base hash and apply all changes atomically.
// Implementations may target a checked-out workspace, a provider branch, or a
// deterministic test double without changing the application service.
type PatchApplier interface {
	ApplyPatch(context.Context, PatchApplyRequest) (PatchApplyResult, error)
}

type CodingAssistant interface {
	Propose(context.Context, CodingCommand) (ChangeProposal, error)
	Apply(context.Context, string, Approval) (ApplyResult, error)
}
