package ports

import (
	"context"

	"github.com/reposense/reposense/internal/domain/common"
	"github.com/reposense/reposense/internal/domain/repository"
)

// 以下 DTO 在编译期明确后续模块的边界，具体实现不属于当前里程碑。
type GraphQuery struct {
	Scope   common.Scope
	RootIDs []string
	Depth   int
}
type GraphResult struct{ NodeIDs, EdgeIDs []string }
type GraphRevision struct{ RevisionID, SnapshotID, Status string }
type GraphStore interface {
	ApplyRevision(context.Context, common.Scope, []repository.CodeArtifact) (GraphRevision, error)
	Query(context.Context, GraphQuery) (GraphResult, error)
}

type RetrievalRequest struct {
	Scope      common.Scope
	Query      string
	Strategies []string
	TopK       int
}
type EvidenceBundle struct {
	ArtifactIDs []string
	Sources     []common.SourceRef
}
type IndexRevision struct{ RevisionID, SnapshotID, Status string }
type Retriever interface {
	Index(context.Context, common.Scope, []repository.CodeArtifact) (IndexRevision, error)
	Search(context.Context, RetrievalRequest) (EvidenceBundle, error)
}

type WikiJob struct{ JobID, Status string }
type WikiPageRevision struct {
	PageID, Slug, ContentMarkdown string
	Citations                     []common.SourceRef
}
type GenerateWiki struct {
	Scope     common.Scope
	Locale    string
	PageScope []string
}
type WikiService interface {
	Generate(context.Context, GenerateWiki) (WikiJob, error)
	GetPage(context.Context, common.Scope, string) (WikiPageRevision, error)
}

type AgentEvent struct {
	Type    string
	Payload any
}
type AskQuestion struct {
	Scope                    common.Scope
	ConversationID, Question string
}
type ResumeInput struct{ Value any }
type RepositoryAgent interface {
	Ask(context.Context, AskQuestion) (<-chan AgentEvent, error)
	Resume(context.Context, string, ResumeInput) (<-chan AgentEvent, error)
}

type VisualizationQuery struct {
	Scope    common.Scope
	ViewType string
	RootIDs  []string
	Depth    int
}
type GraphProjection struct{ Nodes, Edges []map[string]any }
type VisualizationService interface {
	Project(context.Context, VisualizationQuery) (GraphProjection, error)
}

type CodingCommand struct {
	Scope        common.Scope
	Intent       string
	SelectedRefs []common.SourceRef
}
type ChangeProposal struct{ ProposalID, Summary, Diff, ApprovalStatus string }
type Approval struct {
	PrincipalID string
	Approved    bool
}
type ApplyResult struct{ CommitSHA, Status string }
type CodingAssistant interface {
	Propose(context.Context, CodingCommand) (ChangeProposal, error)
	Apply(context.Context, string, Approval) (ApplyResult, error)
}
