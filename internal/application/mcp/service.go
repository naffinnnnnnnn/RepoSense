package mcpapp

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	agentdomain "github.com/reposense/reposense/internal/domain/agent"
	"github.com/reposense/reposense/internal/domain/common"
	"github.com/reposense/reposense/internal/domain/graph"
	mcpdomain "github.com/reposense/reposense/internal/domain/mcp"
	"github.com/reposense/reposense/internal/domain/rag"
	"github.com/reposense/reposense/internal/domain/repository"
	"github.com/reposense/reposense/internal/domain/wiki"
	"github.com/reposense/reposense/internal/ports"
)

const CapabilityVersion = "1.0"

const (
	SearchCodeTool    = "search_code"
	GetSymbolTool     = "get_symbol"
	FindCallChainTool = "find_call_chain"
	GetWikiPageTool   = "get_wiki_page"
	AskRepositoryTool = "ask_repository"
	WikiResource      = "wiki_page"
)

type Services struct {
	Retriever ports.Retriever
	Graph     ports.GraphStore
	Wiki      ports.WikiService
	Agent     ports.RepositoryAgent
}

type Config struct {
	CallTimeout     time.Duration
	AskTimeout      time.Duration
	MaxGraphDepth   int
	MaxGraphResults int
	MaxTopK         int
	MaxPageSlug     int
}

func DefaultConfig() Config {
	return Config{CallTimeout: 15 * time.Second, AskTimeout: 60 * time.Second, MaxGraphDepth: 6,
		MaxGraphResults: 1000, MaxTopK: 100, MaxPageSlug: 256}
}

type Service struct {
	services Services
	audit    mcpdomain.AuditSink
	limiter  mcpdomain.RateLimiter
	observer ports.Observer
	clock    ports.Clock
	config   Config
}

func New(services Services, audit mcpdomain.AuditSink, limiter mcpdomain.RateLimiter, observer ports.Observer, clock ports.Clock, config Config) (*Service, error) {
	if services.Retriever == nil || services.Graph == nil || services.Wiki == nil || services.Agent == nil {
		return nil, errors.New("retriever, graph, wiki, and agent services must not be nil")
	}
	if audit == nil {
		return nil, errors.New("MCP audit sink must not be nil")
	}
	if limiter == nil {
		return nil, errors.New("MCP rate limiter must not be nil")
	}
	if observer == nil {
		observer = noopObserver{}
	}
	if clock == nil {
		clock = systemClock{}
	}
	defaults := DefaultConfig()
	if config.CallTimeout <= 0 {
		config.CallTimeout = defaults.CallTimeout
	}
	if config.AskTimeout <= 0 {
		config.AskTimeout = defaults.AskTimeout
	}
	if config.MaxGraphDepth <= 0 {
		config.MaxGraphDepth = defaults.MaxGraphDepth
	}
	if config.MaxGraphResults <= 0 {
		config.MaxGraphResults = defaults.MaxGraphResults
	}
	if config.MaxTopK <= 0 {
		config.MaxTopK = defaults.MaxTopK
	}
	if config.MaxPageSlug <= 0 {
		config.MaxPageSlug = defaults.MaxPageSlug
	}
	return &Service{services: services, audit: audit, limiter: limiter, observer: observer, clock: clock, config: config}, nil
}

func (s *Service) Capabilities() []mcpdomain.Capability {
	read := []string{mcpdomain.ScopeRepositoryRead}
	return []mcpdomain.Capability{
		{Name: SearchCodeTool, Kind: mcpdomain.CapabilityTool, Version: CapabilityVersion, Description: "Search version-pinned repository code and return source evidence.", RequiredScopes: read, QuotaUnits: 1},
		{Name: GetSymbolTool, Kind: mcpdomain.CapabilityTool, Version: CapabilityVersion, Description: "Read a symbol from the versioned knowledge graph.", RequiredScopes: read, QuotaUnits: 1},
		{Name: FindCallChainTool, Kind: mcpdomain.CapabilityTool, Version: CapabilityVersion, Description: "Traverse versioned CALLS relationships from a symbol.", RequiredScopes: read, QuotaUnits: 2},
		{Name: GetWikiPageTool, Kind: mcpdomain.CapabilityTool, Version: CapabilityVersion, Description: "Read a generated Wiki page with source citations.", RequiredScopes: read, QuotaUnits: 1},
		{Name: AskRepositoryTool, Kind: mcpdomain.CapabilityTool, Version: CapabilityVersion, Description: "Answer a repository question using evidence-backed retrieval and graph analysis.", RequiredScopes: read, QuotaUnits: 5},
		{Name: WikiResource, Kind: mcpdomain.CapabilityResource, Version: CapabilityVersion, Description: "Version-pinned Wiki page resource.", RequiredScopes: read, QuotaUnits: 1},
	}
}

type RequestScope struct {
	RepositoryID string `json:"repository_id"`
	SnapshotID   string `json:"snapshot_id"`
	TraceID      string `json:"trace_id,omitempty"`
}

func (r RequestScope) scope(grant mcpdomain.ClientGrant) common.Scope {
	scope := common.Scope{TenantID: grant.TenantID, RepositoryID: strings.TrimSpace(r.RepositoryID), SnapshotID: strings.TrimSpace(r.SnapshotID), TraceID: strings.TrimSpace(r.TraceID)}
	if scope.TraceID == "" {
		scope.TraceID = newID("tr")
	}
	return scope
}

type SearchCodeInput struct {
	RequestScope
	Query      string      `json:"query"`
	Strategies []string    `json:"strategies,omitempty"`
	Filters    rag.Filters `json:"filters,omitempty"`
	TopK       int         `json:"top_k,omitempty"`
}
type SearchCodeOutput struct {
	Evidence rag.EvidenceBundle `json:"evidence"`
}

func (s *Service) SearchCode(ctx context.Context, grant mcpdomain.ClientGrant, input SearchCodeInput) (out SearchCodeOutput, err error) {
	scope := input.RequestScope.scope(grant)
	done, err := s.guard(ctx, grant, scope, SearchCodeTool, input)
	if err != nil {
		return out, err
	}
	defer func() { err = done(err) }()
	if input.TopK < 0 || input.TopK > s.config.MaxTopK {
		return out, mcpdomain.NewError(mcpdomain.ErrInvalidInput, "search_code", fmt.Sprintf("top_k must be between 0 and %d", s.config.MaxTopK), false, nil)
	}
	callCtx, cancel := context.WithTimeout(ctx, s.config.CallTimeout)
	defer cancel()
	out.Evidence, err = s.services.Retriever.Search(callCtx, rag.RetrievalRequest{Scope: scope, Query: input.Query, Strategies: input.Strategies, Filters: input.Filters, TopK: input.TopK})
	if err != nil {
		err = upstream("search_code", err)
	}
	return out, err
}

type GetSymbolInput struct {
	RequestScope
	SymbolID string `json:"symbol_id"`
}
type GetSymbolOutput struct {
	Symbol graph.Entity `json:"symbol"`
}

func (s *Service) GetSymbol(ctx context.Context, grant mcpdomain.ClientGrant, input GetSymbolInput) (out GetSymbolOutput, err error) {
	scope := input.RequestScope.scope(grant)
	done, err := s.guard(ctx, grant, scope, GetSymbolTool, input)
	if err != nil {
		return out, err
	}
	defer func() { err = done(err) }()
	if strings.TrimSpace(input.SymbolID) == "" {
		return out, mcpdomain.NewError(mcpdomain.ErrInvalidInput, "get_symbol", "symbol_id must not be empty", false, nil)
	}
	callCtx, cancel := context.WithTimeout(ctx, s.config.CallTimeout)
	defer cancel()
	result, queryErr := s.services.Graph.Query(callCtx, graph.Query{Scope: scope, RootIDs: []string{input.SymbolID}, Direction: graph.DirectionBoth, Depth: 0, Limit: 1})
	if queryErr != nil {
		return out, upstream("get_symbol", queryErr)
	}
	if len(result.Nodes) == 0 {
		return out, mcpdomain.NewError(mcpdomain.ErrInvalidInput, "get_symbol", "symbol was not found in the selected snapshot", false, nil)
	}
	out.Symbol = result.Nodes[0]
	return out, nil
}

type FindCallChainInput struct {
	RequestScope
	SymbolID  string          `json:"symbol_id"`
	Direction graph.Direction `json:"direction,omitempty"`
	Depth     int             `json:"depth,omitempty"`
	Limit     int             `json:"limit,omitempty"`
}
type FindCallChainOutput struct {
	Graph graph.Result `json:"graph"`
}

func (s *Service) FindCallChain(ctx context.Context, grant mcpdomain.ClientGrant, input FindCallChainInput) (out FindCallChainOutput, err error) {
	scope := input.RequestScope.scope(grant)
	if input.Depth == 0 {
		input.Depth = 2
	}
	if input.Limit == 0 {
		input.Limit = 100
	}
	if input.Direction == "" {
		input.Direction = graph.DirectionBoth
	}
	done, err := s.guard(ctx, grant, scope, FindCallChainTool, input)
	if err != nil {
		return out, err
	}
	defer func() { err = done(err) }()
	if strings.TrimSpace(input.SymbolID) == "" {
		return out, mcpdomain.NewError(mcpdomain.ErrInvalidInput, "find_call_chain", "symbol_id must not be empty", false, nil)
	}
	if input.Depth < 1 || input.Depth > s.config.MaxGraphDepth || input.Limit < 1 || input.Limit > s.config.MaxGraphResults {
		return out, mcpdomain.NewError(mcpdomain.ErrInvalidInput, "find_call_chain", "depth or limit is outside the MCP safety bounds", false, nil)
	}
	callCtx, cancel := context.WithTimeout(ctx, s.config.CallTimeout)
	defer cancel()
	out.Graph, err = s.services.Graph.Query(callCtx, graph.Query{Scope: scope, RootIDs: []string{input.SymbolID}, RelationTypes: []repository.RelationKind{repository.RelationCalls}, Direction: input.Direction, Depth: input.Depth, Limit: input.Limit})
	if err != nil {
		err = upstream("find_call_chain", err)
	}
	return out, err
}

type GetWikiPageInput struct {
	RequestScope
	Slug string `json:"slug"`
}
type GetWikiPageOutput struct {
	Page wiki.PageRevision `json:"page"`
}

func (s *Service) GetWikiPage(ctx context.Context, grant mcpdomain.ClientGrant, input GetWikiPageInput) (out GetWikiPageOutput, err error) {
	return s.getWikiPage(ctx, grant, input, GetWikiPageTool)
}

// ReadWikiPageResource keeps Resource invocations distinguishable from Tool
// calls in quota accounting, metrics, and append-only audit records.
func (s *Service) ReadWikiPageResource(ctx context.Context, grant mcpdomain.ClientGrant, input GetWikiPageInput) (out GetWikiPageOutput, err error) {
	return s.getWikiPage(ctx, grant, input, WikiResource)
}

func (s *Service) getWikiPage(ctx context.Context, grant mcpdomain.ClientGrant, input GetWikiPageInput, capability string) (out GetWikiPageOutput, err error) {
	scope := input.RequestScope.scope(grant)
	input.Slug = strings.TrimSpace(input.Slug)
	done, err := s.guard(ctx, grant, scope, capability, input)
	if err != nil {
		return out, err
	}
	defer func() { err = done(err) }()
	if input.Slug == "" || len(input.Slug) > s.config.MaxPageSlug || strings.Contains(input.Slug, "..") {
		return out, mcpdomain.NewError(mcpdomain.ErrInvalidInput, "get_wiki_page", "wiki slug is invalid", false, nil)
	}
	callCtx, cancel := context.WithTimeout(ctx, s.config.CallTimeout)
	defer cancel()
	out.Page, err = s.services.Wiki.GetPage(callCtx, scope, input.Slug)
	if err != nil {
		err = upstream("get_wiki_page", err)
	}
	return out, err
}

type AskRepositoryInput struct {
	RequestScope
	ConversationID string `json:"conversation_id,omitempty"`
	Question       string `json:"question"`
	Locale         string `json:"locale,omitempty"`
}
type AskRepositoryOutput struct {
	RunID  string             `json:"run_id"`
	Answer agentdomain.Answer `json:"answer"`
}

func (s *Service) AskRepository(ctx context.Context, grant mcpdomain.ClientGrant, input AskRepositoryInput) (out AskRepositoryOutput, err error) {
	scope := input.RequestScope.scope(grant)
	if strings.TrimSpace(input.ConversationID) == "" {
		input.ConversationID = newID("conv")
	}
	done, err := s.guard(ctx, grant, scope, AskRepositoryTool, input)
	if err != nil {
		return out, err
	}
	defer func() { err = done(err) }()
	callCtx, cancel := context.WithTimeout(ctx, s.config.AskTimeout)
	defer cancel()
	stream, askErr := s.services.Agent.Ask(callCtx, agentdomain.QuestionCommand{Scope: scope, ConversationID: input.ConversationID, UserID: grant.PrincipalID, Question: input.Question, Permissions: []string{agentdomain.ReadPermission}, Locale: input.Locale})
	if askErr != nil {
		return out, upstream("ask_repository", askErr)
	}
	for {
		select {
		case <-callCtx.Done():
			return out, upstream("ask_repository", callCtx.Err())
		case event, ok := <-stream:
			if !ok {
				return out, mcpdomain.NewError(mcpdomain.ErrUpstreamFailure, "ask_repository", "repository agent ended without a terminal answer", true, nil)
			}
			out.RunID = event.RunID
			if event.Type == agentdomain.EventCompleted {
				answer, ok := event.Payload["answer"].(agentdomain.Answer)
				if !ok {
					return out, mcpdomain.NewError(mcpdomain.ErrUpstreamFailure, "ask_repository", "repository agent returned an invalid terminal answer", false, nil)
				}
				out.Answer = answer
				return out, nil
			}
			if event.Type == agentdomain.EventFailed {
				return out, mcpdomain.NewError(mcpdomain.ErrUpstreamFailure, "ask_repository", "repository agent failed to produce an answer", true, nil)
			}
		}
	}
}

type finishInvocation func(error) error

func (s *Service) guard(ctx context.Context, grant mcpdomain.ClientGrant, scope common.Scope, capabilityName string, request any) (finishInvocation, error) {
	capability, ok := s.capability(capabilityName)
	if !ok {
		return nil, mcpdomain.NewError(mcpdomain.ErrCapabilityDisabled, "guard", "MCP capability is disabled", false, nil)
	}
	started := s.clock.Now().UTC()
	invocation := mcpdomain.Invocation{InvocationID: newID("mi"), Capability: capability.Name, Version: capability.Version, RequestHash: mcpdomain.RequestHash(request), ClientID: grant.ClientID, PrincipalID: grant.PrincipalID, TenantID: grant.TenantID, RepositoryID: scope.RepositoryID, SnapshotID: scope.SnapshotID, TraceID: scope.TraceID, QuotaUnits: capability.QuotaUnits, OccurredAt: started}
	finishStage := s.observer.Stage(ctx, "mcp_invocation", map[string]string{"capability": capability.Name, "tenant_id": grant.TenantID, "repository_id": scope.RepositoryID, "snapshot_id": scope.SnapshotID, "trace_id": scope.TraceID})
	finalize := func(operationErr error) error {
		invocation.LatencyMS = s.clock.Now().UTC().Sub(started).Milliseconds()
		invocation.ResultStatus = mcpdomain.InvocationSucceeded
		var domainErr *mcpdomain.DomainError
		if operationErr != nil {
			invocation.ResultStatus = mcpdomain.InvocationFailed
			if errors.As(operationErr, &domainErr) {
				invocation.ErrorCode = domainErr.Code
			}
		}
		if invocation.ErrorCode == mcpdomain.ErrPermissionDenied || invocation.ErrorCode == mcpdomain.ErrUnauthenticated {
			invocation.ResultStatus = mcpdomain.InvocationDenied
		}
		if invocation.ErrorCode == mcpdomain.ErrRateLimited {
			invocation.ResultStatus = mcpdomain.InvocationLimited
		}
		auditCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
		defer cancel()
		if auditErr := s.audit.Record(auditCtx, invocation); auditErr != nil {
			operationErr = mcpdomain.NewError(mcpdomain.ErrAuditFailure, "record_audit", "MCP invocation audit could not be recorded", true, auditErr)
		}
		finishStage(operationErr)
		s.observer.Count("mcp_invocations_total", 1, map[string]string{"capability": capability.Name, "status": string(invocation.ResultStatus)})
		return operationErr
	}
	if scopeErr := scope.Validate(true); scopeErr != nil {
		return nil, finalize(mcpdomain.NewError(mcpdomain.ErrInvalidInput, "validate_scope", scopeErr.Error(), false, scopeErr))
	}
	if authErr := grant.Authorize(started, scope.RepositoryID, capability.RequiredScopes); authErr != nil {
		return nil, finalize(authErr)
	}
	key := grant.TenantID + "\x00" + grant.ClientID + "\x00" + capability.Name
	if retryAfter, limitErr := s.limiter.Allow(ctx, key, capability.QuotaUnits); limitErr != nil {
		return nil, finalize(mcpdomain.NewError(mcpdomain.ErrRateLimited, "rate_limit", fmt.Sprintf("MCP quota exceeded; retry after %s", retryAfter.Round(time.Second)), true, limitErr))
	}
	return finalize, nil
}

func (s *Service) capability(name string) (mcpdomain.Capability, bool) {
	for _, capability := range s.Capabilities() {
		if capability.Name == name {
			return capability, true
		}
	}
	return mcpdomain.Capability{}, false
}
func upstream(operation string, err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return mcpdomain.NewError(mcpdomain.ErrUpstreamFailure, operation, "upstream operation timed out or was cancelled", true, err)
	}
	return mcpdomain.NewError(mcpdomain.ErrUpstreamFailure, operation, "upstream repository capability failed", true, err)
}
func newID(prefix string) string {
	var b [12]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("%s_%d", prefix, time.Now().UnixNano())
	}
	return prefix + "_" + hex.EncodeToString(b[:])
}

type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now() }

type noopObserver struct{}

func (noopObserver) Stage(context.Context, string, map[string]string) func(error) {
	return func(error) {}
}
func (noopObserver) Count(string, int64, map[string]string) {}
