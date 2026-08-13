package agentapp

import (
	"context"
	"strings"

	"github.com/reposense/reposense/internal/domain/agent"
)

type Planner interface {
	Plan(context.Context, agent.QuestionCommand) (agent.Plan, error)
}

// KeywordPlanner is deterministic and intentionally small. A model-backed
// planner can replace it without changing the orchestration or domain model.
type KeywordPlanner struct{}

func (KeywordPlanner) Plan(ctx context.Context, cmd agent.QuestionCommand) (agent.Plan, error) {
	if err := ctx.Err(); err != nil {
		return agent.Plan{}, err
	}
	q := strings.ToLower(cmd.Question)
	intent, depth := agent.IntentGeneral, 1
	switch {
	case containsAny(q, "调用链", "调用关系", "call chain", "calls", "调用路径"):
		intent, depth = agent.IntentCallChain, 4
	case containsAny(q, "影响", "改动", "变更", "impact", "affected"):
		intent, depth = agent.IntentImpactAnalysis, 3
	case containsAny(q, "故障", "报错", "失败", "排查", "bug", "error", "fail", "debug"):
		intent, depth = agent.IntentTroubleshooting, 2
	case containsAny(q, "架构", "模块", "设计", "architecture", "module", "design"):
		intent, depth = agent.IntentArchitecture, 2
	}
	return agent.Plan{Intent: intent, GraphDepth: depth,
		Strategies: []string{"SYMBOL", "KEYWORD", "SEMANTIC", "GRAPH"},
		Steps: []agent.PlanStep{
			{Name: "retrieve", Description: "从混合索引和知识图谱检索快照证据", Status: agent.StepPending},
			{Name: "evaluate", Description: "校验引用、版本一致性和证据覆盖", Status: agent.StepPending},
			{Name: "synthesize", Description: "仅依据已验证证据生成回答", Status: agent.StepPending},
		}}, nil
}

func containsAny(value string, needles ...string) bool {
	for _, needle := range needles {
		if strings.Contains(value, needle) {
			return true
		}
	}
	return false
}
