package agentapp

import (
	"context"
	"fmt"
	"path/filepath"
	"sort"
	"strings"

	"github.com/reposense/reposense/internal/domain/agent"
	"github.com/reposense/reposense/internal/domain/graph"
	"github.com/reposense/reposense/internal/ports"
)

const (
	StructuredAnswerModel   = "reposense-structured-answer"
	StructuredPromptVersion = "qa-evidence-v1"
)

type StructuredGeneratorConfig struct{ MaxGraphItems int }
type StructuredGenerator struct{ maxGraphItems int }

func NewStructuredGenerator(config StructuredGeneratorConfig) *StructuredGenerator {
	if config.MaxGraphItems <= 0 {
		config.MaxGraphItems = 20
	}
	return &StructuredGenerator{maxGraphItems: config.MaxGraphItems}
}

// GenerateAnswer produces a reproducible local answer. It never invents source
// locations: the application service owns and validates all citations.
func (g *StructuredGenerator) GenerateAnswer(ctx context.Context, input ports.AnswerGenerationContext) (ports.AnswerGenerationResult, error) {
	if err := ctx.Err(); err != nil {
		return ports.AnswerGenerationResult{}, err
	}
	if len(input.Citations) == 0 {
		return ports.AnswerGenerationResult{}, fmt.Errorf("no validated citations")
	}
	zh := input.Locale != "en-US"
	var b strings.Builder
	if zh {
		fmt.Fprintf(&b, "基于当前固定快照中的代码证据，对问题“%s”的分析如下：\n\n", strings.TrimSpace(input.Question))
	} else {
		fmt.Fprintf(&b, "Based on code evidence in the pinned snapshot, the analysis of %q is:\n\n", strings.TrimSpace(input.Question))
	}
	g.renderGraph(&b, input.Graph, input.Intent, zh)
	if len(input.Graph.Nodes) == 0 && len(input.Evidence.ArtifactIDs) > 0 {
		fmt.Fprintf(&b, "%s %d %s\n\n", choose(zh, "混合检索命中了", "Hybrid retrieval matched"), len(input.Evidence.ArtifactIDs), choose(zh, "个代码制品。", "code artifacts."))
	}
	b.WriteString(choose(zh, "### 源码依据\n\n", "### Source evidence\n\n"))
	for _, ref := range input.Citations {
		fmt.Fprintf(&b, "- `%s:%d-%d`", filepath.ToSlash(ref.Path), ref.StartLine, ref.EndLine)
		if ref.SymbolID != "" {
			fmt.Fprintf(&b, " (`%s`)", ref.SymbolID)
		}
		b.WriteByte('\n')
	}
	indexes := make([]int, len(input.Citations))
	for i := range indexes {
		indexes[i] = i
	}
	return ports.AnswerGenerationResult{AnswerMarkdown: strings.TrimSpace(b.String()), Model: StructuredAnswerModel, PromptVersion: StructuredPromptVersion, CitationIndexes: indexes}, nil
}

func (g *StructuredGenerator) renderGraph(b *strings.Builder, result graph.Result, intent agent.Intent, zh bool) {
	if len(result.Nodes) == 0 {
		b.WriteString(choose(zh, "图谱未返回可展示实体，结论仅限于检索证据。\n\n", "The graph returned no displayable entities; conclusions are limited to retrieval evidence.\n\n"))
		return
	}
	names := make(map[string]string, len(result.Nodes))
	nodes := append([]graph.Entity(nil), result.Nodes...)
	sort.Slice(nodes, func(i, j int) bool { return displayNode(nodes[i]) < displayNode(nodes[j]) })
	for _, node := range nodes {
		names[node.NodeID] = displayNode(node)
	}
	fmt.Fprintf(b, "%s %d %s %d %s\n\n", choose(zh, "知识图谱返回", "The knowledge graph returned"), len(result.Nodes), choose(zh, "个实体和", "entities and"), len(result.Edges), choose(zh, "条关系。", "relationships."))
	if intent == agent.IntentCallChain || intent == agent.IntentImpactAnalysis || intent == agent.IntentArchitecture {
		shown := 0
		for _, edge := range result.Edges {
			if shown >= g.maxGraphItems {
				break
			}
			fmt.Fprintf(b, "- `%s` —%s→ `%s`\n", names[edge.FromNodeID], edge.RelationType, names[edge.ToNodeID])
			shown++
		}
		if shown > 0 {
			b.WriteByte('\n')
		}
		return
	}
	limit := len(nodes)
	if limit > g.maxGraphItems {
		limit = g.maxGraphItems
	}
	for _, node := range nodes[:limit] {
		fmt.Fprintf(b, "- `%s` (%s)\n", displayNode(node), node.EntityType)
	}
	b.WriteByte('\n')
}

func displayNode(node graph.Entity) string {
	if node.QualifiedName != "" {
		return node.QualifiedName
	}
	if node.Name != "" {
		return node.Name
	}
	return node.NodeID
}
func choose(condition bool, yes, no string) string {
	if condition {
		return yes
	}
	return no
}

var _ ports.AnswerGenerator = (*StructuredGenerator)(nil)
