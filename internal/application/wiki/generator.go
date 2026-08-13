package wikiapp

import (
	"context"
	"fmt"
	"path/filepath"
	"sort"
	"strings"

	"github.com/reposense/reposense/internal/domain/common"
	"github.com/reposense/reposense/internal/domain/graph"
	"github.com/reposense/reposense/internal/domain/repository"
	"github.com/reposense/reposense/internal/domain/wiki"
	"github.com/reposense/reposense/internal/ports"
)

type StructuredGeneratorConfig struct {
	MaxItemsPerSection int
	MaxCitations       int
	Model              string
	PromptVersion      string
}

// StructuredGenerator is a deterministic, evidence-only generator. It is
// useful as the local implementation and as a safe fallback for an LLM adapter.
type StructuredGenerator struct{ config StructuredGeneratorConfig }

func NewStructuredGenerator(config StructuredGeneratorConfig) *StructuredGenerator {
	if config.MaxItemsPerSection <= 0 {
		config.MaxItemsPerSection = 20
	}
	if config.MaxCitations <= 0 {
		config.MaxCitations = 20
	}
	if config.Model == "" {
		config.Model = wiki.DefaultModel
	}
	if config.PromptVersion == "" {
		config.PromptVersion = wiki.DefaultPromptVersion
	}
	return &StructuredGenerator{config: config}
}

func (g *StructuredGenerator) Generate(ctx context.Context, input ports.WikiGenerationContext) (wiki.GenerationResult, error) {
	if err := ctx.Err(); err != nil {
		return wiki.GenerationResult{}, err
	}
	if len(input.Graph.Nodes) == 0 {
		return wiki.GenerationResult{}, fmt.Errorf("knowledge graph contains no nodes")
	}
	refs := graphCitations(input.Graph)
	refs = append(refs, input.Evidence.Sources...)
	refs = wiki.NormalizeCitations(refs)
	valid := refs[:0]
	for _, ref := range refs {
		if ref.Validate() == nil {
			valid = append(valid, ref)
		}
	}
	refs = valid
	if len(refs) == 0 {
		return wiki.GenerationResult{}, fmt.Errorf("knowledge graph contains no valid source references")
	}
	if len(refs) > g.config.MaxCitations {
		refs = refs[:g.config.MaxCitations]
	}
	pages := make([]wiki.PageDraft, 0, len(input.PageSlugs))
	for _, slug := range input.PageSlugs {
		if err := ctx.Err(); err != nil {
			return wiki.GenerationResult{}, err
		}
		title, body, err := g.render(slug, input.Locale, input.Graph)
		if err != nil {
			return wiki.GenerationResult{}, err
		}
		pages = append(pages, wiki.PageDraft{Slug: slug, Title: title, ContentMarkdown: body + citationSection(input.Locale, refs), Citations: append([]common.SourceRef(nil), refs...)})
	}
	return wiki.GenerationResult{Pages: pages, Model: g.config.Model, PromptVersion: g.config.PromptVersion}, nil
}

func (g *StructuredGenerator) render(slug, locale string, result graph.Result) (string, string, error) {
	zh := locale != "en-US"
	switch slug {
	case "overview":
		counts := map[graph.EntityType]int{}
		for _, node := range result.Nodes {
			counts[node.EntityType]++
		}
		title := choose(zh, "项目概览", "Project Overview")
		body := fmt.Sprintf("# %s\n\n%s\n\n| %s | %s |\n|---|---:|\n", title,
			choose(zh, "本页根据当前代码快照的知识图谱自动生成；所有统计均限定于该快照。", "This page is generated from the current snapshot's knowledge graph; all counts are snapshot-scoped."),
			choose(zh, "实体类型", "Entity type"), choose(zh, "数量", "Count"))
		for _, typ := range sortedEntityTypes(counts) {
			body += fmt.Sprintf("| %s | %d |\n", typ, counts[typ])
		}
		body += fmt.Sprintf("\n%s：%d；%s：%d。\n", choose(zh, "实体总数", "Total entities"), len(result.Nodes), choose(zh, "关系总数", "total relations"), len(result.Edges))
		return title, body, nil
	case "architecture":
		title := choose(zh, "系统架构", "System Architecture")
		body := fmt.Sprintf("# %s\n\n%s\n\n", title, choose(zh, "下表展示代码实体间已解析出的主要依赖关系。", "The table lists the principal dependency relationships resolved from code."))
		body += relationTable(zh, result, g.config.MaxItemsPerSection, nil)
		return title, body, nil
	case "modules":
		title := choose(zh, "模块说明", "Modules")
		groups := moduleGroups(result.Nodes)
		body := fmt.Sprintf("# %s\n\n", title)
		for _, name := range sortedKeys(groups) {
			body += fmt.Sprintf("## `%s`\n\n%s：%d\n\n", name, choose(zh, "代码实体", "Code entities"), len(groups[name]))
			for i, node := range groups[name] {
				if i >= g.config.MaxItemsPerSection {
					body += choose(zh, "- 其余实体已省略。\n", "- Remaining entities omitted.\n")
					break
				}
				body += fmt.Sprintf("- `%s` (%s)\n", displayName(node), node.EntityType)
			}
			body += "\n"
		}
		return title, body, nil
	case "key-flows":
		title := choose(zh, "关键流程", "Key Flows")
		allowed := map[repository.RelationKind]bool{repository.RelationCalls: true}
		body := fmt.Sprintf("# %s\n\n%s\n\n", title, choose(zh, "以下调用链来自静态关系抽取，未解析目标会显式保留。", "The following call paths come from static relationship extraction; unresolved targets remain explicit."))
		body += relationTable(zh, result, g.config.MaxItemsPerSection, allowed)
		return title, body, nil
	case "interfaces":
		title := choose(zh, "接口与关键符号", "Interfaces and Key Symbols")
		body := fmt.Sprintf("# %s\n\n", title)
		shown := 0
		for _, node := range result.Nodes {
			if node.EntityType != graph.EntityInterface && node.EntityType != graph.EntityFunction && node.EntityType != graph.EntityMethod {
				continue
			}
			body += fmt.Sprintf("- `%s` — %s\n", displayName(node), node.EntityType)
			shown++
			if shown >= g.config.MaxItemsPerSection {
				break
			}
		}
		if shown == 0 {
			body += choose(zh, "当前快照未解析到接口、函数或方法实体。\n", "No interface, function, or method entities were resolved for this snapshot.\n")
		}
		return title, body, nil
	case "development-guide":
		title := choose(zh, "开发指南", "Development Guide")
		languages, paths := languagesAndPaths(result.Nodes, g.config.MaxItemsPerSection)
		body := fmt.Sprintf("# %s\n\n## %s\n\n%s\n\n## %s\n\n", title, choose(zh, "已识别语言", "Detected Languages"), bulletCode(languages, zh), choose(zh, "主要源码路径", "Primary Source Paths"))
		body += bulletCode(paths, zh)
		body += "\n" + choose(zh, "修改代码后应重新解析目标提交，并仅在图谱修订就绪后刷新受影响的 Wiki 页面。\n", "After changing code, parse the target commit again and refresh affected Wiki pages only after the graph revision is ready.\n")
		return title, body, nil
	default:
		return "", "", fmt.Errorf("unsupported page slug %q", slug)
	}
}

func graphCitations(result graph.Result) []common.SourceRef {
	refs := []common.SourceRef{}
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

func citationSection(locale string, refs []common.SourceRef) string {
	zh := locale != "en-US"
	var b strings.Builder
	b.WriteString("\n## " + choose(zh, "源码依据", "Source Evidence") + "\n\n")
	for _, ref := range refs {
		fmt.Fprintf(&b, "- `%s:%d-%d`", filepath.ToSlash(ref.Path), ref.StartLine, ref.EndLine)
		if ref.SymbolID != "" {
			fmt.Fprintf(&b, " (`%s`)", ref.SymbolID)
		}
		b.WriteByte('\n')
	}
	return b.String()
}

func relationTable(zh bool, result graph.Result, limit int, allowed map[repository.RelationKind]bool) string {
	names := map[string]string{}
	for _, node := range result.Nodes {
		names[node.NodeID] = displayName(node)
	}
	var b strings.Builder
	fmt.Fprintf(&b, "| %s | %s | %s |\n|---|---|---|\n", choose(zh, "来源", "From"), choose(zh, "关系", "Relation"), choose(zh, "目标", "To"))
	shown := 0
	for _, edge := range result.Edges {
		if allowed != nil && !allowed[edge.RelationType] {
			continue
		}
		fmt.Fprintf(&b, "| `%s` | %s | `%s` |\n", names[edge.FromNodeID], edge.RelationType, names[edge.ToNodeID])
		shown++
		if shown >= limit {
			break
		}
	}
	if shown == 0 {
		fmt.Fprintf(&b, "| %s | - | - |\n", choose(zh, "未解析到对应关系", "No matching relationships resolved"))
	}
	return b.String()
}

func moduleGroups(nodes []graph.Entity) map[string][]graph.Entity {
	out := map[string][]graph.Entity{}
	for _, node := range nodes {
		name := "root"
		if node.SourceRef != nil {
			parts := strings.Split(filepath.ToSlash(node.SourceRef.Path), "/")
			if len(parts) > 1 {
				name = parts[0]
			}
		}
		out[name] = append(out[name], node)
	}
	for key := range out {
		sort.Slice(out[key], func(i, j int) bool { return displayName(out[key][i]) < displayName(out[key][j]) })
	}
	return out
}

func languagesAndPaths(nodes []graph.Entity, limit int) ([]string, []string) {
	languages, paths := map[string]bool{}, map[string]bool{}
	for _, node := range nodes {
		if node.Properties["language"] != "" {
			languages[node.Properties["language"]] = true
		}
		if node.SourceRef != nil {
			paths[filepath.ToSlash(node.SourceRef.Path)] = true
			ext := strings.TrimPrefix(strings.ToLower(filepath.Ext(node.SourceRef.Path)), ".")
			if ext != "" {
				languages[ext] = true
			}
		}
	}
	langs, sourcePaths := sortedKeys(languages), sortedKeys(paths)
	if len(sourcePaths) > limit {
		sourcePaths = sourcePaths[:limit]
	}
	return langs, sourcePaths
}

func displayName(node graph.Entity) string {
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
func sortedEntityTypes(values map[graph.EntityType]int) []graph.EntityType {
	out := make([]graph.EntityType, 0, len(values))
	for value := range values {
		out = append(out, value)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}
func sortedKeys[V any](values map[string]V) []string {
	out := make([]string, 0, len(values))
	for value := range values {
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}
func bulletCode(values []string, zh bool) string {
	if len(values) == 0 {
		return choose(zh, "- 暂无可用信息。", "- No information available.")
	}
	var b strings.Builder
	for _, value := range values {
		fmt.Fprintf(&b, "- `%s`\n", value)
	}
	return strings.TrimSuffix(b.String(), "\n")
}
