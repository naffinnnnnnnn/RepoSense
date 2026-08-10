package parser

import (
	"context"
	"regexp"
	"strings"

	"github.com/reposense/reposense/internal/domain/repository"
)

var (
	pyClass  = regexp.MustCompile(`^\s*class\s+([A-Za-z_]\w*)\s*(?:\(([^)]*)\))?\s*:`)
	pyFunc   = regexp.MustCompile(`^\s*(async\s+)?def\s+([A-Za-z_]\w*)\s*(\([^)]*\)(?:\s*->\s*[^:]+)?)\s*:`)
	pyImport = regexp.MustCompile(`^\s*(?:from\s+([\w.]+)\s+import\s+(.+)|import\s+(.+))`)
)

type Python struct{}

func NewPython() *Python             { return &Python{} }
func (*Python) Language() string     { return "python" }
func (*Python) Extensions() []string { return []string{".py"} }
func (*Python) Version() string      { return "python-structural@1.0.0" }

func (*Python) Parse(ctx context.Context, commit string, file repository.FileContent) (repository.ParsedFile, error) {
	result := repository.ParsedFile{}
	root := fileArtifact(commit, file, "python", repository.ArtifactFile)
	result.Artifacts = append(result.Artifacts, root)
	lines := strings.Split(string(file.Content), "\n")
	type frame struct {
		indent   int
		name, id string
		kind     repository.ArtifactKind
	}
	stack := []frame{{indent: -1, name: strings.TrimSuffix(file.Path, ".py"), id: root.ArtifactID, kind: repository.ArtifactFile}}
	for i, raw := range lines {
		if err := ctx.Err(); err != nil {
			return repository.ParsedFile{}, err
		}
		lineNo := i + 1
		trimmed := strings.TrimSpace(raw)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}
		indent := len(raw) - len(strings.TrimLeft(raw, " \t"))
		for len(stack) > 1 && indent <= stack[len(stack)-1].indent {
			stack = stack[:len(stack)-1]
		}
		parent := stack[len(stack)-1]
		if match := pyClass.FindStringSubmatch(raw); match != nil {
			qualified := qualify(parent.name, match[1])
			a := artifact(commit, file.Path, "python", repository.ArtifactClass, match[1], qualified, strings.TrimSpace(raw), lineNo, lineNo, raw, nil)
			result.Artifacts = append(result.Artifacts, a)
			result.Relations = append(result.Relations, relation(commit, file.Path, repository.RelationContains, parent.id, a.ArtifactID, lineNo, raw, 1))
			for _, base := range splitNames(match[2]) {
				result.Relations = append(result.Relations, relation(commit, file.Path, repository.RelationExtends, a.ArtifactID, "symbol:"+base, lineNo, raw, .9))
			}
			stack = append(stack, frame{indent, qualified, a.ArtifactID, repository.ArtifactClass})
			continue
		}
		if match := pyFunc.FindStringSubmatch(raw); match != nil {
			kind := repository.ArtifactFunction
			if parent.kind == repository.ArtifactClass {
				kind = repository.ArtifactMethod
			}
			qualified := qualify(parent.name, match[2])
			attrs := map[string]string{}
			if strings.TrimSpace(match[1]) != "" {
				attrs["async"] = "true"
			}
			a := artifact(commit, file.Path, "python", kind, match[2], qualified, match[2]+match[3], lineNo, lineNo, raw, attrs)
			result.Artifacts = append(result.Artifacts, a)
			result.Relations = append(result.Relations, relation(commit, file.Path, repository.RelationContains, parent.id, a.ArtifactID, lineNo, raw, 1))
			stack = append(stack, frame{indent, qualified, a.ArtifactID, kind})
			continue
		}
		if match := pyImport.FindStringSubmatch(raw); match != nil {
			target := match[1]
			if target == "" {
				target = strings.Fields(match[3])[0]
			}
			a := artifact(commit, file.Path, "python", repository.ArtifactImport, target, target, trimmed, lineNo, lineNo, raw, nil)
			result.Artifacts = append(result.Artifacts, a)
			result.Relations = append(result.Relations, relation(commit, file.Path, repository.RelationImports, root.ArtifactID, "module:"+target, lineNo, raw, 1))
			continue
		}
		if parent.kind == repository.ArtifactFunction || parent.kind == repository.ArtifactMethod {
			for _, name := range callsIn(stripPythonString(raw)) {
				if isCallKeyword(name) {
					continue
				}
				result.Relations = append(result.Relations, relation(commit, file.Path, repository.RelationCalls, parent.id, "symbol:"+name, lineNo, raw, .65))
			}
		}
	}
	finalizePythonRanges(result.Artifacts, lines)
	return result, nil
}

func qualify(parent, name string) string {
	if parent == "" {
		return name
	}
	return parent + "." + name
}
func splitNames(value string) []string {
	var out []string
	for _, item := range strings.Split(value, ",") {
		if name := strings.TrimSpace(item); name != "" {
			out = append(out, name)
		}
	}
	return out
}
func stripPythonString(s string) string {
	for _, quote := range []string{"\"", "'"} {
		if i := strings.Index(s, quote); i >= 0 {
			s = s[:i]
		}
	}
	if i := strings.Index(s, "#"); i >= 0 {
		s = s[:i]
	}
	return s
}
