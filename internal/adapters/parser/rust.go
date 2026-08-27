package parser

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/reposense/reposense/internal/domain/repository"
)

var rustDecl = regexp.MustCompile(`^(?:pub(?:\([^)]*\))?\s+)?(mod|struct|enum|trait|fn)\s+([A-Za-z_]\w*)`)
var rustUse = regexp.MustCompile(`^(?:pub\s+)?use\s+([^;]+);`)
var rustImpl = regexp.MustCompile(`^impl(?:<[^>]+>)?\s+(?:[^ ]+\s+for\s+)?([A-Za-z_]\w*)`)

type Rust struct{}

func NewRust() *Rust               { return &Rust{} }
func (*Rust) Language() string     { return "rust" }
func (*Rust) Extensions() []string { return []string{".rs"} }
func (*Rust) Version() string      { return "rust-structural@1.0.0" }

func (*Rust) Parse(ctx context.Context, commit string, file repository.FileContent) (repository.ParsedFile, error) {
	if err := ctx.Err(); err != nil {
		return repository.ParsedFile{}, err
	}
	clean, err := repository.CanonicalPath(file.Path)
	if err != nil {
		return repository.ParsedFile{}, err
	}
	file.Path = clean
	cleanSource, err := sanitizeRust(string(file.Content))
	if err != nil {
		return repository.ParsedFile{}, err
	}
	root := fileArtifact(commit, file, "rust", repository.ArtifactFile)
	result := repository.ParsedFile{Artifacts: []repository.CodeArtifact{root}}
	lines := strings.Split(cleanSource, "\n")
	originals := strings.Split(string(file.Content), "\n")
	symbols := map[string][]string{}
	impls := rustImplRanges(lines)
	for i, line := range lines {
		if err := ctx.Err(); err != nil {
			return repository.ParsedFile{}, err
		}
		trimmed := strings.TrimSpace(line)
		if match := rustUse.FindStringSubmatch(trimmed); match != nil {
			target := strings.TrimSpace(match[1])
			a := artifact(commit, file.Path, "rust", repository.ArtifactImport, target, target, target, i+1, i+1, originals[i], nil)
			result.Artifacts = append(result.Artifacts, a)
			result.Relations = append(result.Relations, relation(commit, file.Path, repository.RelationImports, root.ArtifactID, "module:"+target, i+1, originals[i], 1))
			continue
		}
		match := rustDecl.FindStringSubmatch(trimmed)
		if match == nil {
			continue
		}
		kind := repository.ArtifactModule
		switch match[1] {
		case "struct", "enum":
			kind = repository.ArtifactClass
		case "trait":
			kind = repository.ArtifactInterface
		case "fn":
			kind = repository.ArtifactFunction
		}
		q := strings.TrimSuffix(file.Path, ".rs") + "::" + match[2]
		if match[1] == "fn" {
			if owner := rustOwnerAt(impls, i+1); owner != "" {
				kind = repository.ArtifactMethod
				q = strings.TrimSuffix(file.Path, ".rs") + "::" + owner + "::" + match[2]
			}
		}
		end := rustDeclarationEnd(lines, i)
		a := artifact(commit, file.Path, "rust", kind, match[2], q, trimmed, i+1, end, strings.Join(originals[i:end], "\n"), nil)
		result.Artifacts = append(result.Artifacts, a)
		result.Relations = append(result.Relations, relation(commit, file.Path, repository.RelationContains, root.ArtifactID, a.ArtifactID, i+1, originals[i], 1))
		symbols[match[2]] = append(symbols[match[2]], a.ArtifactID)
	}
	for _, a := range result.Artifacts {
		if a.Kind != repository.ArtifactFunction && a.Kind != repository.ArtifactMethod {
			continue
		}
		start, end := a.SourceRef.StartLine-1, a.SourceRef.EndLine
		if end > len(lines) {
			end = len(lines)
		}
		for offset, line := range lines[start:end] {
			for _, name := range callsIn(line) {
				if isCallKeyword(name) || name == a.Name {
					continue
				}
				target := "symbol:" + name
				if ids := symbols[name]; len(ids) == 1 {
					target = ids[0]
				}
				lineNo := start + offset + 1
				result.Relations = append(result.Relations, relation(commit, file.Path, repository.RelationCalls, a.ArtifactID, target, lineNo, originals[lineNo-1], .7))
			}
		}
	}
	return result, nil
}

type rustImplRange struct {
	owner      string
	start, end int
}

func rustImplRanges(lines []string) []rustImplRange {
	var ranges []rustImplRange
	for index, line := range lines {
		if match := rustImpl.FindStringSubmatch(strings.TrimSpace(line)); match != nil {
			ranges = append(ranges, rustImplRange{owner: match[1], start: index + 1, end: rustDeclarationEnd(lines, index)})
		}
	}
	return ranges
}
func rustOwnerAt(ranges []rustImplRange, line int) string {
	for _, item := range ranges {
		if line > item.start && line <= item.end {
			return item.owner
		}
	}
	return ""
}
func rustDeclarationEnd(lines []string, start int) int {
	depth, opened := 0, false
	for index := start; index < len(lines); index++ {
		for _, char := range lines[index] {
			switch char {
			case '{':
				depth++
				opened = true
			case '}':
				if opened {
					depth--
					if depth == 0 {
						return index + 1
					}
				}
			}
		}
		if !opened && strings.Contains(lines[index], ";") {
			return index + 1
		}
	}
	return start + 1
}

func sanitizeRust(source string) (string, error) {
	var out strings.Builder
	inBlock := false
	quote := byte(0)
	escaped := false
	depth := 0
	for i := 0; i < len(source); i++ {
		c := source[i]
		if inBlock {
			if i+1 < len(source) && c == '*' && source[i+1] == '/' {
				inBlock = false
				i++
				out.WriteString("  ")
			} else if c == '\n' {
				out.WriteByte('\n')
			} else {
				out.WriteByte(' ')
			}
			continue
		}
		if quote != 0 {
			if escaped {
				escaped = false
			} else if c == '\\' {
				escaped = true
			} else if c == quote {
				quote = 0
			}
			if c == '\n' {
				out.WriteByte('\n')
			} else {
				out.WriteByte(' ')
			}
			continue
		}
		if i+1 < len(source) && c == '/' && source[i+1] == '*' {
			inBlock = true
			i++
			out.WriteString("  ")
			continue
		}
		if i+1 < len(source) && c == '/' && source[i+1] == '/' {
			for i < len(source) && source[i] != '\n' {
				out.WriteByte(' ')
				i++
			}
			if i < len(source) {
				out.WriteByte('\n')
			}
			continue
		}
		if c == '"' || c == '\'' && isRustCharStart(source, i) {
			quote = c
			out.WriteByte(' ')
			continue
		}
		if c == '{' {
			depth++
		}
		if c == '}' {
			depth--
			if depth < 0 {
				return "", fmt.Errorf("Rust 语法无效：多余的右花括号")
			}
		}
		out.WriteByte(c)
	}
	if inBlock || quote != 0 || depth != 0 {
		return "", fmt.Errorf("Rust 语法无效：结构未闭合")
	}
	return out.String(), nil
}

func isRustCharStart(source string, at int) bool {
	for i := at + 1; i < len(source) && i <= at+5 && source[i] != '\n'; i++ {
		if source[i] == '\'' && (i == at+1 || source[i-1] != '\\') {
			return true
		}
	}
	return false
}
