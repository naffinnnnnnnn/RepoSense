package parser

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"github.com/reposense/reposense/internal/domain/repository"
)

type Text struct{}

func NewText() *Text           { return &Text{} }
func (*Text) Language() string { return "text" }
func (*Text) Version() string  { return "text@1.0.0" }
func (*Text) Extensions() []string {
	return []string{".md", ".mdx", ".rst", ".txt", ".json", ".yaml", ".yml", ".toml", ".xml", ".ini", ".properties", ".mod", "dockerfile", "makefile", "requirements.txt"}
}
func (*Text) Parse(ctx context.Context, commit string, file repository.FileContent) (repository.ParsedFile, error) {
	if err := ctx.Err(); err != nil {
		return repository.ParsedFile{}, err
	}
	clean, err := repository.CanonicalPath(file.Path)
	if err != nil {
		return repository.ParsedFile{}, err
	}
	file.Path = clean
	if strings.EqualFold(filepath.Base(file.Path), "package.json") {
		var document any
		if err := json.Unmarshal(file.Content, &document); err != nil {
			return repository.ParsedFile{}, fmt.Errorf("package.json 语法无效: %w", err)
		}
	}
	ext := strings.ToLower(filepath.Ext(file.Path))
	kind := repository.ArtifactConfig
	if ext == ".md" || ext == ".mdx" || ext == ".rst" || ext == ".txt" {
		kind = repository.ArtifactDocument
	}
	root := fileArtifact(commit, file, "text", kind)
	result := repository.ParsedFile{Artifacts: []repository.CodeArtifact{root}}
	for _, dependency := range dependencies(file.Path, string(file.Content)) {
		lineContent := lineAt(string(file.Content), dependency.line)
		a := artifact(commit, file.Path, "text", repository.ArtifactImport, dependency.name, dependency.name, dependency.name,
			dependency.line, dependency.line, lineContent, map[string]string{"type": "dependency"})
		result.Artifacts = append(result.Artifacts, a)
		result.Relations = append(result.Relations, relation(commit, file.Path, repository.RelationDependsOn, root.ArtifactID, "package:"+dependency.name, dependency.line, lineContent, .95))
	}
	return result, nil
}

type dependency struct {
	name string
	line int
}

var pomArtifact = regexp.MustCompile(`<artifactId>\s*([^<\s]+)\s*</artifactId>`)
var versionSplit = regexp.MustCompile(`[<>=!~;\[]`)

func dependencies(filePath, content string) []dependency {
	base := strings.ToLower(filepath.Base(filePath))
	lines := strings.Split(content, "\n")
	var names []string
	switch base {
	case "package.json":
		var document map[string]json.RawMessage
		if json.Unmarshal([]byte(content), &document) == nil {
			for _, section := range []string{"dependencies", "devDependencies", "peerDependencies", "optionalDependencies"} {
				var values map[string]json.RawMessage
				if json.Unmarshal(document[section], &values) == nil {
					for name := range values {
						names = append(names, name)
					}
				}
			}
		}
	case "requirements.txt":
		for _, line := range lines {
			value := strings.TrimSpace(strings.SplitN(line, "#", 2)[0])
			if value == "" || strings.HasPrefix(value, "-") {
				continue
			}
			name := strings.TrimSpace(versionSplit.Split(value, 2)[0])
			if name != "" {
				names = append(names, name)
			}
		}
	case "pom.xml":
		for _, match := range pomArtifact.FindAllStringSubmatch(content, -1) {
			names = append(names, match[1])
		}
	case "go.mod":
		inBlock := false
		for _, line := range lines {
			value := strings.TrimSpace(line)
			if value == "require (" {
				inBlock = true
				continue
			}
			if inBlock && value == ")" {
				inBlock = false
				continue
			}
			if strings.HasPrefix(value, "require ") {
				value = strings.TrimSpace(strings.TrimPrefix(value, "require "))
			} else if !inBlock {
				continue
			}
			if fields := strings.Fields(value); len(fields) >= 1 {
				names = append(names, fields[0])
			}
		}
	}
	sort.Strings(names)
	result := make([]dependency, 0, len(names))
	seen := map[string]bool{}
	for _, name := range names {
		if seen[name] {
			continue
		}
		seen[name] = true
		result = append(result, dependency{name: name, line: findLine(lines, name)})
	}
	return result
}
func findLine(lines []string, value string) int {
	for index, line := range lines {
		if strings.Contains(line, value) {
			return index + 1
		}
	}
	return 1
}
func lineAt(content string, line int) string {
	lines := strings.Split(content, "\n")
	if line < 1 || line > len(lines) {
		return ""
	}
	return lines[line-1]
}
