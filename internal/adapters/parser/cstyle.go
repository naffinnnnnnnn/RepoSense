package parser

import (
	"context"
	"regexp"
	"strings"

	"github.com/reposense/reposense/internal/domain/repository"
)

var (
	typeDecl   = regexp.MustCompile(`\b(class|interface)\s+([A-Za-z_$][\w$]*)`)
	tsFunction = regexp.MustCompile(`^(?:export\s+)?(?:default\s+)?(?:async\s+)?function\s+([A-Za-z_$][\w$]*)\s*(\([^)]*\)(?:\s*:\s*[^={]+)?)`)
	tsMethod   = regexp.MustCompile(`^(?:(?:public|private|protected|static|async|readonly|abstract|get|set)\s+)*([A-Za-z_$][\w$]*)\s*(\([^)]*\)(?:\s*:\s*[^={]+)?)\s*[{;]`)
	tsArrow    = regexp.MustCompile(`^(?:export\s+)?(?:const|let|var)\s+([A-Za-z_$][\w$]*)\s*=\s*(?:async\s*)?(\([^)]*\)|[A-Za-z_$][\w$]*)\s*=>`)
	tsImport   = regexp.MustCompile(`^import\s+(?:.+?\s+from\s+)?["']([^"']+)["']|require\s*\(\s*["']([^"']+)["']\s*\)`)
	javaMethod = regexp.MustCompile(`^(?:(?:public|private|protected|static|final|abstract|synchronized|native|default)\s+)*(?:<[^>]+>\s+)?[\w$<>\[\],.?]+\s+([A-Za-z_$][\w$]*)\s*(\([^)]*\))(?:\s+throws\s+[\w$., ]+)?\s*[{;]`)
	javaImport = regexp.MustCompile(`^import\s+(?:static\s+)?([\w.*]+)\s*;`)
)

type CStyle struct {
	language, version string
	extensions        []string
}

func NewTypeScript() *CStyle {
	return &CStyle{"typescript", "typescript-structural@2.0.0", []string{".ts", ".tsx", ".js", ".jsx", ".mts", ".cts"}}
}
func NewJava() *CStyle                 { return &CStyle{"java", "java-structural@2.0.0", []string{".java"}} }
func (p *CStyle) Language() string     { return p.language }
func (p *CStyle) Extensions() []string { return p.extensions }
func (p *CStyle) Version() string      { return p.version }

type cFrame struct {
	depth    int
	name, id string
	kind     repository.ArtifactKind
}

func (p *CStyle) Parse(ctx context.Context, commit string, file repository.FileContent) (repository.ParsedFile, error) {
	cleanPath, err := repository.CanonicalPath(file.Path)
	if err != nil {
		return repository.ParsedFile{}, err
	}
	file.Path = cleanPath
	result := repository.ParsedFile{}
	root := fileArtifact(commit, file, p.language, repository.ArtifactFile)
	result.Artifacts = append(result.Artifacts, root)
	frames := []cFrame{{-1, file.Path, root.ArtifactID, repository.ArtifactFile}}
	depth := 0
	lines := strings.Split(string(file.Content), "\n")
	inBlockComment := false
	for i, raw := range lines {
		if err := ctx.Err(); err != nil {
			return repository.ParsedFile{}, err
		}
		lineNo := i + 1
		line := strings.TrimSpace(stripCStyle(stripBlockComments(raw, &inBlockComment)))
		for len(frames) > 1 && depth < frames[len(frames)-1].depth {
			frames = frames[:len(frames)-1]
		}
		parent := frames[len(frames)-1]
		if line != "" {
			if match := typeDecl.FindStringSubmatch(line); match != nil {
				kind := repository.ArtifactClass
				if match[1] == "interface" {
					kind = repository.ArtifactInterface
				}
				qualified := qualify(parent.name, match[2])
				a := artifact(commit, file.Path, p.language, kind, match[2], qualified, line, lineNo, lineNo, raw, nil)
				result.Artifacts = append(result.Artifacts, a)
				result.Relations = append(result.Relations, relation(commit, file.Path, repository.RelationContains, parent.id, a.ArtifactID, lineNo, raw, 1))
				extends, implements := inheritanceNames(line)
				for _, name := range extends {
					result.Relations = append(result.Relations, relation(commit, file.Path, repository.RelationExtends, a.ArtifactID, "symbol:"+name, lineNo, raw, .9))
				}
				for _, name := range implements {
					result.Relations = append(result.Relations, relation(commit, file.Path, repository.RelationImplements, a.ArtifactID, "symbol:"+name, lineNo, raw, .9))
				}
				if strings.Contains(line, "{") {
					frames = append(frames, cFrame{depth + 1, qualified, a.ArtifactID, kind})
				}
			} else if target, ok := p.importTarget(line); ok {
				a := artifact(commit, file.Path, p.language, repository.ArtifactImport, target, target, line, lineNo, lineNo, raw, nil)
				result.Artifacts = append(result.Artifacts, a)
				result.Relations = append(result.Relations, relation(commit, file.Path, repository.RelationImports, root.ArtifactID, "module:"+target, lineNo, raw, 1))
			} else if name, signature, ok := p.function(line, parent); ok {
				kind := repository.ArtifactFunction
				if parent.kind == repository.ArtifactClass || parent.kind == repository.ArtifactInterface {
					kind = repository.ArtifactMethod
				}
				qualified := qualify(parent.name, name)
				a := artifact(commit, file.Path, p.language, kind, name, qualified, signature, lineNo, lineNo, raw, nil)
				result.Artifacts = append(result.Artifacts, a)
				result.Relations = append(result.Relations, relation(commit, file.Path, repository.RelationContains, parent.id, a.ArtifactID, lineNo, raw, 1))
				if strings.Contains(line, "{") {
					frames = append(frames, cFrame{depth + 1, qualified, a.ArtifactID, kind})
				}
			} else if parent.kind == repository.ArtifactFunction || parent.kind == repository.ArtifactMethod {
				for _, name := range callsIn(line) {
					if isCallKeyword(name) {
						continue
					}
					result.Relations = append(result.Relations, relation(commit, file.Path, repository.RelationCalls, parent.id, "symbol:"+name, lineNo, raw, .65))
				}
			}
		}
		depth += strings.Count(line, "{") - strings.Count(line, "}")
		if depth < 0 {
			depth = 0
		}
	}
	finalizeCStyleRanges(result.Artifacts, lines)
	return result, nil
}

func stripBlockComments(line string, inBlock *bool) string {
	var out strings.Builder
	for i := 0; i < len(line); {
		if *inBlock {
			end := strings.Index(line[i:], "*/")
			if end < 0 {
				return out.String()
			}
			i += end + 2
			*inBlock = false
			continue
		}
		start := strings.Index(line[i:], "/*")
		if start < 0 {
			out.WriteString(line[i:])
			break
		}
		start += i
		out.WriteString(line[i:start])
		i = start + 2
		*inBlock = true
	}
	return out.String()
}

func (p *CStyle) importTarget(line string) (string, bool) {
	re := tsImport
	if p.language == "java" {
		re = javaImport
	}
	match := re.FindStringSubmatch(line)
	if match == nil {
		return "", false
	}
	for _, item := range match[1:] {
		if item != "" {
			return item, true
		}
	}
	return "", false
}

func (p *CStyle) function(line string, parent cFrame) (string, string, bool) {
	for _, prefix := range []string{"return ", "throw ", "new ", "if ", "for ", "while ", "switch "} {
		if strings.HasPrefix(line, prefix) {
			return "", "", false
		}
	}
	patterns := []*regexp.Regexp{tsFunction, tsArrow}
	if p.language == "java" {
		patterns = []*regexp.Regexp{javaMethod}
	} else if parent.kind == repository.ArtifactClass || parent.kind == repository.ArtifactInterface {
		patterns = append(patterns, tsMethod)
	}
	for _, re := range patterns {
		if match := re.FindStringSubmatch(line); match != nil {
			name := match[1]
			if isCallKeyword(name) || name == "constructor" && parent.kind == repository.ArtifactFile {
				continue
			}
			return name, name + match[2], true
		}
	}
	return "", "", false
}

func inheritanceNames(line string) (extends, implements []string) {
	declaration := line
	if brace := strings.Index(declaration, "{"); brace >= 0 {
		declaration = declaration[:brace]
	}
	if index := strings.Index(declaration, " extends "); index >= 0 {
		value := declaration[index+len(" extends "):]
		if end := strings.Index(value, " implements "); end >= 0 {
			value = value[:end]
		}
		extends = splitNames(value)
	}
	if index := strings.Index(declaration, " implements "); index >= 0 {
		implements = splitNames(declaration[index+len(" implements "):])
	}
	return extends, implements
}
