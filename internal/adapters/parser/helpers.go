package parser

import (
	"crypto/sha256"
	"encoding/hex"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/reposense/reposense/internal/domain/common"
	"github.com/reposense/reposense/internal/domain/repository"
)

var callPattern = regexp.MustCompile(`\b([A-Za-z_$][\w$]*(?:\.[A-Za-z_$][\w$]*)?)\s*\(`)

func digest(parts ...string) string {
	h := sha256.New()
	for _, part := range parts {
		h.Write([]byte(part))
		h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))
}

func stableID(prefix string, parts ...string) string { return prefix + "_" + digest(parts...)[:24] }

func sourceRef(commit, path string, start, end int, content string) common.SourceRef {
	clean, err := repository.CanonicalPath(path)
	if err == nil {
		path = clean
	}
	return common.SourceRef{CommitSHA: commit, Path: filepath.ToSlash(path), StartLine: start, EndLine: end,
		ContentHash: "sha256:" + digest(content)}
}

func artifact(commit, path, language string, kind repository.ArtifactKind, name, qualified, signature string, start, end int, content string, attrs map[string]string) repository.CodeArtifact {
	clean, err := repository.CanonicalPath(path)
	if err == nil {
		path = clean
	}
	identitySignature := ""
	if kind == repository.ArtifactMethod || kind == repository.ArtifactFunction {
		identitySignature = strings.Join(strings.Fields(signature), " ")
	}
	id := stableID("art", path, string(kind), qualified, identitySignature)
	ref := sourceRef(commit, path, start, end, content)
	ref.SymbolID = id
	return repository.CodeArtifact{ArtifactID: id, Kind: kind, Name: name, QualifiedName: qualified,
		Language: language, SourceRef: ref, Signature: strings.TrimSpace(signature), ContentHash: ref.ContentHash, Attributes: attrs}
}

func relation(commit, path string, kind repository.RelationKind, from, to string, line int, evidence string, confidence float64) repository.CodeRelation {
	clean, err := repository.CanonicalPath(path)
	if err == nil {
		path = clean
	}
	return repository.CodeRelation{RelationID: stableID("rel", path, string(kind), from, to), Kind: kind,
		From: from, To: to, Evidence: sourceRef(commit, path, line, line, evidence), Confidence: confidence}
}

func fileArtifact(commit string, file repository.FileContent, language string, kind repository.ArtifactKind) repository.CodeArtifact {
	lines := strings.Count(string(file.Content), "\n") + 1
	name := filepath.Base(file.Path)
	return artifact(commit, file.Path, language, kind, name, filepath.ToSlash(file.Path), "", 1, lines, string(file.Content), nil)
}

func callsIn(line string) []string {
	matches := callPattern.FindAllStringSubmatch(line, -1)
	result := make([]string, 0, len(matches))
	seen := map[string]bool{}
	for _, match := range matches {
		name := match[1]
		if !seen[name] {
			seen[name] = true
			result = append(result, name)
		}
	}
	return result
}

func isCallKeyword(name string) bool {
	switch name {
	case "if", "for", "while", "switch", "catch", "return", "new", "function", "def", "class", "super", "this":
		return true
	default:
		return false
	}
}

func stripCStyle(line string) string {
	if i := strings.Index(line, "//"); i >= 0 {
		line = line[:i]
	}
	var b strings.Builder
	var quote rune
	escaped := false
	for _, r := range line {
		if quote != 0 {
			if escaped {
				escaped = false
				continue
			}
			if r == '\\' {
				escaped = true
				continue
			}
			if r == quote {
				quote = 0
			}
			continue
		}
		if r == '\'' || r == '"' || r == '`' {
			quote = r
			continue
		}
		b.WriteRune(r)
	}
	return b.String()
}

func finalizePythonRanges(artifacts []repository.CodeArtifact, lines []string) {
	for i := range artifacts {
		if artifacts[i].Kind == repository.ArtifactFile || artifacts[i].Kind == repository.ArtifactImport {
			continue
		}
		start := artifacts[i].SourceRef.StartLine - 1
		base := leadingIndent(lines[start])
		end := start
		for cursor := start + 1; cursor < len(lines); cursor++ {
			trimmed := strings.TrimSpace(lines[cursor])
			if trimmed == "" || strings.HasPrefix(trimmed, "#") {
				continue
			}
			if leadingIndent(lines[cursor]) <= base {
				break
			}
			end = cursor
		}
		setArtifactRange(&artifacts[i], lines, start, end)
	}
}

func finalizeCStyleRanges(artifacts []repository.CodeArtifact, lines []string) {
	for i := range artifacts {
		if artifacts[i].Kind == repository.ArtifactFile || artifacts[i].Kind == repository.ArtifactImport {
			continue
		}
		start := artifacts[i].SourceRef.StartLine - 1
		depth, opened, end := 0, false, start
		for cursor := start; cursor < len(lines); cursor++ {
			clean := stripCStyle(lines[cursor])
			opens, closes := strings.Count(clean, "{"), strings.Count(clean, "}")
			if opens > 0 {
				opened = true
			}
			depth += opens - closes
			end = cursor
			if opened && depth <= 0 {
				break
			}
			if !opened && strings.Contains(clean, ";") {
				break
			}
		}
		setArtifactRange(&artifacts[i], lines, start, end)
	}
}

func setArtifactRange(a *repository.CodeArtifact, lines []string, start, end int) {
	content := strings.Join(lines[start:end+1], "\n")
	a.SourceRef.EndLine = end + 1
	a.SourceRef.ContentHash = "sha256:" + digest(content)
	a.ContentHash = a.SourceRef.ContentHash
}

func leadingIndent(line string) int {
	return len(line) - len(strings.TrimLeft(line, " \t"))
}
