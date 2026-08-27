package parser

import (
	"crypto/sha256"
	"encoding/hex"
	"path/filepath"
	"sort"
	"strings"

	"github.com/reposense/reposense/internal/ports"
)

type Registry struct {
	byExtension map[string]ports.LanguageParser
}

func NewRegistry(parsers ...ports.LanguageParser) *Registry {
	r := &Registry{byExtension: make(map[string]ports.LanguageParser)}
	for _, p := range parsers {
		for _, ext := range p.Extensions() {
			r.byExtension[strings.ToLower(ext)] = p
		}
	}
	return r
}

func DefaultRegistry() *Registry {
	return NewRegistry(NewPython(), NewTypeScript(), NewJava(), NewGo(), NewRust(), NewText())
}

func (r *Registry) ForPath(path string) (ports.LanguageParser, bool) {
	ext := strings.ToLower(filepath.Ext(path))
	p, ok := r.byExtension[ext]
	if ok {
		return p, true
	}
	base := strings.ToLower(filepath.Base(path))
	p, ok = r.byExtension[base]
	return p, ok
}

func (r *Registry) Version() string {
	versions := make([]string, 0, len(r.byExtension))
	seen := map[string]bool{}
	for _, parser := range r.byExtension {
		if !seen[parser.Version()] {
			seen[parser.Version()] = true
			versions = append(versions, parser.Version())
		}
	}
	sort.Strings(versions)
	sum := sha256.Sum256([]byte(strings.Join(versions, "\x00")))
	return "registry@2:" + hex.EncodeToString(sum[:8])
}
