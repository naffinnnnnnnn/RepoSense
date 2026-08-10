package parser

import (
	"path/filepath"
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
	return NewRegistry(NewPython(), NewTypeScript(), NewJava(), NewText())
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
