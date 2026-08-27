package parser

import (
	"context"
	"fmt"
	goast "go/ast"
	goparser "go/parser"
	"go/token"
	"strconv"
	"strings"

	"github.com/reposense/reposense/internal/domain/repository"
)

type Go struct{}

func NewGo() *Go                 { return &Go{} }
func (*Go) Language() string     { return "go" }
func (*Go) Extensions() []string { return []string{".go"} }
func (*Go) Version() string      { return "go-ast@1.0.0" }

func (*Go) Parse(ctx context.Context, commit string, file repository.FileContent) (repository.ParsedFile, error) {
	if err := ctx.Err(); err != nil {
		return repository.ParsedFile{}, err
	}
	clean, err := repository.CanonicalPath(file.Path)
	if err != nil {
		return repository.ParsedFile{}, err
	}
	file.Path = clean
	fset := token.NewFileSet()
	tree, err := goparser.ParseFile(fset, file.Path, file.Content, goparser.AllErrors|goparser.ParseComments)
	if err != nil {
		return repository.ParsedFile{}, fmt.Errorf("Go 语法无效: %w", err)
	}
	root := fileArtifact(commit, file, "go", repository.ArtifactFile)
	result := repository.ParsedFile{Artifacts: []repository.CodeArtifact{root}}
	pkgName := tree.Name.Name
	pkg := artifact(commit, file.Path, "go", repository.ArtifactModule, pkgName, pkgName, "package "+pkgName, 1, 1, lineAt(string(file.Content), 1), nil)
	result.Artifacts = append(result.Artifacts, pkg)
	result.Relations = append(result.Relations, relation(commit, file.Path, repository.RelationContains, root.ArtifactID, pkg.ArtifactID, 1, lineAt(string(file.Content), 1), 1))
	symbols := map[string][]string{}
	functions := map[*goast.FuncDecl]repository.CodeArtifact{}
	for _, declaration := range tree.Decls {
		if err := ctx.Err(); err != nil {
			return repository.ParsedFile{}, err
		}
		switch node := declaration.(type) {
		case *goast.GenDecl:
			if node.Tok == token.IMPORT {
				for _, spec := range node.Specs {
					imp := spec.(*goast.ImportSpec)
					target, _ := strconv.Unquote(imp.Path.Value)
					line := fset.Position(imp.Pos()).Line
					a := artifact(commit, file.Path, "go", repository.ArtifactImport, target, target, target, line, line, lineAt(string(file.Content), line), nil)
					result.Artifacts = append(result.Artifacts, a)
					result.Relations = append(result.Relations, relation(commit, file.Path, repository.RelationImports, pkg.ArtifactID, "module:"+target, line, lineAt(string(file.Content), line), 1))
				}
			}
			if node.Tok == token.TYPE {
				for _, spec := range node.Specs {
					ts := spec.(*goast.TypeSpec)
					kind := repository.ArtifactClass
					if _, ok := ts.Type.(*goast.InterfaceType); ok {
						kind = repository.ArtifactInterface
					}
					start, end := fset.Position(ts.Pos()).Line, fset.Position(ts.End()).Line
					q := pkgName + "." + ts.Name.Name
					a := artifact(commit, file.Path, "go", kind, ts.Name.Name, q, strings.TrimSpace(lineAt(string(file.Content), start)), start, end, linesRange(string(file.Content), start, end), nil)
					result.Artifacts = append(result.Artifacts, a)
					result.Relations = append(result.Relations, relation(commit, file.Path, repository.RelationContains, pkg.ArtifactID, a.ArtifactID, start, lineAt(string(file.Content), start), 1))
					symbols[ts.Name.Name] = append(symbols[ts.Name.Name], a.ArtifactID)
				}
			}
		case *goast.FuncDecl:
			kind := repository.ArtifactFunction
			qualified := pkgName + "." + node.Name.Name
			if receiverName(node.Recv) != "" {
				kind = repository.ArtifactMethod
				qualified = pkgName + "." + receiverName(node.Recv) + "." + node.Name.Name
			}
			start, end := fset.Position(node.Pos()).Line, fset.Position(node.End()).Line
			signature := strings.TrimSpace(lineAt(string(file.Content), start))
			a := artifact(commit, file.Path, "go", kind, node.Name.Name, qualified, signature, start, end, linesRange(string(file.Content), start, end), nil)
			result.Artifacts = append(result.Artifacts, a)
			result.Relations = append(result.Relations, relation(commit, file.Path, repository.RelationContains, pkg.ArtifactID, a.ArtifactID, start, lineAt(string(file.Content), start), 1))
			symbols[node.Name.Name] = append(symbols[node.Name.Name], a.ArtifactID)
			symbols[qualified] = []string{a.ArtifactID}
			functions[node] = a
		}
	}
	for declaration, from := range functions {
		goast.Inspect(declaration.Body, func(node goast.Node) bool {
			call, ok := node.(*goast.CallExpr)
			if !ok {
				return true
			}
			name := goCallName(call.Fun)
			if name == "" {
				return true
			}
			target := "symbol:" + name
			short := name
			if dot := strings.LastIndex(short, "."); dot >= 0 {
				short = short[dot+1:]
			}
			if ids := symbols[short]; len(ids) == 1 {
				target = ids[0]
			}
			line := fset.Position(call.Pos()).Line
			result.Relations = append(result.Relations, relation(commit, file.Path, repository.RelationCalls, from.ArtifactID, target, line, lineAt(string(file.Content), line), .8))
			return true
		})
	}
	return result, nil
}

func receiverName(fields *goast.FieldList) string {
	if fields == nil || len(fields.List) == 0 {
		return ""
	}
	expr := fields.List[0].Type
	if star, ok := expr.(*goast.StarExpr); ok {
		expr = star.X
	}
	if id, ok := expr.(*goast.Ident); ok {
		return id.Name
	}
	return ""
}
func goCallName(expr goast.Expr) string {
	switch value := expr.(type) {
	case *goast.Ident:
		return value.Name
	case *goast.SelectorExpr:
		if left := goCallName(value.X); left != "" {
			return left + "." + value.Sel.Name
		}
		return value.Sel.Name
	}
	return ""
}
func linesRange(content string, start, end int) string {
	lines := strings.Split(content, "\n")
	if start < 1 {
		start = 1
	}
	if end > len(lines) {
		end = len(lines)
	}
	if start > end {
		return ""
	}
	return strings.Join(lines[start-1:end], "\n")
}
