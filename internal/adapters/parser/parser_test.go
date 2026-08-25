package parser

import (
	"context"
	"testing"

	"github.com/reposense/reposense/internal/domain/repository"
)

func TestPythonParserExtractsSymbolsAndRelations(t *testing.T) {
	source := "from auth.tokens import Token\n\nclass Service(Base):\n    async def login(self, user: str) -> Token:\n        token = issue(user)\n        return token\n"
	got, err := NewPython().Parse(context.Background(), "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", repository.FileContent{Path: "src/service.py", Content: []byte(source)})
	if err != nil {
		t.Fatal(err)
	}
	assertArtifact(t, got, repository.ArtifactClass, "Service", 3, 6)
	assertArtifact(t, got, repository.ArtifactMethod, "login", 4, 6)
	assertRelation(t, got, repository.RelationExtends, "symbol:Base")
	assertRelation(t, got, repository.RelationCalls, "symbol:issue")
	assertRelation(t, got, repository.RelationImports, "module:auth.tokens")
}

func TestTypeScriptAndJavaParsers(t *testing.T) {
	tests := []struct {
		name                 string
		parser               *CStyle
		path, source         string
		expectedKind         repository.ArtifactKind
		expectedName, target string
	}{
		{"typescript", NewTypeScript(), "src/service.ts", "import { Token } from './token';\nexport class Service extends Base implements Login {\n  async login(user: string): Promise<Token> {\n    return issue(user);\n  }\n}\n", repository.ArtifactMethod, "login", "symbol:issue"},
		{"java", NewJava(), "src/Service.java", "import app.Token;\npublic class Service extends Base implements Login {\n  public Token login(String user) {\n    return issue(user);\n  }\n}\n", repository.ArtifactMethod, "login", "symbol:issue"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.parser.Parse(context.Background(), "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", repository.FileContent{Path: tt.path, Content: []byte(tt.source)})
			if err != nil {
				t.Fatal(err)
			}
			assertArtifact(t, got, tt.expectedKind, tt.expectedName, 3, 5)
			assertRelation(t, got, repository.RelationCalls, tt.target)
			assertRelation(t, got, repository.RelationExtends, "symbol:Base")
			assertRelation(t, got, repository.RelationImplements, "symbol:Login")
		})
	}
}

func TestTextParserExtractsPackageDependencies(t *testing.T) {
	source := `{"dependencies":{"cloudwego/eino":"1.0.0","pg":"8"},"devDependencies":{"testify":"1"}}`
	got, err := NewText().Parse(context.Background(), "cccccccccccccccccccccccccccccccccccccccc", repository.FileContent{Path: "package.json", Content: []byte(source)})
	if err != nil {
		t.Fatal(err)
	}
	for _, target := range []string{"package:cloudwego/eino", "package:pg", "package:testify"} {
		assertRelation(t, got, repository.RelationDependsOn, target)
	}
}

// TestDefaultRegistryDocumentsSupportedAndUnsupportedPaths 固化 MVP 当前真实解析范围：
// 已支持路径必须正确路由且大小写不敏感，未支持语言必须明确返回 unsupported。
func TestDefaultRegistryDocumentsSupportedAndUnsupportedPaths(t *testing.T) {
	registry := DefaultRegistry()
	tests := []struct {
		path         string
		wantLanguage string
		supported    bool
	}{
		{path: "src/service.PY", wantLanguage: "python", supported: true},
		{path: "web/component.TSX", wantLanguage: "typescript", supported: true},
		{path: "src/Main.JAVA", wantLanguage: "java", supported: true},
		{path: "README.MD", wantLanguage: "text", supported: true},
		{path: "Dockerfile", wantLanguage: "text", supported: true},
		{path: "go.mod", wantLanguage: "text", supported: true},
		{path: "main.go", supported: false},
		{path: "lib.rs", supported: false},
		{path: "service.cs", supported: false},
	}
	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			parser, supported := registry.ForPath(tt.path)
			if supported != tt.supported {
				t.Fatalf("解析器支持状态不符：supported=%v want=%v", supported, tt.supported)
			}
			if supported && parser.Language() != tt.wantLanguage {
				t.Fatalf("解析器路由错误：got=%s want=%s", parser.Language(), tt.wantLanguage)
			}
		})
	}
}

func assertArtifact(t *testing.T, parsed repository.ParsedFile, kind repository.ArtifactKind, name string, start, end int) {
	t.Helper()
	for _, item := range parsed.Artifacts {
		if item.Kind == kind && item.Name == name {
			if item.SourceRef.StartLine != start || item.SourceRef.EndLine != end {
				t.Fatalf("%s 的行号范围为 %d-%d，期望为 %d-%d", name, item.SourceRef.StartLine, item.SourceRef.EndLine, start, end)
			}
			return
		}
	}
	t.Fatalf("未找到产物 %s/%s：%#v", kind, name, parsed.Artifacts)
}
func assertRelation(t *testing.T, parsed repository.ParsedFile, kind repository.RelationKind, target string) {
	t.Helper()
	for _, item := range parsed.Relations {
		if item.Kind == kind && item.To == target {
			return
		}
	}
	t.Fatalf("未找到关系 %s -> %s：%#v", kind, target, parsed.Relations)
}
