package parser

import (
	"context"
	"errors"
	"strings"
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

// TestStructuralParsersHandleMultilineDeclarationsAndIgnoreBlockComments 验证结构解析的两个基本边界：
// 合法的多行声明不能漏掉，注释中的伪代码也不能被识别成真实 Artifact；这些能力不依赖 AI。
func TestStructuralParsersHandleMultilineDeclarationsAndIgnoreBlockComments(t *testing.T) {
	t.Run("python_multiline_function", func(t *testing.T) {
		source := "def combine(\n    left: str,\n    right: str,\n) -> str:\n    return left + right\n"
		parsed, err := NewPython().Parse(context.Background(), strings.Repeat("a", 40), repository.FileContent{Path: "src/combine.py", Content: []byte(source)})
		if err != nil {
			t.Fatal(err)
		}
		if findArtifact(parsed, repository.ArtifactFunction, "combine") == nil {
			t.Fatalf("合法的 Python 多行函数声明未被解析：%#v", parsed.Artifacts)
		}
	})
	t.Run("typescript_block_comment", func(t *testing.T) {
		source := "/*\nclass Ghost {\n  haunt() {}\n}\n*/\nexport function real() {}\n"
		parsed, err := NewTypeScript().Parse(context.Background(), strings.Repeat("a", 40), repository.FileContent{Path: "src/service.ts", Content: []byte(source)})
		if err != nil {
			t.Fatal(err)
		}
		if findArtifact(parsed, repository.ArtifactClass, "Ghost") != nil {
			t.Fatalf("块注释中的伪类型不应成为 Artifact：%#v", parsed.Artifacts)
		}
		if findArtifact(parsed, repository.ArtifactFunction, "real") == nil {
			t.Fatalf("真实函数未被解析：%#v", parsed.Artifacts)
		}
	})
}

// TestJavaOverloadsHaveDistinctArtifactIDs 验证 Artifact 身份包含可区分重载的签名。
// 同一类中的同名方法如果参数不同，必须保留两个 Artifact 且 ID 不能冲突。
func TestJavaOverloadsHaveDistinctArtifactIDs(t *testing.T) {
	source := "public class Service {\n  public void run(String value) {}\n  public void run(int value) {}\n}\n"
	parsed, err := NewJava().Parse(context.Background(), strings.Repeat("b", 40), repository.FileContent{Path: "src/Service.java", Content: []byte(source)})
	if err != nil {
		t.Fatal(err)
	}
	var overloads []repository.CodeArtifact
	for _, artifact := range parsed.Artifacts {
		if artifact.Kind == repository.ArtifactMethod && artifact.Name == "run" {
			overloads = append(overloads, artifact)
		}
	}
	if len(overloads) != 2 {
		t.Fatalf("应保留两个重载方法：%#v", overloads)
	}
	if overloads[0].ArtifactID == overloads[1].ArtifactID {
		t.Fatalf("重载方法发生 ArtifactID 冲突：id=%s signatures=%q/%q", overloads[0].ArtifactID, overloads[0].Signature, overloads[1].Signature)
	}
}

// TestSemanticRelationIDSurvivesLineShifts 验证非语义空行只改变 Evidence 行号，
// 不应改变函数 ArtifactID 或同一调用关系的 RelationID，否则增量图会产生无意义删建。
func TestSemanticRelationIDSurvivesLineShifts(t *testing.T) {
	parse := func(source string) repository.ParsedFile {
		t.Helper()
		parsed, err := NewPython().Parse(context.Background(), strings.Repeat("c", 40), repository.FileContent{Path: "src/caller.py", Content: []byte(source)})
		if err != nil {
			t.Fatal(err)
		}
		return parsed
	}
	first := parse("def caller():\n    issue()\n")
	shifted := parse("\n\ndef caller():\n    issue()\n")
	firstFunction := findArtifact(first, repository.ArtifactFunction, "caller")
	shiftedFunction := findArtifact(shifted, repository.ArtifactFunction, "caller")
	if firstFunction == nil || shiftedFunction == nil || firstFunction.ArtifactID != shiftedFunction.ArtifactID {
		t.Fatalf("非语义行移不应改变函数身份：first=%#v shifted=%#v", firstFunction, shiftedFunction)
	}
	firstCall := findRelation(first, repository.RelationCalls, "symbol:issue")
	shiftedCall := findRelation(shifted, repository.RelationCalls, "symbol:issue")
	if firstCall == nil || shiftedCall == nil || firstCall.RelationID != shiftedCall.RelationID {
		t.Fatalf("非语义行移不应改变调用关系身份：first=%#v shifted=%#v", firstCall, shiftedCall)
	}
}

// TestSelfCallResolvesToQualifiedArtifactID 验证同文件存在多个同名方法时，
// self.run() 应解析到当前类 A.run 的唯一 ArtifactID，而不是无法区分的 symbol:self.run。
func TestSelfCallResolvesToQualifiedArtifactID(t *testing.T) {
	source := "class A:\n    def run(self):\n        return 1\n    def caller(self):\n        return self.run()\n\nclass B:\n    def run(self):\n        return 2\n"
	parsed, err := NewPython().Parse(context.Background(), strings.Repeat("d", 40), repository.FileContent{Path: "src/models.py", Content: []byte(source)})
	if err != nil {
		t.Fatal(err)
	}
	var runA, caller *repository.CodeArtifact
	for i := range parsed.Artifacts {
		artifact := &parsed.Artifacts[i]
		if artifact.Kind == repository.ArtifactMethod && strings.HasSuffix(artifact.QualifiedName, ".A.run") {
			runA = artifact
		}
		if artifact.Kind == repository.ArtifactMethod && strings.HasSuffix(artifact.QualifiedName, ".A.caller") {
			caller = artifact
		}
	}
	if runA == nil || caller == nil {
		t.Fatalf("测试方法未完整解析：%#v", parsed.Artifacts)
	}
	for _, relation := range parsed.Relations {
		if relation.Kind == repository.RelationCalls && relation.From == caller.ArtifactID {
			if relation.To != runA.ArtifactID {
				t.Fatalf("同名方法调用目标存在歧义：got=%q want=%q", relation.To, runA.ArtifactID)
			}
			return
		}
	}
	t.Fatal("未找到 A.caller 的调用关系")
}

// TestParsersRejectMalformedSupportedSyntax 验证受支持格式出现确定语法错误时必须返回 error，
// 不能仅生成一个文件 Artifact 后对外宣称解析成功。
func TestParsersRejectMalformedSupportedSyntax(t *testing.T) {
	tests := []struct {
		name  string
		parse func() error
	}{
		{name: "package_json", parse: func() error {
			_, err := NewText().Parse(context.Background(), strings.Repeat("e", 40), repository.FileContent{Path: "package.json", Content: []byte(`{"dependencies":`)})
			return err
		}},
		{name: "python", parse: func() error {
			_, err := NewPython().Parse(context.Background(), strings.Repeat("e", 40), repository.FileContent{Path: "broken.py", Content: []byte("def broken(:\n    pass\n")})
			return err
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.parse(); err == nil {
				t.Fatal("确定的语法错误不应返回解析成功")
			} else if errors.Is(err, context.Canceled) {
				t.Fatalf("语法错误不应被误报为 Context 取消：%v", err)
			}
		})
	}
}

// TestParserNormalizesPathBeforeGeneratingIdentity 验证 ReadFile 清理后的逻辑路径与 Parser 身份一致。
// src/./service.py 和 src/service.py 指向同一文件，不能生成不同 ArtifactID 或 SourceRef.Path。
func TestParserNormalizesPathBeforeGeneratingIdentity(t *testing.T) {
	parse := func(path string) repository.CodeArtifact {
		t.Helper()
		parsed, err := NewPython().Parse(context.Background(), strings.Repeat("f", 40), repository.FileContent{Path: path, Content: []byte("def run():\n    return 1\n")})
		if err != nil {
			t.Fatal(err)
		}
		return parsed.Artifacts[0]
	}
	canonical := parse("src/service.py")
	unclean := parse("src/./service.py")
	if canonical.ArtifactID != unclean.ArtifactID || canonical.SourceRef.Path != unclean.SourceRef.Path {
		t.Fatalf("等价路径生成了不同身份：canonical=%#v unclean=%#v", canonical, unclean)
	}
}

func findArtifact(parsed repository.ParsedFile, kind repository.ArtifactKind, name string) *repository.CodeArtifact {
	for i := range parsed.Artifacts {
		if parsed.Artifacts[i].Kind == kind && parsed.Artifacts[i].Name == name {
			return &parsed.Artifacts[i]
		}
	}
	return nil
}

func findRelation(parsed repository.ParsedFile, kind repository.RelationKind, target string) *repository.CodeRelation {
	for i := range parsed.Relations {
		if parsed.Relations[i].Kind == kind && parsed.Relations[i].To == target {
			return &parsed.Relations[i]
		}
	}
	return nil
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
