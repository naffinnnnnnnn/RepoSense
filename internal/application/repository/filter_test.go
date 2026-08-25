package repositoryapp

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/reposense/reposense/internal/domain/repository"
)

func TestFilterChangesHandlesRenamesAcrossScopeBoundary(t *testing.T) {
	changes := []repository.ChangedPath{
		{OldPath: "src/old.py", Path: "vendor/old.py", Kind: repository.ChangeRenamed},
		{OldPath: "vendor/new.py", Path: "src/new.py", Kind: repository.ChangeRenamed},
		{OldPath: "src/a.py", Path: "src/b.py", Kind: repository.ChangeRenamed},
	}
	got := filterChanges(changes, []string{"src/**"})
	if len(got) != 3 {
		t.Fatalf("变更结果为：%#v", got)
	}
	if got[0].Kind != repository.ChangeDeleted || got[0].Path != "src/old.py" {
		t.Fatalf("移出范围的重命名必须转换为删除：%#v", got[0])
	}
	if got[1].Kind != repository.ChangeAdded || got[1].Path != "src/new.py" {
		t.Fatalf("移入范围的重命名必须转换为新增：%#v", got[1])
	}
	if got[2].Kind != repository.ChangeRenamed {
		t.Fatalf("范围内重命名不应被转换：%#v", got[2])
	}
}

func TestValidatePatternsAllowsOrdinaryDoubleDots(t *testing.T) {
	if err := validatePatterns([]string{"src/foo..bar/**"}); err != nil {
		t.Fatal(err)
	}
	if err := validatePatterns([]string{"src/../secret/**"}); err == nil {
		t.Fatal("不应接受包含父目录穿越的路径")
	}
}

// TestFilterChangesNormalizesWindowsStylePatterns 验证调用方使用 Windows 路径分隔符时，
// IncludePaths 与仓库内部统一使用正斜杠的路径具有相同匹配语义。
func TestFilterChangesNormalizesWindowsStylePatterns(t *testing.T) {
	changes := []repository.ChangedPath{
		{Path: "src/root.py", Kind: repository.ChangeAdded},
		{Path: "src/service/nested.py", Kind: repository.ChangeModified},
		{Path: "docs/readme.md", Kind: repository.ChangeModified},
	}
	want := filterChanges(changes, []string{"src/**"})
	got := filterChanges(changes, []string{`src\**`})
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Windows 风格 pattern 应与正斜杠 pattern 等价：got=%#v want=%#v", got, want)
	}
}

// TestFilterChangesTreatsDoubleStarAsRecursiveGlob 验证 ** 表示任意目录深度，
// 而不是只匹配恰好一层子目录。
func TestFilterChangesTreatsDoubleStarAsRecursiveGlob(t *testing.T) {
	changes := []repository.ChangedPath{
		{Path: "src/root.go", Kind: repository.ChangeAdded},
		{Path: "src/service/one.go", Kind: repository.ChangeAdded},
		{Path: "src/service/internal/deep.go", Kind: repository.ChangeAdded},
		{Path: "src/service/readme.md", Kind: repository.ChangeAdded},
	}
	got := filterChanges(changes, []string{"src/**/*.go"})
	want := []repository.ChangedPath{changes[0], changes[1], changes[2]}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("递归 Glob 应匹配直接及任意层级的 Go 文件：got=%#v want=%#v", got, want)
	}
}

// TestValidatePatternsRejectsWindowsAbsolutePaths 验证 IncludePaths 始终是仓库相对 Glob，
// Windows 盘符和 UNC 路径不能绕过只检查前导正斜杠的绝对路径校验。
func TestValidatePatternsRejectsWindowsAbsolutePaths(t *testing.T) {
	for _, pattern := range []string{`C:\repo\src\**`, `\\server\share\src\**`} {
		t.Run(pattern, func(t *testing.T) {
			if err := validatePatterns([]string{pattern}); err == nil {
				t.Fatalf("不应接受 Windows 绝对 IncludePath：%q", pattern)
			}
		})
	}
}

// TestFilterChangesReturnsIndependentSliceWithoutPatterns 验证无 IncludePaths 时也要返回独立切片。
// Service 后续会对结果排序；如果直接复用 Git 适配器的底层数组，就会反向修改依赖方持有的数据。
func TestFilterChangesReturnsIndependentSliceWithoutPatterns(t *testing.T) {
	changes := []repository.ChangedPath{
		{Path: "z.go", Kind: repository.ChangeModified},
		{Path: "a.go", Kind: repository.ChangeAdded},
	}
	filtered := filterChanges(changes, nil)
	filtered[0].Path = "mutated.go"
	if changes[0].Path != "z.go" {
		t.Fatalf("过滤结果与输入共享底层数组，原始 Git 变更被修改为：%#v", changes)
	}
}

// TestIsBinaryHandlesSampleBoundary 验证 8000 字节采样边界不会漏掉紧邻边界的二进制标记，
// 也不会因为恰好截断一个合法 UTF-8 多字节字符而把文本误判成二进制。
func TestIsBinaryHandlesSampleBoundary(t *testing.T) {
	tests := []struct {
		name    string
		content []byte
		want    bool
	}{
		{name: "nul_after_sample", content: append(bytes.Repeat([]byte{'a'}, 8000), 0), want: true},
		{name: "invalid_utf8_after_sample", content: append(bytes.Repeat([]byte{'a'}, 8000), 0xff), want: true},
		{name: "utf8_rune_split_by_sample", content: append(bytes.Repeat([]byte{'a'}, 7999), []byte("中")...), want: false},
		{name: "ordinary_utf8_text", content: []byte("合法的 UTF-8 文本\n"), want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isBinary(tt.content); got != tt.want {
				t.Fatalf("二进制识别结果错误：got=%v want=%v", got, tt.want)
			}
		})
	}
}
