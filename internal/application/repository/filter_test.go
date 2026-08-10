package repositoryapp

import (
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
