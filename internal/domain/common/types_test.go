package common

import "testing"

func TestSourceRefRejectsTraversalAndBadRanges(t *testing.T) {
	for _, ref := range []SourceRef{{CommitSHA: "a", Path: "../secret", StartLine: 1, EndLine: 1}, {CommitSHA: "a", Path: "a.go", StartLine: 2, EndLine: 1}} {
		if ref.Validate() == nil {
			t.Fatalf("预期源码引用无效：%#v", ref)
		}
	}
}
