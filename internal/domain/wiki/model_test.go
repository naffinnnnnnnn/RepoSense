package wiki

import (
	"testing"

	"github.com/reposense/reposense/internal/domain/common"
)

func TestGenerateCommandValidationAndCanonicalOrder(t *testing.T) {
	base := GenerateCommand{Scope: common.Scope{TenantID: "t", RepositoryID: "r", SnapshotID: "s"}, GraphRevisionID: "gr", IdempotencyKey: "key"}
	if err := base.Validate(); err != nil {
		t.Fatal(err)
	}
	base.PageScope = []string{"interfaces", "overview"}
	got := base.SelectedSlugs()
	if len(got) != 2 || got[0] != "overview" || got[1] != "interfaces" {
		t.Fatalf("unexpected canonical order: %v", got)
	}
	for name, mutate := range map[string]func(*GenerateCommand){
		"missing revision":    func(c *GenerateCommand) { c.GraphRevisionID = "" },
		"missing idempotency": func(c *GenerateCommand) { c.IdempotencyKey = "" },
		"bad locale":          func(c *GenerateCommand) { c.Locale = "fr-FR" },
		"bad page":            func(c *GenerateCommand) { c.PageScope = []string{"secrets"} },
		"duplicate page":      func(c *GenerateCommand) { c.PageScope = []string{"overview", "overview"} },
	} {
		t.Run(name, func(t *testing.T) {
			cmd := base
			mutate(&cmd)
			if cmd.Validate() == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}

func TestPageRevisionRequiresGroundedContent(t *testing.T) {
	page := PageRevision{PageID: "p", RevisionNo: 1, SnapshotID: "s", GraphRevisionID: "gr", Locale: "zh-CN", Slug: "overview", Title: "Overview", ContentMarkdown: "content",
		Citations: []common.SourceRef{{CommitSHA: "sha", Path: "main.go", StartLine: 1, EndLine: 2}}}
	if err := page.Validate(); err != nil {
		t.Fatal(err)
	}
	page.Citations = nil
	if err := page.Validate(); err == nil {
		t.Fatal("expected missing citation error")
	}
}

func TestNormalizeCitationsDeduplicatesAndSorts(t *testing.T) {
	a := common.SourceRef{CommitSHA: "sha", Path: "z.go", StartLine: 2, EndLine: 3}
	b := common.SourceRef{CommitSHA: "sha", Path: "a.go", StartLine: 1, EndLine: 1}
	got := NormalizeCitations([]common.SourceRef{a, b, a})
	if len(got) != 2 || got[0].Path != "a.go" || got[1].Path != "z.go" {
		t.Fatalf("unexpected citations: %#v", got)
	}
}
