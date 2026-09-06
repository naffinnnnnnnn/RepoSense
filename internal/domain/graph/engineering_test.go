package graph

import (
	"testing"
	"time"

	"github.com/reposense/reposense/internal/domain/common"
	"github.com/reposense/reposense/internal/domain/repository"
)

func TestRequestFingerprintCoversEveryVersionedIdentityField(t *testing.T) {
	base := FingerprintInput{
		Scope:     common.Scope{TenantID: "tenant", RepositoryID: "repo", SnapshotID: "snapshot"},
		CommitSHA: "commit",
		Versions:  BuildVersions{ParserResultVersion: "parser-1", GraphSchemaVersion: "graph-1", GraphAlgorithmVersion: "algorithm-1", BuildPolicyVersion: "policy-1"},
	}
	want, err := RequestFingerprint(base)
	if err != nil {
		t.Fatal(err)
	}
	again, err := RequestFingerprint(base)
	if err != nil || again != want {
		t.Fatalf("fingerprint is not deterministic: first=%q second=%q err=%v", want, again, err)
	}
	mutations := map[string]func(*FingerprintInput){
		"tenant":     func(v *FingerprintInput) { v.Scope.TenantID = "other" },
		"repository": func(v *FingerprintInput) { v.Scope.RepositoryID = "other" },
		"snapshot":   func(v *FingerprintInput) { v.Scope.SnapshotID = "other" },
		"commit":     func(v *FingerprintInput) { v.CommitSHA = "other" },
		"parser":     func(v *FingerprintInput) { v.Versions.ParserResultVersion = "other" },
		"schema":     func(v *FingerprintInput) { v.Versions.GraphSchemaVersion = "other" },
		"algorithm":  func(v *FingerprintInput) { v.Versions.GraphAlgorithmVersion = "other" },
		"policy":     func(v *FingerprintInput) { v.Versions.BuildPolicyVersion = "other" },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			changed := base
			mutate(&changed)
			got, err := RequestFingerprint(changed)
			if err != nil {
				t.Fatal(err)
			}
			if got == want {
				t.Fatalf("%s was not covered by fingerprint", name)
			}
		})
	}
}

func TestGraphLifecycleTransitionsAreClosed(t *testing.T) {
	if !CanTransitionJob(JobPending, JobBuilding) || !CanTransitionJob(JobBuilding, JobSucceeded) || CanTransitionJob(JobSucceeded, JobBuilding) {
		t.Fatal("job lifecycle accepts an invalid transition or rejects a valid transition")
	}
	if !CanTransitionAttempt(AttemptRunning, AttemptLost) || CanTransitionAttempt(AttemptLost, AttemptSucceeded) {
		t.Fatal("attempt lifecycle accepts an invalid transition or rejects a valid transition")
	}
	if !CanTransitionCandidate(CandidateStaging, CandidateSealed) || CanTransitionCandidate(CandidateSealed, CandidateStaging) {
		t.Fatal("candidate lifecycle accepts mutation after sealing")
	}
}

func TestResolvedRelationContract(t *testing.T) {
	reference := common.SourceRef{CommitSHA: "commit", Path: "pkg/a.go", StartLine: 1, EndLine: 1, ContentHash: "hash"}
	valid := []ResolvedRelation{
		{RelationID: "r1", Kind: repository.RelationCalls, FromArtifactID: "a", TargetArtifactID: "b", ResolutionStatus: ResolutionResolved, Evidence: reference, Confidence: 1},
		{RelationID: "r2", Kind: repository.RelationCalls, FromArtifactID: "a", ExternalIdentity: "go:fmt", ResolutionStatus: ResolutionExternal, Evidence: reference, Confidence: 1},
		{RelationID: "r3", Kind: repository.RelationCalls, FromArtifactID: "a", RawTargetSymbol: "target", AmbiguousCandidates: []string{"b", "c"}, ResolutionStatus: ResolutionAmbiguous, ResolutionReasonCode: "MULTIPLE_MATCHES", Evidence: reference, Confidence: .5},
		{RelationID: "r4", Kind: repository.RelationCalls, FromArtifactID: "a", RawTargetSymbol: "missing", ResolutionStatus: ResolutionUnresolved, ResolutionReasonCode: "NO_MATCH", Evidence: reference, Confidence: 0},
	}
	for _, relation := range valid {
		if err := relation.Validate(); err != nil {
			t.Fatalf("valid %s relation rejected: %v", relation.ResolutionStatus, err)
		}
	}
	invalid := valid[0]
	invalid.TargetArtifactID = ""
	if invalid.Validate() == nil {
		t.Fatal("RESOLVED relation without target was accepted")
	}
	invalid = valid[3]
	invalid.AmbiguousCandidates = []string{"b"}
	if invalid.Validate() == nil {
		t.Fatal("UNRESOLVED relation with candidates was accepted")
	}
}

func TestSnapshotMetadataRequiresStableCompleteIdentity(t *testing.T) {
	scope := common.Scope{TenantID: "tenant", RepositoryID: "repo", SnapshotID: "snapshot"}
	metadata := SnapshotMetadata{Scope: scope, CommitSHA: "commit", ParserResultVersion: "parser-1", ParseResultChecksum: "checksum", SchemaVersion: "schema-1", CompletedAt: time.Now(), ArtifactCount: 1, RelationCount: 1}
	if err := metadata.Validate(scope); err != nil {
		t.Fatal(err)
	}
	other := scope
	other.TenantID = "other"
	if metadata.Validate(other) == nil {
		t.Fatal("cross-tenant parser metadata was accepted")
	}
}

func TestQualityThresholdUsesStrictExceededBoundary(t *testing.T) {
	policy := QualityPolicy{MaxInvalidArtifacts: 1, MaxInvalidArtifactRatio: .5, MaxInvalidRelations: 1, MaxInvalidRelationRatio: .5}
	status, err := EvaluateQuality(policy, QualityStats{InputArtifacts: 2, WrittenArtifacts: 1, InvalidArtifacts: 1, InputRelations: 2, WrittenRelations: 1, InvalidRelations: 1})
	if err != nil || status != QualityDegraded {
		t.Fatalf("quality exactly at threshold must be DEGRADED: status=%q err=%v", status, err)
	}
	_, err = EvaluateQuality(policy, QualityStats{InputArtifacts: 3, WrittenArtifacts: 1, InvalidArtifacts: 2, InputRelations: 2, WrittenRelations: 1, InvalidRelations: 1})
	if !IsCode(err, ErrGraphValidationFailed) {
		t.Fatalf("quality above threshold must fail: %v", err)
	}
	status, err = EvaluateQuality(policy, QualityStats{})
	if err != nil || status != QualityHealthy {
		t.Fatalf("empty valid snapshot must be HEALTHY: status=%q err=%v", status, err)
	}
}

func TestAttemptLeaseRequiresCurrentOwnerFenceAndExpiry(t *testing.T) {
	now := time.Now()
	attempt := BuildAttempt{Status: AttemptRunning, LeaseOwner: "worker-a", Fence: 2, LeaseExpiresAt: now.Add(time.Minute)}
	if err := attempt.ValidateLease("worker-a", 2, now); err != nil {
		t.Fatal(err)
	}
	for name, check := range map[string]func() error{
		"owner":   func() error { return attempt.ValidateLease("worker-b", 2, now) },
		"fence":   func() error { return attempt.ValidateLease("worker-a", 1, now) },
		"expired": func() error { return attempt.ValidateLease("worker-a", 2, now.Add(time.Minute)) },
	} {
		t.Run(name, func(t *testing.T) {
			if err := check(); !IsCode(err, ErrLeaseLost) {
				t.Fatalf("invalid lease was accepted: %v", err)
			}
		})
	}
}
