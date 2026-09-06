package postgres

import (
	"strings"
	"testing"
	"time"

	"github.com/reposense/reposense/internal/domain/graph"
)

func TestValidateEnqueueJobRequiresCanonicalControlIdentity(t *testing.T) {
	now := time.Now().UTC()
	job := graphControlTestJob("job", "key", strings.Repeat("a", 64), "event", now)
	if err := validateEnqueueJob(job); err != nil {
		t.Fatalf("valid job rejected: %v", err)
	}
	job.RequestFingerprint = "not-sha256"
	if err := validateEnqueueJob(job); !graph.IsCode(err, graph.ErrInvalidInput) {
		t.Fatalf("invalid fingerprint must be rejected: %v", err)
	}
}

func TestValidateActivationRequiresMatchingRevisionAndPublishedEvent(t *testing.T) {
	now := time.Now().UTC()
	job := graphControlTestJob("job", "key", strings.Repeat("b", 64), "event", now)
	job.Status = graph.JobBuilding
	job.RevisionID = "revision"
	attempt := graph.BuildAttempt{AttemptID: "attempt", JobID: job.JobID, RevisionID: job.RevisionID,
		Status: graph.AttemptRunning, LeaseOwner: "worker", LeaseExpiresAt: now.Add(time.Minute), Fence: 1}
	revision := graphControlTestRevision(job, attempt, now)
	event := graphControlTestEvent(job, revision, now)
	if err := validateActivation(job, attempt, revision, event, now); err != nil {
		t.Fatalf("valid activation rejected: %v", err)
	}
	event.AggregateID = "another-revision"
	if err := validateActivation(job, attempt, revision, event, now); !graph.IsCode(err, graph.ErrInvalidInput) {
		t.Fatalf("event bound to another revision must be rejected: %v", err)
	}
}
