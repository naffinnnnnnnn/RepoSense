package graphapp

import (
	"context"
	"encoding/json"
	"math"
	"strings"

	"github.com/reposense/reposense/internal/domain/graph"
	"github.com/reposense/reposense/internal/domain/repository"
)

type workerPageConsumer struct {
	worker   *Worker
	guard    *leaseGuard
	job      graph.BuildJob
	attempt  graph.BuildAttempt
	metadata graph.SnapshotMetadata
	quality  graph.QualityStats
	bytes    int64

	invalidArtifactIDs map[string]struct{}
}

func (c *workerPageConsumer) ConsumeArtifactPage(ctx context.Context, _ graph.SnapshotMetadata, artifacts []repository.CodeArtifact) error {
	c.quality.InputArtifacts += int64(len(artifacts))
	valid := make([]repository.CodeArtifact, 0, len(artifacts))
	sizes := make([]int, 0, len(artifacts))
	for _, artifact := range artifacts {
		size, err := c.artifactSize(artifact)
		if err != nil {
			return err
		}
		softCode, hardErr := classifyArtifact(artifact)
		if hardErr != nil {
			return hardErr
		}
		if softCode != "" {
			c.quality.InvalidArtifacts++
			c.quality.ErrorCounts[softCode]++
			c.recordQualitySample("artifact", artifact.ArtifactID, softCode)
			c.invalidArtifactIDs[artifact.ArtifactID] = struct{}{}
			if exceedsQualityThreshold(c.quality.InvalidArtifacts, c.metadata.ArtifactCount, c.worker.config.QualityPolicy.MaxInvalidArtifacts, c.worker.config.QualityPolicy.MaxInvalidArtifactRatio) {
				return qualityThresholdError()
			}
			continue
		}
		valid = append(valid, artifact)
		sizes = append(sizes, size)
	}
	for start := 0; start < len(valid); {
		end := boundedBatchEnd(sizes, start, c.worker.config.BatchSize, c.worker.config.MaxBatchBytes)
		if err := c.guard.renew(ctx); err != nil {
			return err
		}
		batch := valid[start:end]
		if err := c.worker.retryBatch(ctx, func() error {
			return c.worker.dataStore.WriteArtifactBatch(ctx, c.job, c.attempt, batch)
		}); err != nil {
			return err
		}
		c.quality.WrittenArtifacts += int64(len(batch))
		start = end
	}
	return nil
}

func (c *workerPageConsumer) ConsumeRelationPage(ctx context.Context, _ graph.SnapshotMetadata, relations []graph.ResolvedRelation) error {
	c.quality.InputRelations += int64(len(relations))
	valid := make([]graph.ResolvedRelation, 0, len(relations))
	sizes := make([]int, 0, len(relations))
	for _, relation := range relations {
		size, err := c.relationSize(relation)
		if err != nil {
			return err
		}
		softCode, hardErr := c.classifyRelation(relation)
		if hardErr != nil {
			return hardErr
		}
		if softCode != "" {
			c.quality.InvalidRelations++
			c.quality.ErrorCounts[softCode]++
			c.recordQualitySample("relation", relation.RelationID, softCode)
			if exceedsQualityThreshold(c.quality.InvalidRelations, c.metadata.RelationCount, c.worker.config.QualityPolicy.MaxInvalidRelations, c.worker.config.QualityPolicy.MaxInvalidRelationRatio) {
				return qualityThresholdError()
			}
			continue
		}
		valid = append(valid, relation)
		sizes = append(sizes, size)
		c.quality.ResolutionCounts[string(relation.ResolutionStatus)]++
	}
	for start := 0; start < len(valid); {
		end := boundedBatchEnd(sizes, start, c.worker.config.BatchSize, c.worker.config.MaxBatchBytes)
		if err := c.guard.renew(ctx); err != nil {
			return err
		}
		batch := valid[start:end]
		if err := c.worker.retryBatch(ctx, func() error {
			return c.worker.dataStore.WriteRelationBatch(ctx, c.job, c.attempt, batch)
		}); err != nil {
			return err
		}
		c.quality.WrittenRelations += int64(len(batch))
		start = end
	}
	return nil
}

func (c *workerPageConsumer) recordQualitySample(recordType, recordID, errorCode string) {
	if len(c.quality.ErrorSamples) >= c.worker.config.MaxErrorSamples {
		return
	}
	c.quality.ErrorSamples = append(c.quality.ErrorSamples, graph.QualitySample{RecordType: recordType, RecordID: recordID, ErrorCode: errorCode})
}

func (c *workerPageConsumer) artifactSize(artifact repository.CodeArtifact) (int, error) {
	if len(artifact.Attributes) > c.worker.config.MaxProperties {
		return 0, capacityError("artifact property count exceeds graph build capacity")
	}
	return c.recordSize(artifact, c.worker.config.MaxArtifactBytes, "artifact")
}

func (c *workerPageConsumer) relationSize(relation graph.ResolvedRelation) (int, error) {
	if len(relation.AmbiguousCandidates) > c.worker.config.MaxCandidates {
		return 0, capacityError("relation candidate count exceeds graph build capacity")
	}
	// encoding/json rejects non-finite floats. Size the otherwise identical
	// record with a finite placeholder so confidence remains a quality-policy
	// error instead of being misclassified as an encoding failure.
	if math.IsNaN(relation.Confidence) || math.IsInf(relation.Confidence, 0) {
		relation.Confidence = 0
	}
	return c.recordSize(relation, c.worker.config.MaxRelationBytes, "relation")
}

func (c *workerPageConsumer) recordSize(value any, maximum int, kind string) (int, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return 0, parserDataError(kind+"_pages", "parser record cannot be encoded")
	}
	if len(encoded) > maximum {
		return 0, capacityError(kind + " record exceeds graph build capacity")
	}
	c.bytes += int64(len(encoded))
	if c.bytes > c.worker.config.MaxRevisionBytes {
		return 0, capacityError("parser snapshot bytes exceed graph build capacity")
	}
	return len(encoded), nil
}

func boundedBatchEnd(sizes []int, start, maxRecords, maxBytes int) int {
	end, bytes := start, 0
	for end < len(sizes) && end-start < maxRecords && bytes+sizes[end] <= maxBytes {
		bytes += sizes[end]
		end++
	}
	return end
}

func classifyArtifact(artifact repository.CodeArtifact) (string, error) {
	if strings.TrimSpace(artifact.ArtifactID) == "" {
		return "", parserDataError("artifact_pages", "parser artifact identity is empty")
	}
	if artifact.SourceRef.SymbolID != "" && artifact.SourceRef.SymbolID != artifact.ArtifactID {
		return "", parserDataError("artifact_pages", "parser artifact source identity is inconsistent")
	}
	if !validArtifactKind(artifact.Kind) {
		return "INVALID_ARTIFACT_TYPE", nil
	}
	if strings.TrimSpace(artifact.Name) == "" {
		return "INVALID_ARTIFACT_DATA", nil
	}
	if err := artifact.SourceRef.Validate(); err != nil {
		return "INVALID_ARTIFACT_SOURCE_REF", nil
	}
	return "", nil
}

func (c *workerPageConsumer) classifyRelation(relation graph.ResolvedRelation) (string, error) {
	if strings.TrimSpace(relation.RelationID) == "" || strings.TrimSpace(relation.FromArtifactID) == "" {
		return "", parserDataError("relation_pages", "parser relation identity is incomplete")
	}
	if relation.ResolutionStatus == graph.ResolutionResolved {
		if strings.TrimSpace(relation.TargetArtifactID) == "" || relation.ExternalIdentity != "" || len(relation.AmbiguousCandidates) != 0 {
			return "", parserDataError("relation_pages", "resolved relation target identity is invalid")
		}
	}
	if _, isolated := c.invalidArtifactIDs[relation.FromArtifactID]; isolated {
		return "RELATION_REFERENCES_INVALID_ARTIFACT", nil
	}
	if relation.ResolutionStatus == graph.ResolutionResolved {
		if _, isolated := c.invalidArtifactIDs[relation.TargetArtifactID]; isolated {
			return "RELATION_REFERENCES_INVALID_ARTIFACT", nil
		}
	}
	if relation.ResolutionStatus == graph.ResolutionAmbiguous {
		for _, candidateID := range relation.AmbiguousCandidates {
			if _, isolated := c.invalidArtifactIDs[candidateID]; isolated {
				return "RELATION_REFERENCES_INVALID_ARTIFACT", nil
			}
		}
	}
	if !validRelationKind(relation.Kind) {
		return "INVALID_RELATION_TYPE", nil
	}
	if math.IsNaN(relation.Confidence) || math.IsInf(relation.Confidence, 0) || relation.Confidence < 0 || relation.Confidence > 1 {
		return "INVALID_RELATION_CONFIDENCE", nil
	}
	if err := relation.Evidence.Validate(); err != nil {
		return "INVALID_RELATION_SOURCE_REF", nil
	}
	if err := relation.Validate(); err != nil {
		switch relation.ResolutionStatus {
		case graph.ResolutionExternal:
			return "INVALID_EXTERNAL_IDENTITY", nil
		case graph.ResolutionAmbiguous:
			return "INVALID_AMBIGUOUS_RELATION", nil
		case graph.ResolutionUnresolved:
			return "INVALID_UNRESOLVED_RELATION", nil
		default:
			return "INVALID_RESOLUTION_STATUS", nil
		}
	}
	return "", nil
}

func exceedsQualityThreshold(invalid, total int64, maxCount int, maxRatio float64) bool {
	if invalid > int64(maxCount) {
		return true
	}
	return total > 0 && float64(invalid)/float64(total) > maxRatio
}

func qualityThresholdError() error {
	return &graph.DomainError{Code: graph.ErrGraphValidationFailed, Operation: "graph_worker", Stage: "quality", Message: "parser data quality exceeds build policy", Retryable: false}
}

func capacityError(message string) error {
	return &graph.DomainError{Code: graph.ErrBuildCapacityExceeded, Operation: "graph_worker", Stage: "capacity", Message: message, Retryable: false}
}

func parserDataError(stage, message string) error {
	return &graph.DomainError{Code: graph.ErrParserResultInvalid, Operation: "graph_worker", Stage: stage, Dependency: "parser", Message: message, Retryable: false}
}
