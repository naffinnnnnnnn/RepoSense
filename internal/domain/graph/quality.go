package graph

import (
	"fmt"
	"math"
)

type QualityPolicy struct {
	MaxInvalidArtifacts     int     `json:"max_invalid_artifacts"`
	MaxInvalidArtifactRatio float64 `json:"max_invalid_artifact_ratio"`
	MaxInvalidRelations     int     `json:"max_invalid_relations"`
	MaxInvalidRelationRatio float64 `json:"max_invalid_relation_ratio"`
}

func (p QualityPolicy) Validate() error {
	if p.MaxInvalidArtifacts < 0 || p.MaxInvalidRelations < 0 || math.IsNaN(p.MaxInvalidArtifactRatio) || math.IsInf(p.MaxInvalidArtifactRatio, 0) || p.MaxInvalidArtifactRatio < 0 || p.MaxInvalidArtifactRatio > 1 || math.IsNaN(p.MaxInvalidRelationRatio) || math.IsInf(p.MaxInvalidRelationRatio, 0) || p.MaxInvalidRelationRatio < 0 || p.MaxInvalidRelationRatio > 1 {
		return fmt.Errorf("quality count thresholds must be non-negative and ratios must be finite values between 0 and 1")
	}
	return nil
}

type QualityStats struct {
	InputArtifacts   int64            `json:"input_artifacts"`
	WrittenArtifacts int64            `json:"written_artifacts"`
	InvalidArtifacts int64            `json:"invalid_artifacts"`
	InputRelations   int64            `json:"input_relations"`
	WrittenRelations int64            `json:"written_relations"`
	InvalidRelations int64            `json:"invalid_relations"`
	ResolutionCounts map[string]int64 `json:"resolution_counts,omitempty"`
	ErrorCounts      map[string]int64 `json:"error_counts,omitempty"`
	ErrorSamples     []QualitySample  `json:"error_samples,omitempty"`
}

type QualitySample struct {
	RecordType string `json:"record_type"`
	RecordID   string `json:"record_id"`
	ErrorCode  string `json:"error_code"`
}

func (s QualityStats) Validate() error {
	if s.InputArtifacts < 0 || s.WrittenArtifacts < 0 || s.InvalidArtifacts < 0 || s.InputRelations < 0 || s.WrittenRelations < 0 || s.InvalidRelations < 0 {
		return fmt.Errorf("quality statistics must not be negative")
	}
	if s.WrittenArtifacts+s.InvalidArtifacts != s.InputArtifacts || s.WrittenRelations+s.InvalidRelations != s.InputRelations {
		return fmt.Errorf("quality input counts must equal written plus invalid counts")
	}
	return nil
}

func EvaluateQuality(policy QualityPolicy, stats QualityStats) (QualityStatus, error) {
	if err := policy.Validate(); err != nil {
		return "", err
	}
	if err := stats.Validate(); err != nil {
		return "", err
	}
	artifactRatio := invalidRatio(stats.InvalidArtifacts, stats.InputArtifacts)
	relationRatio := invalidRatio(stats.InvalidRelations, stats.InputRelations)
	if stats.InvalidArtifacts > int64(policy.MaxInvalidArtifacts) || artifactRatio > policy.MaxInvalidArtifactRatio || stats.InvalidRelations > int64(policy.MaxInvalidRelations) || relationRatio > policy.MaxInvalidRelationRatio {
		return "", &DomainError{Code: ErrGraphValidationFailed, Operation: "quality", Stage: "quality", Message: "parser data quality exceeds build policy", Retryable: false}
	}
	if stats.InvalidArtifacts > 0 || stats.InvalidRelations > 0 {
		return QualityDegraded, nil
	}
	return QualityHealthy, nil
}

func invalidRatio(invalid, total int64) float64 {
	if total == 0 {
		return 0
	}
	return float64(invalid) / float64(total)
}
