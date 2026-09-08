package graph

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/reposense/reposense/internal/domain/common"
)

type FingerprintInput struct {
	Scope     common.Scope  `json:"scope"`
	CommitSHA string        `json:"commit_sha"`
	Versions  BuildVersions `json:"versions"`
}

func RequestFingerprint(input FingerprintInput) (string, error) {
	if err := input.Scope.Validate(true); err != nil {
		return "", err
	}
	if hasSurroundingSpace(input.Scope.TenantID) || hasSurroundingSpace(input.Scope.RepositoryID) || hasSurroundingSpace(input.Scope.SnapshotID) || strings.TrimSpace(input.CommitSHA) == "" {
		return "", fmt.Errorf("fingerprint identity fields are invalid")
	}
	if err := input.Versions.Validate(); err != nil {
		return "", err
	}
	// Trace correlation is operational metadata, not logical build identity.
	input.Scope.TraceID = ""
	data, err := json.Marshal(input)
	if err != nil {
		return "", fmt.Errorf("encode fingerprint input: %w", err)
	}
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:]), nil
}
