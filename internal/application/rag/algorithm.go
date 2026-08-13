package ragapp

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"math"
	"strings"
	"unicode"

	"github.com/reposense/reposense/internal/domain/rag"
)

const DefaultVectorizerVersion = "feature-hash@1"

// HashVectorizer is an offline, deterministic semantic baseline. It is not a
// model substitute: its purpose is to keep the complete retrieval pipeline
// operational until a tenant-selected embedding provider is configured.
type HashVectorizer struct{ Dimensions int }

func (v HashVectorizer) Version() string { return DefaultVectorizerVersion }

func (v HashVectorizer) Vectorize(ctx context.Context, texts []string) ([][]float64, error) {
	dimensions := v.Dimensions
	if dimensions <= 0 {
		dimensions = 256
	}
	result := make([][]float64, len(texts))
	for i, text := range texts {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		vector := make([]float64, dimensions)
		features := tokenize(text)
		compact := []rune(strings.ToLower(strings.Join(features, "")))
		for n := 3; n <= 4; n++ {
			for j := 0; j+n <= len(compact); j++ {
				features = append(features, string(compact[j:j+n]))
			}
		}
		for _, feature := range features {
			hash := sha256.Sum256([]byte(feature))
			bucket := int(binary.LittleEndian.Uint64(hash[:8]) % uint64(dimensions))
			sign := 1.0
			if hash[8]&1 == 1 {
				sign = -1
			}
			vector[bucket] += sign
		}
		normalizeVector(vector)
		result[i] = vector
	}
	return result, nil
}

type IdentityReranker struct{}

func (IdentityReranker) Version() string { return "identity@1" }
func (IdentityReranker) Rerank(_ context.Context, _ string, hits []rag.Hit) ([]rag.Hit, error) {
	return append([]rag.Hit(nil), hits...), nil
}

func tokenize(text string) []string {
	var tokens []string
	var current []rune
	flush := func() {
		if len(current) > 0 {
			tokens = append(tokens, strings.ToLower(string(current)))
			current = current[:0]
		}
	}
	var previous rune
	for _, value := range text {
		if unicode.IsLetter(value) || unicode.IsDigit(value) || value == '_' {
			if len(current) > 0 && unicode.IsUpper(value) && (unicode.IsLower(previous) || unicode.IsDigit(previous)) {
				flush()
			}
			current = append(current, value)
			previous = value
			continue
		}
		flush()
		previous = 0
	}
	flush()
	return tokens
}

func normalizeVector(vector []float64) {
	var sum float64
	for _, value := range vector {
		sum += value * value
	}
	if sum == 0 {
		return
	}
	norm := math.Sqrt(sum)
	for i := range vector {
		vector[i] /= norm
	}
}

func cosine(left, right []float64) float64 {
	if len(left) == 0 || len(left) != len(right) {
		return 0
	}
	var value float64
	for i := range left {
		value += left[i] * right[i]
	}
	if value < 0 {
		return 0
	}
	return value
}
