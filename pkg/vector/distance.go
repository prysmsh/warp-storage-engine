//go:build vector

package vector

import (
	"fmt"
	"math"
)

func CosineDistance(a, b []float32) float32 {
	var dot, normA, normB float32
	for i := range a {
		dot += a[i] * b[i]
		normA += a[i] * a[i]
		normB += b[i] * b[i]
	}
	denom := float32(math.Sqrt(float64(normA)) * math.Sqrt(float64(normB)))
	if denom == 0 {
		return 1
	}
	return 1 - dot/denom
}

func L2Distance(a, b []float32) float32 {
	var sum float32
	for i := range a {
		d := a[i] - b[i]
		sum += d * d
	}
	return sum
}

func InnerProductDistance(a, b []float32) float32 {
	var dot float32
	for i := range a {
		dot += a[i] * b[i]
	}
	return -dot
}

func DistanceFuncFor(metric DistanceMetric) (DistanceFunc, error) {
	switch metric {
	case Cosine:
		return CosineDistance, nil
	case L2:
		return L2Distance, nil
	case InnerProduct:
		return InnerProductDistance, nil
	default:
		return nil, fmt.Errorf("unsupported distance metric: %s", metric)
	}
}
