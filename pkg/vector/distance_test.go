//go:build vector

package vector

import (
	"math"
	"testing"
)

func TestCosineDistance_IdenticalVectors(t *testing.T) {
	a := []float32{1, 2, 3}
	d := CosineDistance(a, a)
	if d > 1e-6 {
		t.Fatalf("identical vectors should have distance ~0, got %f", d)
	}
}

func TestCosineDistance_OrthogonalVectors(t *testing.T) {
	a := []float32{1, 0, 0}
	b := []float32{0, 1, 0}
	d := CosineDistance(a, b)
	if math.Abs(float64(d)-1.0) > 1e-6 {
		t.Fatalf("orthogonal vectors should have distance ~1, got %f", d)
	}
}

func TestCosineDistance_OppositeVectors(t *testing.T) {
	a := []float32{1, 0}
	b := []float32{-1, 0}
	d := CosineDistance(a, b)
	if math.Abs(float64(d)-2.0) > 1e-6 {
		t.Fatalf("opposite vectors should have distance ~2, got %f", d)
	}
}

func TestL2Distance(t *testing.T) {
	a := []float32{0, 0, 0}
	b := []float32{3, 4, 0}
	d := L2Distance(a, b)
	if math.Abs(float64(d)-25.0) > 1e-6 {
		t.Fatalf("expected squared L2 distance 25, got %f", d)
	}
}

func TestInnerProductDistance(t *testing.T) {
	a := []float32{1, 2, 3}
	b := []float32{4, 5, 6}
	d := InnerProductDistance(a, b)
	if math.Abs(float64(d)-(-32.0)) > 1e-6 {
		t.Fatalf("expected inner product distance -32, got %f", d)
	}
}

func TestDistanceFuncFor(t *testing.T) {
	tests := []struct {
		metric DistanceMetric
		wantOk bool
	}{
		{Cosine, true},
		{L2, true},
		{InnerProduct, true},
		{DistanceMetric("unknown"), false},
	}
	for _, tt := range tests {
		fn, err := DistanceFuncFor(tt.metric)
		if tt.wantOk && err != nil {
			t.Fatalf("expected ok for %s, got error: %v", tt.metric, err)
		}
		if !tt.wantOk && err == nil {
			t.Fatalf("expected error for %s, got fn: %v", tt.metric, fn)
		}
	}
}
