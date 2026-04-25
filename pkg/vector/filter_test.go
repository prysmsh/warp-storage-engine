//go:build vector

package vector

import (
	"testing"
)

func TestEvalFilter_SimpleEq(t *testing.T) {
	idx := NewBitmapIndex()
	idx.Add(1, "status", "active")
	idx.Add(2, "status", "inactive")
	idx.Add(3, "status", "active")

	f := Filter{Field: "status", Op: OpEq, Value: "active"}
	result := EvalFilter(f, idx)
	if result.GetCardinality() != 2 || !result.Contains(1) || !result.Contains(3) {
		t.Fatalf("expected IDs 1,3 for status=active, got %v", result.ToArray())
	}
}

func TestEvalFilter_Neq(t *testing.T) {
	idx := NewBitmapIndex()
	idx.Add(1, "status", "active")
	idx.Add(2, "status", "inactive")
	idx.Add(3, "status", "active")

	f := Filter{Field: "status", Op: OpNeq, Value: "active"}
	result := EvalFilter(f, idx)
	if result.GetCardinality() != 1 || !result.Contains(2) {
		t.Fatalf("expected ID 2 for status!=active, got %v", result.ToArray())
	}
}

func TestEvalFilter_And(t *testing.T) {
	idx := NewBitmapIndex()
	idx.Add(1, "color", "red")
	idx.Add(1, "size", "large")
	idx.Add(2, "color", "red")
	idx.Add(2, "size", "small")

	f := Filter{
		And: []Filter{
			{Field: "color", Op: OpEq, Value: "red"},
			{Field: "size", Op: OpEq, Value: "large"},
		},
	}
	result := EvalFilter(f, idx)
	if result.GetCardinality() != 1 || !result.Contains(1) {
		t.Fatalf("expected ID 1, got %v", result.ToArray())
	}
}

func TestEvalFilter_Or(t *testing.T) {
	idx := NewBitmapIndex()
	idx.Add(1, "color", "red")
	idx.Add(2, "color", "blue")
	idx.Add(3, "color", "green")

	f := Filter{
		Or: []Filter{
			{Field: "color", Op: OpEq, Value: "red"},
			{Field: "color", Op: OpEq, Value: "blue"},
		},
	}
	result := EvalFilter(f, idx)
	if result.GetCardinality() != 2 || !result.Contains(1) || !result.Contains(2) {
		t.Fatalf("expected IDs 1,2, got %v", result.ToArray())
	}
}

func TestEvalFilter_Not(t *testing.T) {
	idx := NewBitmapIndex()
	idx.Add(1, "color", "red")
	idx.Add(2, "color", "blue")
	idx.Add(3, "color", "red")

	f := Filter{
		Not: &Filter{Field: "color", Op: OpEq, Value: "red"},
	}
	result := EvalFilter(f, idx)
	if result.GetCardinality() != 1 || !result.Contains(2) {
		t.Fatalf("expected ID 2 for NOT color=red, got %v", result.ToArray())
	}
}

func TestEvalFilter_In(t *testing.T) {
	idx := NewBitmapIndex()
	idx.Add(1, "color", "red")
	idx.Add(2, "color", "blue")
	idx.Add(3, "color", "green")

	f := Filter{Field: "color", Op: OpIn, Value: []any{"red", "green"}}
	result := EvalFilter(f, idx)
	if result.GetCardinality() != 2 || !result.Contains(1) || !result.Contains(3) {
		t.Fatalf("expected IDs 1,3, got %v", result.ToArray())
	}
}
