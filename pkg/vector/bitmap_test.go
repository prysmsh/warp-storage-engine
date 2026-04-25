//go:build vector

package vector

import (
	"testing"
)

func TestBitmapIndex_StringField(t *testing.T) {
	idx := NewBitmapIndex()
	idx.Add(1, "color", "red")
	idx.Add(2, "color", "blue")
	idx.Add(3, "color", "red")

	ids := idx.Get("color", "red")
	if !ids.Contains(1) || !ids.Contains(3) {
		t.Fatalf("expected IDs 1 and 3 for color=red, got %v", ids.ToArray())
	}
	if ids.Contains(2) {
		t.Fatalf("ID 2 should not match color=red")
	}
}

func TestBitmapIndex_Intersection(t *testing.T) {
	idx := NewBitmapIndex()
	idx.Add(1, "color", "red")
	idx.Add(1, "size", "large")
	idx.Add(2, "color", "red")
	idx.Add(2, "size", "small")
	idx.Add(3, "color", "blue")
	idx.Add(3, "size", "large")

	colorRed := idx.Get("color", "red")
	sizeLarge := idx.Get("size", "large")
	colorRed.And(sizeLarge)
	if colorRed.GetCardinality() != 1 || !colorRed.Contains(1) {
		t.Fatalf("expected only ID 1 for color=red AND size=large, got %v", colorRed.ToArray())
	}
}

func TestBitmapIndex_SerializeDeserialize(t *testing.T) {
	idx := NewBitmapIndex()
	idx.Add(1, "tag", "backend")
	idx.Add(2, "tag", "frontend")
	idx.Add(3, "tag", "backend")

	data, err := idx.Serialize()
	if err != nil {
		t.Fatalf("serialize: %v", err)
	}

	idx2, err := DeserializeBitmapIndex(data)
	if err != nil {
		t.Fatalf("deserialize: %v", err)
	}

	ids := idx2.Get("tag", "backend")
	if !ids.Contains(1) || !ids.Contains(3) || ids.Contains(2) {
		t.Fatalf("deserialized index mismatch, got %v", ids.ToArray())
	}
}

func TestBitmapIndex_AllIDs(t *testing.T) {
	idx := NewBitmapIndex()
	idx.Add(1, "a", "x")
	idx.Add(2, "b", "y")
	idx.Add(3, "a", "z")

	all := idx.AllIDs()
	if all.GetCardinality() != 3 {
		t.Fatalf("expected 3 IDs, got %d", all.GetCardinality())
	}
}
