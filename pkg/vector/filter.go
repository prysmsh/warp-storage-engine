//go:build vector

package vector

import (
	"fmt"

	"github.com/RoaringBitmap/roaring"
)

// EvalFilter evaluates a Filter against a BitmapIndex, returning matching point IDs.
func EvalFilter(f Filter, idx *BitmapIndex) *roaring.Bitmap {
	if len(f.And) > 0 {
		result := EvalFilter(f.And[0], idx)
		for _, sub := range f.And[1:] {
			result.And(EvalFilter(sub, idx))
		}
		return result
	}

	if len(f.Or) > 0 {
		result := roaring.New()
		for _, sub := range f.Or {
			result.Or(EvalFilter(sub, idx))
		}
		return result
	}

	if f.Not != nil {
		inner := EvalFilter(*f.Not, idx)
		all := idx.AllIDs()
		all.AndNot(inner)
		return all
	}

	return evalLeaf(f, idx)
}

func evalLeaf(f Filter, idx *BitmapIndex) *roaring.Bitmap {
	switch f.Op {
	case OpEq:
		return idx.Get(f.Field, fmt.Sprint(f.Value))

	case OpNeq:
		eq := idx.Get(f.Field, fmt.Sprint(f.Value))
		all := idx.AllIDs()
		all.AndNot(eq)
		return all

	case OpIn:
		result := roaring.New()
		if vals, ok := f.Value.([]any); ok {
			for _, v := range vals {
				result.Or(idx.Get(f.Field, fmt.Sprint(v)))
			}
		}
		return result

	case OpContains:
		return idx.Get(f.Field, fmt.Sprint(f.Value))

	default:
		return roaring.New()
	}
}
