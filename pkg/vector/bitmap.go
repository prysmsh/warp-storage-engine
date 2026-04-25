//go:build vector

package vector

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/RoaringBitmap/roaring"
)

// BitmapIndex maintains roaring bitmaps per field+value for fast filtering.
type BitmapIndex struct {
	bitmaps map[string]map[string]*roaring.Bitmap
	allIDs  *roaring.Bitmap
}

// NewBitmapIndex creates an empty bitmap index.
func NewBitmapIndex() *BitmapIndex {
	return &BitmapIndex{
		bitmaps: make(map[string]map[string]*roaring.Bitmap),
		allIDs:  roaring.New(),
	}
}

// Add registers that the given point has the given field=value.
func (b *BitmapIndex) Add(id uint64, field, value string) {
	if _, ok := b.bitmaps[field]; !ok {
		b.bitmaps[field] = make(map[string]*roaring.Bitmap)
	}
	if _, ok := b.bitmaps[field][value]; !ok {
		b.bitmaps[field][value] = roaring.New()
	}
	b.bitmaps[field][value].Add(uint32(id))
	b.allIDs.Add(uint32(id))
}

// Get returns the bitmap of IDs matching field=value. Returns empty bitmap if not found.
func (b *BitmapIndex) Get(field, value string) *roaring.Bitmap {
	if vals, ok := b.bitmaps[field]; ok {
		if bm, ok := vals[value]; ok {
			return bm.Clone()
		}
	}
	return roaring.New()
}

// AllIDs returns a bitmap of all known point IDs.
func (b *BitmapIndex) AllIDs() *roaring.Bitmap {
	return b.allIDs.Clone()
}

// Serialize encodes the bitmap index to bytes.
func (b *BitmapIndex) Serialize() ([]byte, error) {
	var buf bytes.Buffer

	allBytes, err := b.allIDs.ToBytes()
	if err != nil {
		return nil, fmt.Errorf("serialize allIDs: %w", err)
	}
	binary.Write(&buf, binary.LittleEndian, uint32(len(allBytes)))
	buf.Write(allBytes)

	binary.Write(&buf, binary.LittleEndian, uint32(len(b.bitmaps)))

	for field, vals := range b.bitmaps {
		binary.Write(&buf, binary.LittleEndian, uint32(len(field)))
		buf.WriteString(field)
		binary.Write(&buf, binary.LittleEndian, uint32(len(vals)))

		for value, bm := range vals {
			binary.Write(&buf, binary.LittleEndian, uint32(len(value)))
			buf.WriteString(value)

			bmBytes, err := bm.ToBytes()
			if err != nil {
				return nil, fmt.Errorf("serialize bitmap %s=%s: %w", field, value, err)
			}
			binary.Write(&buf, binary.LittleEndian, uint32(len(bmBytes)))
			buf.Write(bmBytes)
		}
	}

	return buf.Bytes(), nil
}

// DeserializeBitmapIndex reconstructs a bitmap index from bytes.
func DeserializeBitmapIndex(data []byte) (*BitmapIndex, error) {
	r := bytes.NewReader(data)
	idx := &BitmapIndex{
		bitmaps: make(map[string]map[string]*roaring.Bitmap),
	}

	var allLen uint32
	if err := binary.Read(r, binary.LittleEndian, &allLen); err != nil {
		return nil, fmt.Errorf("read allIDs length: %w", err)
	}
	allBytes := make([]byte, allLen)
	if _, err := r.Read(allBytes); err != nil {
		return nil, fmt.Errorf("read allIDs: %w", err)
	}
	idx.allIDs = roaring.New()
	if _, err := idx.allIDs.FromBuffer(allBytes); err != nil {
		return nil, fmt.Errorf("deserialize allIDs: %w", err)
	}

	var fieldCount uint32
	if err := binary.Read(r, binary.LittleEndian, &fieldCount); err != nil {
		return nil, fmt.Errorf("read field count: %w", err)
	}

	for i := uint32(0); i < fieldCount; i++ {
		var nameLen uint32
		if err := binary.Read(r, binary.LittleEndian, &nameLen); err != nil {
			return nil, err
		}
		nameBytes := make([]byte, nameLen)
		if _, err := r.Read(nameBytes); err != nil {
			return nil, err
		}
		field := string(nameBytes)
		idx.bitmaps[field] = make(map[string]*roaring.Bitmap)

		var valCount uint32
		if err := binary.Read(r, binary.LittleEndian, &valCount); err != nil {
			return nil, err
		}

		for j := uint32(0); j < valCount; j++ {
			var vLen uint32
			if err := binary.Read(r, binary.LittleEndian, &vLen); err != nil {
				return nil, err
			}
			vBytes := make([]byte, vLen)
			if _, err := r.Read(vBytes); err != nil {
				return nil, err
			}
			value := string(vBytes)

			var bmLen uint32
			if err := binary.Read(r, binary.LittleEndian, &bmLen); err != nil {
				return nil, err
			}
			bmBytes := make([]byte, bmLen)
			if _, err := r.Read(bmBytes); err != nil {
				return nil, err
			}
			bm := roaring.New()
			if _, err := bm.FromBuffer(bmBytes); err != nil {
				return nil, fmt.Errorf("deserialize bitmap %s=%s: %w", field, value, err)
			}
			idx.bitmaps[field][value] = bm
		}
	}

	return idx, nil
}
