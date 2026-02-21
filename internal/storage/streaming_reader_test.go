package storage

import (
	"bytes"
	"io"
	"testing"
)

func TestNewStreamingReader(t *testing.T) {
	r := bytes.NewReader([]byte("hello"))
	sr := NewStreamingReader(r, 5)
	if sr == nil {
		t.Fatal("NewStreamingReader returned nil")
	}
}

func TestStreamingReader_Read(t *testing.T) {
	data := []byte("hello world")
	r := bytes.NewReader(data)
	sr := NewStreamingReader(r, int64(len(data)))
	buf := make([]byte, 20)
	n, err := sr.Read(buf)
	if err != nil && err != io.EOF {
		t.Fatalf("Read: %v", err)
	}
	if n != len(data) {
		t.Errorf("Read n = %d, want %d", n, len(data))
	}
	if string(buf[:n]) != string(data) {
		t.Errorf("Read data = %q", buf[:n])
	}
}

func TestStreamingReader_SeekWithinBuffer(t *testing.T) {
	data := []byte("0123456789")
	r := bytes.NewReader(data)
	sr := NewStreamingReader(r, int64(len(data)))
	// Read all to fill buffer
	_, _ = io.ReadAll(sr)
	pos, err := sr.Seek(2, io.SeekStart)
	if err != nil {
		t.Fatalf("Seek: %v", err)
	}
	if pos != 2 {
		t.Errorf("Seek position = %d, want 2", pos)
	}
	buf := make([]byte, 3)
	n, _ := sr.Read(buf)
	if n != 3 || string(buf) != "234" {
		t.Errorf("After Seek read = %q", buf[:n])
	}
}

func TestStreamingReader_Close(t *testing.T) {
	sr := NewStreamingReader(bytes.NewReader(nil), 0)
	err := sr.Close()
	if err != nil {
		t.Errorf("Close: %v", err)
	}
}
