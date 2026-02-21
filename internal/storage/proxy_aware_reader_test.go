package storage

import (
	"bytes"
	"io"
	"testing"
)

func TestNewProxyAwareReader(t *testing.T) {
	r := bytes.NewReader([]byte("data"))
	par := NewProxyAwareReader(r, 4)
	if par == nil {
		t.Fatal("NewProxyAwareReader returned nil")
	}
	br, total, _ := par.Progress()
	if br != 0 || total != 4 {
		t.Errorf("Progress() = %d, %d; want 0, 4", br, total)
	}
}

func TestProxyAwareReader_ReadAndProgress(t *testing.T) {
	data := []byte("hello")
	r := bytes.NewReader(data)
	par := NewProxyAwareReader(r, int64(len(data)))
	buf := make([]byte, 10)
	n, err := par.Read(buf)
	if err != nil && err != io.EOF {
		t.Fatalf("Read: %v", err)
	}
	if n != 5 {
		t.Errorf("Read n = %d, want 5", n)
	}
	br, total, _ := par.Progress()
	if br != 5 || total != 5 {
		t.Errorf("Progress() = %d, %d; want 5, 5", br, total)
	}
}
