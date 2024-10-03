package wal

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestSegmentCreate(t *testing.T) {
	opts := WalOptions{
		MaxSegmentSize: 100,
		Path:           t.TempDir(),
	}
	s, err := createSegment(LSN(0), opts)
	if err != nil {
		t.Errorf("failed to create segment: %v", err)
	}
	defer s.Close()
	f, err := os.Stat(filepath.Join(opts.Path, generateFilename(0)))
	if err != nil {
		t.Errorf("segment file was not created: %v", err)
	}
	if f.IsDir() {
		t.Errorf("segment should be a file")
	}
	if sz := f.Size(); sz != 0 {
		t.Errorf("segment file is zero: %d ", sz)
	}
}

func TestSegmentOpen(t *testing.T) {
	opts := WalOptions{
		MaxSegmentSize: 100,
		Path:           t.TempDir(),
	}
	lsn := LSN(0)
	s, err := createSegment(lsn, opts)
	if err != nil {
		t.Errorf("failed to create segment: %v", err)
	}
	err = s.Close()
	if err != nil {
		t.Errorf("failed to close segment: %v", err)
	}
	_, err = os.Stat(filepath.Join(opts.Path, generateFilename(0)))
	if err != nil {
		t.Errorf("segment file was not created: %v", err)
	}
	s, err = openSegment(lsn, opts)
	if err != nil {
		t.Errorf("failed to open segment: %v", err)
	}
	err = s.Close()
	if err != nil {
		t.Errorf("failed to close segment: %v", err)
	}
}

func TestSegmentWrite(t *testing.T) {
	opts := WalOptions{
		MaxSegmentSize: 100,
		Path:           t.TempDir(),
	}
	s, err := createSegment(LSN(0), opts)
	if err != nil {
		t.Errorf("failed to create segment: %v", err)
	}
	defer s.Close()
	n, err := s.Write([]byte("data"))
	if err != nil {
		t.Errorf("failed to write to segment")
	}
	f, err := os.Stat(filepath.Join(opts.Path, generateFilename(0)))
	if err != nil {
		t.Errorf("segment file was not created: %v", err)
	}
	if sz := f.Size(); sz != int64(n) {
		t.Errorf("segment file is zero: %d ", sz)
	}
}

func TestSegmentWriteClose(t *testing.T) {
	opts := WalOptions{
		MaxSegmentSize: 100,
		Path:           t.TempDir(),
	}
	s, err := createSegment(LSN(0), opts)
	if err != nil {
		t.Errorf("failed to create segment: %v", err)
	}
	err = s.Close()
	if err != nil {
		t.Errorf("failed to close segment: %v", err)
	}
	_, err = s.Write([]byte("data"))
	if !errors.Is(err, ErrClosed) {
		t.Errorf("should have failed to write to segment: %v", err)
	}
}

func TestSegmentSeek(t *testing.T) {
	opts := WalOptions{
		MaxSegmentSize: 100,
		Path:           t.TempDir(),
	}
	s, err := createSegment(LSN(0), opts)
	if err != nil {
		t.Errorf("failed to create segment: %v", err)
	}
	defer s.Close()
	dataToWrite := []byte("data")
	_, err = s.Write(dataToWrite)
	if err != nil {
		t.Errorf("failed to write to segment")
	}
	data := make([]byte, len(dataToWrite))
	_, err = s.fd.Read(data)
	if !errors.Is(err, io.EOF) {
		t.Errorf("read to segment file should have failed: %v", err)
	}
	_, err = s.Seek(0, 0)
	if err != nil {
		t.Errorf("failed to seek to segment: %v", err)
	}
	_, err = s.fd.Read(data)
	if err != nil {
		t.Errorf("failed to read to segment file: %v", err)
	}
	if !bytes.Equal(dataToWrite, data) {
		t.Errorf("data read is not equal to data written")
	}
}

func TestSegmentSeekClose(t *testing.T) {
	opts := WalOptions{
		MaxSegmentSize: 100,
		Path:           t.TempDir(),
	}
	s, err := createSegment(LSN(0), opts)
	if err != nil {
		t.Errorf("failed to create segment: %v", err)
	}
	err = s.Close()
	if err != nil {
		t.Errorf("failed to close segment: %v", err)
	}
	_, err = s.Seek(2, 0)
	if !errors.Is(err, ErrClosed) {
		t.Errorf("should have failed to seek to segment: %v", err)
	}
}
