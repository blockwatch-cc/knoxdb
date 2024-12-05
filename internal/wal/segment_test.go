// Copyright (c) 2024 Blockwatch Data Inc.
// Author: oliver@blockwatch.cc

package wal

import (
	"io"
	"os"
	"testing"

	"blockwatch.cc/knoxdb/internal/hash/xxhash"
	"github.com/echa/log"
	"github.com/stretchr/testify/require"
)

func makeWal(t *testing.T) *Wal {
	return &Wal{
		opts: WalOptions{
			MaxSegmentSize: SEG_FILE_MINSIZE,
			Path:           t.TempDir(),
		},
		hash: xxhash.New(),
		csum: 0,
		log:  log.Disabled,
	}
}

func TestSegmentCreate(t *testing.T) {
	w := makeWal(t)
	s, err := w.createSegment(42)
	require.NoError(t, err, "segment create")
	defer s.Close()
	require.Equal(t, 42, s.Id(), "segment id")
	_, err = os.Stat(w.segmentName(0))
	require.Error(t, err, "invalid segment file")
	f, err := os.Stat(w.segmentName(42))
	require.NoError(t, err, "missing segment file")
	require.False(t, f.IsDir(), "segment should be a file")
	require.Equal(t, int64(0), f.Size(), "file length not zero")
	err = s.Close()
	require.NoError(t, err, "segment close")
}

func TestSegmentOpen(t *testing.T) {
	w := makeWal(t)
	s, err := w.createSegment(0)
	require.NoError(t, err, "segment create")
	defer s.Close()
	err = s.Close()
	require.NoError(t, err, "segment close")
	_, err = os.Stat(w.segmentName(0))
	require.NoError(t, err, "missing segment file")
	s, err = w.openSegment(0, false)
	require.NoError(t, err, "segment reopen")
	err = s.Close()
	require.NoError(t, err, "segment close")
}

func TestSegmentDoubleClose(t *testing.T) {
	w := makeWal(t)
	s, err := w.createSegment(0)
	require.NoError(t, err, "segment create")
	defer s.Close()
	err = s.Close()
	require.NoError(t, err, "segment close")
	err = s.Close()
	require.NoError(t, err, "segment 2x close")
}

func TestSegmentWrite(t *testing.T) {
	w := makeWal(t)
	s, err := w.createSegment(0)
	require.NoError(t, err, "segment create")
	defer s.Close()
	n, err := s.Write([]byte("data"))
	require.NoError(t, err, "segment write")
	require.Equal(t, 4, n, "write size")
	err = s.Sync()
	require.NoError(t, err, "segment sync")
	f, err := os.Stat(w.segmentName(0))
	require.NoError(t, err, "missing segment file")
	require.Equal(t, f.Size(), int64(n), "file size")
	require.Equal(t, f.Size(), int64(s.Len()), "segment len")
	require.Equal(t, SEG_FILE_MINSIZE-4, s.Cap(), "segment cap")
	err = s.Close()
	require.NoError(t, err, "segment close")

	// write after close
	_, err = s.Write([]byte("more"))
	require.ErrorIs(t, err, ErrSegmentClosed)

	// re-open for read
	s, err = w.openSegment(0, false)
	require.NoError(t, err, "segment reopen")
	defer s.Close()
	var buf [5]byte
	n, err = s.Read(buf[:])
	require.NoError(t, err, "segment read")
	require.Equal(t, 4, n, "segment read len")
	require.Equal(t, []byte("data"), buf[:n], "segment read content")
	err = s.Close()
	require.NoError(t, err, "segment close")
}

func TestSegmentWriteReadOnly(t *testing.T) {
	w := makeWal(t)
	s, err := w.createSegment(0)
	require.NoError(t, err, "segment create")
	defer s.Close()
	n, err := s.Write([]byte("data"))
	require.NoError(t, err, "segment write")
	require.Equal(t, 4, n, "write size")
	err = s.Sync()
	require.NoError(t, err, "segment sync")
	err = s.Close()
	require.NoError(t, err, "segment close")

	// reopen for read
	s, err = w.openSegment(0, false)
	require.NoError(t, err, "segment reopen")
	defer s.Close()
	_, err = s.Write([]byte("more"))
	require.ErrorIs(t, err, ErrSegmentReadOnly)
}

func TestSegmentSeek(t *testing.T) {
	w := makeWal(t)
	s, err := w.createSegment(0)
	require.NoError(t, err, "segment create")
	defer s.Close()
	_, err = s.Write([]byte("data"))
	require.NoError(t, err, "segment write")

	// active segment is append only
	_, err = s.Seek(0, 0)
	require.ErrorIs(t, err, ErrSegmentAppendOnly)
	err = s.Close()
	require.NoError(t, err, "segment close")

	// reopen for read
	s, err = w.openSegment(0, false)
	require.NoError(t, err, "segment reopen")
	defer s.Close()
	_, err = s.Seek(2, 0)
	require.NoError(t, err, "segment seek 2")

	// seek middle
	var buf [5]byte
	n, err := s.Read(buf[:2])
	require.NoError(t, err, "segment read")
	require.Equal(t, 2, n, "segment read len")
	require.Equal(t, []byte("ta"), buf[:n], "segment read content")

	// seek end
	_, err = s.Seek(4, 0)
	require.NoError(t, err, "segment seek end")
	n, err = s.Read(buf[:])
	require.ErrorIs(t, io.EOF, err, "segment read EOF")
	require.Equal(t, 0, n, "segment read len")
	require.Equal(t, []byte{}, buf[:n], "segment read content")

	// seek beyond
	_, err = s.Seek(10, 0)
	require.NoError(t, err, "segment seek after end")

	// close
	err = s.Close()
	require.NoError(t, err, "segment close")

	// seek after close
	_, err = s.Seek(2, 0)
	require.ErrorIs(t, err, ErrSegmentClosed)
}
