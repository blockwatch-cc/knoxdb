// Copyright (c) 2018-2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package block

import (
	"bytes"
	"io"

	"github.com/golang/snappy"
	"github.com/pierrec/lz4"
)

type SnappyWriter struct {
	w    io.Writer
	buf  *bytes.Buffer
	data []byte
}

func NewSnappyWriter(w io.Writer) *SnappyWriter {
	return &SnappyWriter{
		w:    w,
		buf:  bytes.NewBuffer(make([]byte, 0, BlockSizeHint)),
		data: make([]byte, BlockSizeHint),
	}
}

func (s *SnappyWriter) Close() error {
	s.data = snappy.Encode(s.data[:cap(s.data)], s.buf.Bytes())
	_, err := s.w.Write(s.data)
	return err
}

func (s *SnappyWriter) Write(p []byte) (n int, err error) {
	return s.buf.Write(p)
}

func (s *SnappyWriter) Reset(w io.Writer) error {
	s.w = w
	s.data = s.data[:0]
	s.buf.Reset()
	return nil
}

type nopCloser struct {
	io.Writer
}

func (nopCloser) Close() error {
	return nil
}

func (n nopCloser) ReadFrom(r io.Reader) (int64, error) {
	return io.Copy(n.Writer, r)
}

// NopCloser returns a WriteCloser with a no-op Close method wrapping
// the provided Writer w.
func NopCloser(w io.Writer) io.WriteCloser {
	return nopCloser{w}
}

func getWriter(buf *bytes.Buffer, comp Compression) io.WriteCloser {
	switch comp {
	case SnappyCompression:
		enc := snappyWriterPool.Get().(*SnappyWriter)
		enc.Reset(buf)
		return enc
	case LZ4Compression:
		enc := lz4WriterPool.Get().(*lz4.Writer)
		enc.Reset(buf)
		// FIXME: on average this requires too much memory on decode
		// Note: for allocating sufficient space on decode we
		// we use the dest buffer's capacity as total size hint;
		// the buffer is allocated in `block.EncodeBody()` according
		// to the true storage size of the target data. This is either
		// the blockSizeHint (when actual size is smaller) or the true
		// stored size of the data as calculated by `*ArrayEncodedSize()`.
		enc.Header.Size = uint64(buf.Cap())
		enc.Header.BlockChecksum = true
		enc.Header.NoChecksum = true
		return enc
	default:
		return NopCloser(buf)
	}
}

func putWriter(w io.Writer, comp Compression) {
	switch comp {
	case SnappyCompression:
		snappyWriterPool.Put(w.(*SnappyWriter))
	case LZ4Compression:
		lz4WriterPool.Put(w.(*lz4.Writer))
	}
}
