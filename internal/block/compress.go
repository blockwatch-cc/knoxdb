// Copyright (c) 2023-2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package block

import (
	"io"
	"runtime"

	"github.com/klauspost/compress/s2"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
)

var (
	snappyWriterPool = NewGenericPool(
		runtime.NumCPU(),
		func() any { return s2.NewWriter(nil, s2.WriterConcurrency(1)) },
	)
	snappyReaderPool = NewGenericPool(
		runtime.NumCPU(),
		func() any { return s2.NewReader(nil) },
	)
	lz4WriterPool = NewGenericPool(
		runtime.NumCPU(),
		func() any { return lz4.NewWriter(nil) },
	)
	lz4ReaderPool = NewGenericPool(
		runtime.NumCPU(),
		func() any { return lz4.NewReader(nil) },
	)
	zstdWriterPool = NewGenericPool(
		runtime.NumCPU(),
		func() any {
			w, _ := zstd.NewWriter(nil,
				zstd.WithEncoderConcurrency(1),
				zstd.WithEncoderCRC(true),
				zstd.WithEncoderLevel(zstd.SpeedDefault), // SpeedFastest
			)
			return w
		},
	)
	zstdReaderPool = NewGenericPool(
		runtime.NumCPU(),
		func() any {
			r, _ := zstd.NewReader(nil)
			return r
		},
	)
)

func NewCompressor(w io.Writer, c Compression) io.WriteCloser {
	switch c {
	case CompressSnappy:
		enc := snappyWriterPool.Get().(*s2.Writer)
		enc.Reset(w)
		return &pooledWriteCloser{pool: snappyWriterPool, w: enc}
	case CompressLZ4:
		enc := lz4WriterPool.Get().(*lz4.Writer)
		enc.Reset(w)
		return &pooledWriteCloser{pool: lz4WriterPool, w: enc}
	case CompressZstd:
		enc := zstdWriterPool.Get().(*zstd.Encoder)
		enc.Reset(w)
		return &pooledWriteCloser{pool: zstdWriterPool, w: enc}
	default:
		return nopWriteCloser{w}
	}
}

func NewDecompressor(r io.Reader, c Compression) io.ReadCloser {
	switch c {
	case CompressSnappy:
		dec := snappyReaderPool.Get().(*s2.Reader)
		dec.Reset(r)
		return &pooledReadCloser{pool: snappyReaderPool, r: dec}
	case CompressLZ4:
		dec := lz4ReaderPool.Get().(*lz4.Reader)
		dec.Reset(r)
		return &pooledReadCloser{pool: lz4WriterPool, r: dec}
	case CompressZstd:
		dec := zstdReaderPool.Get().(*zstd.Decoder)
		dec.Reset(r)
		return &pooledReadCloser{pool: zstdWriterPool, r: dec}
	default:
		return io.NopCloser(r)
	}
}

type pooledWriteCloser struct {
	pool *GenericPool
	w    io.WriteCloser
}

func (c *pooledWriteCloser) Close() error {
	err := c.w.Close()
	c.pool.Put(c.w)
	c.pool = nil
	c.w = nil
	return err
}

func (c pooledWriteCloser) Write(p []byte) (n int, err error) {
	return c.w.Write(p)
}

type pooledReadCloser struct {
	pool *GenericPool
	r    io.Reader
}

func (c *pooledReadCloser) Close() error {
	c.pool.Put(c.r)
	c.pool = nil
	c.r = nil
	return nil
}

func (c pooledReadCloser) Read(p []byte) (n int, err error) {
	return c.r.Read(p)
}

type nopWriteCloser struct {
	io.Writer
}

func (nopWriteCloser) Close() error {
	return nil
}
