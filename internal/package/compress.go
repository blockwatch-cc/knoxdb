// Copyright (c) 2023-2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package pack

import (
	"io"
	"runtime"

	"blockwatch.cc/knoxdb/pkg/schema"
	"blockwatch.cc/knoxdb/pkg/util"
	"github.com/klauspost/compress/s2"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4"
)

var (
	snappyWriterPool = util.NewGenericPool(
		runtime.NumCPU(),
		func() any { return s2.NewWriter(nil, s2.WriterConcurrency(1)) },
	)
	snappyReaderPool = util.NewGenericPool(
		runtime.NumCPU(),
		func() any { return s2.NewReader(nil) },
	)
	lz4WriterPool = util.NewGenericPool(
		runtime.NumCPU(),
		func() any { return lz4.NewWriter(nil) },
	)
	lz4ReaderPool = util.NewGenericPool(
		runtime.NumCPU(),
		func() any { return lz4.NewReader(nil) },
	)
	zstdWriterPool = util.NewGenericPool(
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
	zstdReaderPool = util.NewGenericPool(
		runtime.NumCPU(),
		func() any {
			r, _ := zstd.NewReader(nil)
			return r
		},
	)
)

func NewCompressor(w io.Writer, c schema.FieldCompression) io.WriteCloser {
	switch c {
	case schema.FieldCompressSnappy:
		enc := snappyWriterPool.Get().(*s2.Writer)
		enc.Reset(w)
		return pooledWriteCloser{pool: snappyWriterPool, w: enc}
	case schema.FieldCompressLZ4:
		enc := lz4WriterPool.Get().(*lz4.Writer)
		enc.Reset(w)
		return pooledWriteCloser{pool: lz4WriterPool, w: enc}
	case schema.FieldCompressZstd:
		enc := zstdWriterPool.Get().(*zstd.Encoder)
		enc.Reset(w)
		return pooledWriteCloser{pool: zstdWriterPool, w: enc}
	default:
		return nopWriteCloser{w}
	}
}

func NewDecompressor(r io.Reader, c schema.FieldCompression) io.ReadCloser {
	switch c {
	case schema.FieldCompressSnappy:
		dec := snappyReaderPool.Get().(*s2.Reader)
		dec.Reset(r)
		return pooledReadCloser{pool: snappyReaderPool, r: dec}
	case schema.FieldCompressLZ4:
		dec := lz4ReaderPool.Get().(*lz4.Reader)
		dec.Reset(r)
		return pooledReadCloser{pool: lz4WriterPool, r: dec}
	case schema.FieldCompressZstd:
		dec := zstdReaderPool.Get().(*zstd.Decoder)
		dec.Reset(r)
		return pooledReadCloser{pool: zstdWriterPool, r: dec}
	default:
		return io.NopCloser(r)
	}
}

type pooledWriteCloser struct {
	pool *util.GenericPool
	w    io.WriteCloser
}

func (c pooledWriteCloser) Close() error {
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
	pool *util.GenericPool
	r    io.Reader
}

func (c pooledReadCloser) Close() error {
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
