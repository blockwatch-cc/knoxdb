// Copyright (c) 2026 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package schema

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"iter"
)

const (
	BatchHeaderSize   = 12
	BatchSizeMultiple = 8191
)

func batchSize(i int) int {
	return (i + BatchSizeMultiple) &^ BatchSizeMultiple
}

// BatchWriter is a Writer that can produce a compliant batch
// containing a list of records prefixed by schema version and hash.
type BatchWriter struct {
	*Writer
	n int
}

// NewBatchWriter creates a batch writer for schema s
// with pre-allocated space for up to n records. Users write
// records using regular Writer calls and must finish each
// record with Next(). The final result buffer can be obtained
// with Batch(). BatchWriter should be closed after use to reclaim
// resources.
func NewBatchWriter(s *Schema, n int) *BatchWriter {
	n = BatchHeaderSize + s.EstWireSize*n // make space for n records
	buf := make([]byte, 0, batchSize(n))  // round up to 8kB
	buf = LE.AppendUint32(buf, s.Version) // write schema version
	buf = LE.AppendUint64(buf, s.Hash)    // write schema hash
	return &BatchWriter{Writer: NewWriter(s, bytes.NewBuffer(buf))}
}

// Bytes returns the complete batch buffer including header
// and content. For type safety, Bytes calls Writer.Next()
// to finalize partial writes.
func (w *BatchWriter) Bytes() []byte {
	w.Next()
	return w.buf.Bytes()
}

// Batch returns the written content as batch. Note batch itself
// keeps a buffer with header stripped. Use Bytes to access the
// full encoded version including batch header.
func (w *BatchWriter) Batch() *Batch {
	buf := w.Bytes()
	c := len(buf)
	return &Batch{buf: buf[BatchHeaderSize:c:c], schema: w.schema, num: w.n}
}

// Next prepares the writer to append another record to the batch.
// When Next is called early, before all fields were written, it skips
// the remaining fields writing zeros.
func (w *BatchWriter) Next() {
	if w.Writer.n > 0 {
		w.n++
	}
	w.Writer.Next()
}

// Reset resets the writer buffer for reuse.
func (w *BatchWriter) Reset() {
	w.Writer.Reset()
	w.n = 0
}

// Resize ensures the writer buffer has space for up to n records
// for writes to be allocation free. Capacity will only grow. To
// reclaim allocated space and shrink memory usage close the writer
// and allocate a new writer.
func (w *BatchWriter) Resize(n int) {
	need := w.schema.EstWireSize*n + BatchHeaderSize
	if w.buf.Cap() < need {
		w.buf.Grow(need - w.buf.Cap())
	}
}

// Close closes the underlying writer and reclaims resources.
// It is not strictly necessary to call close, but it helps
// reduce allocations for high-performance applications.
func (w *BatchWriter) Close() {
	w.Writer.Close()
	w.Writer = nil
	w.n = 0
}

// Batch contains a type-safe list of records produced by a schema.
// A batch header encodes schema version and hash which is used
// to identify and validate the schema on read. A batch can only be
// produced safely by a BatchWriter and must be opened from a schema
// registry.
//
// Batch buffers have the form [u32 version] + [u64 hash] + [records ...].
// A batch strips the buffer header on load after schema is resolved
// successfully.
type Batch struct {
	buf    []byte  // content only, no header
	schema *Schema // verified schema
	view   *View   // created on demand
	num    int     // number of records, lazy init
}

func (s *Schema) WrapBatch(buf []byte) (*Batch, error) {
	return makeBatch(buf, s)
}

func (s *Schema) WriteBatchHeader(buf *bytes.Buffer) error {
	err := binary.Write(buf, LE, s.Version)
	if err == nil {
		err = binary.Write(buf, LE, s.Hash)
	}
	return err
}

func (s *Schema) AppendBatchHeader(buf []byte) []byte {
	return LE.AppendUint64(LE.AppendUint32(buf, s.Version), s.Hash)
}

func makeBatch(buf []byte, s *Schema) (*Batch, error) {
	if len(buf) < BatchHeaderSize {
		return nil, ErrShortBuffer
	}
	ver, hash := LE.Uint32(buf), LE.Uint64(buf[4:])
	if s.Hash != hash {
		return nil, fmt.Errorf("%s: invalid batch hash %016x", s.Label(), hash)
	}
	if s.Version != ver {
		return nil, fmt.Errorf("%s: invalid batch version %d", s.Label(), ver)
	}
	return &Batch{buf: buf[BatchHeaderSize:], schema: s}, nil
}

// Schema returns the schema that was used to encode records.
func (b *Batch) Schema() *Schema {
	return b.schema
}

// Version returns the schema version that was used to encode records.
func (b *Batch) Version() uint32 {
	return b.schema.Version
}

// Header returns the encoded batch header.
func (b *Batch) Header() []byte {
	var h [BatchHeaderSize]byte
	LE.PutUint32(h[:], b.schema.Version)
	LE.PutUint64(h[4:], b.schema.Hash)
	return h[:]
}

// Size returns the batch buffer's size in bytes excluding the batch header.
func (b *Batch) Size() int {
	return len(b.buf)
}

// Bytes returns the raw batch content buffer without batch header.
func (b *Batch) Bytes() []byte {
	return b.buf
}

// Len returns the number of records in the batch.
func (b *Batch) Len() int {
	if b.num == 0 && len(b.buf) > 0 {
		b.num = b.getView().Count(b.buf)
	}
	return b.num
}

func (b *Batch) TrimAt(pos int) {
	b.buf = b.buf[pos:]
	b.num = 0
}

func (b *Batch) getView() *View {
	if b.view == nil {
		b.view = NewView(b.schema)
	}
	return b.view
}

// Records returns a sequence that iterates through all records
// in the batch yielding a view into each record.
func (b *Batch) Records() iter.Seq2[int, *View] {
	return b.getView().All(b.buf)
}

// Split splits a batch at offset n, returning a shortened original
// batch of at most length n and a new batch with all remaining records.
// Ok is true when the split was successful or false when less than n
// records existed. Then remainder is nil. Both result batches share the
// same internal view and point to the original backing buffer. It is
// not safe to use them concurrently. Panics if n is less than 1.
func (b *Batch) Split(n int) (original *Batch, remainder *Batch, ok bool) {
	if n < 1 {
		panic("cannot be less than 1")
	}
	if b.num > 0 && b.num <= n {
		return b, nil, false
	}
	var ofs, i int
	buf := b.buf
	v := b.getView()
	for len(buf) > 0 && n > 0 {
		v.Reset(buf)
		buf = buf[v.Len():]
		ofs += v.Len()
		n--
		i++
	}
	v.Reset(nil)
	if n > 0 {
		b.num = i
		return b, nil, false
	}
	remainder = &Batch{buf: buf, schema: b.schema, view: b.view}
	b.buf = b.buf[:ofs]
	b.num = i
	return b, remainder, true
}

// Chunk splits a batch into sub-batches of up to n records. All but
// the last batch will have size n and no batch will be empty. If b
// is empty the batch sequence will be empty. Panics if n is less than 1.
// All sub-batches will share the internal view and source batch, i.e.
// chunking is allocation free. It is not safe to retain sub-batches or
// use them concurrently.
func (b *Batch) Chunk(n int) iter.Seq[*Batch] {
	if n < 1 {
		panic("cannot be less than 1")
	}
	return func(yield func(*Batch) bool) {
		if b.num > 0 && b.num <= n {
			yield(b)
			return
		}
		var (
			clone         = *b
			buf           = b.buf
			v             = b.getView()
			start, ofs, m int
		)
		for len(buf) > 0 {
			// find the next split point
			m = n
			for len(buf) > 0 && m > 0 {
				v.Reset(buf)
				buf = buf[v.Len():]
				ofs += v.Len()
				m--
			}

			// call yield with record slice
			b.buf = clone.buf[start:ofs:ofs]
			b.num = n - m
			if !yield(b) {
				break
			}
			start = ofs
		}

		// restore original batch
		*b = clone
		v.Reset(nil)
	}
}
