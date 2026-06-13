// Copyright (c) 2026 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package schema

import "sync"

const (
	// default list field names
	ElementName = "element"
)

var listWriterPool = sync.Pool{}

// ListWriter appends elements to a nested list. It is created
// with Writer.WriteList and must be released with Close.
type ListWriter struct {
	*Writer         // the main writer
	elem    *Schema // list element schema
	ofs     int     // list start offset in writer buffer
	align   int     // field alignment in top schema (reset point for each elem)
}

func newListWriter(w *Writer, s *Schema) *ListWriter {
	var lw *ListWriter
	if iw := listWriterPool.Get(); iw != nil {
		lw = iw.(*ListWriter)
	} else {
		lw = &ListWriter{}
	}

	// write list size, will patch correct size on close
	ofs := w.buf.Len()
	w.writeLen(0)

	lw.Writer = w
	lw.elem = s
	lw.ofs = ofs
	lw.align = w.n

	return lw
}

func (w *ListWriter) Schema() *Schema {
	return w.elem
}

// Done returns true when a list element is fully written.
func (w *ListWriter) Done() bool {
	return w.n == w.align+len(w.elem.Fields)
}

// Next prepares the list writer to accept the next element.
// It is important to call Next after each list element is done.
// Failing to do so will result in undefined behavior and invalid
// buffer contents. When Next is called early, before all element
// fields were written, it skips the remaining fields writing zeros.
func (w *ListWriter) Next() {
	// noop if we expect the first field
	if w.n == w.align {
		return
	}
	// fill remaining fields with zeros
	for !w.Done() {
		w.Skip()
	}
	w.n = w.align
}

// Reset resets the write buffer to a state before list writing
// started, i.e. it clears all data written by this ListWriter
// so far, but preserves the list header length bytes. Use Reset
// to undo all writes to a list.
func (w *ListWriter) Reset() {
	w.buf.Truncate(w.ofs + 4)
}

// Close finalizes a list. It writes the length of the encoded
// data as a prefix back to the writer buffer and resets the
// main writer's write position. It is important to call Close,
// even on empty lists when no data has been written.
func (w *ListWriter) Close() {
	// noop when there were no writes (empty list)
	if w.buf.Len() > w.ofs+4 {

		// fill remaining fields if writing stopped early but do not
		// append another list element if we're at the start (right
		// after a call to Next)
		if w.n > w.align && !w.Done() {
			w.Next()
		}

		// patch list data length in bytes
		buf := w.buf
		n := buf.Len()
		w.layout.PutUint32(buf.Bytes()[w.ofs:], uint32(n-w.ofs-4))

		// advance writer field offset past the nested type
		w.n = w.align + len(w.elem.Fields)
	}

	// clear and reuse
	w.Writer = nil
	w.elem = nil
	w.ofs = 0
	w.align = 0
	listWriterPool.Put(w)
}
