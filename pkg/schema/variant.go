// Copyright (c) 2026 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package schema

import "sync"

const (
	// Variant metadata field names
	VariantTagName   = "vtag"
	VariantIndexName = "vidx"
)

var variantWriterPool = sync.Pool{}

// VariantWriter appends a nested value struct. It is created
// with Writer.WriteVariant and must be released with Close.
type VariantWriter struct {
	*Writer         // the main writer
	tag     uint8   // variant type id
	elem    *Schema // list element schema
	ofs     int     // list start offset in writer buffer
	align   int     // field alignment in top schema (reset point for each elem)
}

func newVariantWriter(w *Writer, s *Schema, tag uint8) *VariantWriter {
	var vw *VariantWriter
	if iw := variantWriterPool.Get(); iw != nil {
		vw = iw.(*VariantWriter)
	} else {
		vw = &VariantWriter{}
	}

	// write variant size, will patch correct size on close
	ofs := w.buf.Len()
	w.writeLen(1)

	// write variant tag (type id)
	w.buf.WriteByte(tag)

	vw.Writer = w
	vw.tag = tag
	vw.elem = s
	vw.ofs = ofs
	vw.align = w.n

	return vw
}

func (w *VariantWriter) Schema() *Schema {
	return w.elem
}

func (w *VariantWriter) CaseId() uint8 {
	return w.tag
}

// Done returns true when a nested type is fully written.
func (w *VariantWriter) Done() bool {
	return w.n >= w.align+len(w.elem.Fields)
}

// Reset resets the write buffer to a state before writing
// started, i.e. it clears all data written by this VariantWriter
// so far, but preserves the header length bytes. Use Reset
// to undo all writes to a variant.
func (w *VariantWriter) Reset() {
	w.buf.Truncate(w.ofs + 4)
}

// Close finalizes a variant. It writes the length of the encoded
// data as a prefix back to the writer buffer and resets the
// main writer's write position. It is important to call Close.
func (w *VariantWriter) Close() {
	// reset when not complete
	if !w.Done() {
		w.Reset()
	}

	// patch data length in bytes
	buf := w.buf
	n := buf.Len()
	w.layout.PutUint32(buf.Bytes()[w.ofs:], uint32(n-w.ofs-4))

	// advance writer field offset past the nested type
	w.n = w.align + len(w.elem.Fields)

	// skip additional variants
	for w.n < len(w.schema.Fields) && w.schema.Fields[w.n].CaseId > 0 {
		w.n++
	}

	// clear and reuse
	w.Writer = nil
	w.tag = 0
	w.elem = nil
	w.ofs = 0
	w.align = 0
	variantWriterPool.Put(w)
}
