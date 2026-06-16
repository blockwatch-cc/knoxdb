// Copyright (c) 2026 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package schema

import (
	"cmp"
	"slices"
	"sync"
	"time"
)

const (
	// default map field names
	EntriesName = "entries"
	KeyName     = "key"
	ValueName   = "value"
)

var mapWriterPool = sync.Pool{}

// MapWriter appends entries to a nested map. It is created
// with Writer.WriteMap and must be released with Close. The
// first WriteXX call to a MapWriter must write the key for
// a map entry, subsequent calls write value fields.
type MapWriter struct {
	*Writer         // the main writer
	entries *Schema // map schema (single key field, one or muliple value fields)
	ofs     int     // map start offset in writer buffer
	align   int     // field alignment in top schema (reset point for each entry)
}

func newMapWriter(w *Writer, s *Schema) *MapWriter {
	var mw *MapWriter
	if iw := mapWriterPool.Get(); iw != nil {
		mw = iw.(*MapWriter)
	} else {
		mw = &MapWriter{}
	}

	// write map size, will patch correct size on close
	ofs := w.buf.Len()
	w.writeLen(0)

	mw.Writer = w
	mw.entries = s
	mw.ofs = ofs
	mw.align = w.n

	return mw
}

func (w *MapWriter) KeyType() *Field {
	return w.entries.Fields[0]
}

func (w *MapWriter) Schema() *Schema {
	return w.entries
}

// Done returns true when a map element is fully written.
func (w *MapWriter) Done() bool {
	return w.n >= w.align+len(w.entries.Fields)
}

// Next prepares the map writer to accept the next map entry.
// It is important to call Next after each map entry is done.
// Failing to do so will result in undefined behavior and invalid
// buffer contents. When Next is called early, before all value
// fields were written and after the key was written, it skips
// the remaining value fields writing zeros.
func (w *MapWriter) Next() {
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

// Reset resets the write buffer to a state before map writing
// started, i.e. it clears all data written by this MapWriter
// so far, but preserves the map header length bytes. Use Reset
// to undo all writes to a map.
func (w *MapWriter) Reset() {
	w.buf.Truncate(w.ofs + 4)
}

// Close finalizes a map. It writes the length of the encoded
// data as a prefix back to the writer buffer and resets the
// main writer's write position. It is important to call Close,
// even on empty maps when no data has been written.
func (w *MapWriter) Close() {
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
		w.n = w.align + len(w.entries.Fields)
	}

	// clear and reuse
	w.Writer = nil
	w.entries = nil
	w.ofs = 0
	w.align = 0
	mapWriterPool.Put(w)
}

// WriteMap is a generic helper that writes a map in sorted key order
// to w. Key types are implicitly limited to ordered types like integers,
// floats and string which is a subset of permitted Map key types.
// Values can be any supported primitive type.
//
// Performance note: Go hash maps are super inefficient to work with
// because they almost always make copies of keys and values. It is
// often better to use lists of key-value pairs and write them directly
// to a MapWriter (optionally inside MarshalSchema). Additionally these
// generic helpers use any-type Write calls which allocate interfaces
// for each key and value.
func WriteMap[K cmp.Ordered, V MapValueTypes](mw *MapWriter, m map[K]V) error {
	// sort keys
	keys := make([]K, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)

	// walk map sorted
	for _, k := range keys {
		if err := mw.Write(k); err != nil {
			return err
		}
		if err := mw.Write(m[k]); err != nil {
			return err
		}
		mw.Next()
	}
	return nil
}

// WriteTimeMap is a generic helper that writes a map using time-based
// keys in sorted order to w. Values can be any supported primitive type.
// See performance note above.
func WriteTimeMap[K time.Time, V MapValueTypes](mw *MapWriter, m map[K]V) error {
	// sort keys
	keys := make([]K, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.SortFunc(keys, CompareTime)

	// walk map sorted
	for _, k := range keys {
		if err := mw.Write(k); err != nil {
			return err
		}
		if err := mw.Write(m[k]); err != nil {
			return err
		}
		mw.Next()
	}
	return nil
}

// MarshalMap is a generic helper that writes a map in sorted key order
// to w. Key types are implicitly limited to ordered types like integers,
// floats and string which is a subset of permitted Map key types.
// Values must implement the Marshaler interface.
// See performance note above.
func MarshalMap[K cmp.Ordered, V Marshaler](mw *MapWriter, m map[K]V) error {
	// sort keys
	keys := make([]K, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)

	// walk map sorted
	for _, k := range keys {
		if err := mw.Write(k); err != nil {
			return err
		}
		if err := mw.Write(m[k]); err != nil {
			return err
		}
		mw.Next()
	}
	return nil
}

// MarshalTimeMap is a generic helper that writes a map using time-based
// keys in sorted order to w. Values must implement the Marshaler interface.
// See performance note above.
func MarshalTimeMap[K time.Time, V Marshaler](mw *MapWriter, m map[K]V) error {
	// sort keys
	keys := make([]K, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.SortFunc(keys, CompareTime)

	// walk map sorted
	for _, k := range keys {
		if err := mw.Write(&k); err != nil {
			return err
		}
		if err := mw.Write(m[k]); err != nil {
			return err
		}
		mw.Next()
	}
	return nil
}
