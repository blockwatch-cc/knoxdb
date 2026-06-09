// Copyright (c) 2026 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package schema

import (
	"bytes"
	"cmp"
	"encoding/binary"
	"io"
	"math"
	"slices"
	"sync"
	"time"

	"blockwatch.cc/knoxdb/pkg/num"
	"blockwatch.cc/knoxdb/pkg/schema/enum"
)

// TODO
// - support generic io.Writer on Writer/ListWriter
// - detect if underlying is *bytes.Buffer, if yes, optimize
//   if not write to new buffer and move into io.Writer on close

// Marshaler is the interface implemented by an object that can
// write itself to a schema Writer. MarshalSchema encodes the
// receiver into the Writer schema in correct field order. All
// writes are direct copies / appends into the Writer buffer.
// Note when nested fields exist in the receiver's schema type
// MarshalSchema must write all of them.
type Marshaler interface {
	MarshalSchema(*Writer) error
}

// Unmarshaler is the interface implemented by an object that can
// unmarshal itself from a schema View. UnmarshalSchema decodes the
// receiver's fields from a View in correct schema field order.
// UnmarshalSchema must copy string and byte fields if it whishes
// to retain the data after the underlying view buffer is released.
// Note when nested fields exist in the receiver's schema type
// UnmarshalSchema must read all of them.
type Unmarshaler interface {
	UnmarshalSchema(*View) error
}

var writerPool = sync.Pool{}

// Writer constructs wire encoded messages from typed values.
// Callers must follow strict sequential field order and nesting
// as defined by the type schema.
type Writer struct {
	schema  *Schema          // target schema
	layout  binary.ByteOrder // int byte order (little endian)
	buf     *bytes.Buffer    // backing buffer
	n       int              // current field offset
	scratch [8]byte          // scratch buffer
}

func NewWriter(s *Schema, buf *bytes.Buffer) *Writer {
	return NewWriterLayout(s, binary.LittleEndian, buf)
}

func NewWriterLayout(s *Schema, layout binary.ByteOrder, buf *bytes.Buffer) *Writer {
	if buf == nil {
		buf = s.NewBuffer(1)
	}
	var w *Writer
	if iw := writerPool.Get(); iw != nil {
		w = iw.(*Writer)
	} else {
		w = &Writer{}
	}
	w.schema = s
	w.layout = layout
	w.buf = buf
	return w
}

func (w *Writer) Close() {
	*w = Writer{}
	writerPool.Put(w)
}

// Schema returns the writer schema at the current level
func (w *Writer) Schema() *Schema {
	return w.schema
}

// Done returns true when all nested fields have been written.
func (w *Writer) Done() bool {
	return w.n == len(w.schema.Fields)
}

// Reset resets the writer buffer for reuse.
func (w *Writer) Reset() {
	w.buf.Reset()
	w.n = 0
}

// Next prepares the writer to append another record to the same buffer.
// When Next is called early, before all fields were written, it skips
// the remaining fields writing zeros.
func (w *Writer) Next() {
	// noop if we expect the first field
	if w.n == 0 {
		return
	}
	// fill remaining fields with zeros
	for !w.Done() {
		w.Skip()
	}
	w.n = 0
}

func (w *Writer) next() {
	w.n++
}

// Bytes returns written bytes. Use in combination with Done
// to ensure a record is complete.
func (w *Writer) Bytes() []byte {
	return w.buf.Bytes()
}

// Len returns the current number of bytes written to the buffer.
func (w *Writer) Len() int {
	return w.buf.Len()
}

// Buffer returns internal buffer. Use in combination with custom
// type marshalers to append data to an existing buffer. Users
// must not call Reset.
func (w *Writer) Buffer() *bytes.Buffer {
	return w.buf
}

// Skip writes a zero value for the current field.
func (w *Writer) Skip(n ...int) error {
	c := 1
	if len(n) > 0 {
		c = n[0]
	}
	for range c {
		f, err := w.getFieldChecked(w.n, 0)
		if err != nil {
			return err
		}

		// write zero (consider array size can be up to 255 byte)
		var zero [32]byte
		for n := f.WireSize(); n > 0; n -= 32 {
			w.buf.Write(zero[:min(n, 32)])
		}

		// advance field offset
		w.n++
		if f.Child != nil {
			w.n += len(f.Child.Fields)
		}
	}
	return nil
}

// Write appends an encoded value to the buffer. Value type
// must match the next expected value in schema field order.
// If value implements the Marshaler interface, call this
// instead.
func (w *Writer) Write(val any) error {
	// redirect to marshaler if implemented
	if m, ok := val.(Marshaler); ok {
		return m.MarshalSchema(w)
	}

	// get the current field
	f, err := w.getFieldChecked(w.n, 0)
	if err != nil {
		return err
	}

	// use field writer (will check for correct type)
	err = f.WriteValue(w.buf, val, w.layout)
	if err != nil {
		return err
	}

	// advance to next field
	w.n++
	return nil
}

func (w *Writer) WriteTimestamp(tv time.Time) error {
	f, err := w.getFieldChecked(w.n, Timestamp)
	if err != nil {
		return err
	}
	w.writeU64(uint64(TimeScale(f.Scale).ToUnix(tv)))
	w.n++
	return nil
}

func (w *Writer) WriteDuration(d time.Duration) error {
	f, err := w.getFieldChecked(w.n, Duration)
	if err != nil {
		return err
	}
	w.writeU64(uint64(TimeScale(f.Scale).Int64(d)))
	w.n++
	return nil
}

func (w *Writer) WriteTime(tv time.Time) error {
	f, err := w.getFieldChecked(w.n, Time)
	if err != nil {
		return err
	}
	w.writeU64(uint64(TimeScale(f.Scale).ToUnix(tv)))
	w.n++
	return nil
}

func (w *Writer) WriteDate(tv time.Time) error {
	f, err := w.getFieldChecked(w.n, Date)
	if err != nil {
		return err
	}
	w.writeU64(uint64(TimeScale(f.Scale).ToUnix(tv)))
	w.n++
	return nil
}

func (w *Writer) WriteInt64(v int64) error {
	_, err := w.getFieldChecked(w.n, Int64)
	if err != nil {
		return err
	}
	w.writeU64(uint64(v))
	w.n++
	return nil
}

func (w *Writer) WriteInt32(v int32) error {
	_, err := w.getFieldChecked(w.n, Int32)
	if err != nil {
		return err
	}
	w.writeU32(uint32(v))
	w.n++
	return nil
}

func (w *Writer) WriteInt16(v int16) error {
	_, err := w.getFieldChecked(w.n, Int16)
	if err != nil {
		return err
	}
	w.writeU16(uint16(v))
	w.n++
	return nil
}

func (w *Writer) WriteInt8(v int8) error {
	_, err := w.getFieldChecked(w.n, Int8)
	if err != nil {
		return err
	}
	w.buf.WriteByte(uint8(v))
	w.n++
	return nil
}

func (w *Writer) WriteUint64(v uint64) error {
	_, err := w.getFieldChecked(w.n, Uint64)
	if err != nil {
		return err
	}
	w.writeU64(v)
	w.n++
	return nil
}

func (w *Writer) WriteUint32(v uint32) error {
	_, err := w.getFieldChecked(w.n, Uint32)
	if err != nil {
		return err
	}
	w.writeU32(v)
	w.n++
	return nil
}

func (w *Writer) WriteUint16(v uint16) error {
	_, err := w.getFieldChecked(w.n, Uint16)
	if err != nil {
		return err
	}
	w.writeU16(v)
	w.n++
	return nil
}

func (w *Writer) WriteUint8(v uint8) error {
	_, err := w.getFieldChecked(w.n, Uint8)
	if err != nil {
		return err
	}
	w.buf.WriteByte(v)
	w.n++
	return nil
}

func (w *Writer) WriteFloat64(v float64) error {
	_, err := w.getFieldChecked(w.n, Float64)
	if err != nil {
		return err
	}
	w.writeU64(math.Float64bits(v))
	w.n++
	return nil
}

func (w *Writer) WriteFloat32(v float32) error {
	_, err := w.getFieldChecked(w.n, Float32)
	if err != nil {
		return err
	}
	w.writeU32(math.Float32bits(v))
	w.n++
	return nil
}

func (w *Writer) WriteBool(v bool) error {
	_, err := w.getFieldChecked(w.n, Boolean)
	if err != nil {
		return err
	}
	if v {
		w.buf.WriteByte(1)
	} else {
		w.buf.WriteByte(0)
	}
	w.n++
	return nil
}

func (w *Writer) WriteEnum(s string) error {
	f, err := w.getFieldChecked(w.n, Uint16)
	if err != nil {
		return err
	}
	if !f.IsEnum() {
		return ErrInvalidField
	}
	if f.Enum == nil {
		return ErrEnumUndefined
	}
	val, ok := f.Enum.Code(s)
	if !ok {
		return enum.ErrEnumNoCode
	}
	w.writeU16(val)
	w.n++
	return nil
}

func (w *Writer) WriteString(s string) error {
	if len(s) > MAX_STRING {
		return ErrLongValue
	}
	f, err := w.getFieldChecked(w.n, String)
	if err != nil {
		return err
	}
	if f.IsArray() {
		if len(s) != int(f.Scale) {
			return ErrShortValue
		}
	} else {
		w.buf.WriteByte(byte(len(s)))
	}
	w.buf.WriteString(s)
	w.n++
	return nil
}

func (w *Writer) WriteText(s string) error {
	_, err := w.getFieldChecked(w.n, Text)
	if err != nil {
		return err
	}
	w.writeLen(len(s))
	w.buf.WriteString(s)
	w.n++
	return nil
}

func (w *Writer) WriteBytes(b []byte) error {
	if len(b) > MAX_BYTES {
		return ErrLongValue
	}
	f, err := w.getFieldChecked(w.n, Bytes)
	if err != nil {
		return err
	}
	if f.IsArray() {
		if len(b) != int(f.Scale) {
			return ErrShortValue
		}
	} else {
		w.buf.WriteByte(byte(len(b)))
	}
	w.buf.Write(b)
	w.n++
	return nil
}

func (w *Writer) WriteBinary(b []byte) error {
	_, err := w.getFieldChecked(w.n, Binary)
	if err != nil {
		return err
	}
	w.writeLen(len(b))
	w.buf.Write(b)
	w.n++
	return nil
}

func (w *Writer) WriteInt256(v num.Int256) error {
	_, err := w.getFieldChecked(w.n, Int256)
	if err != nil {
		return err
	}
	w.buf.Write(v.Bytes())
	w.n++
	return nil
}

func (w *Writer) WriteInt128(v num.Int128) error {
	_, err := w.getFieldChecked(w.n, Int128)
	if err != nil {
		return err
	}
	w.buf.Write(v.Bytes())
	w.n++
	return nil
}

func (w *Writer) WriteDecimal256(v num.Decimal256) error {
	_, err := w.getFieldChecked(w.n, Decimal256)
	if err != nil {
		return err
	}
	w.buf.Write(v.Int256().Bytes())
	w.n++
	return nil
}

func (w *Writer) WriteDecimal128(v num.Decimal128) error {
	_, err := w.getFieldChecked(w.n, Decimal128)
	if err != nil {
		return err
	}
	w.buf.Write(v.Int128().Bytes())
	w.n++
	return nil
}

func (w *Writer) WriteDecimal64(v num.Decimal64) error {
	_, err := w.getFieldChecked(w.n, Decimal64)
	if err != nil {
		return err
	}
	w.writeU64(uint64(v.Int64()))
	w.n++
	return nil
}

func (w *Writer) WriteDecimal32(v num.Decimal32) error {
	_, err := w.getFieldChecked(w.n, Decimal32)
	if err != nil {
		return err
	}
	w.writeU32(uint32(v.Int32()))
	w.n++
	return nil
}

func (w *Writer) WriteBigint(v num.Big) error {
	_, err := w.getFieldChecked(w.n, Bigint)
	if err != nil {
		return err
	}
	buf := v.Bytes()
	if len(buf) >= MAX_BYTES {
		return ErrLongValue
	}
	w.buf.WriteByte(byte(len(buf)))
	w.buf.Write(buf)
	w.n++
	return nil
}

// ListWriter returns a new list writer to append nested elements
// to the buffer. ListWriter must be closed before writing other fields.
func (w *Writer) ListWriter() (*ListWriter, error) {
	f, err := w.getFieldChecked(w.n, List)
	if err != nil {
		return nil, err
	}

	// advance field offset
	w.n++

	// open nested list writer
	return newListWriter(w, f.Child), nil
}

// MapWriter returns a new map writer to append nested key/value pairs
// to the buffer. MapWriter must be closed before writing other fields.
func (w *Writer) MapWriter() (*MapWriter, error) {
	f, err := w.getFieldChecked(w.n, Map)
	if err != nil {
		return nil, err
	}

	// advance field offset
	w.n++

	// open nested map writer
	return newMapWriter(w, f.Child), nil
}

// WriteMap is a generic helper that writes a map in sorted key order
// to w. Key types are implicitly limited to ordered types like integers,
// floats and string which is a subset of permitted Map key types.
// Values can be any supported primitive type.
//
// Performance note: Go hash maps are super inefficient to work with
// because they almost always make copies of keys and values. It is
// often better to use lists of key-value pairs and write them directly
// to a MapWriter (optionally inside MarshalSchema).
func WriteMap[K cmp.Ordered, V MapValueTypes](w *Writer, m map[K]V) error {
	mw, err := w.MapWriter()
	if err != nil {
		return err
	}

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
	mw.Close()
	return nil
}

// WriteTimeMap is a generic helper that writes a map using time-based
// keys in sorted order to w. Values can be any supported primitive type.
// See performance note above.
func WriteTimeMap[K time.Time, V MapValueTypes](w *Writer, m map[K]V) error {
	mw, err := w.MapWriter()
	if err != nil {
		return err
	}

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
	mw.Close()
	return nil
}

// MarshalMap is a generic helper that writes a map in sorted key order
// to w. Key types are implicitly limited to ordered types like integers,
// floats and string which is a subset of permitted Map key types.
// Values must implement the Marshaler interface.
// See performance note above.
func MarshalMap[K cmp.Ordered, V Marshaler](w *Writer, m map[K]V) error {
	mw, err := w.MapWriter()
	if err != nil {
		return err
	}

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
	mw.Close()
	return nil
}

// MarshalTimeMap is a generic helper that writes a map using time-based
// keys in sorted order to w. Values must implement the Marshaler interface.
// See performance note above.
func MarshalTimeMap[K time.Time, V Marshaler](w *Writer, m map[K]V) error {
	mw, err := w.MapWriter()
	if err != nil {
		return err
	}

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
	mw.Close()
	return nil
}

func (w *Writer) getFieldChecked(n int, ty FieldType) (*Field, error) {
	if n >= len(w.schema.Fields) {
		return nil, io.EOF
	}
	f := w.schema.Fields[n]
	if ty > 0 && f.Type != ty {
		return nil, ErrInvalidValueType
	}
	return f, nil
}

func (w *Writer) writeLen(v int) {
	w.layout.PutUint32(w.scratch[:4], uint32(v))
	w.buf.Write(w.scratch[:4])
}

func (w *Writer) writeU64(v uint64) {
	w.layout.PutUint64(w.scratch[:8], v)
	w.buf.Write(w.scratch[:8])
}

func (w *Writer) writeU32(v uint32) {
	w.layout.PutUint32(w.scratch[:4], v)
	w.buf.Write(w.scratch[:4])
}

func (w *Writer) writeU16(v uint16) {
	w.layout.PutUint16(w.scratch[:2], v)
	w.buf.Write(w.scratch[:2])
}

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

// Done returns true when a list element is fully written.
func (w *MapWriter) Done() bool {
	return w.n == w.align+len(w.entries.Fields)
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
