// Copyright (c) 2026 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package schema

import (
	"bytes"
	"encoding/binary"
	"io"
	"math"
	"sync"
	"time"

	"blockwatch.cc/knoxdb/pkg/num"
	"blockwatch.cc/knoxdb/pkg/schema/enum"
)

// TODO
// - field offset n calculations assume no change to nested fields

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
	UnmarshalSchema(Viewer) error
}

var writerPool = sync.Pool{}

// Writer constructs wire encoded messages from typed values.
// Callers must follow strict sequential field order and nesting
// as defined by the type schema.
type Writer struct {
	schema  *Schema          // target schema
	layout  binary.ByteOrder // int byte order (little endian)
	buf     *bytes.Buffer    // backing buffer
	head    int              // buffer header
	n       int              // current field offset
	scratch [8]byte          // scratch buffer
	err     error            // first captured write error
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
	w.head = buf.Len()
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

// Field returns the current field to be written by the next Write call.
func (w *Writer) Field() *Field {
	return w.schema.Fields[w.n]
}

// Done returns true when all nested fields have been written.
func (w *Writer) Done() bool {
	return w.n == 0 && w.buf.Len() >= w.schema.MinWireSize ||
		w.n >= len(w.schema.Fields)
}

// Err returns the first captured write error.
func (w *Writer) Err() error {
	return w.err
}

// Reset resets the writer buffer for reuse.
func (w *Writer) Reset() {
	w.buf.Truncate(w.head)
	w.n = 0
	w.err = nil
}

// Next prepares the writer to append another record to the same buffer.
// When Next is called early, before all fields were written, it skips
// remaining fields writing zeros/nulls.
func (w *Writer) Next() {
	// noop if no field was appended yet
	if w.n == 0 {
		return
	}
	// fill remaining fields with zeros
	for !w.Done() {
		w.AppendNull()
	}
	w.n = 0
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
// must not call Reset or Truncate.
func (w *Writer) Buffer() *bytes.Buffer {
	return w.buf
}

// Skip writes a zero values for up to n fields.
func (w *Writer) Skip(n int) error {
	for range n {
		if err := w.AppendNull(); err != nil {
			return err
		}
	}
	return nil
}

// AppendNull writes a zero value for the current field.
func (w *Writer) AppendNull() error {
	// get the current field
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

	return nil
}

// Append appends an encoded value to the buffer. Value type
// must match the next expected value in schema field order.
// If value implements the Marshaler interface, call this
// instead. Marshalers are expected to use either the public
// Writer API which advances field offsets or call consume(n)
// when writing to the writer buffer directly.
func (w *Writer) Append(val any) error {
	// get the current field
	f, err := w.getFieldChecked(w.n, 0)
	if err != nil {
		return err
	}

	if m, ok := val.(Marshaler); ok {
		// redirect to marshaler if implemented
		err = m.MarshalSchema(w)
	} else {
		// use field writer (will check for correct type)
		err = f.AppendValue(w.buf, val, w.layout)
	}
	if err != nil {
		return w.fail(err)
	}

	// advance to next field
	w.n++
	if f.Child != nil {
		w.n += len(f.Child.Fields)
	}
	return nil
}

func (w *Writer) AppendTimestamp(tv time.Time) error {
	f, err := w.getFieldChecked(w.n, Timestamp)
	if err != nil {
		return err
	}
	w.writeU64(uint64(TimeScale(f.Scale).ToUnix(tv)))
	w.n++
	return nil
}

func (w *Writer) AppendDuration(d time.Duration) error {
	f, err := w.getFieldChecked(w.n, Duration)
	if err != nil {
		return err
	}
	w.writeU64(uint64(TimeScale(f.Scale).Int64(d)))
	w.n++
	return nil
}

func (w *Writer) AppendTime(tv time.Time) error {
	f, err := w.getFieldChecked(w.n, Time)
	if err != nil {
		return err
	}
	w.writeU64(uint64(TimeScale(f.Scale).ToUnix(tv)))
	w.n++
	return nil
}

func (w *Writer) AppendDate(tv time.Time) error {
	f, err := w.getFieldChecked(w.n, Date)
	if err != nil {
		return err
	}
	w.writeU64(uint64(TimeScale(f.Scale).ToUnix(tv)))
	w.n++
	return nil
}

func (w *Writer) AppendInt64(v int64) error {
	_, err := w.getFieldChecked(w.n, Int64)
	if err != nil {
		return err
	}
	w.writeU64(uint64(v))
	w.n++
	return nil
}

func (w *Writer) AppendInt32(v int32) error {
	_, err := w.getFieldChecked(w.n, Int32)
	if err != nil {
		return err
	}
	w.writeU32(uint32(v))
	w.n++
	return nil
}

func (w *Writer) AppendInt16(v int16) error {
	_, err := w.getFieldChecked(w.n, Int16)
	if err != nil {
		return err
	}
	w.writeU16(uint16(v))
	w.n++
	return nil
}

func (w *Writer) AppendInt8(v int8) error {
	_, err := w.getFieldChecked(w.n, Int8)
	if err != nil {
		return err
	}
	w.buf.WriteByte(uint8(v))
	w.n++
	return nil
}

func (w *Writer) AppendUint64(v uint64) error {
	_, err := w.getFieldChecked(w.n, Uint64)
	if err != nil {
		return err
	}
	w.writeU64(v)
	w.n++
	return nil
}

func (w *Writer) AppendUint32(v uint32) error {
	_, err := w.getFieldChecked(w.n, Uint32)
	if err != nil {
		return err
	}
	w.writeU32(v)
	w.n++
	return nil
}

func (w *Writer) AppendUint16(v uint16) error {
	_, err := w.getFieldChecked(w.n, Uint16)
	if err != nil {
		return err
	}
	w.writeU16(v)
	w.n++
	return nil
}

func (w *Writer) AppendUint8(v uint8) error {
	_, err := w.getFieldChecked(w.n, Uint8)
	if err != nil {
		return err
	}
	w.buf.WriteByte(v)
	w.n++
	return nil
}

func (w *Writer) AppendFloat64(v float64) error {
	_, err := w.getFieldChecked(w.n, Float64)
	if err != nil {
		return err
	}
	w.writeU64(math.Float64bits(v))
	w.n++
	return nil
}

func (w *Writer) AppendFloat32(v float32) error {
	_, err := w.getFieldChecked(w.n, Float32)
	if err != nil {
		return err
	}
	w.writeU32(math.Float32bits(v))
	w.n++
	return nil
}

func (w *Writer) AppendBool(v bool) error {
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

func (w *Writer) AppendEnum(s string) error {
	f, err := w.getFieldChecked(w.n, Enum)
	if err != nil {
		return err
	}
	if f.Enum == nil {
		return w.fail(ErrEnumUndefined)
	}
	val, ok := f.Enum.Code(s)
	if !ok {
		return w.fail(enum.ErrEnumNoCode)
	}
	w.writeU16(val)
	w.n++
	return nil
}

func (w *Writer) AppendString(s string) error {
	if len(s) > MAX_STRING {
		return w.fail(ErrLongValue)
	}
	f, err := w.getFieldChecked(w.n, String)
	if err != nil {
		return err
	}
	if f.IsArray() {
		if len(s) != int(f.Scale) {
			return w.fail(ErrShortValue)
		}
	} else {
		w.buf.WriteByte(byte(len(s)))
	}
	w.buf.WriteString(s)
	w.n++
	return nil
}

func (w *Writer) AppendText(s string) error {
	_, err := w.getFieldChecked(w.n, Text)
	if err != nil {
		return err
	}
	w.writeLen(len(s))
	w.buf.WriteString(s)
	w.n++
	return nil
}

func (w *Writer) AppendBytes(b []byte) error {
	if len(b) > MAX_BYTES {
		return w.fail(ErrLongValue)
	}
	f, err := w.getFieldChecked(w.n, Bytes)
	if err != nil {
		return err
	}
	if f.IsArray() {
		if len(b) != int(f.Scale) {
			return w.fail(ErrShortValue)
		}
	} else {
		w.buf.WriteByte(byte(len(b)))
	}
	w.buf.Write(b)
	w.n++
	return nil
}

func (w *Writer) AppendBinary(b []byte) error {
	_, err := w.getFieldChecked(w.n, Binary)
	if err != nil {
		return err
	}
	w.writeLen(len(b))
	w.buf.Write(b)
	w.n++
	return nil
}

func (w *Writer) AppendInt256(v num.Int256) error {
	_, err := w.getFieldChecked(w.n, Int256)
	if err != nil {
		return err
	}
	w.buf.Write(v.Bytes())
	w.n++
	return nil
}

func (w *Writer) AppendInt128(v num.Int128) error {
	_, err := w.getFieldChecked(w.n, Int128)
	if err != nil {
		return err
	}
	w.buf.Write(v.Bytes())
	w.n++
	return nil
}

func (w *Writer) AppendDecimal256(v num.Decimal256) error {
	_, err := w.getFieldChecked(w.n, Decimal256)
	if err != nil {
		return err
	}
	w.buf.Write(v.Int256().Bytes())
	w.n++
	return nil
}

func (w *Writer) AppendDecimal128(v num.Decimal128) error {
	_, err := w.getFieldChecked(w.n, Decimal128)
	if err != nil {
		return err
	}
	w.buf.Write(v.Int128().Bytes())
	w.n++
	return nil
}

func (w *Writer) AppendDecimal64(v num.Decimal64) error {
	_, err := w.getFieldChecked(w.n, Decimal64)
	if err != nil {
		return err
	}
	w.writeU64(uint64(v.Int64()))
	w.n++
	return nil
}

func (w *Writer) AppendDecimal32(v num.Decimal32) error {
	_, err := w.getFieldChecked(w.n, Decimal32)
	if err != nil {
		return err
	}
	w.writeU32(uint32(v.Int32()))
	w.n++
	return nil
}

func (w *Writer) AppendBigint(v num.Big) error {
	_, err := w.getFieldChecked(w.n, Bigint)
	if err != nil {
		return err
	}
	buf := v.Bytes()
	if len(buf) >= MAX_BYTES {
		return w.fail(ErrLongValue)
	}
	w.buf.WriteByte(byte(len(buf)))
	w.buf.Write(buf)
	w.n++
	return nil
}

func (w *Writer) AppendUnion(v UnionValue) error {
	f, err := w.getFieldChecked(w.n, Union)
	if err != nil {
		return err
	}
	err = v.MarshalSchema(w)
	if err != nil {
		return w.fail(err)
	}
	w.n += 1 + len(f.Child.Fields)
	return nil
}

// AppendList calls provided callback with a new list writer to
// append nested elements to the buffer.
func (w *Writer) AppendList(fn func(*ListWriter) error) (err error) {
	var f *Field
	f, err = w.getFieldChecked(w.n, List)
	if err != nil {
		return err
	}

	// advance field offset
	w.n++

	// open nested list writer
	lw := newListWriter(w, f.Child)
	defer lw.Close()

	defer func() {
		if e := recover(); e != nil {
			err = w.fail(e.(error))
		}
	}()

	// call callback, capture error
	return w.fail(fn(lw))
}

// AppendeMap calls provided callback with a new map writer to
// append nested key/value pairs to the buffer.
func (w *Writer) AppendMap(fn func(mw *MapWriter) error) (err error) {
	var f *Field
	f, err = w.getFieldChecked(w.n, Map)
	if err != nil {
		return err
	}

	// advance field offset
	w.n++

	// open nested map writer
	mw := newMapWriter(w, f.Child)
	defer mw.Close()

	defer func() {
		if e := recover(); e != nil {
			err = w.fail(e.(error))
		}
	}()

	// call callback, capture error
	return w.fail(fn(mw))
}

// WriteVariant calls provided callback with a new variant writer to
// append a nested variant type to the buffer.
func (w *Writer) AppendVariant(caseId uint8, fn func(vw *VariantWriter) error) (err error) {
	var f *Field
	f, err = w.getFieldChecked(w.n, Variant)
	if err != nil {
		return err
	}

	// lookup variant type
	s, ok := f.Case(caseId)
	if !ok {
		return w.fail(ErrInvalidVariant)
	}

	// advance field offset to the first field of the chosen case
	// skip the two metadata fields
	w.n++
	for w.n < len(w.schema.Fields) && w.schema.Fields[w.n].CaseId != caseId {
		w.n++
	}

	// open nested variant writer
	vw := newVariantWriter(w, s, caseId)
	defer vw.Close()

	defer func() {
		if e := recover(); e != nil {
			err = w.fail(e.(error))
		}
	}()

	// call callback, capture error
	return w.fail(fn(vw))
}

func (w *Writer) getFieldChecked(n int, ty FieldType) (*Field, error) {
	if n >= len(w.schema.Fields) {
		return nil, w.fail(io.EOF)
	}
	f := w.schema.Fields[n]
	if ty > 0 && f.Type != ty {
		return nil, w.fail(ErrInvalidValueType)
	}
	return f, nil
}

func (w *Writer) fail(err error) error {
	if w.err == nil {
		w.err = err
	}
	return err
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
