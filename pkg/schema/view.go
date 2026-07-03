// Copyright (c) 2024-2026 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package schema

import (
	"encoding/binary"
	"fmt"
	"iter"
	"math"
	"sync"
	"time"
	"unsafe"

	"blockwatch.cc/knoxdb/pkg/num"
	"blockwatch.cc/knoxdb/pkg/util"
)

// Viewer defines an interface for accessing type field values
// from a record. It is independent of the storage layout (record
// or columnar) and supports reading all schema value types.
type Viewer interface {
	Schema() *Schema
	Field(i int) *Field
	GetPk() uint64
	GetTimebase() time.Time
	Uint64(i int) uint64
	Uint32(i int) uint32
	Uint16(i int) uint16
	Uint8(i int) uint8
	Int64(i int) int64
	Int32(i int) int32
	Int16(i int) int16
	Int8(i int) int8
	Float64(i int) float64
	Float32(i int) float32
	Int256(i int) num.Int256
	Int128(i int) num.Int128
	Decimal256(i int) num.Decimal256
	Decimal128(i int) num.Decimal128
	Decimal64(i int) num.Decimal64
	Decimal32(i int) num.Decimal32
	Bool(i int) bool
	Timestamp(i int) time.Time
	Duration(i int) time.Duration
	Time(i int) time.Time
	Date(i int) time.Time
	String(i int) string
	Text(i int) string
	Bytes(i int) []byte
	Binary(i int) []byte
	Bigint(i int) num.Big
	Union(i int) UnionValue
	List(i int) iter.Seq2[int, Viewer]
	Map(i int) iter.Seq2[int, Viewer]
	Variant(i int, views []Viewer) (Viewer, uint8)
	Enum(i int) string
}

// Ensure View implements the Viewer interface.
var _ Viewer = (*View)(nil)

// ViewOption defines a function type for view options.
type ViewOption func(*View)

// WithViewLayout defines the binary encoding layout this
// view should use when decoding data.
func WithViewLayout(l binary.ByteOrder) ViewOption {
	return func(v *View) {
		v.layout = l
	}
}

// WithViewMeta enables reading encoded metadata fields.
// Use this option on buffers that were encoded with
// metadata. (TODO: encoders do not support this yet).
func WithViewMeta(b ...bool) ViewOption {
	return func(v *View) {
		if len(b) == 0 {
			v.meta = true
		} else {
			v.meta = b[0]
		}
	}
}

// View implements fast decode-free read access to wire encoded data buffers.
// Accessor methods return typed values by field position, while internally
// calculating buffer offsets to fixed and variable length fields.
// For fixed size schemas view uses a single pass type analysis for static
// buffer offsets and sizes. If variable sized fields are in use (e.g. strings),
// view performs a second pass during reset to read encoded sizes and updates
// the internal buffer map. For this reason users must call Reset to load
// each record before access. Deleted fields and metadata fields are not part
// of encoded records, so View accessors return false.
//
// Generic accessors like Get(), GetPhy() and Set() perform type and bounds
// checks and can convert byte order. On failure these accessors return nil.
// Type-based accessors like Uint64() in contrast panic on failed type checks
// and are not portable due to direct memory access in native byte order.
//
// Via options it is possible to change the default encoding layout (little
// endian) and tell a view that data was encoded including metadata fields.
// Use generic accessor methods Get/GetPhy or optimized typed accessirs
// (e.g. Uint64, String).
//
// On containers like list and map accessors return sequence iterators
// over elements with a view scoped to the nested element's type. Views
// always only visit fields at the current nesting level which means
// accessor indexes count per level.
type View struct {
	mu     sync.Mutex       // view mutex, use explicit Lock/Unlock methods
	schema *Schema          // encoding schema
	buf    []byte           // backing buffer
	ofs    []int            // field offsets (may update on reset)
	len    []int            // field sizes (may update on reset)
	typs   []FieldType      // field types
	scales []uint8          // field scales
	skip   int              // last fixed size field, skip to on reset
	minsz  int              // min number of bytes to represent schema
	pki    int              // schema position of primary key
	tbi    int              // schema position of timebase field
	layout binary.ByteOrder // encoding byte order for integers
	fixed  bool             // true when the schema is fixed
	meta   bool             // output metadata fields when true
}

func NewView(s *Schema, opts ...ViewOption) *View {
	view := &View{
		schema: s,
		ofs:    make([]int, s.NumFields()),
		len:    make([]int, s.NumFields()),
		typs:   make([]FieldType, s.NumFields()),
		scales: make([]uint8, s.NumFields()),
		pki:    -1,
		tbi:    -1,
		layout: binary.LittleEndian,
		fixed:  true,
		meta:   false,
	}
	for _, o := range opts {
		o(view)
	}
	return view.buildFromSchema()
}

func (v *View) buildFromSchema() *View {
	var (
		ofs, i int
		lvl    = v.schema.Fields[0].Level
	)
	for _, f := range v.schema.Fields {
		// skip all nested fields outside the current level
		if f.Level != lvl {
			continue
		}
		// deleted fields are not in wire format, metadata fields
		// likewise unless explicitly enabled
		if !f.IsActive() || f.IsMeta() && !v.meta {
			v.ofs[i] = -2
			i++
			continue
		}
		v.typs[i] = f.Type
		v.scales[i] = f.Scale
		sz := f.Type.Size()
		if f.IsArray() {
			sz = int(f.Scale)
		}
		// remember the first primary key field (must be uint64)
		if v.pki < 0 && f.IsPrimary() && f.Type == Uint64 {
			v.pki = i
		}
		// remember the first timebase field (must be 64bit type)
		if v.tbi < 0 && f.IsTimebase() && f.Type.Size() == 8 {
			v.tbi = i
		}
		switch {
		case !v.fixed:
			// set ofs to -1 for all fields following a dynamic length field
			v.ofs[i] = ofs
			v.len[i] = sz
			v.minsz += sz
		case !f.IsFixedSize():
			// the first dynamic length field resets fixed flag, but keeps start offset
			v.fixed = false
			v.ofs[i] = ofs
			v.len[i] = sz
			v.minsz += sz
			ofs = -1
		default:
			v.ofs[i] = ofs
			v.len[i] = sz
			ofs += sz
			v.minsz += sz
			v.skip = i
		}
		i++
	}
	return v
}

func (v *View) Lock() {
	v.mu.Lock()
}

func (v *View) Unlock() {
	v.mu.Unlock()
}

// Schema returns the view's configured schema at the current nesting level.
// This may be a top-level schema, a child schema for a list or map or a
// case schema for a variant.
func (v *View) Schema() *Schema {
	return v.schema
}

// Field returns the i-th field at the view's nesting level.
func (v *View) Field(i int) *Field {
	return v.findField(i)
}

// IsValid returns true when the buffer is initialized with a
// full encoded record.
func (v *View) IsValid() bool {
	return len(v.buf) >= v.minsz && v.schema != nil
}

// IsFixed returns true when the view's schema contains fixed size
// fields only.
func (v *View) IsFixed() bool {
	return v.fixed
}

// HasMeta returns whether the view was initialized for reading
// metadata fields.
func (v *View) HasMeta() bool {
	return v.meta
}

// Len returns the lengt of the current record in bytes.
func (v *View) Len() int {
	return len(v.buf)
}

// Buffer returns the current encoded record.
func (v *View) Buffer() []byte {
	return v.buf
}

// Cut initializes view to the first record in a batch buffer and returns
// the remainder of the batch and true when a full record was found.
func (v *View) Cut(buf []byte) (*View, []byte, bool) {
	v.Reset(buf)
	buf = buf[v.Len():]
	return v, buf, v.IsValid()
}

// All returns a sequence that visits all records in a batch buffer.
// Yields a counter and the current view reset to the next record.
func (v *View) All(buf []byte) iter.Seq2[int, *View] {
	return func(yield func(int, *View) bool) {
		var i int
		for len(buf) >= v.minsz {
			v.Reset(buf)
			buf = buf[v.Len():]
			if !yield(i, v) {
				return
			}
			i++
		}
	}
}

// Count returns the number of records encoded in a batch buffer.
func (v *View) Count(buf []byte) int {
	var n int
	for len(buf) >= v.minsz {
		v.Reset(buf)
		buf = buf[v.Len():]
		n++
	}
	return n
}

// Reset resets the view to read an encoded record from the buffer.
// When buf is nil the current read buffer is released. For fixed size
// schemas reset only performs a length check. For variable sized
// schemas Reset scans all fields following the last fixed size field
// and updates its buffer index. Reset will panic on short buffers
// and invalid or corrupt data that does not match the schema.
func (v *View) Reset(buf []byte) *View {
	v.buf = nil
	if len(buf) < v.minsz {
		return v
	}
	var ofs int
	if !v.fixed {
		// start scan at the last fixed size field
		if v.skip > 0 {
			ofs = v.ofs[v.skip]
		}
		for n, typ := range v.typs[v.skip:] {
			// adjust field offset
			i := n + v.skip

			// skip deleted and invisible metadata field
			if v.ofs[i] < -1 {
				continue
			}

			// read variable lengths and update all future offsets
			switch typ {
			case String, Bytes, Bigint, Union:
				if scale := v.scales[i]; scale > 0 {
					// len in schema
					v.ofs[i] = ofs
					v.len[i] = int(scale)
					ofs += int(scale)
				} else {
					// 1 byte len
					l := int(buf[ofs])
					ofs++
					v.ofs[i] = ofs
					v.len[i] = l
					ofs += l
				}
			case Text, Binary, List, Map, Variant:
				// 4 byte len
				l := int(v.layout.Uint32(buf[ofs:]))
				ofs += 4
				v.ofs[i] = ofs
				v.len[i] = l
				ofs += l
			default:
				v.ofs[i] = ofs
				ofs += v.len[i]
			}
		}
	} else {
		ofs = v.minsz
	}
	v.buf = buf[:ofs]
	return v
}

// GetPk returns the current record's primary key or zero when
// no primary key field is defined in the schema.
func (v *View) GetPk() uint64 {
	if v.pki < 0 {
		return 0
	}
	return v.Uint64(v.pki)
}

// SetPk overrides the record's primary key field with a new
// value and returns true on success. SetPk is a noop when the
// schema does not define a primary key field.
func (v *View) SetPk(val uint64) bool {
	if v.pki < 0 {
		return false
	}
	v.layout.PutUint64(v.buf[v.ofs[v.pki]:], val)
	return true
}

// GetTimebase returns the current record's timebase or zero when
// no timebase field is defined in the schema.
func (v *View) GetTimebase() time.Time {
	if v.tbi < 0 {
		return time.Time{}
	}
	u64 := v.layout.Uint64(v.buf[v.ofs[v.tbi]:])
	return TimeScale(v.scales[v.tbi]).FromUnix(int64(u64))
}

// SetTimebase overrides the record's timebase field with a new
// value and returns true on success. SetTimebase is a noop when
// the schema does not define a timebase field.
func (v *View) SetTimebase(val time.Time) bool {
	if v.tbi < 0 {
		return false
	}
	v.layout.PutUint64(
		v.buf[v.ofs[v.tbi]:],
		uint64(TimeScale(v.scales[v.tbi]).ToUnix(val)),
	)
	return true
}

// Get returns the logical type at column i wrapped into a typed
// interface or nil when the field is invalid/hidden. Get applies
// the configured layout when reading integers. Due to how Go
// interfaces work internally, Get may allocate an eface object
// for value types larger than 8 bytes (e.g. string, byte). This
// extra allocation is rather costly (17-20ns vs 2-5ns for primitive
// types).
func (v *View) Get(i int) (val any) {
	if !v.IsValid() {
		return
	}
	x := v.ofs[i]
	if x < 0 {
		return nil
	}
	y := x + v.len[i]
	switch v.typs[i] {
	case Timestamp, Time, Date:
		val = TimeScale(v.scales[i]).FromUnix(int64(v.layout.Uint64(v.buf[x:y])))
	case Duration:
		val = TimeScale(v.scales[i]).Duration(int64(v.layout.Uint64(v.buf[x:y])))
	case Int64:
		val = int64(v.layout.Uint64(v.buf[x:y]))
	case Uint64:
		val = v.layout.Uint64(v.buf[x:y])
	case Float64:
		val = math.Float64frombits(v.layout.Uint64(v.buf[x:y]))
	case Boolean:
		val = v.buf[x] > 0
	case String, Text:
		val = util.UnsafeGetString(v.buf[x:y])
	case Bytes, Binary, List, Map:
		val = v.buf[x:y]
	case Int32:
		val = int32(v.layout.Uint32(v.buf[x:y]))
	case Int16:
		val = int16(v.layout.Uint16(v.buf[x:y]))
	case Int8:
		val = int8(v.buf[x])
	case Uint32:
		val = v.layout.Uint32(v.buf[x:y])
	case Uint16:
		val = v.layout.Uint16(v.buf[x:y])
	case Uint8:
		val = v.buf[x]
	case Float32:
		val = math.Float32frombits(v.layout.Uint32(v.buf[x:y]))
	case Int256:
		val = num.Int256FromBytes(v.buf[x:y])
	case Int128:
		val = num.Int128FromBytes(v.buf[x:y])
	case Decimal256:
		val = num.NewDecimal256(num.Int256FromBytes(v.buf[x:y]), v.scales[i])
	case Decimal128:
		val = num.NewDecimal128(num.Int128FromBytes(v.buf[x:y]), v.scales[i])
	case Decimal64:
		val = num.NewDecimal64(int64(v.layout.Uint64(v.buf[x:y])), v.scales[i])
	case Decimal32:
		val = num.NewDecimal32(int32(v.layout.Uint32(v.buf[x:y])), v.scales[i])
	case Bigint:
		val = num.NewBigFromBytes(v.buf[x:y])
	case Union:
		var u UnionValue
		u.UnmarshalBuffer(v.buf[x:y], v.layout)
		val = u
	case Enum:
		val, _ = v.findField(i).Enum.Value(v.layout.Uint16(v.buf[x:y]))
	}
	return
}

// GetPhy returns the physical type at column i wrapped into a typed
// interface and nil when the value is invalid/hidden. Like Get, GetPhy
// allocates an interface which is costly for slice types.
func (v *View) GetPhy(i int) (val any) {
	if !v.IsValid() || i >= len(v.typs) {
		return
	}
	x := v.ofs[i]
	if x < 0 {
		return nil
	}
	y := x + v.len[i]
	switch v.typs[i] {
	case Timestamp, Duration, Time, Date, Int64, Decimal64:
		val = int64(v.layout.Uint64(v.buf[x:y]))
	case Uint64:
		val = v.layout.Uint64(v.buf[x:y])
	case Float64:
		val = math.Float64frombits(v.layout.Uint64(v.buf[x:y]))
	case Boolean:
		val = v.buf[x] > 0
	case String, Bytes, Bigint, Text, Binary, List, Map, Union, Variant:
		val = v.buf[x:y]
	case Int32, Decimal32:
		val = int32(v.layout.Uint32(v.buf[x:y]))
	case Int16:
		val = int16(v.layout.Uint16(v.buf[x:y]))
	case Int8:
		val = int8(v.buf[x])
	case Uint32:
		val = v.layout.Uint32(v.buf[x:y])
	case Uint16, Enum:
		val = v.layout.Uint16(v.buf[x:y])
	case Uint8:
		val = v.buf[x]
	case Float32:
		val = math.Float32frombits(v.layout.Uint32(v.buf[x:y]))
	case Int256, Decimal256:
		val = num.Int256FromBytes(v.buf[x:y])
	case Int128, Decimal128:
		val = num.Int128FromBytes(v.buf[x:y])
	}
	return
}

// Set replaces any fixed size type at field i with a new logical value
// wrapped into typed interface val. Returns true on success or false on
// failure, e.g. value type mismatch, the field does not exist or is hidden
// in schema.
func (v *View) Set(i int, val any) bool {
	if !v.IsValid() {
		return false
	}
	x := v.ofs[i]
	if x < 0 {
		return false
	}
	y := x + v.len[i]
	switch v.typs[i] {
	case Uint64:
		if u64, ok := val.(uint64); ok {
			v.layout.PutUint64(v.buf[x:y], u64)
			return true
		}
	case Timestamp, Time, Date:
		if tm, ok := val.(time.Time); ok {
			v.layout.PutUint64(v.buf[x:y], uint64(TimeScale(v.scales[i]).ToUnix(tm)))
			return true
		}
	case Duration:
		if d, ok := val.(time.Duration); ok {
			v.layout.PutUint64(v.buf[x:y], uint64(TimeScale(v.scales[i]).Int64(d)))
			return true
		}
	case Int64:
		if i64, ok := val.(int64); ok {
			v.layout.PutUint64(v.buf[x:y], uint64(i64))
			return true
		}
	case Float64:
		if f64, ok := val.(float64); ok {
			v.layout.PutUint64(v.buf[x:y], math.Float64bits(f64))
			return true
		}
	case Float32:
		if f32, ok := val.(float32); ok {
			v.layout.PutUint32(v.buf[x:y], math.Float32bits(f32))
			return true
		}
	case Boolean:
		if b, ok := val.(bool); ok {
			if b {
				v.buf[x] = 1
			} else {
				v.buf[x] = 0
			}
			return true
		}
	case Int32:
		if i32, ok := val.(int32); ok {
			v.layout.PutUint32(v.buf[x:y], uint32(i32))
			return true
		}
	case Int16:
		if i16, ok := val.(int16); ok {
			v.layout.PutUint16(v.buf[x:y], uint16(i16))
			return true
		}
	case Int8:
		if i8, ok := val.(int8); ok {
			v.buf[x] = uint8(i8)
			return true
		}
	case Uint32:
		if u32, ok := val.(uint32); ok {
			v.layout.PutUint32(v.buf[x:y], u32)
			return true
		}
	case Uint16:
		if u16, ok := val.(uint16); ok {
			v.layout.PutUint16(v.buf[x:y], u16)
			return true
		}
	case Uint8:
		if u8, ok := val.(uint8); ok {
			v.buf[x] = u8
			return true
		}
	case Int256:
		if i256, ok := val.(num.Int256); ok {
			copy(v.buf[x:y], i256.Bytes())
			return true
		}
	case Int128:
		if i128, ok := val.(num.Int128); ok {
			copy(v.buf[x:y], i128.Bytes())
			return true
		}
	case Decimal256:
		if d256, ok := val.(num.Decimal256); ok {
			copy(v.buf[x:y], d256.Int256().Bytes())
			return true
		}
	case Decimal128:
		if d128, ok := val.(num.Decimal128); ok {
			copy(v.buf[x:y], d128.Int128().Bytes())
			return true
		}
	case Decimal64:
		if d64, ok := val.(num.Decimal64); ok {
			v.layout.PutUint64(v.buf[x:y], uint64(d64.Int64()))
			return true
		}
	case Decimal32:
		if d32, ok := val.(num.Decimal32); ok {
			v.layout.PutUint32(v.buf[x:y], uint32(d32.Int64()))
			return true
		}
	case Enum:
		if s, ok := val.(string); ok {
			if u16, ok := v.findField(i).Enum.Code(s); ok {
				v.layout.PutUint16(v.buf[x:y], u16)
				return true
			}
		}
	case String, Bytes, Bigint, Text, Binary, List, Map, Union, Variant:
		// unsupported, may alter length
	}
	return false
}

// Uint64 is a fast non-portable accessor to uint64 values.
// It panics on type mismatch or out-of-bounds access.
func (v *View) Uint64(i int) uint64 {
	return *(*uint64)(v.getCheckedPtr(i, Uint64))
}

// Uint32 is a fast non-portable accessor to uint32 values.
// It panics on type mismatch or out-of-bounds access.
func (v *View) Uint32(i int) uint32 {
	return *(*uint32)(v.getCheckedPtr(i, Uint32))
}

// Uint16 is a fast non-portable accessor to uint16 values.
// It panics on type mismatch or out-of-bounds access.
func (v *View) Uint16(i int) uint16 {
	return *(*uint16)(v.getCheckedPtr(i, Uint16))
}

// Uint8 is a fast non-portable accessor to uint8 values.
// It panics on type mismatch or out-of-bounds access.
func (v *View) Uint8(i int) uint8 {
	return *(*uint8)(v.getCheckedPtr(i, Uint8))
}

// Int64 is a fast non-portable accessor to int64 values.
// It panics on type mismatch or out-of-bounds access.
func (v *View) Int64(i int) int64 {
	return *(*int64)(v.getCheckedPtr(i, Int64))
}

// Int32 is a fast non-portable accessor to int32 values.
// It panics on type mismatch or out-of-bounds access.
func (v *View) Int32(i int) int32 {
	return *(*int32)(v.getCheckedPtr(i, Int32))
}

// Int16 is a fast non-portable accessor to int16 values.
// It panics on type mismatch or out-of-bounds access.
func (v *View) Int16(i int) int16 {
	return *(*int16)(v.getCheckedPtr(i, Int16))
}

// Int8 is a fast non-portable accessor to int8 values.
// It panics on type mismatch or out-of-bounds access.
func (v *View) Int8(i int) int8 {
	return *(*int8)(v.getCheckedPtr(i, Int8))
}

// Float64 is a fast non-portable accessor to float64 values.
// It panics on type mismatch or out-of-bounds access.
func (v *View) Float64(i int) float64 {
	return *(*float64)(v.getCheckedPtr(i, Float64))
}

// Float32 is a fast non-portable accessor to float32 values.
// It panics on type mismatch or out-of-bounds access.
func (v *View) Float32(i int) float32 {
	return *(*float32)(v.getCheckedPtr(i, Float32))
}

// Int256 is a fast non-portable accessor to int256 values.
// It panics on type mismatch or out-of-bounds access.
func (v *View) Int256(i int) num.Int256 {
	return num.Int256(*(*[4]uint64)(v.getCheckedPtr(i, Int256)))
}

// Int128 is a fast non-portable accessor to int128 values.
// It panics on type mismatch or out-of-bounds access.
func (v *View) Int128(i int) num.Int128 {
	return num.Int128(*(*[2]uint64)(v.getCheckedPtr(i, Int128)))
}

// Decimal256 is a fast non-portable accessor to decimal256 values.
// It panics on type mismatch or out-of-bounds access.
func (v *View) Decimal256(i int) num.Decimal256 {
	return num.NewDecimal256(num.Int256(*(*[4]uint64)(
		v.getCheckedPtr(i, Decimal256))), v.scales[i])
}

// Decimal128 is a fast non-portable accessor to decimal128 values.
// It panics on type mismatch or out-of-bounds access.
func (v *View) Decimal128(i int) num.Decimal128 {
	return num.NewDecimal128(num.Int128(*(*[2]uint64)(
		v.getCheckedPtr(i, Decimal128))), v.scales[i])
}

// Decimal64 is a fast non-portable accessor to decimal64 values.
// It panics on type mismatch or out-of-bounds access.
func (v *View) Decimal64(i int) num.Decimal64 {
	return num.NewDecimal64(*(*int64)(v.getCheckedPtr(i, Decimal64)), v.scales[i])
}

// Decimal32 is a fast non-portable accessor to decimal32 values.
// It panics on type mismatch or out-of-bounds access.
func (v *View) Decimal32(i int) num.Decimal32 {
	return num.NewDecimal32(*(*int32)(v.getCheckedPtr(i, Decimal32)), v.scales[i])
}

// Bool is a fast non-portable accessor to boolean values.
// It panics on type mismatch or out-of-bounds access.
func (v *View) Bool(i int) bool {
	return *(*byte)(v.getCheckedPtr(i, Boolean)) == 1
}

// Enum is a fast non-portable accessor to enum values. It
// decodes enums from uint16 and returns their string value.
// It panics on type mismatch or out-of-bounds access.
func (v *View) Enum(i int) string {
	val, _ := v.findField(i).Enum.Value(*(*uint16)(v.getCheckedPtr(i, Enum)))
	return val
}

// Timestamp is a fast non-portable accessor to timestamp values.
// It panics on type mismatch or out-of-bounds access.
func (v *View) Timestamp(i int) time.Time {
	p := v.getCheckedPtr(i, Timestamp)
	return TimeScale(v.scales[i]).FromUnix(*(*int64)(p))
}

// Duration is a fast non-portable accessor to duration values.
// It panics on type mismatch or out-of-bounds access.
func (v *View) Duration(i int) time.Duration {
	p := v.getCheckedPtr(i, Duration)
	return TimeScale(v.scales[i]).Duration(*(*int64)(p))
}

// Time is a fast non-portable accessor to time of day values.
// It panics on type mismatch or out-of-bounds access.
func (v *View) Time(i int) time.Time {
	p := v.getCheckedPtr(i, Time)
	return TimeScale(v.scales[i]).FromUnix(*(*int64)(p))
}

// Date is a fast non-portable accessor to date values.
// It panics on type mismatch or out-of-bounds access.
func (v *View) Date(i int) time.Time {
	p := v.getCheckedPtr(i, Date)
	return TimeScale(v.scales[i]).FromUnix(*(*int64)(p))
}

// String is a fast non-portable accessor to short string values and arrays.
// It panics on type mismatch or out-of-bounds access.
func (v *View) String(i int) string {
	return unsafe.String((*byte)(v.getCheckedPtr(i, String)), v.len[i])
}

// Text is a fast non-portable accessor to long string values.
// It panics on type mismatch or out-of-bounds access.
func (v *View) Text(i int) string {
	return unsafe.String((*byte)(v.getCheckedPtr(i, Text)), v.len[i])
}

// Bytes is a fast non-portable accessor to short byte values and arrays.
// It panics on type mismatch or out-of-bounds access.
func (v *View) Bytes(i int) []byte {
	return unsafe.Slice((*byte)(v.getCheckedPtr(i, Bytes)), v.len[i])
}

// Binary is a fast non-portable accessor to long binary values.
// It panics on type mismatch or out-of-bounds access.
func (v *View) Binary(i int) []byte {
	return unsafe.Slice((*byte)(v.getCheckedPtr(i, Bytes)), v.len[i])
}

// Bigint is a fast non-portable accessor to bigint values.
// It panics on type mismatch or out-of-bounds access.
func (v *View) Bigint(i int) num.Big {
	return num.NewBigFromBytes(unsafe.Slice((*byte)(v.getCheckedPtr(i, Bigint)), v.len[i]))
}

// Union is a fast non-portable accessor to union values decoded from
// little-endian format. It panics on type mismatch, out-of-bounds access
// or decoding error.
func (v *View) Union(i int) UnionValue {
	buf := unsafe.Slice((*byte)(v.getCheckedPtr(i, Union)), v.len[i])
	var val UnionValue
	if err := val.UnmarshalBuffer(buf, v.layout); err != nil {
		panic(err)
	}
	return val
}

// List returns an iterator for a list field at position i.
// It panics on type mismatch or when i is out of range.
func (v *View) List(i int) iter.Seq2[int, Viewer] {
	p := v.getCheckedPtr(i, List)
	return v.makeIter(i, p)
}

// Map returns an iterator for a map field at position i.
// It panics on type mismatch or when i is out of range. The first
// index (0) on the returned view accesses the map's key.
func (v *View) Map(i int) iter.Seq2[int, Viewer] {
	p := v.getCheckedPtr(i, Map)
	return v.makeIter(i, p)
}

// Variant returns a view for a nested variant field at position i
// and the selected case id. Note caseId starts at 1.
// It panics on type mismatch or when i is out of range. Users can
// pass in a list of initialized views returned by earlir calls
// to re-use allocations.
func (v *View) Variant(i int, views []Viewer) (Viewer, uint8) {
	// get encoded sub-buffer
	buf := unsafe.Slice((*byte)(v.getCheckedPtr(i, Variant)), v.len[i])

	// read case id
	caseId := buf[0]

	// read typeid, select case schema, init view
	s, ok := v.findField(i).Case(caseId)
	if !ok {
		panic(ErrInvalidVariant)
	}

	// reuse the first view with matching schema
	for _, vv := range views {
		if vv != nil {
			view := vv.(*View)
			if view.schema.Hash == s.Hash {
				return view.Reset(buf[1:]), caseId
			}
		}
	}

	// alloc new view
	return NewView(s, WithViewLayout(v.layout), WithViewMeta(v.meta)).
		Reset(buf[1:]), caseId
}

// find the i-th field at the current nesting level; we don't store
// field pointers and skip nested fields, so we need this extra lookup
func (v *View) findField(i int) *Field {
	var (
		lvl = v.schema.Fields[0].Level
		n   int
	)
	for _, cf := range v.schema.Fields {
		if cf.Level != lvl {
			continue
		}
		if i == n {
			return cf
		}
		n++
	}
	return nil
}

func (v *View) makeIter(i int, p unsafe.Pointer) iter.Seq2[int, Viewer] {
	// make iterator func using child view for nested fields
	return func(yield func(int, Viewer) bool) {
		// slice the list buffer
		buf := unsafe.Slice((*byte)(p), v.len[i])

		// make a new child view
		view := NewView(v.findField(i).Child)

		// loop through all nested records
		var i int
		for len(buf) >= view.minsz {
			view.Reset(buf)
			buf = buf[view.Len():]
			if !yield(i, view) {
				break
			}
			i++
		}
	}
}

func (v *View) getCheckedPtr(i int, ty FieldType) unsafe.Pointer {
	if len(v.buf) < v.minsz {
		panic(fmt.Errorf("view: short buffer"))
	}
	if len(v.typs) <= i {
		panic(fmt.Errorf("view: out of range [%d:%d]", i, len(v.typs)))
	}
	if v.typs[i] != ty {
		panic(fmt.Errorf("view: invalid %s access at %s field %d", ty, v.typs[i], i))
	}
	ofs := v.ofs[i]
	if ofs < 0 {
		panic(fmt.Errorf("view: invalid access to hidden field %d", i))
	}
	// note we do not need to check buffer length at offset
	// because Reset has validated the buffer layout already
	return unsafe.Pointer(unsafe.SliceData(v.buf[ofs:]))
}

// // GetPtr returns a byte pointer into the buffer along with
// // a length for value at schema position i. Returns nil and
// // false when the view is uninitialized or the value at pos
// // i is not available (the field was deleted or metadata
// // fields were not encoded).
// func (v *View) GetPtr(i int) (ptr *byte, size int, ok bool) {
// 	if !v.IsValid() {
// 		return
// 	}
// 	x := v.ofs[i]
// 	if x < 0 {
// 		return
// 	}
// 	return &v.buf[x], v.len[i], true
// }
