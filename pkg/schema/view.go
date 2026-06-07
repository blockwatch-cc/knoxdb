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
// Via options it is possible to change the default encoding layout (little
// endian) and tell a view that data was encoded including metadata fields.
// Use generic accessor methods Get/GetPhy or optimized typed accessirs
// (e.g. Uint64, String).
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
		// remember the first timebase field (must be int64)
		if v.tbi < 0 && f.IsTimebase() && f.Type == Int64 {
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

// Schema returns the view's configured schema. On list elements
// this is the nested child schema. Note schemas contain a flattened
// list of nested fields across all nesting levels in type tree
// pre-order. A view's accessors visit only the top-most nesting
// level.
func (v *View) Schema() *Schema {
	return v.schema
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

// All returns a sequence that visits all records in a buffer. The sequence
// yields the same view instance initialized to the next record in turn.
func (v *View) All(buf []byte) iter.Seq2[int, *View] {
	return func(yield func(int, *View) bool) {
		var i int
		for len(buf) > v.minsz {
			v.Reset(buf)
			buf = buf[v.Len():]
			if !yield(i, v) {
				return
			}
			i++
		}
	}
}

// Count returns the number of records encoded in a given buffer.
func (v *View) Count(buf []byte) int {
	var n int
	for len(buf) > v.minsz {
		v.Reset(buf)
		buf = buf[v.Len():]
		n++
	}
	return n
}

// Reset resets the view to read from a new encoded buffer. When buf is nil
// the current read buffer is released. For fixed size schemas reset only
// performs a length check. For variable sized schemas Reset scans all fields
// following the last fixed size field and updates its buffer index.
// Reset will panic if buffer is short or invalid for the schema or contains
// corrupt data.
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
			case String, Bytes, Bigint:
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
			case Text, Binary, List:
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
	return v.Timestamp(v.tbi)
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
	case Timestamp, Time, Date, Int64, Decimal64:
		val = int64(v.layout.Uint64(v.buf[x:y]))
	case Uint64:
		val = v.layout.Uint64(v.buf[x:y])
	case Float64:
		val = math.Float64frombits(v.layout.Uint64(v.buf[x:y]))
	case Boolean:
		val = v.buf[x] > 0
	case String, Bytes, Bigint, Text, Binary, List, Map:
		val = v.buf[x:y]
	case Int32, Decimal32:
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
		val = num.Int256FromBytes(v.buf[x:y])
	case Decimal128:
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
	case String, Bytes, Bigint, Text, Binary, List, Map:
		// unsupported, may alter length
	}
	return false
}

// Uint64 returns a native endian u64 at schema index i.
// It panics if i is out of range, the type at position i
// is not a uint64 or the field is hidden. This method is
// fast due to direct memory access by is not portable.
func (v *View) Uint64(i int) uint64 {
	return *(*uint64)(v.getCheckedPtr(i, Uint64))
}

func (v *View) Uint32(i int) uint32 {
	return *(*uint32)(v.getCheckedPtr(i, Uint32))
}

func (v *View) Uint16(i int) uint16 {
	return *(*uint16)(v.getCheckedPtr(i, Uint16))
}

func (v *View) Uint8(i int) uint8 {
	return *(*uint8)(v.getCheckedPtr(i, Uint8))
}

func (v *View) Int64(i int) int64 {
	return *(*int64)(v.getCheckedPtr(i, Int64))
}

func (v *View) Int32(i int) int32 {
	return *(*int32)(v.getCheckedPtr(i, Int32))
}

func (v *View) Int16(i int) int16 {
	return *(*int16)(v.getCheckedPtr(i, Int16))
}

func (v *View) Int8(i int) int8 {
	return *(*int8)(v.getCheckedPtr(i, Int8))
}

func (v *View) Float64(i int) float64 {
	return *(*float64)(v.getCheckedPtr(i, Float64))
}

func (v *View) Float32(i int) float32 {
	return *(*float32)(v.getCheckedPtr(i, Float32))
}

func (v *View) Int256(i int) num.Int256 {
	return num.Int256(*(*[4]uint64)(v.getCheckedPtr(i, Int256)))
}

func (v *View) Int128(i int) num.Int128 {
	return num.Int128(*(*[2]uint64)(v.getCheckedPtr(i, Int128)))
}

func (v *View) Decimal256(i int) num.Decimal256 {
	return num.NewDecimal256(num.Int256(*(*[4]uint64)(
		v.getCheckedPtr(i, Decimal256))), v.scales[i])
}

func (v *View) Decimal128(i int) num.Decimal128 {
	return num.NewDecimal128(num.Int128(*(*[2]uint64)(
		v.getCheckedPtr(i, Decimal128))), v.scales[i])
}

func (v *View) Decimal64(i int) num.Decimal64 {
	return num.NewDecimal64(*(*int64)(v.getCheckedPtr(i, Decimal64)), v.scales[i])
}

func (v *View) Decimal32(i int) num.Decimal32 {
	return num.NewDecimal32(*(*int32)(v.getCheckedPtr(i, Decimal32)), v.scales[i])
}

func (v *View) Bool(i int) bool {
	return *(*byte)(v.getCheckedPtr(i, Boolean)) == 1
}

func (v *View) Enum(i int) string {
	val, _ := v.schema.Fields[i].Enum.Value(*(*uint16)(v.getCheckedPtr(i, Uint16)))
	return val
}

func (v *View) Timestamp(i int) time.Time {
	p := v.getCheckedPtr(i, Timestamp)
	return TimeScale(v.scales[i]).FromUnix(*(*int64)(p))
}

func (v *View) Time(i int) time.Time {
	p := v.getCheckedPtr(i, Time)
	return TimeScale(v.scales[i]).FromUnix(*(*int64)(p))
}

func (v *View) Date(i int) time.Time {
	p := v.getCheckedPtr(i, Date)
	return TimeScale(v.scales[i]).FromUnix(*(*int64)(p))
}

func (v *View) String(i int) string {
	return unsafe.String((*byte)(v.getCheckedPtr(i, String)), v.len[i])
}

func (v *View) Text(i int) string {
	return unsafe.String((*byte)(v.getCheckedPtr(i, Text)), v.len[i])
}

func (v *View) Bytes(i int) []byte {
	return unsafe.Slice((*byte)(v.getCheckedPtr(i, Bytes)), v.len[i])
}

func (v *View) Binary(i int) []byte {
	return unsafe.Slice((*byte)(v.getCheckedPtr(i, Bytes)), v.len[i])
}

func (v *View) Bigint(i int) num.Big {
	return num.NewBigFromBytes(unsafe.Slice((*byte)(v.getCheckedPtr(i, Bigint)), v.len[i]))
}

// List returns an iterator for a list field at position i. Will
// panic on type mismatch or when i is out of range.
func (v *View) List(i int) iter.Seq2[int, *View] {
	p := v.getCheckedPtr(i, List)

	// find the i-th field at the current nesting level; we don't store
	// field pointers and skip nested fields, so we need this extra lookup
	// to find the list field and access its child schema
	var (
		f   *Field
		lvl = v.schema.Fields[0].Level
		n   int
	)
	for _, cf := range v.schema.Fields {
		if cf.Level != lvl {
			continue
		}
		if i == n {
			f = cf
			break
		}
		n++
	}

	// make iterator func using child view for nested fields
	return func(yield func(int, *View) bool) {
		// slice the list buffer
		buf := unsafe.Slice((*byte)(p), v.len[i])

		// make a new child view
		view := NewView(f.Child)

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
