// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package schema

import (
	"encoding/binary"
	"math"
	"time"

	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/num"
	"blockwatch.cc/knoxdb/pkg/util"
)

var LE = binary.LittleEndian

type View struct {
	schema   *Schema
	buf      []byte
	ofs      []int
	len      []int
	minsz    int
	pki      int
	fixed    bool
	internal bool
}

func NewView(s *Schema) *View {
	view := &View{
		schema:   s,
		ofs:      make([]int, len(s.fields)),
		len:      make([]int, len(s.fields)),
		fixed:    true,
		internal: false,
		pki:      -1,
	}
	return view.buildFromSchema()
}

func NewInternalView(s *Schema) *View {
	view := &View{
		schema:   s,
		ofs:      make([]int, len(s.fields)),
		len:      make([]int, len(s.fields)),
		fixed:    true,
		internal: true,
		pki:      -1,
	}
	return view.buildFromSchema()
}

func (v *View) buildFromSchema() *View {
	var ofs int
	for i, f := range v.schema.fields {
		if !f.IsActive() {
			v.ofs[i] = -2
			continue
		}
		if f.IsInternal() && !v.internal {
			v.ofs[i] = -2
			continue
		}
		sz := f.typ.Size()
		if f.fixed > 0 {
			sz = int(f.fixed)
		}
		if v.pki < 0 && f.flags.Is(types.FieldFlagPrimary) && f.typ == types.FieldTypeUint64 {
			// remember the first uint64 primary key field
			v.pki = i
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
		}
	}
	return v
}

func (v View) Schema() *Schema {
	return v.schema
}

func (v View) IsValid() bool {
	return len(v.buf) >= v.minsz && v.schema != nil && v.schema.IsValid()
}

func (v View) IsFixed() bool {
	return v.fixed
}

func (v View) IsInternal() bool {
	return v.internal
}

func (v *View) Len() int {
	return len(v.buf)
}

func (v *View) Bytes() []byte {
	return v.buf
}

func (v View) Get(i int) (val any, ok bool) {
	if i < 0 || i > len(v.ofs) || !v.IsValid() {
		return
	}
	x, y := v.ofs[i], v.ofs[i]+v.len[i]
	field := &v.schema.fields[i]
	if x == -2 {
		return nil, false
	}
	switch field.typ {
	case types.FieldTypeDatetime:
		val, ok = TimeScale(field.scale).FromUnix(int64(LE.Uint64(v.buf[x:y]))), true
	case types.FieldTypeInt64:
		val, ok = int64(LE.Uint64(v.buf[x:y])), true
	case types.FieldTypeUint64:
		val, ok = LE.Uint64(v.buf[x:y]), true
	case types.FieldTypeFloat64:
		val, ok = math.Float64frombits(LE.Uint64(v.buf[x:y])), true
	case types.FieldTypeBoolean:
		val, ok = v.buf[x] > 0, true
	case types.FieldTypeString:
		val, ok = util.UnsafeGetString(v.buf[x:y]), true
	case types.FieldTypeBytes:
		val, ok = v.buf[x:y], true
	case types.FieldTypeInt32:
		val, ok = int32(LE.Uint32(v.buf[x:y])), true
	case types.FieldTypeInt16:
		val, ok = int16(LE.Uint16(v.buf[x:y])), true
	case types.FieldTypeInt8:
		val, ok = int8(v.buf[x]), true
	case types.FieldTypeUint32:
		val, ok = LE.Uint32(v.buf[x:y]), true
	case types.FieldTypeUint16:
		val, ok = LE.Uint16(v.buf[x:y]), true
	case types.FieldTypeUint8:
		val, ok = v.buf[x], true
	case types.FieldTypeFloat32:
		val, ok = math.Float32frombits(LE.Uint32(v.buf[x:y])), true
	case types.FieldTypeInt256:
		val, ok = num.Int256FromBytes(v.buf[x:y]), true
	case types.FieldTypeInt128:
		val, ok = num.Int128FromBytes(v.buf[x:y]), true
	case types.FieldTypeDecimal256:
		val, ok = num.NewDecimal256(num.Int256FromBytes(v.buf[x:y]), field.scale), true
	case types.FieldTypeDecimal128:
		val, ok = num.NewDecimal128(num.Int128FromBytes(v.buf[x:y]), field.scale), true
	case types.FieldTypeDecimal64:
		val, ok = num.NewDecimal64(int64(LE.Uint64(v.buf[x:y])), field.scale), true
	case types.FieldTypeDecimal32:
		val, ok = num.NewDecimal32(int32(LE.Uint32(v.buf[x:y])), field.scale), true
	case types.FieldTypeBigint:
		val, ok = num.NewBigFromBytes(v.buf[x:y]), true
	}
	return
}

func (v View) GetPhy(i int) (val any, ok bool) {
	if i < 0 || i > len(v.ofs) || !v.IsValid() {
		return
	}
	x, y := v.ofs[i], v.ofs[i]+v.len[i]
	field := &v.schema.fields[i]
	if x == -2 {
		return nil, false
	}
	switch field.typ {
	case types.FieldTypeDatetime, types.FieldTypeInt64, types.FieldTypeDecimal64:
		val, ok = int64(LE.Uint64(v.buf[x:y])), true
	case types.FieldTypeUint64:
		val, ok = LE.Uint64(v.buf[x:y]), true
	case types.FieldTypeFloat64:
		val, ok = math.Float64frombits(LE.Uint64(v.buf[x:y])), true
	case types.FieldTypeBoolean:
		val, ok = v.buf[x] > 0, true
	case types.FieldTypeString, types.FieldTypeBytes, types.FieldTypeBigint:
		val, ok = v.buf[x:y], true
	case types.FieldTypeInt32, types.FieldTypeDecimal32:
		val, ok = int32(LE.Uint32(v.buf[x:y])), true
	case types.FieldTypeInt16:
		val, ok = int16(LE.Uint16(v.buf[x:y])), true
	case types.FieldTypeInt8:
		val, ok = int8(v.buf[x]), true
	case types.FieldTypeUint32:
		val, ok = LE.Uint32(v.buf[x:y]), true
	case types.FieldTypeUint16:
		val, ok = LE.Uint16(v.buf[x:y]), true
	case types.FieldTypeUint8:
		val, ok = v.buf[x], true
	case types.FieldTypeFloat32:
		val, ok = math.Float32frombits(LE.Uint32(v.buf[x:y])), true
	case types.FieldTypeInt256:
		val, ok = num.Int256FromBytes(v.buf[x:y]), true
	case types.FieldTypeInt128:
		val, ok = num.Int128FromBytes(v.buf[x:y]), true
	case types.FieldTypeDecimal256:
		val, ok = num.Int256FromBytes(v.buf[x:y]), true
	case types.FieldTypeDecimal128:
		val, ok = num.Int128FromBytes(v.buf[x:y]), true
	}
	return
}

func (v View) Append(val any, i int) any {
	if i < 0 || i > len(v.ofs) || !v.IsValid() {
		return val
	}
	x, y := v.ofs[i], v.ofs[i]+v.len[i]
	field := &v.schema.fields[i]
	if x == -2 {
		return val
	}
	switch field.typ {
	case types.FieldTypeDatetime:
		if val == nil {
			val = make([]time.Time, 0)
		}
		val = append(val.([]time.Time), TimeScale(field.scale).FromUnix(int64(LE.Uint64(v.buf[x:y]))))
	case types.FieldTypeInt64:
		if val == nil {
			val = make([]int64, 0)
		}
		val = append(val.([]int64), int64(LE.Uint64(v.buf[x:y])))
	case types.FieldTypeUint64:
		if val == nil {
			val = make([]uint64, 0)
		}
		val = append(val.([]uint64), LE.Uint64(v.buf[x:y]))
	case types.FieldTypeFloat64:
		if val == nil {
			val = make([]float64, 0)
		}
		val = append(val.([]float64), math.Float64frombits(LE.Uint64(v.buf[x:y])))
	case types.FieldTypeBoolean:
		if val == nil {
			val = make([]bool, 0)
		}
		val = append(val.([]bool), v.buf[x] > 0)
	case types.FieldTypeString:
		if val == nil {
			val = make([]string, 0)
		}
		val = append(val.([]string), util.UnsafeGetString(v.buf[x:y]))
	case types.FieldTypeBytes:
		if val == nil {
			val = make([][]byte, 0)
		}
		val = append(val.([][]byte), v.buf[x:y])
	case types.FieldTypeInt32:
		if val == nil {
			val = make([]int32, 0)
		}
		val = append(val.([]int32), int32(LE.Uint32(v.buf[x:y])))
	case types.FieldTypeInt16:
		if val == nil {
			val = make([]int16, 0)
		}
		val = append(val.([]int16), int16(LE.Uint16(v.buf[x:y])))
	case types.FieldTypeInt8:
		if val == nil {
			val = make([]int8, 0)
		}
		val = append(val.([]int8), int8(v.buf[x]))
	case types.FieldTypeUint32:
		if val == nil {
			val = make([]uint32, 0)
		}
		val = append(val.([]uint32), LE.Uint32(v.buf[x:y]))
	case types.FieldTypeUint16:
		if val == nil {
			val = make([]uint16, 0)
		}
		val = append(val.([]uint16), LE.Uint16(v.buf[x:y]))
	case types.FieldTypeUint8:
		if val == nil {
			val = make([]uint8, 0)
		}
		val = append(val.([]uint8), v.buf[x])
	case types.FieldTypeFloat32:
		if val == nil {
			val = make([]float32, 0)
		}
		val = append(val.([]float32), math.Float32frombits(LE.Uint32(v.buf[x:y])))
	case types.FieldTypeInt256:
		if val == nil {
			val = make([]num.Int256, 0)
		}
		val = append(val.([]num.Int256), num.Int256FromBytes(v.buf[x:y]))
	case types.FieldTypeInt128:
		if val == nil {
			val = make([]num.Int128, 0)
		}
		val = append(val.([]num.Int128), num.Int128FromBytes(v.buf[x:y]))
	case types.FieldTypeDecimal256:
		if val == nil {
			val = make([]num.Decimal256, 0)
		}
		val = append(val.([]num.Decimal256), num.NewDecimal256(num.Int256FromBytes(v.buf[x:y]), field.scale))
	case types.FieldTypeDecimal128:
		if val == nil {
			val = make([]num.Decimal128, 0)
		}
		val = append(val.([]num.Decimal128), num.NewDecimal128(num.Int128FromBytes(v.buf[x:y]), field.scale))
	case types.FieldTypeDecimal64:
		if val == nil {
			val = make([]num.Decimal64, 0)
		}
		val = append(val.([]num.Decimal64), num.NewDecimal64(int64(LE.Uint64(v.buf[x:y])), field.scale))
	case types.FieldTypeDecimal32:
		if val == nil {
			val = make([]num.Decimal32, 0)
		}
		val = append(val.([]num.Decimal32), num.NewDecimal32(int32(LE.Uint32(v.buf[x:y])), field.scale))
	case types.FieldTypeBigint:
		if val == nil {
			val = make([]num.Big, 0)
		}
		val = append(val.([]num.Big), num.NewBigFromBytes(v.buf[x:y]))
	}
	return val
}

func (v View) SetPk(val uint64) {
	if v.pki >= 0 {
		LE.PutUint64(v.buf[v.ofs[v.pki]:], val)
	}
}

func (v View) GetPk() uint64 {
	if v.pki >= 0 {
		return LE.Uint64(v.buf[v.ofs[v.pki]:])
	}
	return 0
}

func (v View) Set(i int, val any) {
	if i < 0 || i >= len(v.ofs) || !v.IsValid() {
		return
	}
	x, y := v.ofs[i], v.ofs[i]+v.len[i]
	field := &v.schema.fields[i]
	if x == -2 {
		return
	}
	switch field.typ {
	case types.FieldTypeUint64:
		if u64, ok := val.(uint64); ok {
			LE.PutUint64(v.buf[x:y], u64)
		}
	case types.FieldTypeString, types.FieldTypeBytes, types.FieldTypeBigint:
		// unsupported, may alter length
	case types.FieldTypeDatetime:
		if tm, ok := val.(time.Time); ok {
			LE.PutUint64(v.buf[x:y], uint64(TimeScale(field.scale).ToUnix(tm)))
		}
	case types.FieldTypeInt64:
		if i64, ok := val.(int64); ok {
			LE.PutUint64(v.buf[x:y], uint64(i64))
		}
	case types.FieldTypeFloat64:
		if f64, ok := val.(float64); ok {
			LE.PutUint64(v.buf[x:y], math.Float64bits(f64))
		}
	case types.FieldTypeFloat32:
		if f32, ok := val.(float32); ok {
			LE.PutUint32(v.buf[x:y], math.Float32bits(f32))
		}
	case types.FieldTypeBoolean:
		if b, ok := val.(bool); ok {
			if b {
				v.buf[x] = 1
			} else {
				v.buf[x] = 0
			}
		}
	case types.FieldTypeInt32:
		if i32, ok := val.(int32); ok {
			LE.PutUint32(v.buf[x:y], uint32(i32))
		}
	case types.FieldTypeInt16:
		if i16, ok := val.(int16); ok {
			LE.PutUint16(v.buf[x:y], uint16(i16))
		}
	case types.FieldTypeInt8:
		if i8, ok := val.(int8); ok {
			v.buf[x] = uint8(i8)
		}
	case types.FieldTypeUint32:
		if u32, ok := val.(uint32); ok {
			LE.PutUint32(v.buf[x:y], u32)
		}
	case types.FieldTypeUint16:
		if u16, ok := val.(uint16); ok {
			LE.PutUint16(v.buf[x:y], u16)
		}
	case types.FieldTypeUint8:
		if u8, ok := val.(uint8); ok {
			v.buf[x] = u8
		}
	case types.FieldTypeInt256:
		if i256, ok := val.(num.Int256); ok {
			copy(v.buf[x:y], i256.Bytes())
		}
	case types.FieldTypeInt128:
		if i128, ok := val.(num.Int128); ok {
			copy(v.buf[x:y], i128.Bytes())
		}
	case types.FieldTypeDecimal256:
		if d256, ok := val.(num.Decimal256); ok {
			copy(v.buf[x:y], d256.Int256().Bytes())
		}
	case types.FieldTypeDecimal128:
		if d128, ok := val.(num.Decimal128); ok {
			copy(v.buf[x:y], d128.Int128().Bytes())
		}
	case types.FieldTypeDecimal64:
		if d64, ok := val.(num.Decimal64); ok {
			LE.PutUint64(v.buf[x:y], uint64(d64.Int64()))
		}
	case types.FieldTypeDecimal32:
		if d32, ok := val.(num.Decimal32); ok {
			LE.PutUint32(v.buf[x:y], uint32(d32.Int64()))
		}
	}
}

func (v *View) Reset(buf []byte) *View {
	v.buf = nil
	if len(buf) < v.minsz {
		return v
	}
	v.buf = buf
	var ofs int
	if !v.fixed {
		skip := true
		for i := range v.schema.fields {
			f := &v.schema.fields[i]
			if !f.IsActive() {
				v.ofs[i] = -2
				continue
			}
			if f.IsInternal() && !v.internal {
				v.ofs[i] = -2
				continue
			}
			if f.IsFixedSize() && skip {
				ofs += v.len[i] + int(f.fixed)
				continue
			}
			skip = false
			switch f.typ {
			case types.FieldTypeString, types.FieldTypeBytes, types.FieldTypeBigint:
				if f.fixed > 0 {
					v.ofs[i] = ofs
					v.len[i] = int(f.fixed)
					ofs += int(f.fixed)
				} else {
					u32 := LE.Uint32(buf[ofs:])
					ofs += 4
					v.ofs[i] = ofs
					v.len[i] = int(u32)
					ofs += int(u32)
				}
			default:
				v.ofs[i] = ofs
				ofs += v.len[i]
			}
		}
	} else {
		ofs = v.minsz
	}
	v.buf = v.buf[:ofs]
	return v
}

func (v *View) Cut(buf []byte) (*View, []byte, bool) {
	v.Reset(buf)
	buf = buf[v.Len():]
	return v, buf, len(buf) > 0
}
