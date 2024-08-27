// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package schema

import (
	"encoding/binary"
	"math"
	"time"

	"blockwatch.cc/knoxdb/pkg/num"
	"blockwatch.cc/knoxdb/pkg/util"
)

var LE = binary.LittleEndian

type View struct {
	schema *Schema
	buf    []byte
	ofs    []int
	len    []int
	minsz  int
	pki    int
	fixed  bool
}

func NewView(s *Schema) *View {
	view := &View{
		schema: s,
		ofs:    make([]int, len(s.fields)),
		len:    make([]int, len(s.fields)),
		pki:    -1,
	}
	var ofs int
	for i, f := range s.fields {
		sz := f.typ.Size()
		if view.pki < 0 && f.flags.Is(FieldFlagPrimary) && f.typ == FieldTypeUint64 {
			// remember the first uint64 primary key field
			view.pki = i
		}
		switch {
		case !view.fixed:
			// set ofs to -1 for all fields following a dynamic length field
			view.ofs[i] = ofs
			view.len[i] = sz
			view.minsz++
		case sz < 0:
			// the first dynamic length field resets fixed flag, but keeps start offset
			view.fixed = false
			view.ofs[i] = ofs
			view.len[i] = sz
			view.minsz++
			ofs = -1
		default:
			view.ofs[i] = ofs
			view.len[i] = sz
			ofs += sz
			view.minsz += sz
		}
	}
	return view
}

func (v View) IsValid() bool {
	return len(v.buf) >= v.minsz && v.schema != nil && v.schema.IsValid()
}

func (v View) IsFixed() bool {
	return v.fixed
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
	switch field.typ {
	case FieldTypeDatetime:
		val, ok = time.Unix(0, int64(LE.Uint64(v.buf[x:y]))), true
	case FieldTypeInt64:
		val, ok = int64(LE.Uint64(v.buf[x:y])), true
	case FieldTypeUint64:
		val, ok = LE.Uint64(v.buf[x:y]), true
	case FieldTypeFloat64:
		val, ok = float64(LE.Uint64(v.buf[x:y])), true
	case FieldTypeBoolean:
		val, ok = v.buf[x] > 0, true
	case FieldTypeString:
		val, ok = util.UnsafeGetString(v.buf[x:y]), true
	case FieldTypeBytes:
		val, ok = v.buf[x:y], true
	case FieldTypeInt32:
		val, ok = int32(LE.Uint32(v.buf[x:y])), true
	case FieldTypeInt16:
		val, ok = int16(LE.Uint16(v.buf[x:y])), true
	case FieldTypeInt8:
		val, ok = v.buf[x], true
	case FieldTypeUint32:
		val, ok = LE.Uint32(v.buf[x:y]), true
	case FieldTypeUint16:
		val, ok = LE.Uint16(v.buf[x:y]), true
	case FieldTypeUint8:
		val, ok = v.buf[x], true
	case FieldTypeFloat32:
		val, ok = float32(LE.Uint32(v.buf[x:y])), true
	case FieldTypeInt256:
		val, ok = num.Int256FromBytes(v.buf[x:y]), true
	case FieldTypeInt128:
		val, ok = num.Int128FromBytes(v.buf[x:y]), true
	case FieldTypeDecimal256:
		val, ok = num.NewDecimal256(num.Int256FromBytes(v.buf[x:y]), field.scale), true
	case FieldTypeDecimal128:
		val, ok = num.NewDecimal128(num.Int128FromBytes(v.buf[x:y]), field.scale), true
	case FieldTypeDecimal64:
		val, ok = num.NewDecimal64(int64(LE.Uint64(v.buf[x:y])), field.scale), true
	case FieldTypeDecimal32:
		val, ok = num.NewDecimal32(int32(LE.Uint32(v.buf[x:y])), field.scale), true
	}
	return
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
	if i < 0 || i > len(v.ofs) || !v.IsValid() {
		return
	}
	x, y := v.ofs[i], v.ofs[i]+v.len[i]
	field := &v.schema.fields[i]
	switch field.typ {
	case FieldTypeUint64:
		if u64, ok := val.(uint64); ok {
			LE.PutUint64(v.buf[x:y], u64)
		}
	case FieldTypeString, FieldTypeBytes:
		// unsupported, may alter length
	case FieldTypeDatetime:
		if tm, ok := val.(time.Time); ok {
			LE.PutUint64(v.buf[x:y], uint64(tm.UnixNano()))
		}
	case FieldTypeInt64:
		if i64, ok := val.(int64); ok {
			LE.PutUint64(v.buf[x:y], uint64(i64))
		}
	case FieldTypeFloat64:
		if f64, ok := val.(float64); ok {
			LE.PutUint64(v.buf[x:y], math.Float64bits(f64))
		}
	case FieldTypeFloat32:
		if f32, ok := val.(float32); ok {
			LE.PutUint32(v.buf[x:y], math.Float32bits(f32))
		}
	case FieldTypeBoolean:
		if b, ok := val.(bool); ok {
			if b {
				v.buf[x] = 1
			} else {
				v.buf[x] = 0
			}
		}
	case FieldTypeInt32:
		if i32, ok := val.(int32); ok {
			LE.PutUint32(v.buf[x:y], uint32(i32))
		}
	case FieldTypeInt16:
		if i16, ok := val.(int16); ok {
			LE.PutUint16(v.buf[x:y], uint16(i16))
		}
	case FieldTypeInt8:
		if i8, ok := val.(int8); ok {
			v.buf[x] = uint8(i8)
		}
	case FieldTypeUint32:
		if u32, ok := val.(uint32); ok {
			LE.PutUint32(v.buf[x:y], u32)
		}
	case FieldTypeUint16:
		if u16, ok := val.(uint16); ok {
			LE.PutUint16(v.buf[x:y], u16)
		}
	case FieldTypeUint8:
		if u8, ok := val.(uint8); ok {
			v.buf[x] = u8
		}
	case FieldTypeInt256:
		if i256, ok := val.(num.Int256); ok {
			copy(v.buf[x:y], i256.Bytes())
		}
	case FieldTypeInt128:
		if i128, ok := val.(num.Int128); ok {
			copy(v.buf[x:y], i128.Bytes())
		}
	case FieldTypeDecimal256:
		if d256, ok := val.(num.Decimal256); ok {
			copy(v.buf[x:y], d256.Int256().Bytes())
		}
	case FieldTypeDecimal128:
		if d128, ok := val.(num.Decimal128); ok {
			copy(v.buf[x:y], d128.Int128().Bytes())
		}
	case FieldTypeDecimal64:
		if d64, ok := val.(num.Decimal64); ok {
			LE.PutUint64(v.buf[x:y], uint64(d64.Int64()))
		}
	case FieldTypeDecimal32:
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
		for i, f := range v.schema.fields {
			sz := f.typ.Size()
			if sz >= 0 && skip {
				ofs += sz
				continue
			}
			skip = false
			switch f.typ {
			case FieldTypeString, FieldTypeBytes:
				u32 := LE.Uint32(buf[ofs:])
				ofs += 4
				v.ofs[i] = ofs
				v.len[i] = int(u32)
				ofs += int(u32)
			default:
				v.ofs[i] = ofs
				v.len[i] = sz
				ofs += sz
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
