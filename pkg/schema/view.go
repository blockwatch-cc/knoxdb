// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package schema

import (
	"encoding/binary"
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
	fixed  bool
}

func NewView(s *Schema) *View {
	view := &View{
		schema: s,
		ofs:    make([]int, len(s.fields)),
		len:    make([]int, len(s.fields)),
	}
	var ofs int
	for i, f := range s.fields {
		sz := f.typ.Size()
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

func (v *View) Reset(buf []byte) *View {
	v.buf = nil
	if len(buf) < v.minsz {
		return v
	}
	v.buf = buf
	if !v.fixed {
		skip := true
		var ofs int
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
	}
	return v
}
