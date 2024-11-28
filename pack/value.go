// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc
//go:build ignore
// +build ignore

package pack

import (
	"encoding/binary"
	"time"

	"blockwatch.cc/knoxdb/encoding/decimal"
	"blockwatch.cc/knoxdb/util"
	"blockwatch.cc/knoxdb/vec"
)

type Value struct {
	fields FieldList
	buf    []byte
	ofs    []int
	len    []int
	minsz  int
	fixed  bool
}

func NewValue(fields FieldList) *Value {
	val := &Value{
		fields: fields,
		ofs:    make([]int, len(fields)),
		len:    make([]int, len(fields)),
		fixed:  true,
	}
	var ofs int
	for i, f := range fields {
		sz := f.Type.Len()
		switch {
		case !val.fixed:
			// set ofs to -1 for all fields following a dynamic length field
			val.ofs[i] = ofs
			val.len[i] = sz
			val.minsz++
		case sz < 0:
			// the first dynamic length field resets fixed flag, but keeps start offset
			val.fixed = false
			val.ofs[i] = ofs
			val.len[i] = sz
			val.minsz++
			ofs = -1
		default:
			val.ofs[i] = ofs
			val.len[i] = sz
			ofs += sz
			val.minsz += sz
		}
	}
	return val
}

func (v Value) IsValid() bool {
	return len(v.buf) >= v.minsz && len(v.fields) > 0
}

func (v Value) IsFixed() bool {
	return v.fixed
}

func (v Value) Get(i int) (val any, ok bool) {
	if i < 0 || i > len(v.fields) || !v.IsValid() {
		return
	}
	x, y := v.ofs[i], v.ofs[i]+v.len[i]
	switch v.fields[i].Type {
	case FieldTypeDatetime:
		val, ok = time.Unix(0, int64(bigEndian.Uint64(v.buf[x:y]))), true
	case FieldTypeInt64:
		val, ok = int64(bigEndian.Uint64(v.buf[x:y])), true
	case FieldTypeUint64:
		val, ok = bigEndian.Uint64(v.buf[x:y]), true
	case FieldTypeFloat64:
		val, ok = float64(bigEndian.Uint64(v.buf[x:y])), true
	case FieldTypeBoolean:
		val, ok = v.buf[x] > 0, true
	case FieldTypeString:
		val, ok = util.UnsafeGetString(v.buf[x:y]), true
	case FieldTypeBytes:
		val, ok = v.buf[x:y], true
	case FieldTypeInt32:
		val, ok = int32(bigEndian.Uint32(v.buf[x:y])), true
	case FieldTypeInt16:
		val, ok = int16(bigEndian.Uint16(v.buf[x:y])), true
	case FieldTypeInt8:
		val, ok = v.buf[x], true
	case FieldTypeUint32:
		val, ok = bigEndian.Uint32(v.buf[x:y]), true
	case FieldTypeUint16:
		val, ok = bigEndian.Uint16(v.buf[x:y]), true
	case FieldTypeUint8:
		val, ok = v.buf[x], true
	case FieldTypeFloat32:
		val, ok = float32(bigEndian.Uint32(v.buf[x:y])), true
	case FieldTypeInt256:
		val, ok = vec.Int256FromBytes(v.buf[x:y]), true
	case FieldTypeInt128:
		val, ok = vec.Int128FromBytes(v.buf[x:y]), true
	case FieldTypeDecimal256:
		val, ok = decimal.NewDecimal256(vec.Int256FromBytes(v.buf[x:y]), v.fields[i].Scale), true
	case FieldTypeDecimal128:
		val, ok = decimal.NewDecimal128(vec.Int128FromBytes(v.buf[x:y]), v.fields[i].Scale), true
	case FieldTypeDecimal64:
		val, ok = decimal.NewDecimal64(int64(bigEndian.Uint64(v.buf[x:y])), v.fields[i].Scale), true
	case FieldTypeDecimal32:
		val, ok = decimal.NewDecimal32(int32(bigEndian.Uint32(v.buf[x:y])), v.fields[i].Scale), true
	}
	return
}

func (v *Value) Reset(buf []byte) *Value {
	v.buf = nil
	if len(buf) < v.minsz {
		return v
	}
	v.buf = buf
	if !v.fixed {
		skip := true
		var ofs int
		for i, f := range v.fields {
			sz := f.Type.Len()
			if sz >= 0 && skip {
				ofs += sz
				continue
			}
			skip = false
			switch f.Type {
			case FieldTypeString, FieldTypeBytes:
				u64, n := binary.Uvarint(buf[ofs:])
				ofs += n
				v.ofs[i] = ofs
				v.len[i] = int(u64)
				ofs += int(u64)
			default:
				v.ofs[i] = ofs
				v.len[i] = sz
				ofs += sz
			}
		}
	}
	return v
}
