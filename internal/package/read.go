// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package pack

import (
	"bytes"
	"encoding"
	"math"
	"reflect"
	"sort"
	"time"
	"unsafe"

	"blockwatch.cc/knoxdb/pkg/assert"
	"blockwatch.cc/knoxdb/pkg/num"
	"blockwatch.cc/knoxdb/pkg/schema"
)

func (p *Package) CanRead(col, row int, typ schema.FieldType) bool {
	if col < 0 || len(p.blocks) <= col {
		return false
	}
	if row < 0 || p.nRows <= row {
		return false
	}
	f, ok := p.schema.FieldById(uint16(col))
	if !ok {
		return false
	}
	if f.Type() != typ {
		return false
	}
	if p.blocks[col].Type() != blockTypes[f.Type()] {
		return false
	}
	return true
}

func (p *Package) ReadWire(row int) ([]byte, error) {
	buf := bytes.NewBuffer(make([]byte, 0, p.schema.WireSize()+128))
	err := p.ReadWireBuffer(buf, row)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (p *Package) ReadWireBuffer(buf *bytes.Buffer, row int) error {
	assert.Always(row >= 0 && row < p.nRows, "invalid row",
		"row", row,
		"pack", p.key,
		// "schema", p.schema.Name(),
		// "version", p.schema.Version(),
	)

	for i, field := range p.schema.Exported() {
		// skipped and new blocks in old packages are missing
		b := p.blocks[i]
		if b == nil {
			continue
		}

		// deleted and internal fields are invisible
		if !field.IsVisible {
			continue
		}

		// encoding is based on field type
		var err error
		switch field.Type {
		case FieldTypeInt64, FieldTypeDatetime, FieldTypeDecimal64,
			FieldTypeUint64, FieldTypeFloat64:
			_, err = buf.Write(schema.Uint64Bytes(b.Uint64().Get(row)))
		case FieldTypeInt32, FieldTypeDecimal32, FieldTypeUint32, FieldTypeFloat32:
			_, err = buf.Write(schema.Uint32Bytes(b.Uint32().Get(row)))
		case FieldTypeInt16, FieldTypeUint16:
			_, err = buf.Write(schema.Uint16Bytes(b.Uint16().Get(row)))
		case FieldTypeInt8, FieldTypeUint8:
			_, err = buf.Write(schema.Uint8Bytes(b.Uint8().Get(row)))
		case FieldTypeBoolean:
			v := b.Bool().IsSet(row)
			err = buf.WriteByte(*(*byte)(unsafe.Pointer(&v)))
		case FieldTypeBytes, FieldTypeString:
			if fixed := field.Fixed; fixed > 0 {
				_, err = buf.Write(b.Bytes().Elem(row)[:fixed])
			} else {
				v := b.Bytes().Elem(row)
				_, err = buf.Write(schema.Uint32Bytes(uint32(len(v))))
				if err == nil {
					_, err = buf.Write(v)
				}
			}
		case FieldTypeInt256, FieldTypeDecimal256:
			_, err = buf.Write(b.Int256().Elem(row).Bytes())
		case FieldTypeInt128, FieldTypeDecimal128:
			_, err = buf.Write(b.Int128().Elem(row).Bytes())
		default:
			// oh, its a type we don't support yet
			assert.Unreachable("unhandled field type",
				"typeid", int(field.Type),
				"type", field.Type.String(),
				"field", field.Name,
				"pack", p.key,
				"schema", p.schema.Name(),
				"version", p.schema.Version(),
			)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// Reads package column data at row into custom struct dst. Target schema must be
// compatible to package schema (types must match), but may contain less fields.
// Maps defines the mapping of dst fields to source package columns.
func (p *Package) ReadStruct(row int, dst any, dstSchema *schema.Schema, maps []int) error {
	rval := reflect.Indirect(reflect.ValueOf(dst))
	assert.Always(rval.IsValid() && rval.Kind() == reflect.Struct, "invalid value",
		"kind", rval.Kind().String(),
		"type", rval.Type().String(),
	)
	assert.Always(dstSchema != nil, "nil target schema")
	assert.Always(maps != nil, "nil target mapping")

	var err error
	base := rval.Addr().UnsafePointer()
	for i, field := range dstSchema.Exported() {
		// identify source field
		srcId := maps[i]

		// skip unmapped fields
		if srcId < 0 {
			continue
		}

		// skip missing blocks (e.g. in old package versions)
		b := p.blocks[srcId]
		if b == nil {
			continue
		}

		// use unsafe.Add instead of reflect (except marshal types)
		fptr := unsafe.Add(base, field.Offset)

		switch field.Type {
		case FieldTypeInt64, FieldTypeUint64, FieldTypeFloat64:
			*(*uint64)(fptr) = b.Uint64().Get(row)

		case FieldTypeInt32, FieldTypeUint32, FieldTypeFloat32:
			*(*uint32)(fptr) = b.Uint32().Get(row)

		case FieldTypeInt16, FieldTypeUint16:
			*(*uint16)(fptr) = b.Uint16().Get(row)

		case FieldTypeInt8, FieldTypeUint8:
			*(*uint8)(fptr) = b.Uint8().Get(row)

		case FieldTypeDatetime:
			(*(*time.Time)(fptr)) = time.Unix(0, b.Int64().Get(row)).UTC()

		case FieldTypeBoolean:
			*(*bool)(fptr) = b.Bool().IsSet(row)

		case FieldTypeBytes:
			switch {
			case field.Iface&schema.IfaceBinaryUnmarshaler > 0:
				rfield := field.StructValue(rval)
				err = rfield.Addr().Interface().(encoding.BinaryUnmarshaler).UnmarshalBinary(b.Bytes().Elem(row))
			case field.IsArray:
				copy(unsafe.Slice((*byte)(fptr), field.Fixed), b.Bytes().Elem(row))
			default:
				b := b.Bytes().Elem(row)
				if cap(*(*[]byte)(fptr)) < len(b) {
					*(*[]byte)(fptr) = make([]byte, len(b))
				} else {
					*(*[]byte)(fptr) = (*(*[]byte)(fptr))[:len(b)]
				}
				copy(*(*[]byte)(fptr), b)
			}

		case FieldTypeString:
			switch {
			case field.Iface&schema.IfaceTextUnmarshaler > 0:
				rfield := field.StructValue(rval)
				err = rfield.Addr().Interface().(encoding.TextUnmarshaler).UnmarshalText(b.Bytes().Elem(row))
			default:
				*(*string)(fptr) = string(b.Bytes().Elem(row))
			}

		case FieldTypeInt256:
			*(*num.Int256)(fptr) = b.Int256().Elem(row)

		case FieldTypeInt128:
			*(*num.Int128)(fptr) = b.Int128().Elem(row)

		case FieldTypeDecimal256:
			(*(*num.Decimal256)(fptr)).Set(b.Int256().Elem(row))
			(*(*num.Decimal256)(fptr)).SetScale(field.Scale)

		case FieldTypeDecimal128:
			(*(*num.Decimal128)(fptr)).Set(b.Int128().Elem(row))
			(*(*num.Decimal128)(fptr)).SetScale(field.Scale)

		case FieldTypeDecimal64:
			(*(*num.Decimal64)(fptr)).Set(b.Int64().Get(row))
			(*(*num.Decimal64)(fptr)).SetScale(field.Scale)

		case FieldTypeDecimal32:
			(*(*num.Decimal32)(fptr)).Set(b.Int32().Get(row))
			(*(*num.Decimal32)(fptr)).SetScale(field.Scale)

		default:
			// oh, its a type we don't support yet
			assert.Unreachable("unhandled value type",
				"field", field.Name,
				"type", field.Type.String(),
				"pack", p.key,
				"schema", p.schema.Name(),
				"version", p.schema.Version(),
			)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// Reads a single value at postion col,row. Replaces FieldAt() used in join
func (p *Package) ReadValue(field *schema.ExportedField, col, row int) any {
	// assert.Always(col >= 0 && col < len(p.blocks), "invalid block id", map[string]any{
	// 	"id":      col,
	// 	"pack":    p.key,
	// 	"schema":  p.schema.Name(),
	// 	"version": p.schema.Version(),
	// 	"nFields": p.schema.NumFields(),
	// 	"nBlocks": len(p.blocks),
	// })
	// assert.Always(row >= 0 && row < p.nRows, "invalid row", map[string]any{
	// 	"row":     row,
	// 	"pack":    p.key,
	// 	"schema":  p.schema.Name(),
	// 	"version": p.schema.Version(),
	// })
	b := p.blocks[col]

	switch field.Type {
	case FieldTypeInt64:
		return b.Int64().Get(row)
	case FieldTypeInt32:
		return b.Int32().Get(row)
	case FieldTypeInt16:
		return b.Int16().Get(row)
	case FieldTypeInt8:
		return b.Int8().Get(row)
	case FieldTypeUint64:
		return b.Uint64().Get(row)
	case FieldTypeUint32:
		return b.Uint32().Get(row)
	case FieldTypeUint16:
		return b.Uint16().Get(row)
	case FieldTypeUint8:
		return b.Uint8().Get(row)
	case FieldTypeFloat64:
		return b.Float64().Get(row)
	case FieldTypeFloat32:
		return b.Float32().Get(row)
	case FieldTypeDatetime:
		if ts := b.Int64().Get(row); ts > 0 {
			return time.Unix(0, ts).UTC()
		} else {
			return zeroTime
		}
	case FieldTypeBoolean:
		return b.Bool().IsSet(row)
	case FieldTypeBytes:
		return b.Bytes().Elem(row)
	case FieldTypeString:
		return UnsafeGetString(b.Bytes().Elem(row))
	case FieldTypeInt256:
		return b.Int256().Elem(row)
	case FieldTypeInt128:
		return b.Int128().Elem(row)
	case FieldTypeDecimal256:
		return num.NewDecimal256(b.Int256().Elem(row), field.Scale)
	case FieldTypeDecimal128:
		return num.NewDecimal128(b.Int128().Elem(row), field.Scale)
	case FieldTypeDecimal64:
		return num.NewDecimal64(b.Int64().Get(row), field.Scale)
	case FieldTypeDecimal32:
		return num.NewDecimal32(b.Int32().Get(row), field.Scale)
	default:
		// oh, its a type we don't support yet
		assert.Unreachable("unhandled field type", map[string]any{
			"typeid":  int(field.Type),
			"type":    field.Type.String(),
			"field":   field.Name,
			"pack":    p.key,
			"schema":  p.schema.Name(),
			"version": p.schema.Version(),
		})
	}
	return nil
}

// Reads a single row into a slice of interfaces.
// Replaces RowAt() used for debug only.
func (p *Package) ReadRow(row int, dst []any) []any {
	assert.Always(row >= 0 && row < p.nRows, "invalid row",
		"row", row,
		"pack", p.key,
		"schema", p.schema.Name(),
		"version", p.schema.Version(),
	)
	assert.Always(len(p.blocks) == p.schema.NumFields(), "block mismatch",
		"pack", p.key,
		"schema", p.schema.Name(),
		"version", p.schema.Version(),
		"nFields", p.schema.NumFields(),
		"nBlocks", len(p.blocks),
	)
	// copy one full row of values
	maxFields := p.schema.NumFields()
	if cap(dst) < maxFields {
		dst = make([]any, 0, maxFields)
	} else {
		dst = dst[:maxFields]
	}
	for i, field := range p.schema.Exported() {
		// skip blocks when not selected or missing (e.g. old package versions)
		b := p.blocks[i]
		if b == nil {
			continue
		}
		// deleted and internal fields are invisible
		if !field.IsVisible {
			continue
		}
		// add to result
		dst = append(dst, p.ReadValue(field, i, row))
	}
	return dst
}

// Returns a single materialized column as typed slice.
// Replaces Column()
func (p *Package) ReadCol(col int) any {
	f, ok := p.schema.FieldById(uint16(col))
	assert.Always(ok, "invalid field id",
		"id", col,
		"pack", p.key,
		"schema", p.schema.Name(),
		"version", p.schema.Version(),
		"nFields", p.schema.NumFields(),
		"nBlocks", len(p.blocks),
	)
	assert.Always(col >= 0 && col < len(p.blocks), "invalid block id",
		"id", col,
		"pack", p.key,
		"schema", p.schema.Name(),
		"version", p.schema.Version(),
		"nFields", p.schema.NumFields(),
		"nBlocks", len(p.blocks),
	)
	// skipped and new blocks in old packages are missing
	b := p.blocks[col]
	if b == nil {
		return nil
	}
	// deleted and internal fields are invisible
	if !f.IsVisible() {
		return nil
	}

	switch f.Type() {
	case FieldTypeInt64:
		return b.Int64().Slice()
	case FieldTypeInt32:
		return b.Int32().Slice()
	case FieldTypeInt16:
		return b.Int16().Slice()
	case FieldTypeInt8:
		return b.Int8().Slice()
	case FieldTypeUint64:
		return b.Uint64().Slice()
	case FieldTypeUint32:
		return b.Uint32().Slice()
	case FieldTypeUint16:
		return b.Uint16().Slice()
	case FieldTypeUint8:
		return b.Uint8().Slice()
	case FieldTypeFloat64:
		return b.Float64().Slice()
	case FieldTypeFloat32:
		return b.Float32().Slice()
	case FieldTypeBytes:
		return b.Bytes().Slice()
	case FieldTypeString:
		s := make([]string, p.nRows)
		for i := 0; i < p.nRows; i++ {
			s[i] = UnsafeGetString(b.Bytes().Elem(i))
		}
		return s
	case FieldTypeInt256:
		return b.Int256().Materialize()
	case FieldTypeInt128:
		return b.Int128().Materialize()
	case FieldTypeDatetime:
		res := make([]time.Time, p.nRows)
		for i, v := range b.Int64().Slice() {
			if v > 0 {
				res[i] = time.Unix(0, v).UTC()
			} else {
				res[i] = zeroTime
			}
		}
		return res
	case FieldTypeBoolean:
		return b.Bool().Slice()
	case FieldTypeDecimal256:
		return num.Decimal256Slice{Int256: b.Int256().Materialize(), Scale: f.Scale()}
	case FieldTypeDecimal128:
		return num.Decimal128Slice{Int128: b.Int128().Materialize(), Scale: f.Scale()}
	case FieldTypeDecimal64:
		return num.Decimal64Slice{Int64: b.Int64().Slice(), Scale: f.Scale()}
	case FieldTypeDecimal32:
		return num.Decimal32Slice{Int32: b.Int32().Slice(), Scale: f.Scale()}
	default:
		// oh, its a type we don't support yet
		assert.Unreachable("unhandled field type",
			"typeid", int(f.Type()),
			"type", f.Type().String(),
			"field", f.Name(),
			"pack", p.key,
			"schema", p.schema.Name(),
			"version", p.schema.Version(),
		)
	}
	return nil
}

func (p *Package) Uint64(col, row int) uint64 {
	return p.blocks[col].Uint64().Get(row)
}

func (p *Package) Uint32(col, row int) uint32 {
	return p.blocks[col].Uint32().Get(row)
}

func (p *Package) Uint16(col, row int) uint16 {
	return p.blocks[col].Uint16().Get(row)
}

func (p *Package) Uint8(col, row int) uint8 {
	return p.blocks[col].Uint8().Get(row)
}

func (p *Package) Int64(col, row int) int64 {
	return p.blocks[col].Int64().Get(row)
}

func (p *Package) Int32(col, row int) int32 {
	return p.blocks[col].Int32().Get(row)
}

func (p *Package) Int16(col, row int) int16 {
	return p.blocks[col].Int16().Get(row)
}

func (p *Package) Int8(col, row int) int8 {
	return p.blocks[col].Int8().Get(row)
}

func (p *Package) Float64(col, row int) float64 {
	return p.blocks[col].Float64().Get(row)
}

func (p *Package) Float32(col, row int) float32 {
	return p.blocks[col].Float32().Get(row)
}

func (p *Package) String(col, row int) string {
	return UnsafeGetString(p.blocks[col].Bytes().Elem(row))
}

func (p *Package) Bytes(col, row int) []byte {
	return p.blocks[col].Bytes().Elem(row)
}

func (p *Package) Bool(col, row int) bool {
	return p.blocks[col].Bool().IsSet(row)
}

func (p *Package) Time(col, row int) time.Time {
	if ts := p.blocks[col].Int64().Get(row); ts > 0 {
		return time.Unix(0, ts).UTC()
	} else {
		return zeroTime
	}
}

func (p *Package) Int256(col, row int) num.Int256 {
	return p.blocks[col].Int256().Elem(row)
}

func (p *Package) Int128(col, row int) num.Int128 {
	return p.blocks[col].Int128().Elem(row)
}

func (p *Package) Decimal256(col, row int) num.Decimal256 {
	f, _ := p.schema.FieldById(uint16(col))
	return num.NewDecimal256(p.blocks[col].Int256().Elem(row), f.Scale())
}

func (p *Package) Decimal128(col, row int) num.Decimal128 {
	f, _ := p.schema.FieldById(uint16(col))
	return num.NewDecimal128(p.blocks[col].Int128().Elem(row), f.Scale())
}

func (p *Package) Decimal64(col, row int) num.Decimal64 {
	f, _ := p.schema.FieldById(uint16(col))
	return num.NewDecimal64(p.blocks[col].Int64().Get(row), f.Scale())
}

func (p *Package) Decimal32(col, row int) num.Decimal32 {
	f, _ := p.schema.FieldById(uint16(col))
	return num.NewDecimal32(p.blocks[col].Int32().Get(row), f.Scale())
}

func (p *Package) IsZeroValue(col, row int, zeroIsNull bool) bool {
	f, ok := p.schema.FieldById(uint16(col))
	assert.Always(ok, "invalid field id",
		"id", col,
		"pack", p.key,
		"schema", p.schema.Name(),
		"version", p.schema.Version(),
		"nFields", p.schema.NumFields(),
		"nBlocks", len(p.blocks),
	)
	assert.Always(col >= 0 && col < len(p.blocks), "invalid block id",
		"id", col,
		"pack", p.key,
		"schema", p.schema.Name(),
		"version", p.schema.Version(),
		"nFields", p.schema.NumFields(),
		"nBlocks", len(p.blocks),
	)
	assert.Always(row >= 0 && row < p.nRows, "invalid row",
		"row", row,
		"pack", p.key,
		"schema", p.schema.Name(),
		"version", p.schema.Version(),
	)

	switch f.Type() {
	case FieldTypeInt256, schema.FieldTypeDecimal256:
		return zeroIsNull && p.Int256(col, row).IsZero()
	case FieldTypeInt128, schema.FieldTypeDecimal128:
		return zeroIsNull && p.Int128(col, row).IsZero()
	case FieldTypeInt64, schema.FieldTypeDecimal64:
		return zeroIsNull && p.Int64(col, row) == 0
	case FieldTypeInt32, schema.FieldTypeDecimal32:
		return zeroIsNull && p.Int32(col, row) == 0
	case FieldTypeInt16:
		return zeroIsNull && p.Int16(col, row) == 0
	case FieldTypeInt8:
		return zeroIsNull && p.Int8(col, row) == 0
	case FieldTypeUint64:
		return zeroIsNull && p.Uint64(col, row) == 0
	case FieldTypeUint32:
		return zeroIsNull && p.Uint32(col, row) == 0
	case FieldTypeUint16:
		return zeroIsNull && p.Uint16(col, row) == 0
	case FieldTypeUint8:
		return zeroIsNull && p.Uint8(col, row) == 0
	case FieldTypeBoolean:
		return zeroIsNull && !p.Bool(col, row)
	case FieldTypeFloat64:
		v := p.Float64(col, row)
		return math.IsNaN(v) || math.IsInf(v, 0) || (zeroIsNull && v == 0.0)
	case FieldTypeFloat32:
		v := float64(p.Float32(col, row))
		return math.IsNaN(v) || math.IsInf(v, 0) || (zeroIsNull && v == 0.0)
	case FieldTypeString, schema.FieldTypeBytes:
		return len(p.Bytes(col, row)) == 0
	case FieldTypeDatetime:
		val := p.Int64(col, row)
		return val == 0 || (zeroIsNull && time.Unix(0, val).IsZero())
	}
	return false
}

func (p *Package) PkColumn() []uint64 {
	assert.Always(p.pkIdx >= 0 && p.pkIdx < len(p.blocks), "invalid pk id",
		"pkIdx", p.pkIdx,
		"pack", p.key,
		"schema", p.schema.Name(),
		"version", p.schema.Version(),
		"nFields", p.schema.NumFields(),
		"nBlocks", len(p.blocks),
	)
	return p.blocks[p.pkIdx].Uint64().Slice()
}

// Searches id in primary key column and returns pos or -1 when not found.
// This function is only safe to use when pack is sorted by pk (gaps allowed)!
func (p *Package) FindPk(id uint64, last int) (int, int) {
	assert.Always(p.pkIdx >= 0 && p.pkIdx < len(p.blocks), "invalid pk id",
		"pkIdx", p.pkIdx,
		"pack", p.key,
		"schema", p.schema.Name(),
		"version", p.schema.Version(),
		"nFields", p.schema.NumFields(),
		"nBlocks", len(p.blocks),
	)

	// primary key field required
	// if p.pkIdx < 0 || last >= p.nRows {
	if last >= p.nRows {
		return -1, p.nRows
	}

	// search for id value in pk block (always an uint64) starting at last index
	// this helps limiting search space when ids are pre-sorted
	slice := p.blocks[p.pkIdx].Uint64().Slice()[last:]
	l := len(slice)

	// for sparse pk spaces, use binary search on sorted slices
	idx := sort.Search(l, func(i int) bool { return slice[i] >= id })
	last += idx
	if idx < l && slice[idx] == id {
		return last, last
	}
	return -1, last
}

// FindPkUnsorted searches id in primary key column and returns pos or -1 when not found.
// This function slower than FindPkSorted, but can be used of pack is unsorted, e.g.
// when updates/inserts are out of order.
func (p *Package) FindPkUnsorted(id uint64, last int) int {
	assert.Always(p.pkIdx >= 0 && p.pkIdx < len(p.blocks), "invalid pk id",
		"pkIdx", p.pkIdx,
		"pack", p.key,
		"schema", p.schema.Name(),
		"version", p.schema.Version(),
		"nFields", p.schema.NumFields(),
		"nBlocks", len(p.blocks),
	)

	// primary key field required
	// if p.pkIdx < 0 || p.nRows <= last {
	if p.nRows <= last {
		return -1
	}

	// search for id value in pk block (always an uint64) starting at last index
	// this helps limiting search space when ids are pre-sorted
	slice := p.blocks[p.pkIdx].Uint64().Slice()[last:]

	// run full scan on unsorted slices
	for i, v := range slice {
		if v != id {
			continue
		}
		return i + last
	}
	return -1
}
