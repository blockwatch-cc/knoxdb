// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package pack

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"time"
	"unsafe"

	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/assert"
	"blockwatch.cc/knoxdb/pkg/num"
	"blockwatch.cc/knoxdb/pkg/schema"
	"blockwatch.cc/knoxdb/pkg/schema/reflect"
	"blockwatch.cc/knoxdb/pkg/util"
)

func (p *Package) ReadWire(row int) []byte {
	buf := bytes.NewBuffer(make([]byte, 0, p.schema.EstWireSize))
	p.ReadWireBuffer(buf, row)
	return buf.Bytes()
}

// Extract a change set from selected columns, used in WAL update mode.
func (p *Package) ReadWireFields(buf *bytes.Buffer, row int, cols []int) {
	for _, v := range cols {
		var (
			b     = p.blocks[v]
			field = p.schema.Fields[v]
			x     [8]byte
		)
		switch b.Type() {
		case types.BlockUint64:
			LE.PutUint64(x[:], b.Uint64().Get(row))
			buf.Write(x[:])
		case types.BlockInt64:
			LE.PutUint64(x[:], uint64(b.Int64().Get(row)))
			buf.Write(x[:])
		case types.BlockFloat64:
			LE.PutUint64(x[:], math.Float64bits(b.Float64().Get(row)))
			buf.Write(x[:])
		case types.BlockUint32:
			LE.PutUint32(x[:], b.Uint32().Get(row))
			buf.Write(x[:4])
		case types.BlockInt32:
			LE.PutUint32(x[:], uint32(b.Int32().Get(row)))
			buf.Write(x[:4])
		case types.BlockFloat32:
			LE.PutUint32(x[:], math.Float32bits(b.Float32().Get(row)))
			buf.Write(x[:4])
		case types.BlockUint16:
			LE.PutUint16(x[:], b.Uint16().Get(row))
			buf.Write(x[:2])
		case types.BlockInt16:
			LE.PutUint16(x[:], uint16(b.Int16().Get(row)))
			buf.Write(x[:2])
		case types.BlockUint8:
			buf.WriteByte(b.Uint8().Get(row))
		case types.BlockInt8:
			buf.WriteByte(uint8(b.Int8().Get(row)))
		case types.BlockBool:
			v := b.Bool().Get(row)
			buf.WriteByte(*(*byte)(unsafe.Pointer(&v)))
		case types.BlockBytes:
			v := b.Bytes().Get(row)
			switch field.Type {
			case types.FT_BYTES, types.FT_STRING, types.FT_BIGINT:
				if field.IsArray() {
					// length in schema
					buf.Write(v[:field.Scale])
				} else {
					// 1 byte length
					buf.WriteByte(byte(len(v)))
					buf.Write(v)
				}
			default:
				// 4 byte length
				LE.PutUint32(x[:], uint32(len(v)))
				buf.Write(x[:4])
			}

		case types.BlockInt256:
			buf.Write(b.Int256().Get(row).Bytes())
		case types.BlockInt128:
			buf.Write(b.Int128().Get(row).Bytes())
		default:
			// oh, its a type we don't support yet
			assert.Unreachable("unhandled field type",
				"typeid", int(field.Type),
				"type", b.Type().String(),
				"field", field.Name,
				"pack", p.key,
				"schema", p.schema.Name,
				"version", p.schema.Version,
			)
		}
	}
}

var (
	zeros [32]byte
	LE    = binary.LittleEndian // values
)

func (p *Package) ReadWireBuffer(buf *bytes.Buffer, row int) {
	assert.Always(row >= 0 && row < p.nRows, "invalid row",
		"row", row,
		"pack", p.key,
		"schema", p.schema.Name,
		"version", p.schema.Version,
	)

	for i, field := range p.schema.Fields {
		// skip deleted and internal fields
		if !field.IsVisible() {
			continue
		}

		// insert zero value when block is not available (e.g. after schema change)
		b := p.blocks[i]
		if b == nil {
			for sz := field.WireSize(); sz > 0; sz -= 32 {
				buf.Write(zeros[:min(sz, 32)])
			}
			continue
		}

		// encoding is based on field type
		var x [8]byte
		switch b.Type() {
		case types.BlockUint64:
			LE.PutUint64(x[:], b.Uint64().Get(row))
			buf.Write(x[:])
		case types.BlockInt64:
			LE.PutUint64(x[:], uint64(b.Int64().Get(row)))
			buf.Write(x[:])
		case types.BlockFloat64:
			LE.PutUint64(x[:], math.Float64bits(b.Float64().Get(row)))
			buf.Write(x[:])
		case types.BlockUint32:
			LE.PutUint32(x[:], b.Uint32().Get(row))
			buf.Write(x[:4])
		case types.BlockInt32:
			LE.PutUint32(x[:], uint32(b.Int32().Get(row)))
			buf.Write(x[:4])
		case types.BlockFloat32:
			LE.PutUint32(x[:], math.Float32bits(b.Float32().Get(row)))
			buf.Write(x[:4])
		case types.BlockUint16:
			LE.PutUint16(x[:], b.Uint16().Get(row))
			buf.Write(x[:2])
		case types.BlockInt16:
			LE.PutUint16(x[:], uint16(b.Int16().Get(row)))
			buf.Write(x[:2])
		case types.BlockUint8:
			buf.WriteByte(b.Uint8().Get(row))
		case types.BlockInt8:
			buf.WriteByte(uint8(b.Int8().Get(row)))
		case types.BlockBool:
			v := b.Bool().Get(row)
			buf.WriteByte(*(*byte)(unsafe.Pointer(&v)))
		case types.BlockBytes:
			v := b.Bytes().Get(row)
			switch field.Type {
			case types.FT_BYTES, types.FT_STRING, types.FT_BIGINT:
				if field.IsArray() {
					buf.Write(v[:field.Scale])
				} else {
					// 1 byte length
					buf.WriteByte(byte(len(v)))
					buf.Write(v)
				}
			default:
				// 4 byte length
				LE.PutUint32(x[:], uint32(len(v)))
				buf.Write(x[:4])
				buf.Write(v)
			}
		case types.BlockInt256:
			buf.Write(b.Int256().Get(row).Bytes())
		case types.BlockInt128:
			buf.Write(b.Int128().Get(row).Bytes())
		default:
			// oh, its a type we don't support yet
			assert.Unreachable("unhandled field type",
				"typeid", int(field.Type),
				"type", b.Type().String(),
				"field", field.Name,
				"pack", p.key,
				"schema", p.schema.Name,
				"version", p.schema.Version,
			)
		}
	}
}

// Reads package column data at row into custom struct dst. Target schema must be
// compatible to package schema (types must match), but may contain less fields.
// Maps defines the mapping of dst fields to source package columns.
func (p *Package) ReadStruct(row int, dst any, dstSchema *schema.Schema, dstLayout *reflect.Layout, maps []int) error {
	assert.Always(dstSchema != nil, "nil target schema")
	assert.Always(maps != nil, "nil target mapping")
	assert.Always(row >= 0, "negative row index")

	// extract the pointer inside the dst interface
	base := util.UnboxAny(dst)

	for i, field := range dstSchema.Fields {
		// identify source field
		srcId := maps[i]

		// skip unmapped fields
		if srcId < 0 {
			continue
		}

		// use unsafe.Add instead of reflect
		fptr := unsafe.Add(base, dstLayout.Offsets[i])

		// insert zero value when block is not available (e.g. after schema change)
		b := p.blocks[srcId]
		if b == nil {
			if !field.IsEnum() {
				sz := field.WireSize()
				buf := unsafe.Slice((*byte)(fptr), sz)

				// loop copy 32 zeros (some fixed types may be larger)
				for sz > 0 {
					copy(buf, zeros[:])
					buf = buf[min(sz, 32):]
					sz -= 32
				}
			}
			continue
		}

		switch field.Type {
		case types.FT_I64:
			*(*int64)(fptr) = b.Int64().Get(row)

		case types.FT_U64:
			*(*uint64)(fptr) = b.Uint64().Get(row)

		case types.FT_F64:
			*(*float64)(fptr) = b.Float64().Get(row)

		case types.FT_I32:
			*(*int32)(fptr) = b.Int32().Get(row)

		case types.FT_U32:
			*(*uint32)(fptr) = b.Uint32().Get(row)

		case types.FT_F32:
			*(*float32)(fptr) = b.Float32().Get(row)

		case types.FT_I16:
			*(*int16)(fptr) = b.Int16().Get(row)

		case types.FT_U16:
			if field.IsEnum() {
				u16 := b.Uint16().Get(row)
				val, ok := field.Enum.Value(u16)
				if !ok {
					return fmt.Errorf("%s: invalid enum value %d", field.Name, u16)
				}
				*(*string)(fptr) = val // FIXME: may break when enum dict grows
			} else {
				*(*uint16)(fptr) = b.Uint16().Get(row)
			}

		case types.FT_I8:
			*(*int8)(fptr) = b.Int8().Get(row)

		case types.FT_U8:
			*(*uint8)(fptr) = b.Uint8().Get(row)

		case types.FT_TIMESTAMP, types.FT_DATE, types.FT_TIME:
			(*(*time.Time)(fptr)) = types.TimeScale(field.Scale).FromUnix(b.Int64().Get(row))

		case types.FT_BOOL:
			*(*bool)(fptr) = b.Bool().Get(row)

		case types.FT_BYTES, types.FT_BLOB:
			if field.IsArray() {
				copy(unsafe.Slice((*byte)(fptr), field.Scale), b.Bytes().Get(row))
			} else {
				// safe version with copy (check length of struct in slice or alloc
				// then copy)
				// b := b.Bytes().Get(row)
				// if cap(*(*[]byte)(fptr)) < len(b) {
				// 	*(*[]byte)(fptr) = make([]byte, len(b))
				// } else {
				// 	*(*[]byte)(fptr) = (*(*[]byte)(fptr))[:len(b)]
				// }
				// copy(*(*[]byte)(fptr), b)
				*(*[]byte)(fptr) = b.Bytes().Get(row)
			}

		case types.FT_STRING, types.FT_TEXT:
			// safe version with copy
			// *(*string)(fptr) = string(b.Bytes().Get(row))

			// unsafe zero-copy
			*(*string)(fptr) = util.UnsafeGetString(b.Bytes().Get(row))

		case types.FT_I256:
			*(*num.Int256)(fptr) = b.Int256().Get(row)

		case types.FT_I128:
			*(*num.Int128)(fptr) = b.Int128().Get(row)

		case types.FT_D256:
			(*(*num.Decimal256)(fptr)).Set(b.Int256().Get(row))
			(*(*num.Decimal256)(fptr)).SetScale(field.Scale)

		case types.FT_D128:
			(*(*num.Decimal128)(fptr)).Set(b.Int128().Get(row))
			(*(*num.Decimal128)(fptr)).SetScale(field.Scale)

		case types.FT_D64:
			(*(*num.Decimal64)(fptr)).Set(b.Int64().Get(row))
			(*(*num.Decimal64)(fptr)).SetScale(field.Scale)

		case types.FT_D32:
			(*(*num.Decimal32)(fptr)).Set(b.Int32().Get(row))
			(*(*num.Decimal32)(fptr)).SetScale(field.Scale)

		case types.FT_BIGINT:
			(*(*num.Big)(fptr)).SetBytes(b.Bytes().Get(row))

		default:
			// oh, its a type we don't support yet
			assert.Unreachable("unhandled value type",
				"field", field.Name,
				"type", field.Type.String(),
				"pack", p.key,
				"schema", p.schema.Name,
				"version", p.schema.Version,
			)
		}
	}
	return nil
}

// ForEach walks a pack decoding each row into type T. If T is invalid (not
// a struct type) or incompatible with the packs schema an error is returned.
func ForEach[T any](pkg *Package, fn func(i int, v *T) error) error {
	dst, err := reflect.SchemaFor[T]()
	if err != nil {
		return err
	}
	layout, err := reflect.LayoutFor[T]()
	if err != nil {
		return err
	}
	if !pkg.schema.ContainsSchema(dst) {
		return schema.ErrSchemaMismatch
	}
	maps, err := pkg.schema.MapSchema(dst)
	if err != nil {
		return err
	}
	var t T
	for i := range pkg.nRows {
		if err := pkg.ReadStruct(i, &t, dst, layout, maps); err != nil {
			return err
		}
		if err := fn(i, &t); err != nil {
			if err == types.EndStream {
				break
			}
			return err
		}
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
	return util.UnsafeGetString(p.blocks[col].Bytes().Get(row))
}

func (p *Package) Bytes(col, row int) []byte {
	return p.blocks[col].Bytes().Get(row)
}

func (p *Package) Bool(col, row int) bool {
	return p.blocks[col].Bool().Get(row)
}

func (p *Package) Time(col, row int) time.Time {
	if ts := p.blocks[col].Int64().Get(row); ts > 0 {
		return types.TimeScale(p.schema.Fields[col].Scale).FromUnix(ts)
	} else {
		return zeroTime
	}
}

func (p *Package) Int256(col, row int) num.Int256 {
	return p.blocks[col].Int256().Get(row)
}

func (p *Package) Int128(col, row int) num.Int128 {
	return p.blocks[col].Int128().Get(row)
}

func (p *Package) Decimal256(col, row int) num.Decimal256 {
	return num.NewDecimal256(p.blocks[col].Int256().Get(row), p.schema.Fields[col].Scale)
}

func (p *Package) Decimal128(col, row int) num.Decimal128 {
	return num.NewDecimal128(p.blocks[col].Int128().Get(row), p.schema.Fields[col].Scale)
}

func (p *Package) Decimal64(col, row int) num.Decimal64 {
	return num.NewDecimal64(p.blocks[col].Int64().Get(row), p.schema.Fields[col].Scale)
}

func (p *Package) Decimal32(col, row int) num.Decimal32 {
	return num.NewDecimal32(p.blocks[col].Int32().Get(row), p.schema.Fields[col].Scale)
}

func (p *Package) Big(col, row int) num.Big {
	return num.NewBigFromBytes(p.blocks[col].Bytes().Get(row))
}
