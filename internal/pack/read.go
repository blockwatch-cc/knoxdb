// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package pack

import (
	"bytes"
	"encoding"
	"encoding/binary"
	"fmt"
	"math"
	"reflect"
	"time"
	"unsafe"

	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/assert"
	"blockwatch.cc/knoxdb/pkg/num"
	"blockwatch.cc/knoxdb/pkg/schema"
	"blockwatch.cc/knoxdb/pkg/util"
)

func (p *Package) ReadWire(row int) ([]byte, error) {
	buf := bytes.NewBuffer(make([]byte, 0, p.schema.WireSize()+128))
	err := p.ReadWireBuffer(buf, row)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Extract a change set from selected columns, used in WAL update mode.
func (p *Package) ReadWireFields(buf *bytes.Buffer, row int, cols []int) error {
	for _, v := range cols {
		var (
			b   = p.blocks[v]
			f   = p.schema.Field(v)
			x   [8]byte
			err error
		)
		switch b.Type() {
		case types.BlockUint64:
			LE.PutUint64(x[:], b.Uint64().Get(row))
			_, err = buf.Write(x[:])
		case types.BlockInt64:
			LE.PutUint64(x[:], uint64(b.Int64().Get(row)))
			_, err = buf.Write(x[:])
		case types.BlockFloat64:
			LE.PutUint64(x[:], math.Float64bits(b.Float64().Get(row)))
			_, err = buf.Write(x[:])
		case types.BlockUint32:
			LE.PutUint32(x[:], b.Uint32().Get(row))
			_, err = buf.Write(x[:4])
		case types.BlockInt32:
			LE.PutUint32(x[:], uint32(b.Int32().Get(row)))
			_, err = buf.Write(x[:4])
		case types.BlockFloat32:
			LE.PutUint32(x[:], math.Float32bits(b.Float32().Get(row)))
			_, err = buf.Write(x[:4])
		case types.BlockUint16:
			LE.PutUint16(x[:], b.Uint16().Get(row))
			_, err = buf.Write(x[:2])
		case types.BlockInt16:
			LE.PutUint16(x[:], uint16(b.Int16().Get(row)))
			_, err = buf.Write(x[:2])
		case types.BlockUint8:
			_, err = buf.Write([]byte{b.Uint8().Get(row)})
		case types.BlockInt8:
			_, err = buf.Write([]byte{uint8(b.Int8().Get(row))})
		case types.BlockBool:
			v := b.Bool().Get(row)
			err = buf.WriteByte(*(*byte)(unsafe.Pointer(&v)))
		case types.BlockBytes:
			if fixed := f.Fixed(); fixed > 0 {
				_, err = buf.Write(b.Bytes().Get(row)[:fixed])
			} else {
				v := b.Bytes().Get(row)
				LE.PutUint32(x[:], uint32(len(v)))
				_, err = buf.Write(x[:4])
				if err == nil {
					_, err = buf.Write(v)
				}
			}
		case types.BlockInt256:
			_, err = buf.Write(b.Int256().Get(row).Bytes())
		case types.BlockInt128:
			_, err = buf.Write(b.Int128().Get(row).Bytes())
		default:
			// oh, its a type we don't support yet
			assert.Unreachable("unhandled field type",
				"typeid", int(f.Type()),
				"type", b.Type().String(),
				"field", f.Name(),
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

var (
	zeros [32]byte
	LE    = binary.LittleEndian // values
)

func (p *Package) ReadWireBuffer(buf *bytes.Buffer, row int) error {
	assert.Always(row >= 0 && row < p.nRows, "invalid row",
		"row", row,
		"pack", p.key,
		"schema", p.schema.Name(),
		"version", p.schema.Version(),
	)

	for i, field := range p.schema.Exported() {
		// skip deleted and internal fields
		if !field.IsVisible {
			continue
		}

		// insert zero value when block is not available (e.g. after schema change)
		var err error
		b := p.blocks[i]
		if b == nil {
			for sz := field.WireSize(); sz > 0 && err == nil; sz -= 32 {
				_, err = buf.Write(zeros[:min(sz, 32)])
			}
			if err != nil {
				return err
			}
			continue
		}

		// encoding is based on field type
		var x [8]byte
		switch b.Type() {
		case types.BlockUint64:
			LE.PutUint64(x[:], b.Uint64().Get(row))
			_, err = buf.Write(x[:])
		case types.BlockInt64:
			LE.PutUint64(x[:], uint64(b.Int64().Get(row)))
			_, err = buf.Write(x[:])
		case types.BlockFloat64:
			LE.PutUint64(x[:], math.Float64bits(b.Float64().Get(row)))
			_, err = buf.Write(x[:])
		case types.BlockUint32:
			LE.PutUint32(x[:], b.Uint32().Get(row))
			_, err = buf.Write(x[:4])
		case types.BlockInt32:
			LE.PutUint32(x[:], uint32(b.Int32().Get(row)))
			_, err = buf.Write(x[:4])
		case types.BlockFloat32:
			LE.PutUint32(x[:], math.Float32bits(b.Float32().Get(row)))
			_, err = buf.Write(x[:4])
		case types.BlockUint16:
			LE.PutUint16(x[:], b.Uint16().Get(row))
			_, err = buf.Write(x[:2])
		case types.BlockInt16:
			LE.PutUint16(x[:], uint16(b.Int16().Get(row)))
			_, err = buf.Write(x[:2])
		case types.BlockUint8:
			_, err = buf.Write([]byte{b.Uint8().Get(row)})
		case types.BlockInt8:
			_, err = buf.Write([]byte{uint8(b.Int8().Get(row))})
		case types.BlockBool:
			v := b.Bool().Get(row)
			err = buf.WriteByte(*(*byte)(unsafe.Pointer(&v)))
		case types.BlockBytes:
			if fixed := field.Fixed; fixed > 0 {
				_, err = buf.Write(b.Bytes().Get(row)[:fixed])
			} else {
				v := b.Bytes().Get(row)
				LE.PutUint32(x[:], uint32(len(v)))
				_, err = buf.Write(x[:4])
				if err == nil {
					_, err = buf.Write(v)
				}
			}
		case types.BlockInt256:
			_, err = buf.Write(b.Int256().Get(row).Bytes())
		case types.BlockInt128:
			_, err = buf.Write(b.Int128().Get(row).Bytes())
		default:
			// oh, its a type we don't support yet
			assert.Unreachable("unhandled field type",
				"typeid", int(field.Type),
				"type", b.Type().String(),
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
	enums := dstSchema.Enums()
	base := rval.Addr().UnsafePointer()
	for i, field := range dstSchema.Exported() {
		// identify source field
		srcId := maps[i]

		// skip unmapped fields
		if srcId < 0 {
			continue
		}

		// use unsafe.Add instead of reflect (except marshal types)
		fptr := unsafe.Add(base, field.Offset)

		// insert zero value when block is not available (e.g. after schema change)
		b := p.blocks[srcId]
		if b == nil {
			if !field.Flags.Is(types.FieldFlagEnum) {
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
		case types.FieldTypeInt64:
			*(*int64)(fptr) = b.Int64().Get(row)

		case types.FieldTypeUint64:
			*(*uint64)(fptr) = b.Uint64().Get(row)

		case types.FieldTypeFloat64:
			*(*float64)(fptr) = b.Float64().Get(row)

		case types.FieldTypeInt32:
			*(*int32)(fptr) = b.Int32().Get(row)

		case types.FieldTypeUint32:
			*(*uint32)(fptr) = b.Uint32().Get(row)

		case types.FieldTypeFloat32:
			*(*float32)(fptr) = b.Float32().Get(row)

		case types.FieldTypeInt16:
			*(*int16)(fptr) = b.Int16().Get(row)

		case types.FieldTypeUint16:
			if field.IsEnum && enums != nil {
				enum, ok := enums.Lookup(field.Name)
				if !ok {
					return fmt.Errorf("%s: missing enum dictionary", field.Name)
				}
				u16 := b.Uint16().Get(row)
				val, ok := enum.Value(u16)
				if !ok {
					return fmt.Errorf("%s: invalid enum value %d", field.Name, u16)
				}
				*(*string)(fptr) = val // FIXME: may break when enum dict grows
			} else {
				*(*uint16)(fptr) = b.Uint16().Get(row)
			}

		case types.FieldTypeInt8:
			*(*int8)(fptr) = b.Int8().Get(row)

		case types.FieldTypeUint8:
			*(*uint8)(fptr) = b.Uint8().Get(row)

		case types.FieldTypeTimestamp, types.FieldTypeDate, types.FieldTypeTime:
			(*(*time.Time)(fptr)) = schema.TimeScale(field.Scale).FromUnix(b.Int64().Get(row))

		case types.FieldTypeBoolean:
			*(*bool)(fptr) = b.Bool().Get(row)

		case types.FieldTypeBytes:
			switch {
			case field.Iface&types.IfaceBinaryUnmarshaler > 0:
				rfield := field.StructValue(rval)
				err = rfield.Addr().Interface().(encoding.BinaryUnmarshaler).UnmarshalBinary(b.Bytes().Get(row))
			case field.Fixed > 0:
				copy(unsafe.Slice((*byte)(fptr), field.Fixed), b.Bytes().Get(row))
			default:
				// b := b.Bytes().Get(row)
				// if cap(*(*[]byte)(fptr)) < len(b) {
				// 	*(*[]byte)(fptr) = make([]byte, len(b))
				// } else {
				// 	*(*[]byte)(fptr) = (*(*[]byte)(fptr))[:len(b)]
				// }
				// copy(*(*[]byte)(fptr), b)
				*(*[]byte)(fptr) = b.Bytes().Get(row)
			}

		case types.FieldTypeString:
			switch {
			case field.Iface&types.IfaceTextUnmarshaler > 0:
				rfield := field.StructValue(rval)
				err = rfield.Addr().Interface().(encoding.TextUnmarshaler).UnmarshalText(b.Bytes().Get(row))
			default:
				// safe version with copy
				// *(*string)(fptr) = string(b.Bytes().Get(row))
				*(*string)(fptr) = util.UnsafeGetString(b.Bytes().Get(row))
			}

		case types.FieldTypeInt256:
			*(*num.Int256)(fptr) = b.Int256().Get(row)

		case types.FieldTypeInt128:
			*(*num.Int128)(fptr) = b.Int128().Get(row)

		case types.FieldTypeDecimal256:
			(*(*num.Decimal256)(fptr)).Set(b.Int256().Get(row))
			(*(*num.Decimal256)(fptr)).SetScale(field.Scale)

		case types.FieldTypeDecimal128:
			(*(*num.Decimal128)(fptr)).Set(b.Int128().Get(row))
			(*(*num.Decimal128)(fptr)).SetScale(field.Scale)

		case types.FieldTypeDecimal64:
			(*(*num.Decimal64)(fptr)).Set(b.Int64().Get(row))
			(*(*num.Decimal64)(fptr)).SetScale(field.Scale)

		case types.FieldTypeDecimal32:
			(*(*num.Decimal32)(fptr)).Set(b.Int32().Get(row))
			(*(*num.Decimal32)(fptr)).SetScale(field.Scale)

		case types.FieldTypeBigint:
			(*(*num.Big)(fptr)).SetBytes(b.Bytes().Get(row))

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

// ForEach walks a pack decoding each row into type T. If T is invalid (not
// a struct type) or incompatible with the packs schema an error is returned.
func ForEach[T any](pkg *Package, fn func(i int, v *T) error) error {
	var t T
	dst, err := schema.SchemaOf(t)
	if err != nil {
		return err
	}
	if err := pkg.schema.CanSelect(dst); err != nil {
		return err
	}
	maps, err := pkg.schema.MapTo(dst)
	if err != nil {
		return err
	}
	for i := 0; i < pkg.nRows; i++ {
		if err := pkg.ReadStruct(i, &t, dst, maps); err != nil {
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
		f, _ := p.schema.FieldByIndex(col)
		return schema.TimeScale(f.Scale()).FromUnix(ts)
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
	f, _ := p.schema.FieldByIndex(col)
	return num.NewDecimal256(p.blocks[col].Int256().Get(row), f.Scale())
}

func (p *Package) Decimal128(col, row int) num.Decimal128 {
	f, _ := p.schema.FieldByIndex(col)
	return num.NewDecimal128(p.blocks[col].Int128().Get(row), f.Scale())
}

func (p *Package) Decimal64(col, row int) num.Decimal64 {
	f, _ := p.schema.FieldByIndex(col)
	return num.NewDecimal64(p.blocks[col].Int64().Get(row), f.Scale())
}

func (p *Package) Decimal32(col, row int) num.Decimal32 {
	f, _ := p.schema.FieldByIndex(col)
	return num.NewDecimal32(p.blocks[col].Int32().Get(row), f.Scale())
}

func (p *Package) Big(col, row int) num.Big {
	return num.NewBigFromBytes(p.blocks[col].Bytes().Get(row))
}
