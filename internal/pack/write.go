// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package pack

import (
	"encoding"
	"fmt"
	"reflect"
	"runtime/debug"
	"time"
	"unsafe"

	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/assert"
	"blockwatch.cc/knoxdb/pkg/num"
	"blockwatch.cc/knoxdb/pkg/schema"
	"blockwatch.cc/knoxdb/pkg/util"
)

// AppendWire appends a new row of values from a wire protocol message. The caller must
// ensure the message matches the currrent package schema.
func (p *Package) AppendWire(buf []byte, meta *schema.Meta) {
	assert.Always(p.CanGrow(1), "pack: overflow on wire append",
		"pack", p.key,
		"len", p.nRows,
		"cap", p.maxRows,
	)
	for i, field := range p.schema.Exported() {
		// deleted and internal fields are invisible
		if !field.IsVisible {
			continue
		}

		// skip missing blocks (e.g. after schema change)
		b := p.blocks[i]
		if b == nil {
			continue
		}

		// fill internal fields from metadata
		if field.IsInternal {
			if meta != nil {
				switch field.Id {
				case schema.MetaRid:
					b.Uint64().Append(meta.Rid)
				case schema.MetaRef:
					b.Uint64().Append(meta.Ref)
				case schema.MetaXmin:
					b.Uint64().Append(meta.Xmin)
				case schema.MetaXmax:
					b.Uint64().Append(meta.Xmax)
				case schema.MetaLive:
					b.Bool().Append(meta.Xmax == 0)
				}
			} else {
				switch field.Type {
				case types.FieldTypeUint64:
					b.Uint64().Append(0)
				case types.FieldTypeBoolean:
					b.Bool().Append(false)
				}
			}
			continue
		}

		switch field.Type {
		case types.FieldTypeUint64, types.FieldTypeInt64,
			types.FieldTypeDatetime, types.FieldTypeFloat64,
			types.FieldTypeDecimal64:
			b.Uint64().Append(*(*uint64)(unsafe.Pointer(&buf[0])))
			buf = buf[8:]

		case types.FieldTypeUint32, types.FieldTypeInt32,
			types.FieldTypeFloat32, types.FieldTypeDecimal32:
			b.Uint32().Append(*(*uint32)(unsafe.Pointer(&buf[0])))
			buf = buf[4:]

		case types.FieldTypeUint16, types.FieldTypeInt16:
			b.Uint16().Append(*(*uint16)(unsafe.Pointer(&buf[0])))
			buf = buf[2:]

		case types.FieldTypeUint8, types.FieldTypeInt8:
			b.Uint8().Append(*(*uint8)(unsafe.Pointer(&buf[0])))
			buf = buf[1:]

		case types.FieldTypeBoolean:
			b.Bool().Append(*(*bool)(unsafe.Pointer(&buf[0])))
			buf = buf[1:]

		case types.FieldTypeString, types.FieldTypeBytes:
			if fixed := field.Fixed; fixed > 0 {
				b.Bytes().Append(buf[:fixed])
				buf = buf[fixed:]
			} else {
				l, n := schema.ReadUint32(buf)
				buf = buf[n:]
				b.Bytes().Append(buf[:l])
				buf = buf[l:]
			}

		case types.FieldTypeInt256, types.FieldTypeDecimal256:
			b.Int256().Append(num.Int256FromBytes(buf[:32]))
			buf = buf[32:]

		case types.FieldTypeInt128, types.FieldTypeDecimal128:
			b.Int128().Append(num.Int128FromBytes(buf[:16]))
			buf = buf[16:]

		default:
			// oh, its a type we don't support yet
			assert.Unreachable("unhandled field type",
				"field", field.Name,
				"type", field.Type.String(),
				"pack", p.key,
				"schema", p.schema.Name(),
				"version", p.schema.Version(),
			)
		}
		b.SetDirty()
	}
	p.nRows++
}

// SetValue overwrites a single value at a given col/row offset. The caller must
// ensure strict type match as no additional check, cast or conversion is done.
func (p *Package) SetValue(col, row int, val any) error {
	f, ok := p.schema.FieldById(uint16(col))
	assert.Always(ok, "invalid field id",
		"id", col,
		"pack", p.key,
		"schema", p.schema.Name(),
		"version", p.schema.Version(),
		"nFields", p.schema.NumFields(),
		"nBlocks", len(p.blocks),
	)
	assert.Always(f.IsVisible(), "field is invisble",
		"id", col,
		"pack", p.key,
		"schema", p.schema.Name(),
		"version", p.schema.Version(),
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

	b := p.blocks[col]
	assert.Always(b != nil, "nil block",
		"id", col,
		"pack", p.key,
		"schema", p.schema.Name(),
		"version", p.schema.Version(),
		"nFields", p.schema.NumFields(),
		"nBlocks", len(p.blocks),
	)
	if b == nil {
		return nil
	}

	// try direct types first
	switch v := val.(type) {
	case int64:
		b.Int64().Set(row, v)
	case int32:
		b.Int32().Set(row, v)
	case int16:
		b.Int16().Set(row, v)
	case int8:
		b.Int8().Set(row, v)
	case int:
		b.Int64().Set(row, int64(v))
	case uint64:
		b.Uint64().Set(row, v)
	case uint32:
		b.Uint32().Set(row, v)
	case uint16:
		b.Uint16().Set(row, v)
	case uint8:
		b.Uint8().Set(row, v)
	case uint:
		b.Uint64().Set(row, uint64(v))
	case float64:
		b.Float64().Set(row, v)
	case float32:
		b.Float32().Set(row, v)
	case time.Time:
		if v.IsZero() {
			b.Int64().Set(row, 0)
		} else {
			b.Int64().Set(row, v.UnixNano())
		}
	case bool:
		if v {
			b.Bool().Set(row)
		} else {
			b.Bool().Clear(row)
		}
	case string:
		b.Bytes().Set(row, util.UnsafeGetBytes(v))
	case []byte:
		b.Bytes().Set(row, v)
	case num.Int256:
		b.Int256().Set(row, v)
	case num.Int128:
		b.Int128().Set(row, v)
	case num.Decimal256:
		// re-quantize nums to allow table joins, etc
		b.Int256().Set(row, v.Quantize(f.Scale()).Int256())
	case num.Decimal128:
		b.Int128().Set(row, v.Quantize(f.Scale()).Int128())
	case num.Decimal64:
		b.Int64().Set(row, v.Quantize(f.Scale()).Int64())
	case num.Decimal32:
		b.Int32().Set(row, v.Quantize(f.Scale()).Int32())
	default:
		// fallback to reflect for enum types
		rval := reflect.Indirect(reflect.ValueOf(val))
		switch rval.Type().Kind() {
		case reflect.Uint8:
			b.Uint8().Set(row, uint8(rval.Uint()))
		case reflect.Uint16:
			b.Uint16().Set(row, uint16(rval.Uint()))
		case reflect.Int8:
			b.Int8().Set(row, int8(rval.Int()))
		case reflect.Int16:
			b.Int16().Set(row, int16(rval.Int()))
		case reflect.Uint32:
			b.Uint32().Set(row, uint32(rval.Uint()))
		case reflect.Int32:
			b.Int32().Set(row, int32(rval.Int()))
		case reflect.Uint, reflect.Uint64:
			b.Uint64().Set(row, rval.Uint())
		case reflect.Int, reflect.Int64:
			b.Int64().Set(row, rval.Int())
		default:
			// for all other types, check if they implement marshalers
			// this is unlikely due to the internal use of this feature
			// but allows for future extension of DB internals like
			// aggregators, reducers, etc
			switch {
			case f.Can(types.IfaceBinaryMarshaler):
				buf, err := val.(encoding.BinaryMarshaler).MarshalBinary()
				if err != nil {
					return fmt.Errorf("set_value: marshal failed on %s field %s: %v",
						f.Type(), f.Name(), err)
				}
				b.Bytes().SetZeroCopy(row, buf)
			case f.Can(types.IfaceTextMarshaler):
				buf, err := val.(encoding.TextMarshaler).MarshalText()
				if err != nil {
					return fmt.Errorf("set_value: marshal failed on %s field %s: %v",
						f.Type(), f.Name(), err)
				}
				b.Bytes().SetZeroCopy(row, buf)
			case f.Can(types.IfaceStringer):
				b.Bytes().SetZeroCopy(row, util.UnsafeGetBytes(val.((fmt.Stringer)).String()))
			default:
				// oh, its a type we don't support yet
				assert.Unreachable("unhandled value type",
					"rtype", rval.Type().String(),
					"rkind", rval.Kind().String(),
					"field", f.Name(),
					"type", f.Type().String(),
					"pack", p.key,
					"schema", p.schema.Name(),
					"version", p.schema.Version(),
				)
			}
		}
	}
	return nil
}

// SetWire overwrites an entire row at a given offset with values from
// a wire protocol message. The caller must ensure strict type match
// as no additional check, cast or conversion is done.
func (p *Package) SetWire(row int, buf []byte) {
	assert.Always(p.nRows < row, "set: invalid row",
		"pack", p.key,
		"row", row,
		"len", p.nRows,
		"cap", p.maxRows,
	)

	for i, field := range p.schema.Exported() {
		// deleted and internal fields are invisible
		if !field.IsVisible {
			continue
		}
		// skipped and new blocks in old packages are missing
		b := p.blocks[i]
		if b == nil {
			continue
		}
		switch field.Type {
		case types.FieldTypeUint64, types.FieldTypeInt64, types.FieldTypeDatetime, types.FieldTypeFloat64, types.FieldTypeDecimal64:
			b.Uint64().Set(row, *(*uint64)(unsafe.Pointer(&buf[0])))
			buf = buf[8:]

		case types.FieldTypeUint32, types.FieldTypeInt32, types.FieldTypeFloat32, types.FieldTypeDecimal32:
			b.Uint32().Set(row, *(*uint32)(unsafe.Pointer(&buf[0])))
			buf = buf[4:]

		case types.FieldTypeUint16, types.FieldTypeInt16:
			b.Uint16().Set(row, *(*uint16)(unsafe.Pointer(&buf[0])))
			buf = buf[2:]

		case types.FieldTypeUint8, types.FieldTypeInt8:
			b.Uint8().Set(row, *(*uint8)(unsafe.Pointer(&buf[0])))
			buf = buf[1:]

		case types.FieldTypeBoolean:
			if *(*bool)(unsafe.Pointer(&buf[0])) {
				b.Bool().Set(row)
			} else {
				b.Bool().Clear(row)
			}
			buf = buf[1:]

		case types.FieldTypeString, types.FieldTypeBytes:
			if fixed := field.Fixed; fixed > 0 {
				b.Bytes().Set(row, buf[:fixed])
				buf = buf[fixed:]
			} else {
				l, n := schema.ReadUint32(buf)
				buf = buf[n:]
				b.Bytes().Set(row, buf[:l])
				buf = buf[l:]
			}

		case types.FieldTypeInt256, types.FieldTypeDecimal256:
			b.Int256().Set(row, num.Int256FromBytes(buf[:32]))
			buf = buf[32:]

		case types.FieldTypeInt128, types.FieldTypeDecimal128:
			b.Int128().Set(row, num.Int128FromBytes(buf[:16]))
			buf = buf[16:]

		default:
			// oh, its a type we don't support yet
			assert.Unreachable("unhandled field type",
				"field", field.Name,
				"type", field.Type.String(),
				"pack", p.key,
				"schema", p.schema.Name(),
				"version", p.schema.Version(),
			)
		}
		b.SetDirty()
	}
}

// ReplacePack replaces at most n rows in the current package starting at
// offset `to` with rows from `spack` starting at offset `from`.
// Both packages must have same schema and block order.
func (p *Package) ReplacePack(spack *Package, to, from, n int) error {
	if spack.schema.Hash() != p.schema.Hash() {
		return fmt.Errorf("replace: schema mismatch src=%s dst=%s", spack.schema.Name(), p.schema.Name())
	}
	if spack.nRows <= from {
		return fmt.Errorf("replace: invalid src offset=%d rows=%d", from, spack.nRows)
	}
	if spack.nRows <= from+n-1 {
		return fmt.Errorf("replace: src overflow from+n=%d rows=%d", from+n, spack.nRows)
	}
	if p.nRows <= to {
		return fmt.Errorf("replace: invalid dst offset=%d rows=%d", to, p.nRows)
	}
	if p.nRows < to+n {
		return fmt.Errorf("replace: dst overflow to+n=%d rows=%d", to+n, p.nRows)
	}
	defer func() {
		if e := recover(); e != nil {
			fmt.Printf("Replace: %v\n", e)
			fmt.Printf("SRC: id=%d rows=%d pklen=%d\n", spack.key, spack.nRows, len(spack.PkColumn()))
			fmt.Printf("DST: id=%d rows=%d pklen=%d\n", p.key, p.nRows, len(p.PkColumn()))
			fmt.Printf("REQ: dst:to=%d src:from=%d n=%d\n", to, from, n)
			fmt.Printf("%s\n", string(debug.Stack()))
			panic(e)
		}
	}()
	// copy at most N rows without overflowing dst
	n = min(p.nRows-to, n)
	for i, b := range p.blocks {
		b.ReplaceBlock(spack.blocks[i], from, to, n)
	}
	return nil
}

// Append copies `n` rows from `spack` starting at offset `from` to the end of
// the package. Both packages must have same schema and block order.
func (p *Package) AppendPack(spack *Package, from, n int) error {
	if spack.schema.Hash() != p.schema.Hash() {
		return fmt.Errorf("append: schema mismatch src=%s dst=%s", spack.schema.Name(), p.schema.Name())
	}
	if spack.nRows <= from {
		return fmt.Errorf("append: invalid src offset=%d rows=%d", from, spack.nRows)
	}
	if spack.nRows <= from+n-1 {
		return fmt.Errorf("append: src overflow from+n=%d rows=%d", from+n, spack.nRows)
	}
	assert.Always(p.CanGrow(n), "pack: overflow on append",
		"rows", n,
		"pack", p.key,
		"len", p.nRows,
		"cap", p.maxRows,
		"blockLen", p.blocks[0].Len(),
		"blockCap", p.blocks[0].Cap(),
	)
	defer func() {
		if e := recover(); e != nil {
			fmt.Printf("Append: %v\n", e)
			fmt.Printf("SRC: id=%d rows=%d pklen=%d\n", spack.key, spack.nRows, len(spack.PkColumn()))
			fmt.Printf("DST: id=%d rows=%d pklen=%d\n", p.key, p.nRows, len(p.PkColumn()))
			fmt.Printf("REQ: src:from=%d n=%d\n", from, n)
			fmt.Printf("%s\n", string(debug.Stack()))
			panic(e)
		}
	}()
	for i, b := range p.blocks {
		b.AppendBlock(spack.blocks[i], from, n)
	}
	p.nRows += n
	return nil
}

// Insert copies `n` rows from `spack` starting at offset `from` into the
// current package from position `to`. Both packages must have same schema
// and block order.
func (p *Package) InsertPack(spack *Package, to, from, n int) error {
	if spack.schema.Hash() != p.schema.Hash() {
		return fmt.Errorf("insert: schema mismatch src=%s dst=%s", spack.schema.Name(), p.schema.Name())
	}
	if spack.nRows <= from {
		return fmt.Errorf("insert: invalid src offset=%d rows=%d", from, spack.nRows)
	}
	if spack.nRows <= from+n-1 {
		return fmt.Errorf("insert: src overflow from+n=%d rows=%d", from+n, spack.nRows)
	}
	assert.Always(p.CanGrow(n), "pack: overflow on insert",
		"rows", n,
		"pack", p.key,
		"len", p.nRows,
		"cap", p.maxRows,
		"blockLen", p.blocks[0].Len(),
		"blockCap", p.blocks[0].Cap(),
	)
	defer func() {
		if e := recover(); e != nil {
			fmt.Printf("Insert: %v\n", e)
			fmt.Printf("SRC: id=%d rows=%d pklen=%d\n", spack.key, spack.nRows, len(spack.PkColumn()))
			fmt.Printf("DST: id=%d rows=%d pklen=%d\n", p.key, p.nRows, len(p.PkColumn()))
			fmt.Printf("REQ: dst:to=%d src:from=%d n=%d\n", to, from, n)
			fmt.Printf("%s\n", string(debug.Stack()))
			panic(e)
		}
	}()
	for i, b := range p.blocks {
		b.InsertBlock(spack.blocks[i], from, to, n)
	}
	p.nRows += n
	return nil
}

// Grow appends a new rows with zero values to all underlying blocks.
func (p *Package) Grow(n int) error {
	if n <= 0 {
		return nil
	}
	assert.Always(p.CanGrow(n), "pack: overflow on grow",
		"rows", n,
		"pack", p.key,
		"len", p.nRows,
		"cap", p.maxRows,
		"blockLen", p.blocks[0].Len(),
		"blockCap", p.blocks[0].Cap(),
	)
	for _, b := range p.blocks {
		b.Grow(n)
	}
	p.nRows += n
	return nil
}

func (p *Package) Delete(start, n int) error {
	if start <= 0 || n <= 0 {
		return nil
	}
	// n = min(p.rows-start, n)
	if p.nRows <= start+n {
		return fmt.Errorf("delete: invalid range [%d:%d] (max %d)", start, start+n, p.nRows)
	}
	for _, b := range p.blocks {
		b.Delete(start, n)
	}
	p.nRows -= n
	return nil
}
