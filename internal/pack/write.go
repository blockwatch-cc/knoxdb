// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package pack

import (
	"fmt"
	"reflect"
	"time"
	"unsafe"

	"blockwatch.cc/knoxdb/internal/arena"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/assert"
	"blockwatch.cc/knoxdb/pkg/num"
	"blockwatch.cc/knoxdb/pkg/schema"
	"blockwatch.cc/knoxdb/pkg/util"
)

// AppendWire appends a new row of values from a wire protocol message. The caller must
// ensure the message matches the currrent package schema.
func (p *Package) AppendWire(buf []byte, meta *schema.Meta) {
	// assert.Always(p.CanGrow(1), "pack: overflow on wire append",
	// 	"pack", p.key,
	// 	"len", p.nRows,
	// 	"cap", p.maxRows,
	// )
	// assert.Always(len(buf) >= p.schema.WireSize(), "pack: short buffer",
	// 	"len", len(buf),
	// 	"wiresz", p.schema.WireSize(),
	// )
	for i, field := range p.schema.Fields {
		// skip missing blocks (e.g. after schema change)
		b := p.blocks[i]
		if b == nil {
			continue
		}

		// fill internal fields from metadata
		if field.IsMeta() {
			if meta != nil {
				switch field.Id {
				case schema.MetaRid:
					b.Uint64().Append(meta.Rid)
				case schema.MetaRef:
					b.Uint64().Append(meta.Ref)
				case schema.MetaXmin:
					b.Uint64().Append(uint64(meta.Xmin))
				case schema.MetaXmax:
					b.Uint64().Append(uint64(meta.Xmax))
				case schema.MetaDel:
					b.Bool().Append(meta.IsDel)
				}
			} else {
				switch field.Type {
				case types.FieldTypeUint64:
					b.Uint64().Append(0)
				case types.FieldTypeBoolean:
					b.Bool().Append(false)
				}
			}
			b.SetDirty()
			continue
		}

		// deleted and internal fields are invisible
		if !field.IsVisible() {
			continue
		}

		switch b.Type() {
		case types.BlockUint64, types.BlockInt64, types.BlockFloat64:
			b.Uint64().Append(*(*uint64)(unsafe.Pointer(&buf[0])))
			buf = buf[8:]

		case types.BlockUint32, types.BlockInt32, types.BlockFloat32:
			b.Uint32().Append(*(*uint32)(unsafe.Pointer(&buf[0])))
			buf = buf[4:]

		case types.BlockUint16, types.BlockInt16:
			b.Uint16().Append(*(*uint16)(unsafe.Pointer(&buf[0])))
			buf = buf[2:]

		case types.BlockUint8, types.BlockInt8:
			b.Uint8().Append(*(*uint8)(unsafe.Pointer(&buf[0])))
			buf = buf[1:]

		case types.BlockBool:
			b.Bool().Append(*(*bool)(unsafe.Pointer(&buf[0])))
			buf = buf[1:]

		case types.BlockBytes:
			if fixed := field.Fixed; fixed > 0 {
				b.Bytes().Append(buf[:fixed])
				buf = buf[fixed:]
			} else {
				l := LE.Uint32(buf)
				buf = buf[4:]
				b.Bytes().Append(buf[:l])
				buf = buf[l:]
			}

		case types.BlockInt256:
			b.Int256().Append(num.Int256FromBytes(buf[:32]))
			buf = buf[32:]

		case types.BlockInt128:
			b.Int128().Append(num.Int128FromBytes(buf[:16]))
			buf = buf[16:]

		default:
			// oh, its a type we don't support yet
			assert.Unreachable("unhandled block type",
				"field", field.Name,
				"type", b.Type().String(),
				"pack", p.key,
				"schema", p.schema.Name,
				"version", p.schema.Version,
			)
		}
		b.SetDirty()
	}
	p.nRows++
}

// SetValue overwrites a single value at a given col/row offset. The caller must
// ensure strict type match as no additional check, cast or conversion is done.
func (p *Package) SetValue(col, row int, val any) error {
	// f, ok := p.schema.FieldByIndex(col)
	// assert.Always(ok, "invalid field id",
	// 	"id", col,
	// 	"pack", p.key,
	// 	"schema", p.schema.Name(),
	// 	"version", p.schema.Version(),
	// 	"nFields", p.schema.NumFields(),
	// 	"nBlocks", len(p.blocks),
	// )
	// assert.Always(f.IsVisible(), "field is invisble",
	// 	"id", col,
	// 	"pack", p.key,
	// 	"schema", p.schema.Name(),
	// 	"version", p.schema.Version(),
	// )
	// assert.Always(col >= 0 && col < len(p.blocks), "invalid block id",
	// 	"id", col,
	// 	"pack", p.key,
	// 	"schema", p.schema.Name(),
	// 	"version", p.schema.Version(),
	// 	"nFields", p.schema.NumFields(),
	// 	"nBlocks", len(p.blocks),
	// )
	// assert.Always(row >= 0 && row < p.nRows, "invalid row",
	// 	"row", row,
	// 	"pack", p.key,
	// 	"schema", p.schema.Name(),
	// 	"version", p.schema.Version(),
	// )

	b := p.blocks[col]
	// assert.Always(b != nil, "nil block",
	// 	"id", col,
	// 	"pack", p.key,
	// 	"schema", p.schema.Name(),
	// 	"version", p.schema.Version(),
	// 	"nFields", p.schema.NumFields(),
	// 	"nBlocks", len(p.blocks),
	// )
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
			b.Int64().Set(row, schema.TimeScale(p.schema.Fields[col].Scale).ToUnix(v))
		}
	case bool:
		if v {
			b.Bool().Set(row)
		} else {
			b.Bool().Unset(row)
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
		b.Int256().Set(row, v.Quantize(p.schema.Fields[col].Scale).Int256())
	case num.Decimal128:
		b.Int128().Set(row, v.Quantize(p.schema.Fields[col].Scale).Int128())
	case num.Decimal64:
		b.Int64().Set(row, v.Quantize(p.schema.Fields[col].Scale).Int64())
	case num.Decimal32:
		b.Int32().Set(row, v.Quantize(p.schema.Fields[col].Scale).Int32())
	case num.Big:
		b.Bytes().Set(row, v.Bytes())
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
			// oh, its a type we don't support yet
			f := p.schema.Fields[col]
			assert.Unreachable("unhandled value type",
				"rtype", rval.Type().String(),
				"rkind", rval.Kind().String(),
				"field", f.Name,
				"type", f.Type.String(),
				"pack", p.key,
				"schema", p.schema.Name,
				"version", p.schema.Version,
			)
		}
		b.SetDirty()
	}
	return nil
}

// SetWire overwrites an entire row at a given offset with values from
// a wire protocol message. The caller must ensure strict type match
// as no additional check, cast or conversion is done.
func (p *Package) SetWire(row int, buf []byte) {
	// assert.Always(row >= 0 && row < p.nRows, "set: invalid row",
	// 	"pack", p.key,
	// 	"row", row,
	// 	"len", p.nRows,
	// 	"cap", p.maxRows,
	// )

	for i, field := range p.schema.Fields {
		// deleted and internal fields are invisible
		if !field.IsVisible() {
			continue
		}
		// skipped and new blocks in old packages are missing
		b := p.blocks[i]
		if b == nil {
			continue
		}
		switch b.Type() {
		case types.BlockUint64, types.BlockInt64, types.BlockFloat64:
			b.Uint64().Set(row, *(*uint64)(unsafe.Pointer(&buf[0])))
			buf = buf[8:]

		case types.BlockUint32, types.BlockInt32, types.BlockFloat32:
			b.Uint32().Set(row, *(*uint32)(unsafe.Pointer(&buf[0])))
			buf = buf[4:]

		case types.BlockUint16, types.BlockInt16:
			b.Uint16().Set(row, *(*uint16)(unsafe.Pointer(&buf[0])))
			buf = buf[2:]

		case types.BlockUint8, types.BlockInt8:
			b.Uint8().Set(row, *(*uint8)(unsafe.Pointer(&buf[0])))
			buf = buf[1:]

		case types.BlockBool:
			if *(*bool)(unsafe.Pointer(&buf[0])) {
				b.Bool().Set(row)
			} else {
				b.Bool().Unset(row)
			}
			buf = buf[1:]

		case types.BlockBytes:
			if fixed := field.Fixed; fixed > 0 {
				b.Bytes().Set(row, buf[:fixed])
				buf = buf[fixed:]
			} else {
				l := LE.Uint32(buf)
				buf = buf[4:]
				b.Bytes().Set(row, buf[:l])
				buf = buf[l:]
			}

		case types.BlockInt256:
			b.Int256().Set(row, num.Int256FromBytes(buf[:32]))
			buf = buf[32:]

		case types.BlockInt128:
			b.Int128().Set(row, num.Int128FromBytes(buf[:16]))
			buf = buf[16:]

		default:
			// oh, its a type we don't support yet
			assert.Unreachable("unhandled field type",
				"field", field.Name,
				"type", field.Type.String(),
				"pack", p.key,
				"schema", p.schema.Name,
				"version", p.schema.Version,
			)
		}
		b.SetDirty()
	}
}

// AppendRange appends `src[i:j]` to the package. Packages must have
// the same schema and block order. Range indices form a half open
// interval [i,j) similar to Go slices. Panics on failed bounds checks
// and overflow.
func (p *Package) AppendRange(src *Package, i, j int) {
	n := j - i
	// assert.Always(
	// 	src.schema.Hash() == p.schema.Hash(),
	// 	"append: schema mismatch",
	// 	"src", src.schema.Name(), "dst", p.schema.Name(),
	// )
	// assert.Always(i <= j, "append: src out of bounds", "i", i, "j", j)
	// assert.Always(src.nRows > i, "append: src out of bounds", "i", i, "rows", src.nRows)
	// assert.Always(src.nRows >= j, "append: src out of bounds", "j", j, "rows", src.nRows)
	// assert.Always(p.CanGrow(n), "append: overflow",
	// 	"rows", n,
	// 	"pack", p.key,
	// 	"len", p.nRows,
	// 	"cap", p.maxRows,
	// 	"blockLen", p.blocks[0].Len(),
	// 	"blockCap", p.blocks[0].Cap(),
	// )

	// debug
	// defer func() {
	// 	if e := recover(); e != nil {
	// 		fmt.Printf("AppendRange: %v\n", e)
	// 		fmt.Printf("REQ: src[%d:%d]\n", i, j)
	// 		debug.PrintStack()
	// 		panic(e)
	// 	}
	// }()

	for k, b := range p.blocks {
		if b == nil || src.blocks[k] == nil {
			continue
		}
		b.AppendRange(src.blocks[k], i, j)
	}
	p.nRows += n
}

// AppendTo appends selected entries in package p to dst without
// overflowing dst and returns how many entries were appended.
// Both packages must have the same schema and block order.
func (p *Package) AppendTo(dst *Package, sel []uint32) int {
	// assert.Always(
	// 	dst.schema.Hash() == p.schema.Hash(),
	// 	"append: schema mismatch",
	// 	"src", p.schema.Name(),
	// 	"dst", dst.schema.Name(),
	// )
	// don't overflow dst
	n := min(p.nRows, dst.maxRows-dst.nRows)
	if sel != nil {
		n = min(len(sel), n)
		sel = sel[:n]
	}
	// defer func() {
	// 	if e := recover(); e != nil {
	// 		fmt.Printf("AppendTo: %v\n", e)
	// 		fmt.Printf("REQ: p[%d:%d:%d] => dst[%d:%d:%d]\n",
	// 			0, n, p.maxRows, dst.nRows, dst.nRows+n, dst.maxRows)
	// 		fmt.Printf("%s\n", string(debug.Stack()))
	// 		panic(e)
	// 	}
	// }()
	for k, b := range p.blocks {
		if b == nil || dst.blocks[k] == nil {
			continue
		}
		b.AppendTo(dst.blocks[k], sel)
	}
	dst.nRows += n
	return n
}

func (p *Package) Delete(i, j int) error {
	if i < 0 || j < 0 || j < i || p.nRows < j {
		return fmt.Errorf("delete: invalid range [%d:%d] (nrows=%d)", i, j, p.nRows)
	}
	for _, b := range p.blocks {
		if b == nil {
			continue
		}
		b.Delete(i, j)
	}
	p.nRows -= j - i
	return nil
}

func (p *Package) UpdateLen() {
	for _, b := range p.blocks {
		if b == nil {
			continue
		}
		p.nRows = b.Len()
		break
	}
}

type WriteMode byte

const (
	WriteModeAll WriteMode = iota
	WriteModeIncludeSelected
	WriteModeExcludeSelected
)

type AppendState struct {
	srcOffset int
	selOffset int
	hasMore   bool
}

func (s AppendState) More() bool {
	return s.hasMore
}

// AppendSelected appends records from src to pkg according to mode until pkg is full.
// To split vectors longer than capacity use AppendState and multiple target packs
// and chain calls in a for loop like
//
//	var state AppendState
//
//	for {
//		// call append and use last version of state, returns next state
//		state = pkg.AppendSelected(src, mode, state)
//
//		// handle full package (store, create new package)
//		if pkg.IsFull() {
//		}
//
//		// stop when src is exhausted
//		if !state.More() {
//			break
//		}
//	}
func (p *Package) AppendSelected(src *Package, mode WriteMode, state AppendState) (int, AppendState) {
	switch mode {
	case WriteModeAll:
		var (
			sel  []uint32
			free = p.maxRows - p.nRows
			n    = src.nRows - state.srcOffset
		)
		if state.srcOffset > 0 || free < n {
			// create selection vector when src offs > 0 or src would overflow p
			// i.e. src will be split across multiple target packs
			sel = types.NewRange(state.srcOffset, state.srcOffset+min(free, n)).AsSelection()
			// fmt.Printf("Made sel range [%d:%d] len=%d/%d\n",
			// 	state.srcOffset, state.srcOffset+min(free, n),
			// 	len(sel), min(free, n))
		}

		// copy at most n records from src until p is full
		n = src.AppendTo(p, sel)

		// update state
		state.srcOffset += n
		state.hasMore = src.nRows > state.srcOffset
		return n, state

	case WriteModeIncludeSelected:
		// append selected vector data while keeping selection order
		sel := src.selected[state.selOffset:]

		// copy at most n records from src until p is full
		n := src.AppendTo(p, sel)

		// update state
		state.selOffset += n
		state.hasMore = len(sel) > state.selOffset
		return n, state

	case WriteModeExcludeSelected:
		// append unselected vector data (i.e. gaps in the selection vector)
		// assumes sorted selection vector
		sel := src.selected[state.selOffset:]
		last := uint32(state.srcOffset)
		free := p.maxRows - p.nRows
		neg := arena.AllocUint32(free)

		for {
			// find the next gap
			for free > 0 && len(sel) > 0 && last == sel[0] {
				last++
				sel = sel[1:]
				state.selOffset++
				free--
			}

			// calculate gap size
			var k int
			if len(sel) > 0 {
				k = int(sel[0] - last)
			} else {
				k = min(free, src.Len()-int(last))
				free -= k
			}

			// add k positions
			for range k {
				neg = append(neg, last)
				last++
			}

			// stop when tail is full or all src data was copied
			if free == 0 || len(sel) == 0 {
				break
			}
		}

		// copy at most n records from src until p is full
		n := src.AppendTo(p, neg)

		// update state
		arena.Free(neg)
		state.srcOffset = int(last)
		state.hasMore = src.nRows > state.srcOffset
		return n, state

	default:
		assert.Unreachable("invalid write mode", mode)
		return 0, state
	}
}
