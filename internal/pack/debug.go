// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package pack

import (
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/assert"
	"blockwatch.cc/knoxdb/pkg/num"
	"blockwatch.cc/knoxdb/pkg/util"
)

// Reads a single row into a slice of interfaces.
// Used for debug only.
func (p *Package) ReadRow(row int, dst []any) []any {
	// assert.Always(row >= 0 && row < p.nRows, "invalid row",
	// 	"row", row,
	// 	"pack", p.key,
	// 	"schema", p.schema.Name(),
	// 	"version", p.schema.Version(),
	// )
	// assert.Always(len(p.blocks) == p.schema.NumFields(), "block mismatch",
	// 	"pack", p.key,
	// 	"schema", p.schema.Name(),
	// 	"version", p.schema.Version(),
	// 	"nFields", p.schema.NumFields(),
	// 	"nBlocks", len(p.blocks),
	// )
	// copy one full row of values
	maxFields := p.schema.NumFields()
	if cap(dst) < maxFields {
		dst = make([]any, 0, maxFields)
	} else {
		dst = dst[:maxFields]
	}
	for i, f := range p.schema.Fields {
		// skip deleted fields
		if !f.IsActive() {
			continue
		}

		// insert zero value when block is not available (e.g. after schema change)
		b := p.blocks[i]
		if b == nil {
			dst = append(dst, f.Type.Zero())
			continue
		}

		// add to result
		dst = append(dst, p.ReadValue(i, row, f.Type, f.Scale))
	}
	return dst
}

// Reads a single value at postion col,row.
func (p *Package) ReadValue(col, row int, typ types.FieldType, scale uint8) any {
	// assert.Always(col >= 0 && col < len(p.blocks), "invalid block id", map[string]any{
	//  "id":      col,
	//  "pack":    p.key,
	//  "schema":  p.schema.Name(),
	//  "version": p.schema.Version(),
	//  "nFields": p.schema.NumFields(),
	//  "nBlocks": len(p.blocks),
	// })
	// assert.Always(row >= 0 && row < p.nRows, "invalid row", map[string]any{
	//  "row":     row,
	//  "pack":    p.key,
	//  "schema":  p.schema.Name(),
	//  "version": p.schema.Version(),
	// })
	b := p.blocks[col]

	switch typ {
	case types.FT_I64:
		return b.Int64().Get(row)
	case types.FT_I32:
		return b.Int32().Get(row)
	case types.FT_I16:
		return b.Int16().Get(row)
	case types.FT_I8:
		return b.Int8().Get(row)
	case types.FT_U64:
		return b.Uint64().Get(row)
	case types.FT_U32:
		return b.Uint32().Get(row)
	case types.FT_U16:
		return b.Uint16().Get(row)
	case types.FT_U8:
		return b.Uint8().Get(row)
	case types.FT_F64:
		return b.Float64().Get(row)
	case types.FT_F32:
		return b.Float32().Get(row)
	case types.FT_TIMESTAMP, types.FT_DATE, types.FT_TIME:
		if ts := b.Int64().Get(row); ts > 0 {
			return types.TimeScale(scale).FromUnix(ts)
		} else {
			return zeroTime
		}
	case types.FT_DURATION:
		return types.TimeScale(scale).Duration(b.Int64().Get(row))
	case types.FT_BOOL:
		return b.Bool().Get(row)
	case types.FT_BYTES, types.FT_BINARY:
		return b.Bytes().Get(row)
	case types.FT_STRING, types.FT_TEXT:
		return util.UnsafeGetString(b.Bytes().Get(row))
	case types.FT_I256:
		return b.Int256().Get(row)
	case types.FT_I128:
		return b.Int128().Get(row)
	case types.FT_D256:
		return num.NewDecimal256(b.Int256().Get(row), scale)
	case types.FT_D128:
		return num.NewDecimal128(b.Int128().Get(row), scale)
	case types.FT_D64:
		return num.NewDecimal64(b.Int64().Get(row), scale)
	case types.FT_D32:
		return num.NewDecimal32(b.Int32().Get(row), scale)
	case types.FT_BIGINT:
		return num.NewBigFromBytes(b.Bytes().Get(row))
	default:
		// oh, its a type we don't support yet
		assert.Unreachable("unhandled field type", map[string]any{
			"field":   col,
			"typeid":  int(typ),
			"type":    typ.String(),
			"pack":    p.key,
			"schema":  p.schema.Name,
			"version": p.schema.Version,
		})
	}
	return nil
}
