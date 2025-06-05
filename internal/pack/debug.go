// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package pack

import (
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/assert"
	"blockwatch.cc/knoxdb/pkg/num"
	"blockwatch.cc/knoxdb/pkg/schema"
	"blockwatch.cc/knoxdb/pkg/util"
)

// Reads a single row into a slice of interfaces.
// Used for debug only.
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
		// skip deleted and internal fields
		if !field.IsVisible {
			continue
		}

		// insert zero value when block is not available (e.g. after schema change)
		b := p.blocks[i]
		if b == nil {
			dst = append(dst, field.Type.Zero())
			continue
		}

		// add to result
		dst = append(dst, p.ReadValue(i, row, field.Type, field.Scale))
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
	case types.FieldTypeInt64:
		return b.Int64().Get(row)
	case types.FieldTypeInt32:
		return b.Int32().Get(row)
	case types.FieldTypeInt16:
		return b.Int16().Get(row)
	case types.FieldTypeInt8:
		return b.Int8().Get(row)
	case types.FieldTypeUint64:
		return b.Uint64().Get(row)
	case types.FieldTypeUint32:
		return b.Uint32().Get(row)
	case types.FieldTypeUint16:
		return b.Uint16().Get(row)
	case types.FieldTypeUint8:
		return b.Uint8().Get(row)
	case types.FieldTypeFloat64:
		return b.Float64().Get(row)
	case types.FieldTypeFloat32:
		return b.Float32().Get(row)
	case types.FieldTypeDatetime:
		if ts := b.Int64().Get(row); ts > 0 {
			return schema.TimeScale(scale).FromUnix(ts)
		} else {
			return zeroTime
		}
	case types.FieldTypeBoolean:
		return b.Bool().Get(row)
	case types.FieldTypeBytes:
		return b.Bytes().Get(row)
	case types.FieldTypeString:
		return util.UnsafeGetString(b.Bytes().Get(row))
	case types.FieldTypeInt256:
		return b.Int256().Get(row)
	case types.FieldTypeInt128:
		return b.Int128().Get(row)
	case types.FieldTypeDecimal256:
		return num.NewDecimal256(b.Int256().Get(row), scale)
	case types.FieldTypeDecimal128:
		return num.NewDecimal128(b.Int128().Get(row), scale)
	case types.FieldTypeDecimal64:
		return num.NewDecimal64(b.Int64().Get(row), scale)
	case types.FieldTypeDecimal32:
		return num.NewDecimal32(b.Int32().Get(row), scale)
	case types.FieldTypeBigint:
		return num.NewBigFromBytes(b.Bytes().Get(row))
	default:
		// oh, its a type we don't support yet
		assert.Unreachable("unhandled field type", map[string]any{
			"field":   col,
			"typeid":  int(typ),
			"type":    typ.String(),
			"pack":    p.key,
			"schema":  p.schema.Name(),
			"version": p.schema.Version(),
		})
	}
	return nil
}
