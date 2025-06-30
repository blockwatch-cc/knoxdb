// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package operator

import (
	"context"
	"encoding/hex"
	"io"
	"strconv"
	"time"

	"blockwatch.cc/knoxdb/internal/pack"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/schema"
	"blockwatch.cc/knoxdb/pkg/slicex"
	"github.com/jedib0t/go-pretty/v6/table"
)

// Outputs n rows to writer formatted as ASCII table.
type LogOperator struct {
	w     io.Writer
	limit int
}

func NewLogOperator(w io.Writer, limit int) *LogOperator {
	if limit <= 0 {
		limit = 10
	}
	return &LogOperator{
		w:     w,
		limit: limit,
	}
}

func (op *LogOperator) Process(_ context.Context, src *pack.Package) (*pack.Package, Result) {
	s := src.Schema()
	t := table.NewWriter()
	t.SetOutputMirror(op.w)

	// configure text transformers for byte and enum columns
	var cfgs []table.ColumnConfig
	for _, field := range s.Exported() {
		switch field.Type {
		case types.FieldTypeBytes:
			cfgs = append(cfgs, table.ColumnConfig{
				Name: field.Name,
				Transformer: func(val any) string {
					return hex.EncodeToString(val.([]byte))
				},
			})
		case types.FieldTypeUint16:
			if field.Flags.Is(types.FieldFlagEnum) && s.HasEnums() {
				if lut, ok := s.Enums().Lookup(field.Name); ok {
					cfgs = append(cfgs, table.ColumnConfig{
						Name: field.Name,
						Transformer: func(val any) string {
							enum, ok := lut.Value(val.(uint16))
							if ok {
								return enum
							}
							return strconv.Itoa(int(val.(uint16)))
						},
					})
				}
			}
		case types.FieldTypeTimestamp, types.FieldTypeDate, types.FieldTypeTime:
			cfgs = append(cfgs, table.ColumnConfig{
				Name: field.Name,
				Transformer: func(val any) string {
					return schema.TimeScale(field.Scale).Format(val.(time.Time))
				},
			})
		}
	}
	if cfgs != nil {
		t.SetColumnConfigs(cfgs)
	}

	// assemble rows
	t.AppendHeader(slicex.Any(s.AllFieldNames()).Slice())
	if sel := src.Selected(); sel != nil {
		for _, v := range sel[:min(op.limit, len(sel))] {
			t.AppendRow(src.ReadRow(int(v), nil))
		}
	} else {
		for i := range min(src.Len(), op.limit) {
			t.AppendRow(src.ReadRow(i, nil))
		}
	}
	t.Render()
	return nil, ResultDone
}

func (op *LogOperator) Finalize(_ context.Context) (*pack.Package, Result) {
	return nil, ResultDone
}

func (op *LogOperator) Err() error {
	return nil
}

func (op *LogOperator) Close() {
	// noop
}
