// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package operator

import (
	"context"
	"io"

	"blockwatch.cc/knoxdb/internal/pack"
	"github.com/jedib0t/go-pretty/v6/table"
)

// Outputs package schema definition as ASCII table to writer.
type DescribeOperator struct {
	w io.Writer
}

func NewDescribeOperator(w io.Writer) *DescribeOperator {
	return &DescribeOperator{
		w: w,
	}
}

func (d *DescribeOperator) Process(_ context.Context, src *pack.Package) (*pack.Package, Result) {
	s := src.Schema()
	t := table.NewWriter()
	t.SetOutputMirror(d.w)
	t.SetTitle("Schema %s [0x%x] - %d fields", s.Name(), s.Hash(), s.NumFields())
	t.AppendHeader(table.Row{"#", "Name", "Type", "Flags", "Index", "Visible", "Scale", "Size", "Fixed", "Compress"})
	for _, field := range s.Exported() {
		t.AppendRow([]any{
			field.Id,
			field.Name,
			field.Type,
			field.Flags,
			field.Index,
			field.IsVisible,
			field.Scale,
			field.Type.Size(),
			field.Fixed,
			field.Compress,
		})
	}
	t.Render()
	return nil, ResultDone
}

func (d *DescribeOperator) Finalize(_ context.Context) (*pack.Package, Result) {
	return nil, ResultDone
}

func (d *DescribeOperator) Err() error {
	return nil
}

func (d *DescribeOperator) Close() {
	// noop
}
