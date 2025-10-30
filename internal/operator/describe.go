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
type Describer struct {
	w io.Writer
}

func NewDescriber(w io.Writer) *Describer {
	return &Describer{
		w: w,
	}
}

func (d *Describer) Process(_ context.Context, src *pack.Package) (*pack.Package, Result) {
	s := src.Schema()
	t := table.NewWriter()
	t.SetOutputMirror(d.w)
	t.SetTitle("Schema %s [0x%x] - %d fields", s.Name, s.Hash, s.NumFields())
	t.AppendHeader(table.Row{"#", "Name", "Type", "Flags", "Filter", "Visible", "Scale", "Size", "Fixed", "Compress"})
	for _, field := range s.Fields {
		t.AppendRow([]any{
			field.Id,
			field.Name,
			field.Type,
			field.Flags,
			field.Filter,
			field.IsVisible(),
			field.Scale,
			field.Type.Size(),
			field.Fixed,
			field.Compress,
		})
	}
	t.Render()
	return src, ResultOK
}

func (d *Describer) Finalize(_ context.Context) error {
	return nil
}

func (d *Describer) Err() error {
	return nil
}

func (d *Describer) Close() {
	d.w = nil
}
