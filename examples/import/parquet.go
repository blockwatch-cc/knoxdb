// Copyright (c) 2025 Blockwatch Data Inc.
// Author: abdul@blockwatch.cc

package main

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"

	"blockwatch.cc/knoxdb/internal/operator"
	"blockwatch.cc/knoxdb/internal/pack"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/num"
	"blockwatch.cc/knoxdb/pkg/schema"
	"github.com/parquet-go/parquet-go"
)

var (
	_ operator.PullOperator = (*ParquetImporter)(nil)
)

type Field struct {
	Index int
	Type  parquet.Type
}

type ParquetImporter struct {
	parqFile *parquet.File
	pkg      *pack.Package
	sch      *schema.Schema
	rows     []parquet.RowGroup
	values   []parquet.Value
	cur      int
	maxRows  int64
	err      error
}

func OpenParquetImporter(fpath string) (*ParquetImporter, error) {
	f, err := os.Open(fpath)
	if err != nil {
		return nil, err
	}
	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}

	parqFile, err := parquet.OpenFile(f, fi.Size())
	if err != nil {
		return nil, err
	}

	sch, err := BuildSchema(parqFile)
	if err != nil {
		return nil, err
	}

	rows := parqFile.RowGroups()
	var maxRows int64
	if len(rows) > 0 {
		maxRows = rows[0].NumRows()
	}

	return &ParquetImporter{
		parqFile: parqFile,
		sch:      sch,
		rows:     rows,
		maxRows:  maxRows,
	}, nil
}

func (op *ParquetImporter) Next(ctx context.Context) (*pack.Package, operator.Result) {
	if len(op.rows) == op.cur {
		return nil, operator.ResultDone
	}

	if op.pkg == nil {
		pkg := pack.New().
			WithKey(0).
			WithVersion(0).
			WithMaxRows(int(op.maxRows)).
			WithSchema(op.sch).
			Alloc()
		op.pkg = pkg
	} else {
		op.pkg.WithKey(op.pkg.Key() + 1)
		op.pkg.Clear()
	}

	if cap(op.values) < int(op.maxRows) {
		op.values = make([]parquet.Value, op.maxRows)
	}

	rowGroup := op.rows[op.cur]
	for idx, col := range rowGroup.ColumnChunks() {
		block := op.pkg.Block(idx)
		pages := col.Pages()

		switch col.Type().Kind() {
		case parquet.Int32:
			for {
				page, err := pages.ReadPage()
				if err != nil {
					if errors.Is(err, io.EOF) {
						break
					}
					op.err = err
					return nil, operator.ResultError
				}

				switch pVal := page.Values().(type) {
				case parquet.Int32Reader:
					values := block.Int32().Slice()[block.Len():block.Cap()]
					sz, err := pVal.ReadInt32s(values)
					if err != nil {
						if !errors.Is(err, io.EOF) {
							op.err = err
							return nil, operator.ResultError
						}
					}
					values = values[:sz]
					if sz > 0 {
						block.SetDirty()
						block.AddLen(uint32(sz))
						op.pkg.UpdateLen()
					}
				default:
					sz, err := pVal.ReadValues(op.values)
					if err != nil {
						if !errors.Is(err, io.EOF) {
							op.err = err
							return nil, operator.ResultError
						}
					}
					intLogicalType := col.Type().LogicalType().Integer
					if intLogicalType.IsSigned {
						for _, val := range op.values[:sz] {
							block.Int32().Append(val.Int32())
						}
					} else {
						for _, val := range op.values[:sz] {
							block.Uint32().Append(val.Uint32())
						}
					}
				}
			}

		case parquet.Int64:
			for {
				page, err := pages.ReadPage()
				if err != nil {
					if errors.Is(err, io.EOF) {
						break
					}
					op.err = err
					return nil, operator.ResultError
				}

				switch pVal := page.Values().(type) {
				case parquet.Int64Reader:
					values := block.Int64().Slice()[block.Len():block.Cap()]
					sz, err := pVal.ReadInt64s(values)
					if err != nil {
						if !errors.Is(err, io.EOF) {
							op.err = err
							return nil, operator.ResultError
						}
					}
					values = values[:sz]
					if sz > 0 {
						block.SetDirty()
						block.AddLen(uint32(sz))
						op.pkg.UpdateLen()
					}
				default:
					sz, err := pVal.ReadValues(op.values)
					if err != nil {
						if !errors.Is(err, io.EOF) {
							op.err = err
							return nil, operator.ResultError
						}
					}
					intLogicalType := col.Type().LogicalType().Integer
					if intLogicalType.IsSigned {
						for _, val := range op.values[:sz] {
							block.Int64().Append(val.Int64())
						}
					} else {
						for _, val := range op.values[:sz] {
							block.Uint64().Append(val.Uint64())
						}
					}
				}
			}

		case parquet.Int96:
			for {
				page, err := pages.ReadPage()
				if err != nil {
					if errors.Is(err, io.EOF) {
						break
					}
					op.err = err
					return nil, operator.ResultError
				}
				sz, err := page.Values().ReadValues(op.values)
				if err != nil {
					if !errors.Is(err, io.EOF) {
						op.err = err
						return nil, operator.ResultError
					}
				}
				for _, val := range op.values[:sz] {
					v := val.Bytes()
					var x num.Int128
					x[0] = binary.BigEndian.Uint64(v[0:8])
					x[1] = uint64(binary.BigEndian.Uint32(v[8:12]))
					block.Int128().Append(x)
				}
			}

		case parquet.Float:
			for {
				page, err := pages.ReadPage()
				if err != nil {
					if errors.Is(err, io.EOF) {
						break
					}
					op.err = err
					return nil, operator.ResultError
				}

				switch pVal := page.Values().(type) {
				case parquet.FloatReader:
					values := block.Float32().Slice()[block.Len():block.Cap()]
					sz, err := pVal.ReadFloats(values)
					if err != nil {
						if !errors.Is(err, io.EOF) {
							op.err = err
							return nil, operator.ResultError
						}
					}
					values = values[:sz]
					if sz > 0 {
						block.SetDirty()
						block.AddLen(uint32(sz))
						op.pkg.UpdateLen()
					}
				default:
					sz, err := pVal.ReadValues(op.values)
					if err != nil {
						if !errors.Is(err, io.EOF) {
							op.err = err
							return nil, operator.ResultError
						}
					}
					for _, val := range op.values[:sz] {
						block.Float32().Append(val.Float())
					}
				}
			}

		case parquet.Double:
			for {
				page, err := pages.ReadPage()
				if err != nil {
					if errors.Is(err, io.EOF) {
						break
					}
					op.err = err
					return nil, operator.ResultError
				}

				switch pVal := page.Values().(type) {
				case parquet.DoubleReader:
					values := block.Float64().Slice()[block.Len():block.Cap()]
					sz, err := pVal.ReadDoubles(values)
					if err != nil {
						if !errors.Is(err, io.EOF) {
							op.err = err
							return nil, operator.ResultError
						}
					}
					values = values[:sz]
					if sz > 0 {
						block.SetDirty()
						block.AddLen(uint32(sz))
						op.pkg.UpdateLen()
					}
				default:
					sz, err := pVal.ReadValues(op.values)
					if err != nil {
						if !errors.Is(err, io.EOF) {
							op.err = err
							return nil, operator.ResultError
						}
					}
					for _, val := range op.values[:sz] {
						block.Float64().Append(val.Double())
					}
				}
			}

		case parquet.ByteArray, parquet.FixedLenByteArray:
			for {
				page, err := pages.ReadPage()
				if err != nil {
					if errors.Is(err, io.EOF) {
						break
					}
					op.err = err
					return nil, operator.ResultError
				}
				sz, err := page.Values().ReadValues(op.values)
				if err != nil {
					if !errors.Is(err, io.EOF) {
						op.err = err
						return nil, operator.ResultError
					}
				}
				for _, val := range op.values[:sz] {
					block.Bytes().Append(val.Bytes())
				}
			}

		case parquet.Boolean:
			for {
				page, err := pages.ReadPage()
				if err != nil {
					if errors.Is(err, io.EOF) {
						break
					}
					op.err = err
					return nil, operator.ResultError
				}
				sz, err := page.Values().ReadValues(op.values)
				if err != nil {
					if !errors.Is(err, io.EOF) {
						op.err = err
						return nil, operator.ResultError
					}
				}
				for _, val := range op.values[:sz] {
					block.Bool().Append(val.Boolean())
				}
			}
		default:
			op.err = fmt.Errorf("column %q kind is not supported", col.Type())
			return nil, operator.ResultError
		}

		err := pages.Close()
		if err != nil {
			op.err = err
			return nil, operator.ResultError
		}
	}

	op.cur++
	if op.cur < len(op.rows) {
		return op.pkg, operator.ResultMore
	}
	return op.pkg, operator.ResultOK
}

func (p *ParquetImporter) Err() error {
	return p.err
}

func (p *ParquetImporter) Close() {
	p.values = nil
	p.pkg = nil
	p.parqFile = nil

}

func BuildSchema(parqFile *parquet.File) (*schema.Schema, error) {
	cols := parqFile.Root().Columns()
	b := schema.NewBuilder()
	for _, c := range cols {
		var ftyp types.FieldType
		switch c.Type().Kind() {
		case parquet.Int32:
			ftyp = types.FieldTypeInt32
		case parquet.Int64:
			ftyp = types.FieldTypeInt64
		case parquet.Int96:
			ftyp = types.FieldTypeInt128
		case parquet.Float:
			ftyp = types.FieldTypeFloat32
		case parquet.Double:
			ftyp = types.FieldTypeFloat64
		case parquet.ByteArray, parquet.FixedLenByteArray:
			ftyp = types.FieldTypeBytes
		case parquet.Boolean:
			ftyp = types.FieldTypeBoolean
		default:
			return nil, fmt.Errorf("field kind is not supported, name => %s, type => %s", c.Name(), c.Type())
		}
		b.AddField(c.Name(), ftyp)
	}
	return b.Finalize().Schema(), nil
}
