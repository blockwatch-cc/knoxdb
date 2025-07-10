// Copyright (c) 2024 Blockwatch Data Inc.
// Author: abdul@blockwatch.cc

package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"blockwatch.cc/knoxdb/internal/operator"
	"blockwatch.cc/knoxdb/internal/pack"
	"blockwatch.cc/knoxdb/pkg/csv"
	"blockwatch.cc/knoxdb/pkg/schema"
)

var (
	_ operator.PullOperator = (*CsvImporter)(nil)
)

type CsvImporter struct {
	enc     *schema.Encoder
	dec     *csv.Decoder
	pkg     *pack.Package
	buf     *bytes.Buffer
	sniff   *csv.Sniffer
	in      *os.File
	err     error
	maxRows int
	nRows   int
}

func OpenCsvImporter(fpath string) (*CsvImporter, error) {
	ext := filepath.Ext(fpath)
	if len(ext) == 0 || ext[1:] != "csv" {
		return nil, fmt.Errorf("in file path has no extension")
	}
	f, err := os.Open(fpath)
	if err != nil {
		return nil, err
	}
	sniff := csv.NewSniffer(f, 0)
	if err = sniff.Sniff(); err != nil {
		return nil, err
	}

	// seek to origin
	_, err = f.Seek(0, 0)
	if err != nil {
		return nil, err
	}

	return &CsvImporter{
		sniff:   sniff,
		in:      f,
		maxRows: 64 << 10,
	}, nil
}

func (c *CsvImporter) Next(_ context.Context) (*pack.Package, operator.Result) {
	if c.pkg == nil {
		sch := c.sniff.Schema()
		c.enc = schema.NewEncoder(sch)
		c.dec = c.sniff.NewDecoder(c.in)
		c.pkg = pack.New().
			WithKey(0).
			WithVersion(0).
			WithMaxRows(c.maxRows).
			WithSchema(sch).
			Alloc()
		c.buf = c.enc.NewBuffer(1)
	} else {
		c.pkg.WithKey(c.pkg.Key() + 1)
		c.pkg.Clear()
	}

	for !c.pkg.IsFull() {
		v, err := c.dec.Decode()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, operator.ResultError
		}

		buf, err := c.enc.Encode(v, c.buf)
		if err != nil {
			return nil, operator.ResultError
		}

		c.buf.Reset()
		c.pkg.AppendWire(buf, &schema.Meta{Rid: uint64(c.nRows), Ref: uint64(c.nRows), Xmin: 1})
	}

	return c.pkg, operator.ResultOK
}

func (c *CsvImporter) Close() {
	c.enc = nil
	c.err = nil
	c.dec = nil
	if c.pkg != nil {
		c.pkg.Clear()
		c.pkg = nil
	}
}

func (c *CsvImporter) Err() error {
	return c.err
}
