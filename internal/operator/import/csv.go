// Copyright (c) 2025 Blockwatch Data Inc.
// Author: abdul@blockwatch.cc

package importer

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

const CsvMaxRows = 128 << 10

var (
	_ operator.PullOperator = (*CsvImporter)(nil)
)

type CsvImporter struct {
	enc   *schema.Encoder
	dec   *csv.Decoder
	pkg   *pack.Package
	buf   *bytes.Buffer
	sniff *csv.Sniffer
	in    *os.File
	err   error
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
		sniff: sniff,
		in:    f,
	}, nil
}

func (c *CsvImporter) Next(_ context.Context) (*pack.Package, operator.Result) {
	if c.pkg == nil {
		sch := c.sniff.Schema().WithMeta()
		c.enc = schema.NewEncoder(sch)
		c.dec = c.sniff.NewDecoder(c.in)
		c.pkg = pack.New().
			WithKey(0).
			WithVersion(0).
			WithMaxRows(CsvMaxRows).
			WithSchema(sch).
			Alloc()
		c.buf = c.enc.NewBuffer(1)
	} else {
		c.pkg.WithKey(c.pkg.Key() + 1)
		c.pkg.Clear()
	}

	n, err := c.dec.DecodePack(c.pkg)
	if err != nil {
		if !errors.Is(err, io.EOF) {
			c.err = err
			return nil, operator.ResultError
		}
	}
	if n == 0 {
		return nil, operator.ResultDone
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
