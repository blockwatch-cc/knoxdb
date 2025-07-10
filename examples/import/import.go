// Copyright (c) 2025 Blockwatch Data Inc.
// Author: abdul@blockwatch.cc

package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"blockwatch.cc/knoxdb/internal/operator"
)

func Import(fpath string) error {
	ext := filepath.Ext(fpath)
	if len(ext) == 0 {
		return fmt.Errorf("in file path has no extension")
	}

	var err error
	var importOperator operator.PullOperator

	switch e := ext[1:]; e {
	case "parquet":
		importOperator, err = OpenParquetImporter(fpath)
		if err != nil {
			return err
		}
	case "csv":
		importOperator, err = OpenCsvImporter(fpath)
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("in file type %q is not supported", e)
	}

	pl := operator.NewPhysicalPipeline().
		WithSource(importOperator).
		WithOperator(operator.NewDescriber(os.Stdout)).
		WithSink(operator.NewLogger(os.Stdout, 10))
	defer pl.Close()

	return operator.NewExecutor().
		AddPipeline(pl).
		Run(context.Background())
}
