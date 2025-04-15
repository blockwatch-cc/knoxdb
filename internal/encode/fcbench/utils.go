// Copyright (c) 2025 Blockwatch Data Inc.
// Author: oliver@blockwatch.cc
//
// Package fcbench provides a benchmarking suite for KnoxDB, inspired by FCBench.
// It generates synthetic datasets and tests encoding performance across various
// vector lengths and encoder types, exporting results to CSV for analysis.

package fcbench

import (
	"encoding/csv"
	"fmt"
	"os"
)

// =====================================================================
// EXPORT UTILITIES
// =====================================================================

// exportCSV writes string slices to a CSV file.
func exportCSV(filename string, headers []string, records [][]string) error {
	f, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("exportCSV: failed to create file %s: %w", filename, err)
	}
	defer f.Close()
	w := csv.NewWriter(f)
	if err := w.Write(headers); err != nil {
		return fmt.Errorf("exportCSV: failed to write headers: %w", err)
	}
	if err := w.WriteAll(records); err != nil {
		return fmt.Errorf("exportCSV: failed to write records: %w", err)
	}
	w.Flush()
	if err := w.Error(); err != nil {
		return fmt.Errorf("exportCSV: failed to flush writer: %w", err)
	}
	return nil
}
