// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package types

type DumpMode int

const (
    DumpModeDec DumpMode = iota
    DumpModeHex
    DumpModeCSV
)
