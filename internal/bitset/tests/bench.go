// Copyright (c) 2023-2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc
//
// Test-usage only

package tests

var Patterns = []byte{
	0xfa,
	0x08,
	0x11,
	0x01,
	0x80,
	0xff,
}

type BenchmarkSize struct {
	Name string
	L    int
}

var BenchmarkSizes = []BenchmarkSize{
	{"1K", 1 * 1024},
	{"16K", 16 * 1024},
	{"64K", 64 * 1024},
}

type BenchmarkDensity struct {
	Name string
	D    float64
}

var BenchmarkDensities = []BenchmarkDensity{
	{"D100", 1.0 / 1},
	// {"D80", 1.0 / 1.25},
	// {"D66", 1.0 / 1.5},
	{"D50", 1.0 / 2},
	// {"D33", 1.0 / 3},
	// {"D40", 1.0 / 2.5},
	// {"D25", 1.0 / 4},
	// {"D12", 1.0 / 8},
	// {"D6", 1.0 / 16},
	// {"D3", 1.0 / 32},
	{"D1", 1.0 / 128},
}

type BenchmarkRange struct {
	Name  string
	Range int
}

var BenchmarkRanges = []BenchmarkRange{
	{"R10", 10},
	{"R100", 100},
	{"R1000", 1000},
}
