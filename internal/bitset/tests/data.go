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
	// AVX2 multiples
	// {"16K", 16 * 1024},
	// {"32K", 32 * 1024},
	// {"64K", 64 * 1024},
	{"100k", 100 * 1024},

	// not multiples of AVX2 size
	// {"1K-8", 1*1024 - 8},
	// {"16K-8", 16*1024 - 8},
	// {"32K-8", 32*1024 - 8},
	// {"64K-8", 64*1024 - 8},
}

type BenchmarkDensity struct {
	Name string
	D    float64
}

var BenchmarkDensities = []BenchmarkDensity{
	{"1/2", 1.0 / 2},
	// {"1/16", 1.0 / 16},
	{"1/32", 1.0 / 30},
	// {"1/64", 1.0 / 64},
	{"1/128", 1.0 / 128},
	// {"1/1024", 1.0 / 1024},
	// {"1/16384", 1.0 / 16384},
}
