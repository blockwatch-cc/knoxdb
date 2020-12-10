// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package vec

type vecBenchmarkSize struct {
	name string
	l    int
}

var vecBenchmarkSizes = []vecBenchmarkSize{
	// {"32", 32}, // exclude for AVX512
	{"128", 128},
	{"1K", 1 * 1024},
	{"16K", 16 * 1024},
	{"64K", 64 * 1024},
	{"128K", 128 * 1024},
	{"1M", 1024 * 1024},
	{"128M", 128 * 1024 * 1024},
}

/*
// for optimizing small data blocks
var vecBenchmarkSizes = []vecBenchmarkSize{
	// {"32", 32}, // exclude for AVX512
	{"128", 128},
	{"256", 256},
	{"384", 384},
	{"512", 512},
	{"640", 640},
	{"768", 768},
}
*/
