// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package vec

type vecBenchmarkSize struct {
	name string
	l    int
}

var vecBenchmarkSizes = []vecBenchmarkSize{
	{"1K", 1 * 1024},
	{"16K", 16 * 1024},
	{"64K", 64 * 1024},
	{"128K", 128 * 1024},
	{"1M", 1024 * 1024},
	{"128M", 128 * 1024 * 1024},
}
