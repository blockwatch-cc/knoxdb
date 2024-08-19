// Copyright (c) 2023 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

package tests

type BenchmarkSize struct {
	Name string
	L    int
}

var BenchmarkSizes = []BenchmarkSize{
	{"1K", 1 * 1024},
	{"16K", 16 * 1024},
	{"64K", 64 * 1024},
	{"128K", 128 * 1024},
}
