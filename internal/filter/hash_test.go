// Copyright (c) 2020-2025 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc, alex@blockwatch.cc

package filter

import (
	"testing"

	"github.com/zeebo/xxh3"
)

func BenchmarkHash(b *testing.B) {
	for b.Loop() {
		Hash([]byte("Hello"))
	}
}

func BenchmarkHashXXH3(b *testing.B) {
	for b.Loop() {
		xxh3.Hash([]byte("Hello"))
	}
}
