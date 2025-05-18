//go:build ignore
// +build ignore

/*
 * Copyright 2021 Dgraph Labs, Inc. and Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package xroar

import (
	"testing"

	"blockwatch.cc/knoxdb/pkg/util"
	"github.com/RoaringBitmap/roaring/roaring64"
)

func BenchmarkSetRoaring(b *testing.B) {
	sz := uint64(1000000)
	bm := roaring64.New()
	for b.Loop() {
		bm.Add(util.RandUint64n(sz))
	}
}

func BenchmarkRemoveRangeRoaring64(b *testing.B) {
	bm := roaring64.New()
	N := uint64(1e5)
	for i := uint64(0); i < N; i++ {
		bm.Add(i)
	}

	bench := func(b *testing.B, factor uint64) {
		sz := N / factor
		cnt := N / sz
		for j := 0; j < b.N; j++ {
			b.StopTimer()
			bm2 := bm.Clone()
			b.StartTimer()
			for i := uint64(0); i < cnt; i++ {
				bm2.RemoveRange(i*sz, (i+1)*sz)
			}
		}
	}
	b.Run("N/2", func(b *testing.B) {
		bench(b, 2)
	})
	b.Run("N/4", func(b *testing.B) {
		bench(b, 4)
	})
	b.Run("N/16", func(b *testing.B) {
		bench(b, 16)
	})
	b.Run("N/256", func(b *testing.B) {
		bench(b, 256)
	})
}

func BenchmarkSelectRoaring64(b *testing.B) {
	bm := roaring64.New()
	N := uint64(1e5)
	for i := uint64(0); i < N; i++ {
		bm.Add(i)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := uint64(0); j < N; j++ {
			bm.Select(j)
		}
	}
}
