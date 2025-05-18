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
	"sort"
	"testing"

	"blockwatch.cc/knoxdb/pkg/util"
	"github.com/stretchr/testify/require"
)

func TestIteratorBasic(t *testing.T) {
	n := uint64(1e5)
	bm := New()
	for i := uint64(1); i <= n; i++ {
		bm.Set(i)
	}

	it := bm.NewIterator()
	for i := uint64(1); i <= n; i++ {
		v, _ := it.Next()
		require.Equal(t, i, v)
	}
	v, _ := it.Next()
	require.Equal(t, uint64(0), v)
}

func TestIteratorRanges(t *testing.T) {
	n := uint64(1e5)
	bm := New()
	for i := uint64(1); i <= n; i++ {
		bm.Set(i)
	}

	iters := bm.NewRangeIterators(8)
	cnt := uint64(1)
	for idx := 0; idx < 8; idx++ {
		it := iters[idx]
		for v, ok := it.Next(); ok; v, ok = it.Next() {
			require.Equal(t, cnt, v)
			cnt++
		}
	}
}

func TestIteratorRandom(t *testing.T) {
	n := uint64(1e6)
	bm := New()
	mp := make(map[uint64]struct{})
	var arr []uint64
	for i := uint64(1); i <= n; i++ {
		v := uint64(util.RandIntn(int(n) * 5))
		if v == 0 {
			continue
		}
		if _, ok := mp[v]; ok {
			continue
		}
		mp[v] = struct{}{}
		arr = append(arr, v)
		bm.Set(v)
	}

	sort.Slice(arr, func(i, j int) bool {
		return arr[i] < arr[j]
	})

	it := bm.NewIterator()
	v, _ := it.Next()
	for i := uint64(0); i < uint64(len(arr)); i++ {
		require.Equal(t, arr[i], v)
		v, _ = it.Next()
	}
}

func TestIteratorWithRemoveKeys(t *testing.T) {
	b := New()
	N := uint64(1e6)
	for i := uint64(0); i < N; i++ {
		b.Set(i)
	}

	b.UnsetRange(0, N)
	it := b.NewIterator()

	cnt := 0
	for _, ok := it.Next(); ok; _, ok = it.Next() {
		cnt++
	}
	require.Equal(t, 0, cnt)
}

func BenchmarkIterator(b *testing.B) {
	bm := New()
	for i := 0; i < int(1e5); i++ {
		bm.Set(uint64(i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		it := bm.NewIterator()
		for _, ok := it.Next(); ok; _, ok = it.Next() {
		}
	}
}
