// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package stringx

import (
	"bytes"
	"testing"

	"blockwatch.cc/knoxdb/pkg/util"
	"github.com/stretchr/testify/require"
)

func TestDedupPoolAppend(t *testing.T) {
	pool := NewDedupPool(64)
	unique := util.RandByteSlices(32, 8) // 32x length 8
	for i := range 64 {
		v := unique[util.RandIntn(len(unique))]
		n := pool.Append(v)
		require.Equal(t, i, n, "append index")
		require.Equal(t, i+1, pool.Len(), "len")
		require.Equal(t, v, pool.Get(n), "get content")
		require.Equal(t, string(v), pool.GetString(n), "get content as string")
	}
}

func TestDedupPoolSet(t *testing.T) {
	pool := NewDedupPool(64)
	pool.Append(util.RandBytes(8))
	require.Panics(t, func() { pool.Set(0, []byte{0}) })
}

func TestDedupPoolDelete(t *testing.T) {
	pool := NewDedupPool(64)
	pool.Append(util.RandBytes(8))
	require.Panics(t, func() { pool.Delete(0, 1) })
}

func TestDedupPoolCmp(t *testing.T) {
	for range 16 {
		pool := NewDedupPool(64)
		pool.AppendMany(util.RandByteSlices(64, 8)...) // 64x length 8
		a, b := util.RandIntn(64), util.RandIntn(64)
		require.Equal(t, bytes.Compare(pool.Get(a), pool.Get(b)), pool.Cmp(a, b), "cmp")
	}
}

func TestDedupPoolExtremes(t *testing.T) {
	// min max
	pool := NewDedupPool(64)
	require.Equal(t, []byte(nil), pool.Min(), "empty min")
	require.Equal(t, []byte(nil), pool.Max(), "empty max")
	la, lb := pool.MinMaxLen()
	require.Equal(t, 0, la, "empty min len")
	require.Equal(t, 0, lb, "empty max len")

	// add a couple strings
	pool.AppendString("hello")
	pool.AppendString("hello")
	pool.AppendString("world")
	pool.AppendString("world")
	pool.AppendString("how")
	pool.AppendString("how")
	pool.AppendString("are") // min
	pool.AppendString("are") // min
	pool.AppendString("you") // max
	pool.AppendString("you") // max

	// check we get the correct min/max out
	require.Equal(t, "are", string(pool.Min()), "min")
	require.Equal(t, "you", string(pool.Max()), "max")
	la, lb = pool.MinMaxLen()
	require.Equal(t, 3, la, "min len")
	require.Equal(t, 5, lb, "max len")
}

func TestDedupPoolIterators(t *testing.T) {
	pool := NewDedupPool(64)
	data := util.RandByteSlices(64, 8) // 64x length 8
	pool.AppendMany(data...)

	// values
	var i int
	for v := range pool.Values() {
		require.Equal(t, data[i], v, "value", i)
		i++
	}

	// iterator
	for i, v := range pool.Iterator() {
		require.Equal(t, data[i], v, "it", i)
	}

	// StringIterator
	it := pool.Chunks()
	require.Equal(t, len(data), it.Len(), "it len")
	for i, v := range data {
		require.Equal(t, v, it.Get(i))
	}
}
