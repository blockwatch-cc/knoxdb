// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package block

import (
	"math"
	"testing"

	"blockwatch.cc/knoxdb/pkg/util"
	"github.com/stretchr/testify/require"
)

var (
	// little endian
	maxIntBytes = []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x7f}
)

func TestAppend64(t *testing.T) {
	n := 2  // 2 elements
	sz := 8 // int64
	block := New(BlockInt64, n)
	require.NotNil(t, block)
	require.True(t, block.IsMaterialized())
	require.Equal(t, int(block.len), 0)
	require.Equal(t, int(block.cap), n)

	// first append writes buf[0:8]
	block.Int64().Append(math.MaxInt64)
	require.Equal(t, block.data()[0:sz], maxIntBytes[8-sz:])
	require.Equal(t, int(block.len), 1)
	require.Equal(t, int(block.cap), n)

	// second append writes buf[8:16]
	block.Int64().Append(math.MaxInt64)
	require.Equal(t, block.data()[sz:sz+sz], maxIntBytes[8-sz:])
	require.Equal(t, int(block.len), 2)
	require.Equal(t, int(block.cap), n)

	// third append panics (capacity reached)
	require.Panics(t, func() {
		block.Int64().Append(math.MaxInt64)
	})
	require.Equal(t, int(block.len), 2)
	require.Equal(t, int(block.cap), n)
}

func TestAppend32(t *testing.T) {
	n := 2  // 2 elements
	sz := 4 // int32
	block := New(BlockInt32, n)
	require.NotNil(t, block)
	require.True(t, block.IsMaterialized())
	require.Equal(t, int(block.len), 0)
	require.Equal(t, int(block.cap), n)

	// first append writes buf[0:4]
	block.Int32().Append(math.MaxInt32)
	require.Equal(t, block.data()[0:sz], maxIntBytes[8-sz:])
	require.Equal(t, int(block.len), 1)
	require.Equal(t, int(block.cap), n)

	// second append writes buf[4:8]
	block.Int32().Append(math.MaxInt32)
	require.Equal(t, block.data()[sz:sz+sz], maxIntBytes[8-sz:])
	require.Equal(t, int(block.len), 2)
	require.Equal(t, int(block.cap), n)

	// third append panics (capacity reached)
	require.Panics(t, func() {
		block.Int32().Append(math.MaxInt32)
	})
	require.Equal(t, int(block.len), 2)
	require.Equal(t, int(block.cap), n)
}

func TestAppend16(t *testing.T) {
	n := 2  // 2 elements
	sz := 2 // int16
	block := New(BlockInt16, n)
	require.NotNil(t, block)
	require.True(t, block.IsMaterialized())
	require.Equal(t, int(block.len), 0)
	require.Equal(t, int(block.cap), n)

	// first append writes buf[0:2]
	block.Int16().Append(math.MaxInt16)
	require.Equal(t, block.data()[0:sz], maxIntBytes[8-sz:])
	require.Equal(t, int(block.len), 1)
	require.Equal(t, int(block.cap), n)

	// second append writes buf[2:4]
	block.Int16().Append(math.MaxInt16)
	require.Equal(t, block.data()[sz:sz+sz], maxIntBytes[8-sz:])
	require.Equal(t, int(block.len), 2)
	require.Equal(t, int(block.cap), n)

	// third append panics (capacity reached)
	require.Panics(t, func() {
		block.Int16().Append(math.MaxInt16)
	})
	require.Equal(t, int(block.len), 2)
	require.Equal(t, int(block.cap), n)
}

func TestAppend8(t *testing.T) {
	n := 2  // 2 elements
	sz := 1 // int8
	block := New(BlockInt8, n)
	require.NotNil(t, block)
	require.True(t, block.IsMaterialized())
	require.Equal(t, int(block.len), 0)
	require.Equal(t, int(block.cap), n)

	// first append writes buf[0:1]
	block.Int8().Append(math.MaxInt8)
	require.Equal(t, block.data()[0:sz], maxIntBytes[8-sz:])
	require.Equal(t, int(block.len), 1)
	require.Equal(t, int(block.cap), n)

	// second append writes buf[1:2]
	block.Int8().Append(math.MaxInt8)
	require.Equal(t, block.data()[sz:sz+sz], maxIntBytes[8-sz:])
	require.Equal(t, int(block.len), 2)
	require.Equal(t, int(block.cap), n)

	// third append panics (capacity reached)
	require.Panics(t, func() {
		block.Int8().Append(math.MaxInt8)
	})
	require.Equal(t, int(block.len), 2)
	require.Equal(t, int(block.cap), n)
}

func TestGet64(t *testing.T) {
	block := New(BlockInt64, 1024)
	for i := 0; i < 1024; i++ {
		v := util.RandInt64()
		block.Int64().Append(v)
		w := block.Int64().Get(i)
		require.Equal(t, v, w)
	}
}

func TestSet64(t *testing.T) {
	block := New(BlockInt64, 1024)
	for i := 0; i < 1024; i++ {
		block.Int64().Append(util.RandInt64())
		block.Int64().Set(i, int64(i))
		require.Equal(t, int64(i), block.Int64().Get(i))
	}
}

func BenchmarkAppend(b *testing.B) {
	block := New(BlockInt64, 1024)
	b.ReportAllocs()
	for i := range b.N {
		if i%1024 == 0 {
			block.Clear()
		}
		block.Int64().Append(int64(i))
	}
}

func BenchmarkRead(b *testing.B) {
	block := New(BlockInt64, 1024)
	block.Int64().Append(math.MaxInt64)
	b.ReportAllocs()
	for b.Loop() {
		block.Int64().Get(0)
	}
}

func BenchmarkSet(b *testing.B) {
	block := New(BlockInt64, 1024)
	block.Int64().Append(math.MaxInt64)
	b.ReportAllocs()
	for b.Loop() {
		block.Int64().Set(0, math.MaxInt64)
	}
}
