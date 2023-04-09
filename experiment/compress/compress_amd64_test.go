// Copyright (c) 2022 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

//go:build go1.7 && amd64 && !gccgo && !appengine
// +build go1.7,amd64,!gccgo,!appengine

package main

import (
	"bytes"
	"math"
	"math/rand"
	"reflect"
	"testing"

	"blockwatch.cc/knoxdb/encoding/block"
	"blockwatch.cc/knoxdb/encoding/num"
	"blockwatch.cc/knoxdb/util"
	"blockwatch.cc/knoxdb/vec"
)

func bitsN(b int) func(n int) func() []uint64 {
	return func(n int) func() []uint64 {
		return bits(n, b)
	}
}

func ones(n int) func() []uint64 {
	return func() []uint64 {
		in := make([]uint64, n)
		for i := 0; i < n; i++ {
			in[i] = 1
		}
		return in
	}
}

func onesN() func(n int) func() []uint64 {
	return func(n int) func() []uint64 {
		return ones(n)
	}
}

func combineN(fns ...func(n int) func() []uint64) func(n int) func() []uint64 {
	return func(n int) func() []uint64 {
		var out []func() []uint64
		for _, fn := range fns {
			out = append(out, fn(n))
		}
		return combine(out...)
	}
}

// bits generates sequence of n numbers with max bits,
// ensuring max bit is set for 50% of the values.
func bits(n, bits int) func() []uint64 {
	return func() []uint64 {
		out := make([]uint64, n)
		maxVal := uint64(1 << uint8(bits))
		for i := range out {
			topBit := uint64((i & 1) << uint8(bits-1))
			out[i] = uint64(rand.Int63n(int64(maxVal))) | topBit
			if out[i] >= maxVal {
				panic("max")
			}
		}
		return out
	}
}

func combine(fns ...func() []uint64) func() []uint64 {
	return func() []uint64 {
		var out []uint64
		for _, fn := range fns {
			out = append(out, fn()...)
		}
		return out
	}
}

var s8bBenchmarkSize = 3000

var s8bBenchmarksUint64 = []struct {
	name string
	fn   func(n int) func() []uint64
	size int
}{
	{name: "0 bit", fn: onesN(), size: s8bBenchmarkSize},
	{name: "1 bit", fn: bitsN(1), size: s8bBenchmarkSize},
	{name: "2 bits", fn: bitsN(2), size: s8bBenchmarkSize},
	{name: "3 bits", fn: bitsN(3), size: s8bBenchmarkSize},
	{name: "4 bits", fn: bitsN(4), size: s8bBenchmarkSize},
	{name: "5 bits", fn: bitsN(5), size: s8bBenchmarkSize},
	{name: "6 bits", fn: bitsN(6), size: s8bBenchmarkSize},
	{name: "7 bits", fn: bitsN(7), size: s8bBenchmarkSize},
	{name: "8 bits", fn: bitsN(8), size: s8bBenchmarkSize},
	{name: "10 bits", fn: bitsN(10), size: s8bBenchmarkSize},
	{name: "12 bits", fn: bitsN(12), size: s8bBenchmarkSize},
	{name: "15 bits", fn: bitsN(15), size: s8bBenchmarkSize},
	{name: "20 bits", fn: bitsN(20), size: s8bBenchmarkSize},
	{name: "30 bits", fn: bitsN(30), size: s8bBenchmarkSize},
	{name: "60 bits", fn: bitsN(60), size: s8bBenchmarkSize},
	{name: "combination", fn: combineN(
		onesN(),
		bitsN(1),
		bitsN(2),
		bitsN(3),
		bitsN(4),
		bitsN(5),
		bitsN(6),
		bitsN(7),
		bitsN(8),
		bitsN(10),
		bitsN(12),
		bitsN(15),
		bitsN(20),
		bitsN(30),
		bitsN(60),
	), size: 15 * s8bBenchmarkSize},
}

func BenchmarkMatchUint64L2Unomp(b *testing.B) {
	if !util.UseAVX2 {
		b.Skip()
	}

	bm := s8bBenchmarksUint64[15]
	in := bm.fn(s8bBenchmarkSize)()

	bin := block.NewBlock(block.BlockTypeUint64, block.NoCompression, len(in), 0, 0)
	// bin.Uint64 = bin.Uint64[:len(in)]
	// copy(bin.Uint64, in)
	reflect.ValueOf(bin.Slice()).SetLen(len(in))
	copy(bin.Slice().([]uint64), in)

	//bout := block.NewBlock(block.BlockUint64, block.NoCompression, len(in))
	//buf := bytes.NewBuffer(make([]byte, 0, bin.MaxStoredSize()))

	//bin.Encode(buf)

	bits := vec.NewBitset(len(in))

	b.Run(bm.name, func(b *testing.B) {
		b.SetBytes(int64(8 * bm.size))
		for i := 0; i < b.N; i++ {
			//			bout.Decode(buf.Bytes(), len(in), len(in))
			num.MatchUint64Equal(bin.Slice().([]uint64), math.MaxUint64/2, bits, nil)
		}
	})
}

func BenchmarkMatchUint64L2Comp(b *testing.B) {
	if !util.UseAVX2 {
		b.Skip()
	}

	bm := s8bBenchmarksUint64[15]
	in := bm.fn(s8bBenchmarkSize)()

	bin := block.NewBlock(block.BlockTypeUint64, block.NoCompression, len(in), 0, 0)
	//	bin.Uint64 = bin.Uint64[:len(in)]
	//	copy(bin.Uint64, in)
	reflect.ValueOf(bin.Slice()).SetLen(len(in))
	copy(bin.Slice().([]uint64), in)

	bout := block.NewBlock(block.BlockTypeUint64, block.NoCompression, len(in), 0, 0)
	buf := bytes.NewBuffer(make([]byte, 0, bin.MaxStoredSize()))

	bin.Encode(buf)

	bits := vec.NewBitset(len(in))

	b.Run(bm.name, func(b *testing.B) {
		b.SetBytes(int64(8 * bm.size))
		for i := 0; i < b.N; i++ {
			bout.Decode(buf.Bytes(), len(in), len(in))
			num.MatchUint64Equal(bout.Slice().([]uint64), math.MaxUint64/2, bits, nil)
		}
	})
}
