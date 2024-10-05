// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package pack

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAppend(t *testing.T) {
	for _, v := range testStructs {
		t.Run(fmt.Sprintf("%T", v), func(t *testing.T) {
			pkg := makeTypedPackage(v, 1, 0)
			err := pkg.AppendStruct(v)
			require.NoError(t, err)
		})
	}
}

func BenchmarkAppend(b *testing.B) {
	for _, v := range testStructs {
		pkg := makeTypedPackage(v, PACK_SIZE, 0)
		b.Run(fmt.Sprintf("%T/%d", v, PACK_SIZE), func(b *testing.B) {
			b.ReportAllocs()
			for n := 0; n < b.N; n++ {
				for i := 0; i < PACK_SIZE; i++ {
					pkg.AppendStruct(v)
				}
				pkg.Clear()
			}
		})
	}
}

func TestAppendSlice(t *testing.T) {
	for _, v := range testStructs {
		t.Run(fmt.Sprintf("%T", v), func(t *testing.T) {
			pkg := makeTypedPackage(v, PACK_SIZE, 0)
			rslice := makeZeroSlice(v, PACK_SIZE)
			err := pkg.AppendSlice(rslice)
			require.NoError(t, err)
			require.Equal(t, PACK_SIZE, pkg.Len())
		})
	}
}

func BenchmarkAppendSlice(b *testing.B) {
	for _, v := range testStructs {
		pkg := makeTypedPackage(v, PACK_SIZE, 0)
		rslice := makeZeroSlice(v, PACK_SIZE)
		b.Run(fmt.Sprintf("%T/%d", v, PACK_SIZE), func(b *testing.B) {
			b.ReportAllocs()
			for n := 0; n < b.N; n++ {
				pkg.AppendSlice(rslice)
				pkg.Clear()
			}
		})
	}
}

func BenchmarkAppendWire(b *testing.B) {
	for _, v := range testStructs {
		pkg := makeTypedPackage(v, PACK_SIZE, 0)
		s := makeZeroStruct(v)
		buf := s.(Encodable).Encode()
		b.Run(fmt.Sprintf("%T/%d", v, PACK_SIZE), func(b *testing.B) {
			b.ReportAllocs()
			for n := 0; n < b.N; n++ {
				for i := 0; i < PACK_SIZE; i++ {
					pkg.AppendWire(buf)
				}
				pkg.Clear()
			}
		})
	}
}

func BenchmarkAppendWireE2E(b *testing.B) {
	for _, v := range testStructs {
		pkg := makeTypedPackage(v, PACK_SIZE, 0)
		z := makeZeroStruct(v)
		b.Run(fmt.Sprintf("%T/%d", v, PACK_SIZE), func(b *testing.B) {
			b.ReportAllocs()
			for n := 0; n < b.N; n++ {
				for i := 0; i < PACK_SIZE; i++ {
					pkg.AppendWire(z.(Encodable).Encode())
				}
				pkg.Clear()
			}
		})
	}
}
