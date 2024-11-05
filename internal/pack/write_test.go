// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package pack

import (
	"fmt"
	"testing"
)

func BenchmarkAppendWire(b *testing.B) {
	for _, v := range testStructs {
		pkg := makeTypedPackage(v, PACK_SIZE, 0)
		s := makeZeroStruct(v)
		buf := s.(Encodable).Encode()
		b.Run(fmt.Sprintf("%T/%d", v, PACK_SIZE), func(b *testing.B) {
			b.ReportAllocs()
			for n := 0; n < b.N; n++ {
				for i := 0; i < PACK_SIZE; i++ {
					pkg.AppendWire(buf, nil)
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
					pkg.AppendWire(z.(Encodable).Encode(), nil)
				}
				pkg.Clear()
			}
		})
	}
}
