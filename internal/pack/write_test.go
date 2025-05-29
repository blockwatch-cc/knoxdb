// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package pack

import (
	"fmt"
	"testing"
)

func BenchmarkAppendWire(b *testing.B) {
	for _, v := range testStructs {
		pkg := makeTypedPackage(v, 0)
		s := makeZeroStruct(v)
		buf := s.(Encodable).Encode()
		b.Run(fmt.Sprintf("%T/%d", v, PACK_SIZE), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				for range PACK_SIZE {
					pkg.AppendWire(buf, nil)
				}
				pkg.Clear()
			}
			b.ReportMetric(float64(PACK_SIZE*b.N)/float64(b.Elapsed().Nanoseconds()), "rec/ns")
		})
	}
}

func BenchmarkAppendWireE2E(b *testing.B) {
	for _, v := range testStructs {
		pkg := makeTypedPackage(v, 0)
		z := makeZeroStruct(v)
		b.Run(fmt.Sprintf("%T/%d", v, PACK_SIZE), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				for range PACK_SIZE {
					pkg.AppendWire(z.(Encodable).Encode(), nil)
				}
				pkg.Clear()
			}
			b.ReportMetric(float64(PACK_SIZE*b.N)/float64(b.Elapsed().Nanoseconds()), "rec/ns")

		})
	}
}
