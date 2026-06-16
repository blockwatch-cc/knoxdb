// Copyright (c) 2024-2026 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package schema_tests

import (
	"bytes"
	"cmp"
	"slices"
	"testing"
	"time"

	"blockwatch.cc/knoxdb/pkg/schema"
	"blockwatch.cc/knoxdb/pkg/schema/encode"
	"blockwatch.cc/knoxdb/pkg/schema/reflect"
	"github.com/stretchr/testify/require"
)

// goos: darwin
// goarch: arm64
// pkg: blockwatch.cc/knoxdb/pkg/schema/tests
// cpu: Apple M1 Max
// BenchmarkViewCut-10        	    13.28 ns/op        0 B/op       0 allocs/op
// BenchmarkViewCutSkip-10    	    13.29 ns/op        0 B/op       0 allocs/op
// BenchmarkView/reset-10     	    12.84 ns/op        0 B/op       0 allocs/op
// BenchmarkView/set_pk-10    	     2.248 ns/op       0 B/op       0 allocs/op
// BenchmarkView/get_fixed_any-10    4.852 ns/op       0 B/op       0 allocs/op
// BenchmarkView/get_fixed_phy-10    4.701 ns/op       0 B/op       0 allocs/op
// BenchmarkView/get_fixed_ptr-10    2.102 ns/op       0 B/op       0 allocs/op
// BenchmarkView/get_fixed_u64-10    2.096 ns/op       0 B/op       0 allocs/op
// BenchmarkView/get_var_any-10     17.75 ns/op       16 B/op       1 allocs/op
// BenchmarkView/get_var_phy-10     19.42 ns/op       24 B/op       1 allocs/op
// BenchmarkView/get_var_ptr-10      2.102 ns/op       0 B/op       0 allocs/op
// BenchmarkView/get_var_string-10   2.448 ns/op       0 B/op       0 allocs/op
// BenchmarkView/get_var_i128-10     2.304 ns/op       0 B/op       0 allocs/op
// BenchmarkView/get_var_i256-10     3.071 ns/op       0 B/op       0 allocs/op
//
// goos: linux
// goarch: amd64
// pkg: blockwatch.cc/knoxdb/pkg/schema/tests
// cpu: 12th Gen Intel(R) Core(TM) i9-12900K
// BenchmarkViewCut-24             10.68 ns/op        0 B/op       0 allocs/op
// BenchmarkViewCutSkip-24         10.01 ns/op        0 B/op       0 allocs/op
// BenchmarkView/reset-24           9.788 ns/op       0 B/op       0 allocs/op
// BenchmarkView/set_pk-24          1.475 ns/op       0 B/op       0 allocs/op
// BenchmarkView/get_fixed_any-24   3.495 ns/op       0 B/op       0 allocs/op
// BenchmarkView/get_fixed_phy-24   3.409 ns/op       0 B/op       0 allocs/op
// BenchmarkView/get_fixed_ptr-24   1.169 ns/op       0 B/op       0 allocs/op
// BenchmarkView/get_fixed_u64-24   1.178 ns/op       0 B/op       0 allocs/op
// BenchmarkView/get_var_any-24    46.32 ns/op       16 B/op       1 allocs/op
// BenchmarkView/get_var_phy-24    61.12 ns/op       24 B/op       1 allocs/op
// BenchmarkView/get_var_ptr-24     1.112 ns/op       0 B/op       0 allocs/op
// BenchmarkView/get_var_string-24  1.748 ns/op       0 B/op       0 allocs/op
// BenchmarkView/get_var_i128-24    1.382 ns/op       0 B/op       0 allocs/op
// BenchmarkView/get_var_i256-24    2.071 ns/op       0 B/op       0 allocs/op

// func (v *View) Int256(i int) num.Int256 {
// 	// 3.38ns
// 	// p, ok := v.getPtr(i, Int256)
// 	// if !ok {
// 	// 	return num.ZeroInt256
// 	// }
// 	// return num.Int256(*(*[4]uint64)(p))
// 	return num.Int256(*(*[4]uint64)(v.getPtr(i, Int256)))
//
// 	// 6.52ns
// 	// buf, ok := v.getBuf(i, Int256)
// 	// if !ok {
// 	// 	return num.ZeroInt256, false
// 	// }
// 	// return num.Int256FromBytes(buf), true
// }
//
// func (v *View) Int128(i int) num.Int128 {
// 	// p, ok := v.getPtr(i, Int128)
// 	// if !ok {
// 	// 	return num.ZeroInt128, false
// 	// }
// 	// 2.5ns
// 	// return num.Int128(*(*[2]uint64)(p)), true
// 	return num.Int128(*(*[2]uint64)(v.getPtr(i, Int128)))

// 	// 4.78ns
// 	// return num.Int128FromBytes(unsafe.Slice(p, 16)), true
// }

func BenchmarkViewCut(b *testing.B) {
	baseSchema := reflect.MustSchemaFor[AllTypes]()
	base := NewAllTypes(int64(0x0faf0faf0faf0faf))
	baseEnc := encode.NewEncoder(baseSchema)
	buf := bytes.NewBuffer(nil)
	_, err := baseEnc.Encode(&base, buf)
	require.NoError(b, err)
	_, err = baseEnc.Encode(&base, buf)
	require.NoError(b, err)
	view := schema.NewView(baseSchema)

	b.ReportAllocs()
	for b.Loop() {
		view.Cut(buf.Bytes())
	}
}

func BenchmarkViewCutSkip(b *testing.B) {
	var err error
	baseSchema := reflect.MustSchemaFor[AllTypes]()
	baseSchema, err = baseSchema.DeleteId(2)
	require.NoError(b, err)
	baseSchema, err = baseSchema.DeleteId(10)
	require.NoError(b, err)
	base := NewAllTypes(int64(0x0faf0faf0faf0faf))
	baseEnc := encode.NewEncoder(baseSchema)
	buf := bytes.NewBuffer(nil)
	_, err = baseEnc.Encode(&base, buf)
	require.NoError(b, err)
	_, err = baseEnc.Encode(&base, buf)
	require.NoError(b, err)
	view := schema.NewView(baseSchema)

	b.ReportAllocs()
	for b.Loop() {
		view.Cut(buf.Bytes())
	}
}

func BenchmarkView(b *testing.B) {
	baseSchema := reflect.MustSchemaFor[AllTypes]()
	base := NewAllTypes(int64(0x0faf0faf0faf0faf))
	baseEnc := encode.NewEncoder(baseSchema)
	buf := bytes.NewBuffer(nil)
	_, err := baseEnc.Encode(&base, buf)
	require.NoError(b, err)
	view := schema.NewView(baseSchema)
	view.Reset(buf.Bytes())
	b.Log(view.Schema().String())

	b.Run("reset", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			view.Reset(buf.Bytes())
		}
	})

	b.Run("set_pk", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			view.SetPk(1)
		}
	})

	b.Run("get_fixed_any", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			p := view.Get(0)
			_ = p
		}
	})

	b.Run("get_fixed_phy", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			p := view.GetPhy(0)
			_ = p
		}
	})

	b.Run("get_fixed_u64", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			p := view.Uint64(0)
			_ = p
		}
	})

	b.Run("get_var_any", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			p := view.Get(21)
			_ = p
		}
	})

	b.Run("get_var_phy", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			p := view.GetPhy(21)
			_ = p
		}
	})

	b.Run("get_var_string", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			p := view.String(21)
			_ = p
		}
	})

	b.Run("get_var_bytes", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			p := view.Bytes(19)
			_ = p
		}
	})

	b.Run("get_var_i128", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			p := view.Int128(15)
			_ = p
		}
	})

	b.Run("get_var_i256", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			p := view.Int256(16)
			_ = p
		}
	})
}

func BenchmarkWriterList(b *testing.B) {
	b.Run("lists_l1", func(b *testing.B) {
		// one level nested
		baseSchema := reflect.MustSchemaFor[ListFields]()
		w := schema.NewWriter(baseSchema, baseSchema.NewBuffer(1))
		base := NewListFields()

		b.ReportAllocs()
		for b.Loop() {
			w.Reset()
			w.WriteInt64(base.Int64a)
			// []uint64
			w.WriteList(func(lw *schema.ListWriter) error {
				lw.WriteUint64(base.U64List[0])
				lw.Next()
				lw.WriteUint64(base.U64List[1])
				return nil
			})

			// []time
			w.WriteList(func(lw *schema.ListWriter) error {
				lw.WriteDate(base.TimeList[0])
				lw.Next()
				lw.WriteDate(base.TimeList[1])
				return nil
			})

			// []Pair
			w.WriteList(func(lw *schema.ListWriter) error {
				lw.WriteInt64(base.PairList[0].Key)
				lw.WriteInt64(base.PairList[0].Val)
				lw.Next()
				lw.WriteInt64(base.PairList[1].Key)
				lw.WriteInt64(base.PairList[1].Val)
				return nil
			})

			// [][]byte
			w.WriteList(func(lw *schema.ListWriter) error {
				lw.WriteBytes(base.ByteList[0])
				lw.Next()
				lw.WriteBytes(base.ByteList[1])
				return nil
			})

			// [][2]byte
			w.WriteList(func(lw *schema.ListWriter) error {
				lw.WriteBytes(base.ArrList[0][:])
				lw.Next()
				lw.WriteBytes(base.ArrList[1][:])
				return nil
			})

			// []Decimal32
			w.WriteList(func(lw *schema.ListWriter) error {
				lw.WriteDecimal32(base.DecimalList[0])
				lw.Next()
				lw.WriteDecimal32(base.DecimalList[1])
				lw.Next()
				lw.WriteDecimal32(base.DecimalList[2])
				return nil
			})

			w.WriteInt64(base.Int64b)
		}
	})
}

func BenchmarkWriterMap(b *testing.B) {
	prim, err := reflect.SchemaFor[PrimMapRecord]()
	require.NoError(b, err)

	base := NewPrimMapRecord()
	attr := NewUnionMapRecord()

	b.Run("prim-generic", func(b *testing.B) {
		w := schema.NewWriter(prim, prim.NewBuffer(1))
		b.ReportAllocs()
		for b.Loop() {
			w.Reset()
			w.WriteMap(func(mw *schema.MapWriter) error {
				return schema.WriteMap(mw, base.U64)
			})
		}
	})

	b.Run("time-generic", func(b *testing.B) {
		w := schema.NewWriter(prim, prim.NewBuffer(1))
		b.ReportAllocs()
		for b.Loop() {
			w.Reset()
			w.Skip(5)
			w.WriteMap(func(mw *schema.MapWriter) error {
				return schema.WriteTimeMap(mw, base.Times)
			})
		}
	})

	b.Run("union-generic", func(b *testing.B) {
		w := schema.NewWriter(UnionMapRecordSchema, UnionMapRecordSchema.NewBuffer(1))
		b.ReportAllocs()
		for b.Loop() {
			w.Reset()
			w.WriteMap(func(mw *schema.MapWriter) error {
				return schema.MarshalMap(mw, attr.Unions)
			})
		}
	})

	b.Run("union-direct", func(b *testing.B) {
		w := schema.NewWriter(UnionMapRecordSchema, UnionMapRecordSchema.NewBuffer(1))
		b.ReportAllocs()
		for b.Loop() {
			w.Reset()
			w.WriteMap(func(mw *schema.MapWriter) error {
				keys := make([]string, 0, len(attr.Unions))
				for k := range attr.Unions {
					keys = append(keys, k)
				}
				slices.Sort(keys)

				for _, k := range keys {
					mw.WriteString(k)
					mw.WriteUnion(attr.Unions[k])
					mw.Next()
				}
				return mw.Err()
			})
		}
	})

	b.Run("union-unsorted", func(b *testing.B) {
		w := schema.NewWriter(UnionMapRecordSchema, UnionMapRecordSchema.NewBuffer(1))
		b.ReportAllocs()
		for b.Loop() {
			w.Reset()
			w.WriteMap(func(mw *schema.MapWriter) error {
				for n, u := range attr.Unions {
					mw.WriteString(n)
					mw.WriteUnion(u)
					mw.Next()
				}
				return mw.Err()
			})
		}
	})
}

func SortedKeys[K cmp.Ordered, V any](m map[K]V) []K {
	keys := make([]K, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}

func SortedTimes[K time.Time, V any](m map[K]V) []K {
	keys := make([]K, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.SortFunc(keys, schema.CompareTime)
	return keys
}
