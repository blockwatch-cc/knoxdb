// Copyright (c) 2018-2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc
//

package pack

import (
	"math/rand"
	"testing"

	"blockwatch.cc/knoxdb/hash"
	"blockwatch.cc/knoxdb/hash/xxhash"
	"blockwatch.cc/knoxdb/util"
	"blockwatch.cc/knoxdb/vec"
)

type packBenchmarkSize struct {
	name string
	l    int
}

// generates n slices of length u
func randByteSlice(n, u int) [][]byte {
	s := make([][]byte, n)
	for i := 0; i < n; i++ {
		s[i] = randBytes(u)
	}
	return s
}

func randBytes(n int) []byte {
	v := make([]byte, n)
	for i, _ := range v {
		v[i] = byte(rand.Intn(256))
	}
	return v
}

var packBenchmarkSizes = []packBenchmarkSize{
	{"32", 32},
	{"128", 128},
	{"1K", 1 * 1024},
	{"16K", 16 * 1024},
	{"64K", 64 * 1024},
	{"128K", 128 * 1024},
	{"1M", 1024 * 1024},
	// {"16M", 16 * 1024 * 1024},
}

var f1 = Field{
	Index: 0,
	Name:  "uint64",
	Alias: "",
	Type:  FieldTypeUint64,
	Flags: FlagPrimary,
}

var f2 = Field{
	Index: 1,
	Name:  "int64",
	Alias: "",
	Type:  FieldTypeInt64,
	Flags: 0,
}

var f3 = Field{
	Index: 2,
	Name:  "float64",
	Alias: "",
	Type:  FieldTypeFloat64,
	Flags: 0,
}

var f4 = Field{
	Index: 3,
	Name:  "bytes",
	Alias: "",
	Type:  FieldTypeBytes,
	Flags: 0,
}

func makeTestPackage(sz int) *Package {
	pkg := NewPackage(sz)
	pkg.InitFields(FieldList{f1, f2, f3, f4}, nil)
	for i := 0; i < sz; i++ {
		pkg.Grow(1)
		pkg.SetFieldAt(0, i, uint64(i+1))
		pkg.SetFieldAt(1, i, rand.Intn(10))
		pkg.SetFieldAt(2, i, rand.Float64())
		pkg.SetFieldAt(3, i, randBytes(32))
	}
	return pkg
}

func BenchmarkConditionLoop1(B *testing.B) {
	for _, n := range packBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			pkg := makeTestPackage(n.l)
			conds := ConditionList{
				Condition{
					Field: f1,
					Mode:  FilterModeGt,
					Value: uint64(n.l / 2),
				},
			}
			conds.Compile(&Table{name: "test"})
			B.ResetTimer()
			B.ReportAllocs()
			B.SetBytes(int64(n.l) * 8)
			for b := 0; b < B.N; b++ {
				// this is the core of a typical matching loop
				// as used in the current table implementation
				for i, l := 0, pkg.Len(); i < l; i++ {
					ismatch := true
					for c, cl := 0, len(conds); c < cl; c++ {
						ismatch = conds[c].MatchAt(pkg, i)
						if !ismatch {
							break
						}
					}
					// skip non-matches
					if !ismatch {
						continue
					}
					// handle row
				}
			}
		})
	}
}

func BenchmarkConditionLoop2(B *testing.B) {
	for _, n := range packBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			pkg := makeTestPackage(n.l)
			conds := ConditionList{
				Condition{
					Field: f1,
					Mode:  FilterModeGt,
					Value: uint64(n.l / 2),
				},
				Condition{
					Field: f2,
					Mode:  FilterModeLt,
					Value: int64(8),
				},
			}
			conds.Compile(&Table{name: "test"})
			B.ResetTimer()
			B.ReportAllocs()
			B.SetBytes(int64(n.l) * 8)
			for b := 0; b < B.N; b++ {
				// this is the core of a typical matching loop
				// as used in the current table implementation
				for i, l := 0, pkg.Len(); i < l; i++ {
					ismatch := true
					for c, cl := 0, len(conds); c < cl; c++ {
						ismatch = conds[c].MatchAt(pkg, i)
						if !ismatch {
							break
						}
					}
					// skip non-matches
					if !ismatch {
						continue
					}
					// handle row
				}
			}
		})
	}
}

func BenchmarkConditionLoop4(B *testing.B) {
	for _, n := range packBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			pkg := makeTestPackage(n.l)
			conds := ConditionList{
				Condition{
					Field: f1,
					Mode:  FilterModeGt,
					Value: uint64(n.l / 2),
				},
				Condition{
					Field: f1,
					Mode:  FilterModeLt,
					Value: uint64(n.l / 4 * 3),
				},
				Condition{
					Field: f2,
					Mode:  FilterModeLt,
					Value: int64(8),
				},
				Condition{
					Field: f2,
					Mode:  FilterModeGt,
					Value: int64(3),
				},
			}
			conds.Compile(&Table{name: "test"})
			B.ResetTimer()
			B.ReportAllocs()
			B.SetBytes(int64(n.l) * 8)
			for b := 0; b < B.N; b++ {
				// this is the core of a typical matching loop
				// as used in the current table implementation
				for i, l := 0, pkg.Len(); i < l; i++ {
					ismatch := true
					for c, cl := 0, len(conds); c < cl; c++ {
						ismatch = conds[c].MatchAt(pkg, i)
						if !ismatch {
							break
						}
					}
					// skip non-matches
					if !ismatch {
						continue
					}
					// handle row
				}
			}
		})
	}
}

func BenchmarkConditionLoop6(B *testing.B) {
	for _, n := range packBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			pkg := makeTestPackage(n.l)
			conds := ConditionList{
				Condition{
					Field: f1,
					Mode:  FilterModeGt,
					Value: uint64(n.l / 2),
				},
				Condition{
					Field: f1,
					Mode:  FilterModeLt,
					Value: uint64(n.l / 4 * 3),
				},
				Condition{
					Field: f2,
					Mode:  FilterModeLt,
					Value: int64(8),
				},
				Condition{
					Field: f2,
					Mode:  FilterModeGt,
					Value: int64(3),
				},
				Condition{
					Field: f3,
					Mode:  FilterModeLt,
					Value: float64(100.0),
				},
				Condition{
					Field: f3,
					Mode:  FilterModeGt,
					Value: float64(-10000.1),
				},
			}
			conds.Compile(&Table{name: "test"})
			B.ResetTimer()
			B.ReportAllocs()
			B.SetBytes(int64(n.l) * 8)
			for b := 0; b < B.N; b++ {
				// this is the core of a typical matching loop
				// as used in the current table implementation
				for i, l := 0, pkg.Len(); i < l; i++ {
					ismatch := true
					for c, cl := 0, len(conds); c < cl; c++ {
						ismatch = conds[c].MatchAt(pkg, i)
						if !ismatch {
							break
						}
					}
					// skip non-matches
					if !ismatch {
						continue
					}
					// handle row
				}
			}
		})
	}
}

func BenchmarkConditionVector1(B *testing.B) {
	for _, n := range packBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			pkg := makeTestPackage(n.l)
			conds := ConditionList{
				Condition{
					Field: f1,
					Mode:  FilterModeGte,
					Value: uint64(n.l / 2),
				},
			}
			conds.Compile(&Table{name: "test"})
			B.ResetTimer()
			B.ReportAllocs()
			B.SetBytes(int64(n.l) * 8)
			for b := 0; b < B.N; b++ {
				// this is the core of a new matching loop design
				bits := conds.MatchPack(pkg, PackInfo{})
				for idx, length := bits.Run(0); idx >= 0; idx, length = bits.Run(idx + length) {
					// handle rows
				}
				bits.Close()
			}
		})
	}
}

func BenchmarkConditionVector2(B *testing.B) {
	for _, n := range packBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			pkg := makeTestPackage(n.l)
			conds := ConditionList{
				Condition{
					Field: f1,
					Mode:  FilterModeGte,
					Value: uint64(n.l / 2),
				},
				Condition{
					Field: f2,
					Mode:  FilterModeLt,
					Value: int64(8),
				},
			}
			conds.Compile(&Table{name: "test"})
			B.ResetTimer()
			B.ReportAllocs()
			B.SetBytes(int64(n.l) * 8)
			for b := 0; b < B.N; b++ {
				// this is the core of a new matching loop design
				bits := conds.MatchPack(pkg, PackInfo{})
				for idx, length := bits.Run(0); idx >= 0; idx, length = bits.Run(idx + length) {
					// handle rows
				}
				bits.Close()
			}
		})
	}
}

func BenchmarkConditionVector4(B *testing.B) {
	for _, n := range packBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			pkg := makeTestPackage(n.l)
			conds := ConditionList{
				Condition{
					Field: f1,
					Mode:  FilterModeGte,
					Value: uint64(n.l / 2),
				},
				Condition{
					Field: f1,
					Mode:  FilterModeLt,
					Value: uint64(n.l / 4 * 3),
				},
				Condition{
					Field: f2,
					Mode:  FilterModeLt,
					Value: int64(8),
				},
				Condition{
					Field: f2,
					Mode:  FilterModeGt,
					Value: int64(3),
				},
			}
			conds.Compile(&Table{name: "test"})
			B.ResetTimer()
			B.ReportAllocs()
			B.SetBytes(int64(n.l) * 8)
			for b := 0; b < B.N; b++ {
				// this is the core of a new matching loop design
				bits := conds.MatchPack(pkg, PackInfo{})
				for idx, length := bits.Run(0); idx >= 0; idx, length = bits.Run(idx + length) {
					// handle rows
				}
				bits.Close()
			}
		})
	}
}

func BenchmarkConditionVector6(B *testing.B) {
	for _, n := range packBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			pkg := makeTestPackage(n.l)
			conds := ConditionList{
				Condition{
					Field: f1,
					Mode:  FilterModeGte,
					Value: uint64(n.l / 2),
				},
				Condition{
					Field: f1,
					Mode:  FilterModeLt,
					Value: uint64(n.l / 4 * 3),
				},
				Condition{
					Field: f2,
					Mode:  FilterModeLt,
					Value: int64(8),
				},
				Condition{
					Field: f2,
					Mode:  FilterModeGt,
					Value: int64(3),
				},
				Condition{
					Field: f3,
					Mode:  FilterModeLt,
					Value: float64(100.0),
				},
				Condition{
					Field: f3,
					Mode:  FilterModeGt,
					Value: float64(-10000.1),
				},
			}
			conds.Compile(&Table{name: "test"})
			B.ResetTimer()
			B.ReportAllocs()
			B.SetBytes(int64(n.l) * 8)
			for b := 0; b < B.N; b++ {
				// this is the core of a new matching loop design
				bits := conds.MatchPack(pkg, PackInfo{})
				for idx, length := bits.Run(0); idx >= 0; idx, length = bits.Run(idx + length) {
					// handle rows
				}
				bits.Close()
			}
		})
	}
}

func BenchmarkFNVHash(B *testing.B) {
	testslice := randByteSlice(64*1024, 32)
	B.ResetTimer()
	B.ReportAllocs()
	for b := 0; b < B.N; b++ {
		h := hash.NewInlineFNV64a()
		h.Write(testslice[b%len(testslice)])
	}
}

func BenchmarkXXHash(B *testing.B) {
	testslice := randByteSlice(64*1024, 32)
	B.ResetTimer()
	B.ReportAllocs()
	for b := 0; b < B.N; b++ {
		xxhash.Sum64(testslice[b%len(testslice)])
	}
}

func BenchmarkInConditionLoop(B *testing.B) {
	for _, n := range packBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			pkg := makeTestPackage(n.l)
			// build IN slice of size 0.1*pack.Size() from
			// - 5% (min 2) pack values
			// - 5% random values
			checkN := util.Max(n.l/20, 2)
			inSlice := make([][]byte, 0, 2*checkN)
			for i := 0; i < checkN; i++ {
				// add existing values
				buf, err := pkg.BytesAt(3, rand.Intn(n.l))
				if err != nil {
					B.Fatalf("error with pack bytes: %v", err)
				}
				inSlice = append(inSlice, buf)
			}
			// add random values
			inSlice = append(inSlice, randByteSlice(checkN, 32)...)

			conds := ConditionList{
				Condition{
					Field: f4,
					Mode:  FilterModeIn,
					Value: inSlice,
				},
			}
			conds.Compile(&Table{name: "test"})
			B.ResetTimer()
			B.ReportAllocs()
			B.SetBytes(int64(n.l) * 32)
			for b := 0; b < B.N; b++ {
				// this is the core of a typical matching loop
				// as used in the current table implementation
				for i, l := 0, pkg.Len(); i < l; i++ {
					ismatch := true
					for c, cl := 0, len(conds); c < cl; c++ {
						ismatch = conds[c].MatchAt(pkg, i)
						if !ismatch {
							break
						}
					}
					// skip non-matches
					if !ismatch {
						continue
					}
					// handle row
				}
			}
		})
	}
}

func BenchmarkInConditionVector(B *testing.B) {
	for _, n := range packBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			pkg := makeTestPackage(n.l)
			// build IN slice of size 0.1*pack.Size() from
			// - 5% (min 2) pack values
			// - 5% random values
			checkN := util.Max(n.l/20, 2)
			inSlice := make([][]byte, 0, 2*checkN)
			for i := 0; i < checkN; i++ {
				// add existing values
				buf, err := pkg.BytesAt(3, rand.Intn(n.l))
				if err != nil {
					B.Fatalf("error with pack bytes: %v", err)
				}
				inSlice = append(inSlice, buf)
			}
			// add random values
			inSlice = append(inSlice, randByteSlice(checkN, 32)...)

			conds := ConditionList{
				Condition{
					Field: f4,
					Mode:  FilterModeIn,
					Value: inSlice,
				},
			}
			conds.Compile(&Table{name: "test"})
			B.ResetTimer()
			B.ReportAllocs()
			B.SetBytes(int64(n.l) * 32)
			for b := 0; b < B.N; b++ {
				// this is the core of a new matching loop design
				bits := conds.MatchPack(pkg, PackInfo{})
				for idx, length := bits.Run(0); idx >= 0; idx, length = bits.Run(idx + length) {
					// handle rows
				}
				bits.Close()
			}
		})
	}
}

func loopCheck(in, pk []uint64, bits *vec.BitSet) *vec.BitSet {
	for i, p, il, pl := 0, 0, len(in), len(pk); i < il && p < pl; {
		if pk[p] < in[i] {
			p++
		}
		if p == pl {
			break
		}
		if pk[p] > in[i] {
			i++
		}
		if i == il {
			break
		}
		if pk[p] == in[i] {
			bits.Set(p)
			i++
		}
	}
	return bits
}

func nestedLoopCheck(in, pk []uint64, bits *vec.BitSet) *vec.BitSet {
	maxin, maxpk := in[len(in)-1], pk[len(pk)-1]
	for i, p, il, pl := 0, 0, len(in), len(pk); i < il; {
		if pk[p] > maxin || maxpk < in[i] {
			// no more matches in this pack
			break
		}
		for pk[p] < in[i] && p < pl {
			p++
		}
		if p == pl {
			break
		}
		for pk[p] > in[i] && i < il {
			i++
		}
		if i == il {
			break
		}
		if pk[p] == in[i] {
			bits.Set(p)
			i++
		}
	}
	return bits
}

func mapCheck(in map[uint64]struct{}, pk []uint64, bits *vec.BitSet) *vec.BitSet {
	for i, v := range pk {
		if _, ok := in[v]; !ok {
			bits.Set(i)
		}
	}
	return bits
}

func BenchmarkInLoop(B *testing.B) {
	for _, n := range packBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			pk := make([]uint64, n.l)
			for i := 0; i < n.l; i++ {
				pk[i] = uint64(i + 1)
			}
			// build IN slice of size 0.1*pack.Size() from
			// - 10% (min 2) pack values
			checkN := util.Max(n.l/10, 2)
			inSlice := make([]uint64, checkN)
			for i := 0; i < checkN; i++ {
				// add existing values
				inSlice[i] = pk[rand.Intn(n.l)]
			}
			// unique and sort
			inSlice = vec.UniqueUint64Slice(inSlice)

			B.ResetTimer()
			B.ReportAllocs()
			B.SetBytes(int64(n.l) * 8)
			for b := 0; b < B.N; b++ {
				// this is the core of a new matching loop design
				loopCheck(inSlice, pk, vec.NewBitSet(n.l)).Close()
			}
		})
	}
}

func BenchmarkInNestedLoop(B *testing.B) {
	for _, n := range packBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			pk := make([]uint64, n.l)
			for i := 0; i < n.l; i++ {
				pk[i] = uint64(i + 1)
			}
			// build IN slice of size 0.1*pack.Size() from
			// - 10% (min 2) pack values
			checkN := util.Max(n.l/10, 2)
			inSlice := make([]uint64, checkN)
			for i := 0; i < checkN; i++ {
				// add existing values
				inSlice[i] = pk[rand.Intn(n.l)]
			}
			// unique and sort
			inSlice = vec.UniqueUint64Slice(inSlice)

			B.ResetTimer()
			B.ReportAllocs()
			B.SetBytes(int64(n.l) * 8)
			for b := 0; b < B.N; b++ {
				// this is the core of a new matching loop design
				nestedLoopCheck(inSlice, pk, vec.NewBitSet(n.l)).Close()
			}
		})
	}
}

func BenchmarkInMap(B *testing.B) {
	for _, n := range packBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			pk := make([]uint64, n.l)
			inmap := make(map[uint64]struct{}, n.l)
			for i := 0; i < n.l; i++ {
				pk[i] = uint64(i + 1)
			}
			// build IN slice of size 0.1*pack.Size() from
			// - 10% (min 2) pack values
			checkN := util.Max(n.l/10, 2)
			for i := 0; i < checkN; i++ {
				// add existing values
				inmap[pk[rand.Intn(n.l)]] = struct{}{}
			}

			B.ResetTimer()
			B.ReportAllocs()
			B.SetBytes(int64(n.l) * 8)
			for b := 0; b < B.N; b++ {
				// this is the core of a new matching loop design
				mapCheck(inmap, pk, vec.NewBitSet(n.l)).Close()
			}
		})
	}
}
