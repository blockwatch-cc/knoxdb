package hashprobe

import (
	"fmt"
	"testing"

	"blockwatch.cc/knoxdb/internal/arena"
	"blockwatch.cc/knoxdb/internal/tests"
	"blockwatch.cc/knoxdb/pkg/util"
	"github.com/stretchr/testify/require"
)

// ----------------------------------------------------
// Tests
func TestDictGeneric(t *testing.T) {
	DictTest[uint64](t, buildDictGeneric)
	DictTest[uint32](t, buildDictGeneric)
	DictTest[int64](t, buildDictGeneric)
	DictTest[int32](t, buildDictGeneric)
}

func TestDictAVX2(t *testing.T) {
	if !util.UseAVX2 {
		t.Skip()
	}
	DictTest[uint64](t, buildDictAVX2)
	DictTest[int64](t, buildDictAVX2)
	DictTest[uint32](t, buildDictAVX2)
	DictTest[int32](t, buildDictAVX2)
}

type buildFunc[T Integer] func([]T, int) ([]T, []uint16)

type DictTestCase[T Integer] struct {
	Name string
	Data []T
}

func MakeDictTests[T Integer]() []DictTestCase[T] {
	return []DictTestCase[T]{
		{"tail4", tests.GenDups[T](3, 2, -1)},
		{"loop4", tests.GenDups[T](4, 2, -1)},
		{"loop_and_tail4", tests.GenDups[T](5, 2, -1)},
		{"tail8", tests.GenDups[T](7, 2, -1)},
		{"loop8", tests.GenDups[T](8, 2, -1)},
		{"loop_and_tail8", tests.GenDups[T](9, 2, -1)},
		{"large", tests.GenDups[T](1024, 128, -1)},
	}
}

var WarnSym = map[bool]string{false: "!!!"}

func DictTest[T Integer](t *testing.T, fn buildFunc[T]) {
	for _, c := range MakeDictTests[T]() {
		t.Run(fmt.Sprintf("%T/%s", T(0), c.Name), func(t *testing.T) {
			card := estimateCardinality(c.Data)
			dict, codes := fn(c.Data, card)
			require.Equal(t, card, len(dict), "dict len")
			require.Equal(t, len(c.Data), len(codes), "codes len")
			if len(codes) < 16 {
				for i, v := range codes {
					t.Logf("Val 0x%x => 0x%x code=%d %s", c.Data[i], dict[v], v, WarnSym[c.Data[i] == dict[v]])
				}
			}
			for i, v := range codes {
				require.Equal(t, c.Data[i], dict[v], "bad code")
			}
			if util.UseAVX2 {
				dictGen, codesGen := buildDictGeneric[T](c.Data, card)
				require.Equal(t, dictGen, dict, "dict mismatch")
				require.Equal(t, codesGen, codes, "codes mismatch")
			}
		})
	}
}

// ----------------------------------------------------
// Benchmarks
//

func BenchmarkDictMap(b *testing.B) {
	DictBenchmark[uint64](b, buildDictMap)
	DictBenchmark[uint32](b, buildDictMap)
}

func BenchmarkDictGeneric(b *testing.B) {
	DictBenchmark[uint64](b, buildDictGeneric)
	DictBenchmark[uint32](b, buildDictGeneric)
}

func BenchmarkDictAVX2(b *testing.B) {
	if !util.UseAVX2 {
		b.Skip()
	}
	DictBenchmark[uint64](b, buildDictAVX2)
	DictBenchmark[uint32](b, buildDictAVX2)
}

func DictBenchmark[T Integer](b *testing.B, fn buildFunc[T]) {
	for _, p := range tests.BenchmarkPatterns {
		for _, c := range tests.BenchmarkSizes {
			data := tests.GenDups[T](c.N, min(c.N, p.Size), -1)
			card := estimateCardinality(data)
			b.Run(fmt.Sprintf("%T/%s/%s", T(0), c.Name, p.Name), func(b *testing.B) {
				b.ReportAllocs()
				b.SetBytes(int64(c.N * 8))
				for range b.N {
					dict, codes := fn(data, card)
					card = len(dict)
					arena.FreeT(dict)
					arena.FreeT(codes)
				}
				_ = card
			})
		}
	}
}

func estimateCardinality[T Integer](vals []T) int {
	m := make(map[T]struct{})
	for _, v := range vals {
		m[v] = struct{}{}
	}
	return len(m)
}

func buildDictMap[T Integer](vals []T, numUnique int) ([]T, []uint16) {
	// construct unique values map
	uniqueMap := make(map[T]uint16, numUnique)

	for _, v := range vals {
		uniqueMap[v] = 0
	}

	// construct dict from unique values (apply FOR)
	dict := arena.AllocT[T](len(uniqueMap))[:0]
	for v := range uniqueMap {
		dict = append(dict, v)
	}

	// sort dict
	util.Sort(dict, 0)

	// remap dict codes to original values
	for i, v := range dict {
		uniqueMap[v] = uint16(i)
	}

	// translate values to codes
	codes := arena.AllocT[uint16](len(vals))[:0]
	for _, v := range vals {
		codes = append(codes, uniqueMap[v])
	}

	return dict, codes
}
