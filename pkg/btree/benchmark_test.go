// Copyright (c) 2026 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package btree

import (
	"bytes"
	"fmt"
	"iter"
	"math/rand"
	"testing"

	"github.com/RaduBerinde/btreemap"
)

// goos: darwin
// goarch: arm64
// pkg: blockwatch.cc/knoxvm/core/btree
// cpu: Apple M1 Max
// BenchmarkBTreeMapSet/size_1000-10                986.8 ns/op   106 B/op   0 allocs/op
// BenchmarkBTreeMapSet/size_10000-10              1158 ns/op     106 B/op   0 allocs/op
// BenchmarkBTreeMapSet/size_100000-10             1090 ns/op     106 B/op   0 allocs/op
//
// BenchmarkBTreeMapGet/size_1000-10                 80.62 ns/op	0 B/op	 0 allocs/op
// BenchmarkBTreeMapGet/size_10000-10               147.2 ns/op	    0 B/op	 0 allocs/op
// BenchmarkBTreeMapGet/size_100000-10              259.5 ns/op	    0 B/op	 0 allocs/op
//
// BenchmarkBTreeMapDelete/size_1000-10              89.19 ns/op    0 B/op   0 allocs/op
// BenchmarkBTreeMapDelete/size_10000-10            155.6 ns/op     0 B/op   0 allocs/op
// BenchmarkBTreeMapDelete/size_100000-10           283.4 ns/op     0 B/op   0 allocs/op
//
// BenchmarkBTreeMapIter/size_1000-10              3263 ns/op        3.263 ns/key          0 B/op          0 allocs/op
// BenchmarkBTreeMapIter/size_10000-10            34494 ns/op        3.449 ns/key          0 B/op          0 allocs/op
// BenchmarkBTreeMapIter/size_100000-10          346003 ns/op        3.460 ns/key          0 B/op          0 allocs/op
//
// BenchmarkMergeK/size_1000-10                  233537 ns/op        77.85 ns/key	     944 B/op	      19 allocs/op
// BenchmarkMergeK/size_10000-10                2347618 ns/op        78.25 ns/key	     944 B/op	      19 allocs/op
// BenchmarkMergeK/size_100000-10              24244036 ns/op        80.81 ns/key	     975 B/op	      19 allocs/op
//
// BenchmarkMerge2/db_1000_tx_10-10               17391 ns/op        17.30 ns/key	   20568 B/op	      29 allocs/op
// BenchmarkMerge2/db_1000_tx_100-10              19121 ns/op        18.21 ns/key	   20568 B/op	      29 allocs/op
// BenchmarkMerge2/db_10000_tx_100-10            149372 ns/op        14.86 ns/key	   20568 B/op	      29 allocs/op
// BenchmarkMerge2/db_10000_tx_1000-10           179228 ns/op        17.07 ns/key	   20568 B/op	      29 allocs/op
// BenchmarkMerge2/db_100000_tx_1000-10         2070317 ns/op        20.60 ns/key	   20569 B/op	      29 allocs/op
// BenchmarkMerge2/db_100000_tx_10000-10        2036904 ns/op        19.40 ns/key	   20568 B/op	      29 allocs/op

// 32 byte random values
func generateRandomByteValues(n int) [][]byte {
	values := make([][]byte, n)
	for i := range values {
		buf := make([]byte, 32)
		for j := range buf {
			buf[j] = byte(rand.Intn(256))
		}
		values[i] = buf
	}
	return values
}

// 32 byte random keys
func generateRandomByteKeys(n int) [][]byte {
	keys := make([][]byte, n)
	for i := range keys {
		buf := make([]byte, 32)
		for j := range buf {
			buf[j] = byte(rand.Intn(256))
		}
		keys[i] = buf
	}
	return keys
}

func BenchmarkBTreeMapSet(b *testing.B) {
	sizes := []int{1000, 10000, 100000}
	for _, size := range sizes {
		b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
			tr := btreemap.New[[]byte, []byte](DefaultBtreeDegree, bytes.Compare)
			keys := generateRandomByteKeys(b.N)
			values := generateRandomByteValues(b.N)
			b.ResetTimer()
			b.ReportAllocs()
			totalKeys := 0
			for i := range b.N {
				tr.ReplaceOrInsert(keys[i%len(keys)], values[i%len(values)])
				totalKeys++
			}
			// Report time per visited key
			if totalKeys > 0 {
				nsPerKey := float64(b.Elapsed().Nanoseconds()) / float64(totalKeys)
				b.ReportMetric(nsPerKey, "ns/key")
			}
		})
	}
}

func BenchmarkBTreeMapGet(b *testing.B) {
	sizes := []int{1000, 10000, 100000}
	for _, size := range sizes {
		b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
			tr := btreemap.New[[]byte, []byte](DefaultBtreeDegree, bytes.Compare)
			keys := generateRandomByteKeys(size)
			values := generateRandomByteValues(size)
			for i, k := range keys {
				tr.ReplaceOrInsert(k, values[i])
			}
			b.ResetTimer()
			b.ReportAllocs()
			totalKeys := 0
			for i := range b.N {
				_, _, ok := tr.Get(keys[i%len(keys)])
				if ok {
					totalKeys++
				}
			}
			// Report time per visited key
			if totalKeys > 0 {
				nsPerKey := float64(b.Elapsed().Nanoseconds()) / float64(totalKeys)
				b.ReportMetric(nsPerKey, "ns/key")
			}
		})
	}
}

func BenchmarkBTreeMapDelete(b *testing.B) {
	sizes := []int{1000, 10000, 100000}
	for _, size := range sizes {
		b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
			tr := btreemap.New[[]byte, []byte](DefaultBtreeDegree, bytes.Compare)
			keys := generateRandomByteKeys(size)
			values := generateRandomByteValues(size)
			for i, k := range keys {
				tr.ReplaceOrInsert(k, values[i])
			}
			deleteKeys := generateRandomByteKeys(b.N)
			b.ResetTimer()
			b.ReportAllocs()
			totalKeys := 0
			for i := range b.N {
				_, _, _ = tr.Delete(deleteKeys[i%len(deleteKeys)])
				totalKeys++
			}
			// Report time per visited key
			if totalKeys > 0 {
				nsPerKey := float64(b.Elapsed().Nanoseconds()) / float64(totalKeys)
				b.ReportMetric(nsPerKey, "ns/key")
			}

		})
	}
}

func BenchmarkBTreeMapIter(b *testing.B) {
	sizes := []int{1000, 10000, 100000}
	for _, size := range sizes {
		b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
			tr := btreemap.New[[]byte, []byte](DefaultBtreeDegree, bytes.Compare)
			keys := generateRandomByteKeys(size)
			values := generateRandomByteValues(size)
			for i, k := range keys {
				tr.ReplaceOrInsert(k, values[i])
			}
			b.ResetTimer()
			b.ReportAllocs()
			totalKeys := 0
			for range b.N {
				for range tr.Ascend(btreemap.Min[[]byte](), btreemap.Max[[]byte]()) {
					// iterate all
					totalKeys++
				}
			}
			// Report time per visited key
			if totalKeys > 0 {
				nsPerKey := float64(b.Elapsed().Nanoseconds()) / float64(totalKeys)
				b.ReportMetric(nsPerKey, "ns/key")
			}
		})
	}
}

func BenchmarkMergeK(b *testing.B) {
	sizes := []int{1000, 10000, 100000}
	for _, size := range sizes {
		b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
			maps := make([]*btreemap.BTreeMap[[]byte, []byte], 3)
			seqs := make([]iter.Seq2[[]byte, []byte], len(maps))
			for i := range maps {
				maps[i] = btreemap.New[[]byte, []byte](DefaultBtreeDegree, bytes.Compare)
				keys := generateRandomByteKeys(size)
				values := generateRandomByteValues(size)
				for j, k := range keys {
					maps[i].ReplaceOrInsert(k, values[j])
				}
				seqs[i] = maps[i].Ascend(btreemap.Min[[]byte](), btreemap.Max[[]byte]())
			}
			b.ResetTimer()
			b.ReportAllocs()
			totalKeys := 0
			for range b.N {
				for range MergeK(seqs...) {
					// iterate all
					totalKeys++
				}
			}
			// Report time per visited key
			if totalKeys > 0 {
				nsPerKey := float64(b.Elapsed().Nanoseconds()) / float64(totalKeys)
				b.ReportMetric(nsPerKey, "ns/key")
			}
		})
	}
}

func BenchmarkMerge2(b *testing.B) {
	dbSizes := []int{1000, 10000, 100000}
	txFractions := []float64{0.01, 0.1} // 1/100th, 1/10th
	for _, dbSize := range dbSizes {
		for _, frac := range txFractions {
			txSize := max(int(float64(dbSize)*frac), 1)
			b.Run(fmt.Sprintf("db_%d_tx_%d", dbSize, txSize), func(b *testing.B) {
				// Create database btree
				db := btreemap.New[[]byte, []byte](DefaultBtreeDegree, bytes.Compare)
				dbKeys := generateRandomByteKeys(dbSize)
				dbValues := generateRandomByteValues(dbSize)
				for i, k := range dbKeys {
					db.ReplaceOrInsert(k, dbValues[i])
				}

				// Create transaction ChangeTree2
				tx := btreemap.New[[]byte, []byte](DefaultBtreeDegree, bytes.Compare)
				txKeys := generateRandomByteKeys(txSize)
				txValues := generateRandomByteValues(txSize)
				for i, k := range txKeys {
					if i%2 == 0 {
						// update
						tx.ReplaceOrInsert(k, txValues[i])
					} else {
						// delete
						tx.ReplaceOrInsert(k, nil)
					}
				}

				// Merge tx on top of db
				b.ResetTimer()
				b.ReportAllocs()
				totalKeys := 0
				for range b.N {
					merged := Merge2(
						tx.Ascend(btreemap.Min[[]byte](), btreemap.Max[[]byte]()),
						db.Ascend(btreemap.Min[[]byte](), btreemap.Max[[]byte]()),
					)
					for range merged {
						totalKeys++
					}
				}
				// Report time per visited key
				if totalKeys > 0 {
					nsPerKey := float64(b.Elapsed().Nanoseconds()) / float64(totalKeys)
					b.ReportMetric(nsPerKey, "ns/key")
				}
			})
		}
	}
}
