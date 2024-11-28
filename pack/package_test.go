// Copyright (c) 2018-2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc
//go:build ignore
// +build ignore

package pack

import (
	"math/rand"
	"testing"
	"time"

	"blockwatch.cc/knoxdb/encoding/block"
)

var packBenchmarkReadWriteSizes = []packBenchmarkSize{
	{"1", 1},
	{"1K", 1 * 1024},
	{"16K", 16 * 1024},
	{"32K", 32 * 1024},
	{"64K", 64 * 1024},
	{"128K", 128 * 1024},
}

var readWriteTestFields = FieldList{
	&Field{Index: 0, Name: "I", Alias: "row_id", Type: FieldTypeUint64, Flags: FlagPrimary},
	&Field{Index: 1, Name: "T", Alias: "time", Type: FieldTypeDatetime, Flags: 0},
	&Field{Index: 2, Name: "h", Alias: "height", Type: FieldTypeUint64, Flags: 0},
	&Field{Index: 3, Name: "p", Alias: "tx_n", Type: FieldTypeInt64, Flags: 0},
	&Field{Index: 4, Name: "H", Alias: "tx_id", Type: FieldTypeBytes, Flags: 0},
	&Field{Index: 5, Name: "L", Alias: "locktime", Type: FieldTypeInt64, Flags: 0},
	&Field{Index: 6, Name: "s", Alias: "size", Type: FieldTypeInt64, Flags: 0},
	&Field{Index: 7, Name: "S", Alias: "vsize", Type: FieldTypeInt64, Flags: 0},
	&Field{Index: 8, Name: "V", Alias: "version", Type: FieldTypeInt64, Flags: 0},
	&Field{Index: 9, Name: "N", Alias: "n_in", Type: FieldTypeInt64, Flags: 0},
	&Field{Index: 10, Name: "n", Alias: "n_out", Type: FieldTypeInt64, Flags: 0},
	&Field{Index: 11, Name: "t", Alias: "type", Type: FieldTypeInt64, Flags: 0},
	&Field{Index: 12, Name: "D", Alias: "has_data", Type: FieldTypeBoolean, Flags: 0},
	&Field{Index: 13, Name: "v", Alias: "volume", Type: FieldTypeUint64, Flags: 0},
	&Field{Index: 14, Name: "f", Alias: "fee", Type: FieldTypeUint64, Flags: 0},
	&Field{Index: 15, Name: "d", Alias: "days", Type: FieldTypeFloat64, Flags: 0},

	// &Field{Index: 16, Name: "1", Alias: "i128", Type: FieldTypeInt128, Flags: 0},
	// &Field{Index: 17, Name: "2", Alias: "i256", Type: FieldTypeInt256, Flags: 0},
	// &Field{Index: 18, Name: "3", Alias: "d32", Type: FieldTypeDecimal32, Flags: 0, Scale: 5},
	// &Field{Index: 19, Name: "4", Alias: "d64", Type: FieldTypeDecimal64, Flags: 0, Scale: 12},
	// &Field{Index: 20, Name: "5", Alias: "d128", Type: FieldTypeDecimal128, Flags: 0, Scale: 18},
	// &Field{Index: 21, Name: "6", Alias: "d256", Type: FieldTypeDecimal256, Flags: 0, Scale: 24},
}

// Data size is compressed size
//
// goos: darwin
// goarch: amd64
// pkg: blockwatch.cc/toolkit/pack
// BenchmarkPackWriteLZ4/1K        	    1000	   1167718 ns/op	  53.35 MB/s	 4460816 B/op	      87 allocs/op
// BenchmarkPackWriteLZ4/16K       	     200	   9512211 ns/op	 103.26 MB/s	 5601496 B/op	      93 allocs/op
// BenchmarkPackWriteLZ4/32K       	     100	  19081336 ns/op	 102.87 MB/s	 9263065 B/op	      88 allocs/op
// BenchmarkPackWriteLZ4/64K       	      50	  39172961 ns/op	 100.17 MB/s	21299375 B/op	     101 allocs/op
// BenchmarkPackWriteLZ4/128K      	      20	  80532083 ns/op	  97.43 MB/s	50872220 B/op	     103 allocs/op
// BenchmarkPackWriteSnappy/1K     	    2000	    915603 ns/op	  66.84 MB/s	 4438657 B/op	     101 allocs/op
// BenchmarkPackWriteSnappy/16K    	     200	   5678233 ns/op	 169.77 MB/s	 6237840 B/op	     109 allocs/op
// BenchmarkPackWriteSnappy/32K    	     100	  11477969 ns/op	 168.55 MB/s	10365631 B/op	     102 allocs/op
// BenchmarkPackWriteSnappy/64K    	     100	  23876029 ns/op	 162.02 MB/s	23494856 B/op	     115 allocs/op
// BenchmarkPackWriteSnappy/128K   	      30	  50087700 ns/op	 154.43 MB/s	55272280 B/op	     118 allocs/op
// BenchmarkPackWriteNoCompression/1K   2000	    813656 ns/op	  78.01 MB/s	 4453714 B/op	     102 allocs/op
// BenchmarkPackWriteNoCompression/16K   300	   5375057 ns/op	 187.17 MB/s	 5617469 B/op	     108 allocs/op
// BenchmarkPackWriteNoCompression/32K   100	  10948443 ns/op	 183.28 MB/s	 9205593 B/op	     102 allocs/op
// BenchmarkPackWriteNoCompression/64K    50	  22763296 ns/op	 176.81 MB/s	21262894 B/op	     115 allocs/op
// BenchmarkPackWriteNoCompression/128K   30	  48718267 ns/op	 165.18 MB/s	50841860 B/op	     117 allocs/op
// PASS
// ok  	blockwatch.cc/toolkit/pack	30.219s

// goos: darwin
// goarch: amd64
// pkg: blockwatch.cc/toolkit/pack
// BenchmarkPackReadLZ4/1K        	   10000	    107795 ns/op	 578.64 MB/s	  192019 B/op	     131 allocs/op
// BenchmarkPackReadLZ4/16K       	    1000	   1447803 ns/op	 678.42 MB/s	 2834599 B/op	     128 allocs/op
// BenchmarkPackReadLZ4/32K       	     500	   3347326 ns/op	 586.44 MB/s	10012935 B/op	     129 allocs/op
// BenchmarkPackReadLZ4/64K       	     200	   7251108 ns/op	 541.21 MB/s	19843122 B/op	     130 allocs/op
// BenchmarkPackReadLZ4/128K      	     100	  15656870 ns/op	 501.14 MB/s	39376785 B/op	     131 allocs/op
// BenchmarkPackReadSnappy/1K     	   20000	     95153 ns/op	 643.49 MB/s	  221735 B/op	     116 allocs/op
// BenchmarkPackReadSnappy/16K    	    1000	   1337367 ns/op	 723.43 MB/s	 3313732 B/op	     113 allocs/op
// BenchmarkPackReadSnappy/32K    	     500	   2711739 ns/op	 713.45 MB/s	 6601602 B/op	     112 allocs/op
// BenchmarkPackReadSnappy/64K    	     300	   5639873 ns/op	 685.85 MB/s	13147904 B/op	     113 allocs/op
// BenchmarkPackReadSnappy/128K   	     100	  12194095 ns/op	 634.38 MB/s	26172983 B/op	     114 allocs/op
// BenchmarkPackReadNoCompression/1K   20000	     84247 ns/op	 752.92 MB/s	  187678 B/op	      85 allocs/op
// BenchmarkPackReadNoCompression/16K   2000	   1234316 ns/op	 815.40 MB/s	 2799879 B/op	      82 allocs/op
// BenchmarkPackReadNoCompression/32K   1000	   2395720 ns/op	 840.12 MB/s	 5585170 B/op	      83 allocs/op
// BenchmarkPackReadNoCompression/64K    300	   4936602 ns/op	 815.27 MB/s	11155720 B/op	      82 allocs/op
// BenchmarkPackReadNoCompression/128K   100	  10104651 ns/op	 796.56 MB/s	22296842 B/op	      83 allocs/op
// PASS
// ok  	blockwatch.cc/toolkit/pack	33.510s

func makeReadWriteTestPackage(fields FieldList, c block.Compression, sz int) *Package {
	switch c {
	case block.SnappyCompression:
		for i := range fields {
			// store hashes uncompressed
			if fields[i].Name == "H" {
				continue
			}
			fields[i].Flags &^= FlagCompressLZ4
			fields[i].Flags |= FlagCompressSnappy
		}
	case block.LZ4Compression:
		for i := range fields {
			// store hashes uncompressed
			if fields[i].Name == "H" {
				continue
			}
			fields[i].Flags &^= FlagCompressSnappy
			fields[i].Flags |= FlagCompressLZ4
		}
	case block.NoCompression:
		for i := range fields {
			// store hashes uncompressed
			if fields[i].Name == "H" {
				continue
			}
			fields[i].Flags &^= FlagCompressLZ4 | FlagCompressSnappy
		}
	}
	pkg := NewPackage(sz, nil)
	pkg.InitFields(fields, nil)
	now := time.Now().UTC()
	for i := 0; i < sz; i++ {
		pkg.Grow(1)
		pkg.SetFieldAt(0, i, uint64(i+1))
		pkg.SetFieldAt(1, i, now.Add(time.Duration(i+rand.Intn(10))*time.Minute))
		pkg.SetFieldAt(2, i, uint64(i+1))                            // height
		pkg.SetFieldAt(3, i, rand.Intn(1000))                        // tx_n
		pkg.SetFieldAt(4, i, randBytes(32))                          // tx_id
		pkg.SetFieldAt(5, i, int64(i+rand.Intn(1000)))               // locktime
		pkg.SetFieldAt(6, i, rand.Intn(4096))                        // size
		pkg.SetFieldAt(7, i, rand.Intn(4096))                        // vsize
		pkg.SetFieldAt(8, i, 2)                                      // version
		pkg.SetFieldAt(9, i, rand.Intn(5))                           // n_in
		pkg.SetFieldAt(10, i, rand.Intn(100))                        // n_out
		pkg.SetFieldAt(11, i, rand.Intn(5))                          // type
		pkg.SetFieldAt(12, i, rand.Intn(1) > 0)                      // has_data
		pkg.SetFieldAt(13, i, uint64(rand.Int63n(2100000000000000))) // volume
		pkg.SetFieldAt(14, i, uint64(rand.Intn(100000000)))          // fee
		pkg.SetFieldAt(15, i, float64(rand.Intn(100000))/1000.0)     // days

		// pkg.SetFieldAt(16, i, vec.Int128FromInt64(rand.Int63n(2100000000000000)))
		// pkg.SetFieldAt(17, i, vec.Int256FromInt64(rand.Int63n(2100000000000000)))
		// pkg.SetFieldAt(18, i, decimal.NewDecimal32(int32(rand.Intn(100000000)), 2))
		// pkg.SetFieldAt(19, i, decimal.NewDecimal64(int64(rand.Intn(100000000)), 4))
		// pkg.SetFieldAt(20, i, decimal.NewDecimal128(vec.Int128FromInt64(rand.Int63n(2100000000000000)).Mul64(1000000000), 10))
		// pkg.SetFieldAt(21, i, decimal.NewDecimal256(vec.Int256FromInt64(rand.Int63n(2100000000000000)).Mul(vec.Int256FromInt64(1000000000)), 20))

	}
	return pkg
}

func BenchmarkPackWriteLZ4(B *testing.B) {
	for _, n := range packBenchmarkReadWriteSizes {
		B.Run(n.name, func(B *testing.B) {
			pkg := makeReadWriteTestPackage(readWriteTestFields, block.LZ4Compression, n.l)
			buf, err := pkg.MarshalBinary()
			if err != nil {
				B.Fatalf("write error: %v", err)
			}
			B.ResetTimer()
			B.ReportAllocs()
			B.SetBytes(int64(len(buf)))
			for b := 0; b < B.N; b++ {
				_, _ = pkg.MarshalBinary()
			}
		})
	}
}

func BenchmarkPackWriteSnappy(B *testing.B) {
	for _, n := range packBenchmarkReadWriteSizes {
		B.Run(n.name, func(B *testing.B) {
			pkg := makeReadWriteTestPackage(readWriteTestFields, block.SnappyCompression, n.l)
			buf, err := pkg.MarshalBinary()
			if err != nil {
				B.Fatalf("write error: %v", err)
			}
			B.ResetTimer()
			B.ReportAllocs()
			B.SetBytes(int64(len(buf)))
			for b := 0; b < B.N; b++ {
				_, err := pkg.MarshalBinary()
				if err != nil {
					B.Fatalf("write error: %v", err)
				}
			}
		})
	}
}

func BenchmarkPackWriteNoCompression(B *testing.B) {
	for _, n := range packBenchmarkReadWriteSizes {
		B.Run(n.name, func(B *testing.B) {
			pkg := makeReadWriteTestPackage(readWriteTestFields, block.NoCompression, n.l)
			buf, err := pkg.MarshalBinary()
			if err != nil {
				B.Fatalf("write error: %v", err)
			}
			B.ResetTimer()
			B.ReportAllocs()
			B.SetBytes(int64(len(buf)))
			for b := 0; b < B.N; b++ {
				_, err := pkg.MarshalBinary()
				if err != nil {
					B.Fatalf("write error: %v", err)
				}
			}
		})
	}
}

func BenchmarkPackReadLZ4(B *testing.B) {
	for _, n := range packBenchmarkReadWriteSizes {
		B.Run(n.name, func(B *testing.B) {
			pkg := makeReadWriteTestPackage(readWriteTestFields, block.LZ4Compression, n.l)
			buf, err := pkg.MarshalBinary()
			if err != nil {
				B.Fatalf("write error: %v", err)
			}
			B.ResetTimer()
			B.ReportAllocs()
			B.SetBytes(int64(len(buf)))
			for b := 0; b < B.N; b++ {
				pkg2 := NewPackage(0, nil)
				err := pkg2.UnmarshalBinary(buf)
				if err != nil {
					B.Fatalf("read error: %v", err)
				}
				pkg2.Release()
			}
		})
	}
}

func BenchmarkPackReadSnappy(B *testing.B) {
	for _, n := range packBenchmarkReadWriteSizes {
		B.Run(n.name, func(B *testing.B) {
			pkg := makeReadWriteTestPackage(readWriteTestFields, block.SnappyCompression, n.l)
			buf, err := pkg.MarshalBinary()
			if err != nil {
				B.Fatalf("write error: %v", err)
			}
			B.ResetTimer()
			B.ReportAllocs()
			B.SetBytes(int64(len(buf)))
			for b := 0; b < B.N; b++ {
				pkg2 := NewPackage(0, nil)
				err := pkg2.UnmarshalBinary(buf)
				if err != nil {
					B.Fatalf("read error: %v", err)
				}
				pkg2.Release()
			}
		})
	}
}

func BenchmarkPackReadNoCompression(B *testing.B) {
	for _, n := range packBenchmarkReadWriteSizes {
		B.Run(n.name, func(B *testing.B) {
			pkg := makeReadWriteTestPackage(readWriteTestFields, block.NoCompression, n.l)
			buf, err := pkg.MarshalBinary()
			if err != nil {
				B.Fatalf("write error: %v", err)
			}
			B.ResetTimer()
			B.ReportAllocs()
			B.SetBytes(int64(len(buf)))
			for b := 0; b < B.N; b++ {
				pkg2 := NewPackage(0, nil)
				err := pkg2.UnmarshalBinary(buf)
				if err != nil {
					B.Fatalf("read error: %v", err)
				}
				pkg2.Release()
			}
		})
	}
}
