// Copyright (c) 2018-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package dedup

import (
	"bytes"
	"math/rand"
	"testing"
)

type optimizeTest struct {
	Name string
	New  func(n int) [][]byte
}

var optTests = []optimizeTest{
	{
		Name: "native->fixed(empty)",
		New: func(n int) [][]byte {
			return [][]byte{}
		},
	},
	{
		Name: "native->fixed(nils)",
		New: func(n int) [][]byte {
			return [][]byte{nil, nil}
		},
	},
	{
		Name: "native->fixed(zero-len)",
		New: func(n int) [][]byte {
			return [][]byte{[]byte{}, []byte{}}
		},
	},
	{
		Name: "native->fixed",
		New: func(n int) [][]byte {
			return makeNumberedData(n)
		},
	},
	{
		Name: "native->compact(nil)",
		New: func(n int) [][]byte {
			d := makeNumberedData(n)
			return append(d, nil)
		},
	},
	{
		Name: "native->compact(double)",
		New: func(n int) [][]byte {
			d := makeNumberedData(n)
			return append(d, d...)
		},
	},
	{
		Name: "native->dict",
		New: func(n int) [][]byte {
			d := makeNumberedData(n)
			d = append(d, d...)
			d = append(d, d...)
			return d
		},
	},
}

func TestOptimize(t *testing.T) {
	rand.Seed(1337)
	for _, test := range optTests {
		t.Run(test.Name, func(t *testing.T) {
			data := test.New(1024)
			src := newNativeByteArrayFromBytes(append([][]byte{}, data...))

			if got, want := src.Len(), len(data); got != want {
				t.Errorf("Len mismatch got=%d want=%d", got, want)
			}

			// check source data
			for i := range data {
				if got, want := src.Elem(i), data[i]; bytes.Compare(got, want) != 0 {
					t.Errorf("1st Elem %d mismatch got=%x want=%x", i, got, want)
					t.FailNow()
				}
			}

			// optimize
			opt := src.Optimize()
			t.Logf("Optimized to %T\n", opt)

			// check target
			if got, want := opt.Len(), len(data); got != want {
				t.Errorf("Len mismatch got=%d want=%d", got, want)
			}
			for i := range data {
				if got, want := opt.Elem(i), data[i]; bytes.Compare(got, want) != 0 {
					t.Errorf("1st Elem %d mismatch got=%x want=%x", i, got, want)
					t.FailNow()
				}
			}

			// clear src
			src.Clear()

			// check target again
			if got, want := opt.Len(), len(data); got != want {
				t.Errorf("Len mismatch got=%d want=%d", got, want)
			}
			for i := range data {
				if got, want := opt.Elem(i), data[i]; bytes.Compare(got, want) != 0 {
					t.Errorf("1st Elem %d mismatch got=%x want=%x", i, got, want)
					t.FailNow()
				}
			}

			// materialize again
			mat := opt.Materialize()
			t.Logf("Materialized to %T\n", mat)

			// check target
			if got, want := mat.Len(), len(data); got != want {
				t.Errorf("Len mismatch got=%d want=%d", got, want)
			}
			for i := range data {
				if got, want := mat.Elem(i), data[i]; bytes.Compare(got, want) != 0 {
					t.Errorf("1st Elem %d mismatch got=%x want=%x", i, got, want)
					t.FailNow()
				}
			}

			// clear optimized
			opt.Clear()

			// check target
			if got, want := mat.Len(), len(data); got != want {
				t.Errorf("Len mismatch got=%d want=%d", got, want)
			}
			for i := range data {
				if got, want := mat.Elem(i), data[i]; bytes.Compare(got, want) != 0 {
					t.Errorf("1st Elem %d mismatch got=%x want=%x", i, got, want)
					t.FailNow()
				}
			}
		})
	}
}

// func TestNormalize(t *testing.T) {
//     rand.Seed(1337)
//     for i := 0; i < 100; i++ {
//         t.Run(strconv.Itoa(i), func(t *testing.T) {
//             data := makeRandData(DefaultMaxPointsPerBlock, nativeBufLen)
//             arr := newNativeByteArray(DefaultMaxPointsPerBlock)
//             if got, want := arr.Len(), 0; got != want {
//                 t.Errorf("Len mismatch got=%d want=%d", got, want)
//             }
//             if got, want := arr.Cap(), DefaultMaxPointsPerBlock; got != want {
//                 t.Errorf("Cap mismatch got=%d want=%d", got, want)
//             }
//             for i := range data {
//                 arr.Append(data[i])
//                 if got, want := arr.Elem(i), data[i]; bytes.Compare(got, want) != 0 {
//                     t.Errorf("Elem %d mismatch got=%x want=%x", i, got, want)
//                 }
//             }
//         })
//     }
// }
