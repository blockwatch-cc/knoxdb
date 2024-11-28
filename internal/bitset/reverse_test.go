// Copyright (c) 2023 Blockwatch Data Inc.
// Author: alex@blockwatch.cc
//

package bitset

import (
	"testing"

	"blockwatch.cc/knoxdb/internal/bitset/tests"
)

var (
	runTestcases = tests.RunTestcases
)

func TestBitsetRunReverse(T *testing.T) {
	for _, c := range runTestcases {
		if c.Rruns == nil {
			continue
		}
		bits := NewBitsetFromBytes(c.Buf, c.Size)
		rev := bits.Reverse()
		var length int
		idx := bits.Len() - 1
		for i, r := range c.Rruns {
			T.Run(f("%s_%d", c.Name, i), func(t *testing.T) {
				idx, length = rev.Run(idx - length)
				if got, want := idx, r[0]; got != want {
					// fmt.Printf("%d - %s: Reverse Bitfield %08b\n", x, c.name, rev.Bytes())
					// fmt.Printf("%d - %s: Runs %#v\n", x, c.name, c.rruns)
					T.Errorf("unexpected index %d, expected %d", got, want)
				}
				if got, want := length, r[1]; got != want {
					// fmt.Printf("%d - %s: Reverse Bitfield %08b\n", x, c.name, rev.Bytes())
					// fmt.Printf("%d - %s: Runs %#v\n", x, c.name, c.rruns)
					T.Errorf("unexpected length %d, expected %d", got, want)
				}
			})
		}
	}
}
