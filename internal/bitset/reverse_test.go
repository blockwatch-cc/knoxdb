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

func TestBitsetRunReverse(t *testing.T) {
	for _, c := range runTestcases {
		if c.Rruns == nil {
			continue
		}
		bits := NewBitsetFromBytes(c.Buf, c.Size)
		rev := bits.Reverse()
		var length int
		idx := bits.Len() - 1
		for i, r := range c.Rruns {
			n := f("%s_%d", c.Name, i)
			idx, length = rev.Run(idx - length)
			if got, want := idx, r[0]; got != want {
				// fmt.Printf("%d - %s: Reverse Bitfield %08b\n", x, c.name, rev.Bytes())
				// fmt.Printf("%d - %s: Runs %#v\n", x, c.name, c.rruns)
				t.Errorf("%s: unexpected index %d, expected %d", n, got, want)
			}
			if got, want := length, r[1]; got != want {
				// fmt.Printf("%d - %s: Reverse Bitfield %08b\n", x, c.name, rev.Bytes())
				// fmt.Printf("%d - %s: Runs %#v\n", x, c.name, c.rruns)
				t.Errorf("%s: unexpected length %d, expected %d", n, got, want)
			}
		}
	}
}
