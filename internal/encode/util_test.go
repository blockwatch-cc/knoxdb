// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package encode

import (
	"slices"
	"testing"

	"blockwatch.cc/knoxdb/internal/tests"
	"blockwatch.cc/knoxdb/pkg/util"
	"github.com/stretchr/testify/require"
)

func TestUniqueArray(t *testing.T) {
	for _, c := range tests.BenchmarkSizes {
		data := util.RandInts[int16](c.N)
		minx := slices.Min(data)
		maxx := slices.Max(data)

		// map
		u := make(map[int16]struct{}, c.N)
		for _, v := range data {
			u[v] = struct{}{}
		}

		// array
		var card int
		a := make([]uint16, int(maxx)-int(minx)+1)
		for _, v := range data {
			a[int(v)-int(minx)] = 1
		}
		for _, v := range a {
			if v > 0 {
				card++
			}
		}
		require.Equal(t, card, len(u))
	}
}
