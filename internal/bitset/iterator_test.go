// Copyright (c) 2023-2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package bitset

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIterator(t *testing.T) {
	for _, sz := range bitsetSizes {
		for _, pat := range bitsetPatterns {
			t.Run(f("%d_%x", sz, pat), func(t *testing.T) {
				for _, src := range randBitsets(sz) {
					require.Equal(t, sz, src.Len(), "length")
					require.Equal(t, src.Count(), popcount(src.Bytes()), "popcount")

					// check all iterator bits are set
					for v := range src.Iterator() {
						require.True(t, src.Contains(v), v, "bit %d in 0x%x", v, src.Bytes())
					}

					// ensure pow2 space to prevent out of bounds AVX2 write
					idx := src.Indexes(make([]uint32, src.Count()+8))
					require.Len(t, idx, src.Count(), "indexes")

					var i int
					for v := range src.Iterator() {
						require.Equal(t, int(idx[i]), v, "bit %d in 0x%x", i, src.Bytes())
						i++
					}
				}
			})
		}
	}
}

func TestIterate(t *testing.T) {
	for _, sz := range bitsetSizes {
		for _, pat := range bitsetPatterns {
			t.Run(f("%d_%x", sz, pat), func(t *testing.T) {
				for _, src := range randBitsets(sz) {
					require.Equal(t, sz, src.Len(), "length")
					require.Equal(t, src.Count(), popcount(src.Bytes()), "popcount")

					// ensure pow2 space to prevent out of bounds AVX2 write
					idx := src.Indexes(make([]uint32, src.Count()+8))
					require.Len(t, idx, src.Count(), "indexes")

					var (
						buf  [128]int // alloc once and reuse
						last = -1
						i    int
					)
					for {
						// fetch next chunk
						vals, ok := src.Iterate(last, buf[:])
						if !ok {
							break
						}
						for _, v := range vals {
							// check all iterator bits are set
							require.True(t, src.Contains(v), "bit %d in 0x%x", v, src.Bytes())

							// check iterator against indexes
							require.Equal(t, int(idx[i]), v, "bit %d in 0x%x", i, src.Bytes())
							i++
						}

						// update loop
						last = vals[len(vals)-1]
					}
				}
			})
		}
	}
}

func TestIterateChunk(t *testing.T) {
	for _, sz := range bitsetSizes {
		for _, pat := range bitsetPatterns {
			t.Run(f("%d_%x", sz, pat), func(t *testing.T) {
				for _, src := range randBitsets(sz) {
					require.Equal(t, sz, src.Len(), "length")
					require.Equal(t, src.Count(), popcount(src.Bytes()), "popcount")

					// ensure pow2 space to prevent out of bounds AVX2 write
					idx := src.Indexes(make([]uint32, src.Count()+8))
					require.Len(t, idx, src.Count(), "indexes")

					var (
						it = src.Chunks()
						i  int
					)
					for {
						// fetch next chunk
						vals, ok := it.Next()
						if !ok {
							break
						}
						for _, v := range vals {
							// check all iterator bits are set
							require.True(t, src.Contains(v), "bit %d in 0x%x", v, src.Bytes())

							// check iterator against indexes
							require.Equal(t, int(idx[i]), v, "bit %d in 0x%x", i, src.Bytes())
							i++
						}
					}
				}
			})
		}
	}
}
