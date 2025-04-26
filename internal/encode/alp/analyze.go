// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package alp

import (
	"math"
	"math/bits"
	"unsafe"

	"blockwatch.cc/knoxdb/internal/arena"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/num"
	"blockwatch.cc/knoxdb/pkg/util"
)

type Analysis struct {
	Scheme Scheme    // suggested ALP scheme
	Exp    Exponents // classic ALP exponents
	Dict   bool      // use dict for left RD compression
	Rate   float64   // rate of suggested encoding
	Split  int       // ALP RD split
}

type Scheme byte

const (
	INVALID_SCHEME Scheme = iota
	ALP_SCHEME
	ALP_RD_SCHEME
)

type Exponents struct {
	E uint8
	F uint8
}

func (e Exponents) Key() uint16 {
	return uint16(e.E)<<8 | uint16(e.F)
}

const RD_MAX_DICT_SIZE = 8

// Note: keep these calculations in sync with integer container costs
func dictCosts(n, w, c int) int {
	return 1 + bitPackCosts(n, bits.Len(uint(c-1))) + bitPackCosts(c, w)
}

func bitPackCosts(n, w int) int {
	return 2 + 2*num.MaxVarintLen32 + (n*w+7)/8
}

// Find the best combinations of Factor-Exponent from a sampled vector
// This operates over ALP first level samples
func Analyze[T Float, E Int](src []T) Analysis {
	// sample source vector to extract SAMPLE_SIZE (32) elements
	var buf [SAMPLE_SIZE]T
	sample := Sample(buf[:], src)
	c := getConstantPtr[T]()

	// find the best factor / exponent pair
	var (
		bestExp  Exponents
		bestSize = math.MaxInt
		maxE     = types.MaxVal[E]()
		maxEx    = len(sample) >> 2 // max 25% exceptions
	)

	// We try all combinations in search for the one which minimize the compression size
	//
	// TODO: try different strategies to reduce brute force search costs
	// A: try even exponents only, jumping 2 pow-10 at a time
	// B: coarse E search (known useful exponents 18, 16, 14, 12, 10, ..) then fine F search
	// C: count trailing decimal zeros when encoding E, then choose F
	for e := c.MAX_EXPONENT; e < 254; e -= 1 {
	floop:
		for f := e; f < 254; f -= 1 {
			var (
				nNonEx     int
				minv, maxv E = maxE, 0
			)

			// lookup current factors
			encF := c.IF10[f]
			decF := c.F10[f]
			encE := c.F10[e]
			decE := c.IF10[e]

			// analyze probe (32 values)
			for i, val := range sample {
				enc := E(((val * encE * encF) + c.MAGIC_NUMBER) - c.MAGIC_NUMBER)
				if val == T(enc)*decF*decE {
					nNonEx++
					maxv = max(maxv, enc)
					minv = min(minv, enc)
				} else if i-nNonEx > maxEx {
					// early break & ignore combinations with more than 50% exceptions
					continue floop
				}
			}

			// evaluate performance
			nBits := bits.Len64(uint64(maxv) - uint64(minv))
			size := (len(sample) * nBits) + (len(sample)-nNonEx)*(c.PATCH_SIZE+PATCH_POSITION_SIZE)

			// keep better compressing versions
			if size < bestSize {
				bestSize = size
				bestExp = Exponents{e, f}
			} else if size == bestSize && e-f < bestExp.E-bestExp.F {
				bestExp = Exponents{e, f}
			}
		}
	}

	// switch to RD scheme if we were not able to achieve compression
	if bestSize >= c.RD_SIZE_THRESHOLD_LIMIT {
		switch any(T(0)).(type) {
		case float64:
			return analyzeRD[T, uint64](sample)
		default:
			return analyzeRD[T, uint32](sample)
		}
	}

	return Analysis{
		Scheme: ALP_SCHEME,
		Exp:    bestExp,
		Rate:   float64(bestSize>>3*100/(len(sample)*c.WIDTH)) / 100.0,
	}
}

func AnalyzeRD[T Float, U Uint](src []T) Analysis {
	var buf [SAMPLE_SIZE]T
	sample := Sample(buf[:], src)
	return analyzeRD[T, U](sample)
}

func analyzeRD[T Float, U Uint](sample []T) Analysis {
	var (
		bestShift int      = 16
		bestSize  int      = math.MaxInt32
		useDict   bool     = false
		sz        int      = len(sample)
		w         int      = int(unsafe.Sizeof(T(0)))
		unique    []uint16 = arena.Alloc[uint16](1 << 16)[:1<<16]
	)

	for i := 1; i <= 16; i++ {
		shift := w*8 - i
		mask := U(1<<shift - 1)

		var (
			lmin, lmax uint16 = math.MaxUint16, 0
			rmin, rmax U      = types.MaxVal[U](), 0
			lUnique    int
		)

		for _, v := range util.ReinterpretSlice[T, U](sample) {
			l, r := uint16(v>>shift), v&mask
			lmin = min(lmin, l)
			lmax = max(lmax, l)
			rmin = min(rmin, r)
			rmax = max(rmax, r)
		}
		for _, v := range util.ReinterpretSlice[T, U](sample) {
			k := uint16(v>>shift) - lmin
			if unique[k] == 0 {
				lUnique++
				if lUnique >= RD_MAX_DICT_SIZE {
					break
				}
				unique[k] = 1
			}
		}

		// estimate encoded size
		// - left side may be dict compressed
		// - right side will be bitpacked
		lbits, rbits := bits.Len16(lmax-lmin), bits.Len64(uint64(rmax-rmin))
		ldcost := dictCosts(sz, lbits, lUnique)
		lbcost := bitPackCosts(sz, lbits)

		var maxSz int
		if lUnique <= RD_MAX_DICT_SIZE && ldcost < lbcost {
			maxSz += ldcost
		} else {
			maxSz += lbcost
		}
		maxSz += bitPackCosts(sz, rbits) // bitpack only

		// compare against previous know best ratio and keep best containers
		if maxSz <= bestSize {
			bestSize = maxSz
			bestShift = shift
			useDict = lUnique <= RD_MAX_DICT_SIZE
		}

		// cleanup
		clear(unique[:uint(lmax-lmin)+1])
	}

	arena.Free(unique)

	return Analysis{
		Scheme: ALP_RD_SCHEME,
		Split:  bestShift,
		Rate:   float64(bestSize) / float64(len(sample)*w),
		Dict:   useDict,
	}
}
