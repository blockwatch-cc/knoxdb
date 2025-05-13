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
	"blockwatch.cc/knoxdb/pkg/slicex"
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
func dictCosts(n, w, c, minv int) int {
	return 1 + bitPackCosts(n, bits.Len(uint(c-1)), minv) + bitPackCosts(c, w, 0)
}

func bitPackCosts(n, w, minv int) int {
	return 2 + num.UvarintLen(minv) + num.UvarintLen(n) + (w*n+63)&^63/8
}

func countZeros[T types.Float](src []T) (n int) {
	for _, v := range src {
		if v == 0.0 {
			n++
		}
	}
	return
}

// Find the best combinations of Factor-Exponent from a sampled vector
// This operates over ALP first level samples
func Analyze[T Float, E Int](src []T) Analysis {
	// sample source vector to extract SAMPLE_SIZE (32) elements
	var buf [SAMPLE_SIZE]T
	sample := Sample(buf[:], src)
	if len(sample) == 0 {
		return Analysis{Scheme: ALP_SCHEME}
	}

	// zeros are bad for short vectors like ours, so we count and remove them
	nonZeroSample := sample
	nZero := countZeros(sample)

	// find the best factor / exponent pair
	var (
		c        = getConstantPtr[T]()
		bestExp  Exponents
		bestSize = math.MaxInt
		maxE     = types.MaxVal[E]()
		maxEx    = (len(sample) - nZero) >> 2 // max 25% exceptions on non-zero values
	)

	// Exclude zero values (0.0) from the sample to avoid picking bad exponents.
	// Too many zeros make an all-exception case cheaper since only a few
	// non-zero values are left for encoding exceptions. For correct min-FOR
	// bit width estimation we start minv at zero.
	if nZero > 0 {
		maxE = 0
		nonZeroSample = slicex.RemoveZeroFloats(sample)
	}

	// Search for an exponent/factor combination which minimizes compression size.
	// Notable changes to vanilla ALP
	// - we try even exponents only which saves 50% of the search cost
	// - we break early when more than 25% non-zero values are exceptions
	// - we encode/decode all values including NaN, Inf and out of bounds
	//   since the check is more expensive than straight processing
	// - we exclude zero values (0.0) since they skew costs
	for e := c.MAX_EXPONENT; e < 254; e -= 2 {
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

			// analyze probe (32 values minus zeros)
			for i, val := range nonZeroSample {
				enc := E((val*encE*encF + c.MAGIC_NUMBER) - c.MAGIC_NUMBER)
				if val == T(enc)*decF*decE {
					nNonEx++
					maxv = max(maxv, enc)
					minv = min(minv, enc)
				} else if i-nNonEx > maxEx {
					// early break & ignore combinations with too many exceptions
					continue floop
				}
			}

			// evaluate performance
			nBits := bits.Len64(uint64(maxv) - uint64(minv))
			size := (len(sample) * nBits) + (len(sample)-nZero-nNonEx)*(c.PATCH_SIZE+PATCH_POSITION_SIZE)

			// keep better compressing versions
			if size < bestSize {
				bestSize = size
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
		var (
			shift      int    = w*8 - i
			mask       U      = 1<<shift - 1
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
					lUnique = sz
					break
				}
				unique[k] = 1
			}
		}

		// estimate encoded size
		// - left side may be dict compressed
		// - right side will be bitpacked
		lbits, rbits := bits.Len16(lmax-lmin), bits.Len64(uint64(rmax-rmin))
		ldcost := dictCosts(sz, lbits, lUnique, int(lmin))
		lbcost := bitPackCosts(sz, lbits, int(lmin))

		maxSz := bitPackCosts(sz, rbits, int(rmin)) // bitpack only
		if lUnique <= RD_MAX_DICT_SIZE && ldcost < lbcost {
			maxSz += ldcost
		} else {
			maxSz += lbcost
		}

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
