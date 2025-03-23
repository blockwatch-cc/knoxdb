package generic

// Package simple8b implements the 64bit integer encoding algoritm as published
// by Ann and Moffat in "Index compression using 64-bit words", Softw. Pract.
// Exper. 2010; 40:131–147
//
// It is capable of encoding multiple integers with values betweeen 0 and to 1^60 -1,
// in a single word. Code adapted from github.com/jwilder/encoding
//
// Notable changes
// - removed Encoder and Decoder
// - changed layout to LittleEndian from BigEndian
// - use Go generics to support multiple input bit widths

// Simple8b is 64bit word-sized encoder that packs multiple integers into a
// single word using a 4 bit selector values and up to 60 bits for the remaining
// values.  Integers are encoded using the following table:
//
// ┌──────────────┬─────────────────────────────────────────────────────────────┐
// │   Selector   │       0    1   2   3   4   5   6   7  8  9  0 11 12 13 14 15│
// ├──────────────┼─────────────────────────────────────────────────────────────┤
// │     Bits     │       0    0   1   2   3   4   5   6  7  8 10 12 15 20 30 60│
// ├──────────────┼─────────────────────────────────────────────────────────────┤
// │      N       │     240  120  60  30  20  15  12  10  8  7  6  5  4  3  2  1│
// ├──────────────┼─────────────────────────────────────────────────────────────┤
// │   Wasted Bits│      60   60   0   0   0   0  12   0  4  4  0  0  0  0  0  0│
// └──────────────┴─────────────────────────────────────────────────────────────┘
//
// For example, when the number of values can be encoded using 4 bits, selected 5
// is encoded in the 4 most significant bits followed by 15 values encoded used
// 4 bits each in the remaing 60 bits.
import (
	"encoding/binary"
	"errors"
	"math/bits"
	"unsafe"

	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/util"
	"golang.org/x/sys/cpu"
)

const MaxValue = (1 << 60) - 1

const (
	S8B_BIT_SIZE = 60
)

var (
	shiftBits = [...]byte{
		0,  // code 0 (240 1s)
		0,  // code 1 (120 1s)
		1,  // code 2
		2,  // code 3
		3,  // code 4
		4,  // code 5
		5,  // code 6
		6,  // code 7
		7,  // code 8
		8,  // code 9
		10, // code 10
		12, // code 11
		15, // code 12
		20, // code 13
		30, // code 14
		60, // code 15
	}
	maxValsPerBits = [...]struct {
		N    byte
		Code byte
	}{
		// {max values per bit width, selector code}
		{60, 2}, // 0 -- 60x 0bit values per uint64
		{60, 2}, // 1 -- 60x 1bit values per uint64
		{30, 3}, // 2 -- 30x 2bit values per uint64
		{20, 4}, // 3 -- 20x 3bit values per uint64
		{15, 5}, // 4 -- 15x 4bit values per uint64
		{12, 6}, // 5 -- 12x 5bit values per uint64
		{10, 7}, // 6 -- 10x 6bit values per uint64
		{8, 8},  // 7 -- 8x 7bit values per uint64
		{7, 9},  // 8 -- 7x 8bit values per uint64
		// 6x 10bit values per uint64
		{6, 10}, // 9
		{6, 10}, // 10
		// 5x 12bit values per uint64
		{5, 11}, // 11
		{5, 11}, // 12
		// 4x 15bit values per uint64
		{4, 12}, // 13
		{4, 12}, // 14
		{4, 12}, // 15
		// 3x 20 bit values per uint64
		{3, 13}, // 16
		{3, 13}, // 17
		{3, 13}, // 18
		{3, 13}, // 19
		{3, 13}, // 20
		// 2x 30bit values per uint64
		{2, 14}, // 21
		{2, 14}, // 22
		{2, 14}, // 23
		{2, 14}, // 24
		{2, 14}, // 25
		{2, 14}, // 26
		{2, 14}, // 27
		{2, 14}, // 28
		{2, 14}, // 29
		{2, 14}, // 30
		// 1x 60bit value per uint64
		{1, 15}, // 31
		{1, 15}, // 32
		{1, 15}, // 33
		{1, 15}, // 34
		{1, 15}, // 35
		{1, 15}, // 36
		{1, 15}, // 37
		{1, 15}, // 38
		{1, 15}, // 39
		{1, 15}, // 40
		{1, 15}, // 41
		{1, 15}, // 42
		{1, 15}, // 43
		{1, 15}, // 44
		{1, 15}, // 45
		{1, 15}, // 46
		{1, 15}, // 47
		{1, 15}, // 48
		{1, 15}, // 49
		{1, 15}, // 50
		{1, 15}, // 51
		{1, 15}, // 42
		{1, 15}, // 53
		{1, 15}, // 54
		{1, 15}, // 55
		{1, 15}, // 56
		{1, 15}, // 57
		{1, 15}, // 58
		{1, 15}, // 59
		{1, 15}, // 60
	}

	ErrValueOutOfBounds    = errors.New("value out of bounds")
	ErrInvalidBufferLength = errors.New("src length is not multiple of 8")
)

func Encode[T types.Integer](dst []byte, src []T, minv, maxv T) ([]byte, error) {
	if len(src) == 0 {
		return nil, nil
	}
	if len(dst)&7 != 0 {
		return nil, ErrInvalidBufferLength
	}

	out := util.FromByteSlice[uint64](dst)
	var i, j int
	for i < len(src) {
		remaining := src[i:]

		// try to pack run of 240 or 120 1s
		if len(remaining) >= 120 {
			// Invariant: len(a) is fixed to 120 or 240 values
			var a []T
			if len(remaining) >= 240 {
				a = remaining[:240]
			} else {
				a = remaining[:120]
			}

			// search for the longest sequence of 1s in a
			// Postcondition: k equals the index of the last 1 or -1
			k := 0
			for k = range a {
				if a[k]-minv != 1 {
					k--
					break
				}
			}

			v := uint64(0)
			switch {
			case k == 239:
				// 240 1s
				i += 240
			case k >= 119:
				// at least 120 1s
				v = 1 << 60
				i += 120
			default:
				goto CODES
			}

			out[j] = v
			j++
			continue
		}

	CODES:
		var (
			n        int
			maxSeen  uint64
			usedBits int
			isFull   bool
		)
		code := maxValsPerBits[0]

		// Incremental packing
		for n < len(remaining) {
			val := uint64(remaining[n]) - uint64(minv)
			if val > maxSeen {
				maxSeen = val
				usedBits = bits.Len64(val)
				if usedBits > 60 {
					return nil, ErrValueOutOfBounds
				}
				code = maxValsPerBits[usedBits]
				if n > int(code.N) {
					// cannot use this value this round
					break
				}
			}
			n++
			if n == int(code.N) {
				isFull = true
				break
			}
		}

		// adjust selector when uint64 is not full by increasing usedBits
		// and possible adjusting down n
		sel := code.Code
		if !isFull {
			for sel < 15 && n < selector64[sel].n {
				sel++
			}
			n = min(n, selector64[sel].n)
		}

		// pack values
		shift := shiftBits[sel]
		var shl byte
		val := uint64(sel) << S8B_BIT_SIZE
		for k := 0; k < n; k++ {
			val |= uint64(remaining[k]-minv) << shl
			shl += shift
		}
		out[j] = val
		j++
		i += n
	}

	return dst[:j*8], nil
}

func Decode[T types.Unsigned](dst []T, buf []byte) (int, error) {
	if len(buf) == 0 {
		return 0, nil
	}
	if len(buf)&7 != 0 {
		return 0, ErrInvalidBufferLength
	}
	var selector [16]packing
	w := unsafe.Sizeof(T(0))
	switch w {
	case 8:
		selector = selector64
	case 4:
		selector = selector32
	case 2:
		selector = selector16
	case 1:
		selector = selector8
	}

	// assuming little endian machine
	j := 0
	if cpu.IsBigEndian {
		for i := 0; i < len(buf); i += 8 {
			v := binary.LittleEndian.Uint64(buf[i:])
			sel := (v >> 60) & 0xf
			selector[sel].unpack(v, unsafe.Pointer(&dst[j]))
			j += selector[sel].n
		}
	} else {
		for _, v := range util.FromByteSlice[uint64](buf) {
			sel := (v >> 60) & 0xf
			selector[sel].unpack(v, unsafe.Pointer(&dst[j]))
			j += selector[sel].n
		}
	}
	return j, nil

}
