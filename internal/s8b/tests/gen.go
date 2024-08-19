package tests

import (
	"math/rand"
)

func ones(n int) func() []uint64 {
	return func() []uint64 {
		in := make([]uint64, n)
		for i := 0; i < n; i++ {
			in[i] = 1
		}
		return in
	}
}

func ones32(n int) func() []uint32 {
	return func() []uint32 {
		in := make([]uint32, n)
		for i := 0; i < n; i++ {
			in[i] = 1
		}
		return in
	}
}

func ones16(n int) func() []uint16 {
	return func() []uint16 {
		in := make([]uint16, n)
		for i := 0; i < n; i++ {
			in[i] = 1
		}
		return in
	}
}

func ones8(n int) func() []uint8 {
	return func() []uint8 {
		in := make([]uint8, n)
		for i := 0; i < n; i++ {
			in[i] = 1
		}
		return in
	}
}

func onesN() func(n int) func() []uint64 {
	return func(n int) func() []uint64 {
		return ones(n)
	}
}

func bitsN(b int) func(n int) func() []uint64 {
	return func(n int) func() []uint64 {
		return bits(n, b)
	}
}

func bitsN32(b int) func(n int) func() []uint32 {
	return func(n int) func() []uint32 {
		return bits32(n, b)
	}
}

func bitsN16(b int) func(n int) func() []uint16 {
	return func(n int) func() []uint16 {
		return bits16(n, b)
	}
}

func bitsN8(b int) func(n int) func() []uint8 {
	return func(n int) func() []uint8 {
		return bits8(n, b)
	}
}

func combineN(fns ...func(n int) func() []uint64) func(n int) func() []uint64 {
	return func(n int) func() []uint64 {
		var out []func() []uint64
		for _, fn := range fns {
			out = append(out, fn(n))
		}
		return combine(out...)
	}
}

func combineN32(fns ...func(n int) func() []uint32) func(n int) func() []uint32 {
	return func(n int) func() []uint32 {
		var out []func() []uint32
		for _, fn := range fns {
			out = append(out, fn(n))
		}
		return combine32(out...)
	}
}

func combineN16(fns ...func(n int) func() []uint16) func(n int) func() []uint16 {
	return func(n int) func() []uint16 {
		var out []func() []uint16
		for _, fn := range fns {
			out = append(out, fn(n))
		}
		return combine16(out...)
	}
}

func combineN8(fns ...func(n int) func() []uint8) func(n int) func() []uint8 {
	return func(n int) func() []uint8 {
		var out []func() []uint8
		for _, fn := range fns {
			out = append(out, fn(n))
		}
		return combine8(out...)
	}
}

// bits generates sequence of n numbers with max bits,
// ensuring max bit is set for 50% of the values.
func bits(n, bits int) func() []uint64 {
	return func() []uint64 {
		out := make([]uint64, n)
		maxVal := uint64(1 << uint8(bits))
		for i := range out {
			topBit := uint64((i & 1) << uint8(bits-1))
			out[i] = uint64(rand.Int63n(int64(maxVal))) | topBit
			if out[i] >= maxVal {
				panic("max")
			}
		}
		return out
	}
}

func bits32(n, bits int) func() []uint32 {
	return func() []uint32 {
		out := make([]uint32, n)
		maxVal := uint64(1 << uint8(bits))
		for i := range out {
			topBit := uint32((i & 1) << uint8(bits-1))
			out[i] = uint32(rand.Int63n(int64(maxVal))) | topBit
			if uint64(out[i]) >= maxVal {
				panic("max")
			}
		}
		return out
	}
}

func bits16(n, bits int) func() []uint16 {
	return func() []uint16 {
		out := make([]uint16, n)
		maxVal := uint64(1 << uint8(bits))
		for i := range out {
			topBit := uint16((i & 1) << uint8(bits-1))
			out[i] = uint16(rand.Int63n(int64(maxVal))) | topBit
			if uint64(out[i]) >= maxVal {
				panic("max")
			}
		}
		return out
	}
}

func bits8(n, bits int) func() []uint8 {
	return func() []uint8 {
		out := make([]uint8, n)
		maxVal := uint64(1 << uint8(bits))
		for i := range out {
			topBit := uint8((i & 1) << uint8(bits-1))
			out[i] = uint8(rand.Int63n(int64(maxVal))) | topBit
			if uint64(out[i]) >= maxVal {
				panic("max")
			}
		}
		return out
	}
}

func combine(fns ...func() []uint64) func() []uint64 {
	return func() []uint64 {
		var out []uint64
		for _, fn := range fns {
			out = append(out, fn()...)
		}
		return out
	}
}

func combine32(fns ...func() []uint32) func() []uint32 {
	return func() []uint32 {
		var out []uint32
		for _, fn := range fns {
			out = append(out, fn()...)
		}
		return out
	}
}

func combine16(fns ...func() []uint16) func() []uint16 {
	return func() []uint16 {
		var out []uint16
		for _, fn := range fns {
			out = append(out, fn()...)
		}
		return out
	}
}

func combine8(fns ...func() []uint8) func() []uint8 {
	return func() []uint8 {
		var out []uint8
		for _, fn := range fns {
			out = append(out, fn()...)
		}
		return out
	}
}
