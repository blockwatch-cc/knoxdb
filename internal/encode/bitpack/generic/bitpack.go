// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package generic

import "blockwatch.cc/knoxdb/internal/types"

// Pack packs integer value as n-bit packed integer into buf to position index
// This is a write-once, read-only datastructure. Assumes buffer is zeroed
// at start, does not support value overwrite once written.
func Pack(buf []byte, index, log2 int, val uint64) {
	// shift
	shift := (64 - log2) & 7 * (index + 1) & 7
	mask := uint64((1 << log2) - 1)

	// output position
	pos := (index * log2) >> 3

	// most significant byte
	msb := (log2 + shift - 1) >> 3

	// some large values >=59 bit do not fit 8 bytes
	if msb == 8 {
		// mask out extra bits
		val &= mask

		// merge top byte
		buf[pos] |= byte(val >> (64 - shift))

		// shift for correct remaining byte positions
		val <<= shift

		// write non-overlapping bytes
		for i := msb; i > 0; i-- {
			buf[pos+i] = byte(val)
			val >>= 8
		}

	} else {
		// mask & shift value
		val &= mask
		val <<= shift

		// write non-overlapping bytes
		for i := msb; i > 0; i-- {
			buf[pos+i] = byte(val)
			val >>= 8
		}

		// merge top byte
		buf[pos] |= byte(val)
	}
}

type PackFunc func(buf []byte, index int, val uint64)

// Packer returns a pack function locked to a specific bit width. Use it
// for slightly faster performance when packing many values at once.
func Packer(log2 int) PackFunc {
	mask := uint64((1 << log2) - 1)
	shift1 := (64 - log2) & 7

	// handle uint64 >= 59bit which do not fit 64 bit during assembly in a custom func
	if log2 >= 59 {
		return func(buf []byte, index int, val uint64) {
			// shift
			shift := shift1 * (index + 1) & 7

			// output position
			pos := (index * log2) >> 3

			// most significant byte
			msb := (log2 + shift - 1) >> 3

			// patch and consider shift overflow on uint64
			if msb == 8 {
				// mask out extra bits
				val &= mask

				// merge top byte
				buf[pos] |= byte(val >> (64 - shift))

				// shift for correct remaining byte positions
				val <<= shift

				// write non-overlapping bytes
				for i := msb; i > 0; i-- {
					buf[pos+i] = byte(val)
					val >>= 8
				}

			} else {
				// mask & shift value
				val &= mask
				val <<= shift

				// write non-overlapping bytes
				for i := msb; i > 0; i-- {
					buf[pos+i] = byte(val)
					val >>= 8
				}

				// merge top byte
				buf[pos] |= byte(val)
			}
		}
	} else {
		return func(buf []byte, index int, val uint64) {
			// shift
			shift := shift1 * (index + 1) & 7

			// output position
			pos := (index * log2) >> 3

			// most significant byte
			msb := (log2 + shift - 1) >> 3

			// mask & shift value
			val &= mask
			val <<= shift

			// write non-overlapping bytes
			for i := msb; i > 0; i-- {
				buf[pos+i] = byte(val)
				val >>= 8
			}

			// merge top byte
			buf[pos] |= byte(val)
		}
	}
}

// Unpack unpacks integer value from n-bit packed int at position index in buf
func Unpack(buf []byte, index, log2 int) uint64 {
	// output shift and mask
	shift := (64 - log2) & 7 * (index + 1) & 7
	mask := uint64((1 << log2) - 1)

	// input position
	pos := (index * log2) >> 3

	// most significant byte
	msb := (log2 + shift - 1) >> 3

	// assemble value, handle uint64 >= 59bit which do not fit 64 bit during assembly
	if msb == 8 {
		// some >= 59bit values occupy 9 bytes
		var val uint64
		for i := 1; i <= msb; i++ {
			val <<= 8
			val |= uint64(buf[pos+i])
		}

		// shift into position
		val >>= shift

		// patch top byte
		val |= uint64(buf[pos]) << (64 - shift)

		return val & mask
	} else {
		// regular values
		var val uint64
		for i := 0; i <= msb; i++ {
			val <<= 8
			val |= uint64(buf[pos+i])
		}

		// shift and mask output
		return (val >> shift) & mask
	}
}

type UnpackFunc func(buf []byte, index int) uint64

// Unpacker returns an unpack function locked to a specific bit width. Use it
// for slightly faster performance when unpacking many values at once.
func Unpacker(log2 int) UnpackFunc {
	mask := uint64((1 << log2) - 1)
	shift1 := (64 - log2) & 7

	// handle uint64 >= 59bit which do not fit 64 bit during assembly in a custom func
	if log2 >= 59 {
		return func(buf []byte, index int) uint64 {
			// output shift
			shift := shift1 * (index + 1) & 7

			// input position
			pos := (index * log2) >> 3

			// most significant byte
			msb := (log2 + shift - 1) >> 3

			// assemble value
			if msb == 8 {
				// handle uint64 > 59bit
				var val uint64
				for i := 1; i <= msb; i++ {
					val <<= 8
					val |= uint64(buf[pos+i])
				}

				// shift into position
				val >>= shift

				// patch top byte
				val |= uint64(buf[pos]) << (64 - shift)

				return val & mask
			} else {
				var val uint64
				for i := 0; i <= msb; i++ {
					val <<= 8
					val += uint64(buf[pos+i])
				}
				// shift and mask output
				return (val >> shift) & mask
			}
		}
	} else {
		return func(buf []byte, index int) uint64 {
			// output shift
			shift := shift1 * (index + 1) & 7

			// input position
			pos := (index * log2) >> 3

			// most significant byte
			msb := (log2 + shift - 1) >> 3

			// assemble value
			var val uint64
			for i := 0; i <= msb; i++ {
				val <<= 8
				val += uint64(buf[pos+i])
			}

			// shift and mask output
			return (val >> shift) & mask
		}
	}
}

// PackVec packs a vector of unsigned values of type uint8, uint16, uint32 or
// uint64 into buffer at bit width log2.
func PackVec[T types.Unsigned](buf []byte, vals []T, log2 int) {
	pack := Packer(log2)
	for i, v := range vals {
		pack(buf, i, uint64(v))
	}
}

// UnpackVec unpacks a vector of len(vals) unsigned values of type uint8,
// uint16, uint32 or uint64 into vals at bit width log2. Vals must be
// allocated and have the desired length. If buf contains less values the
// function panics.
func UnpackVec[T types.Unsigned](buf []byte, vals []T, log2 int) {
	unpack := Unpacker(log2)
	for i := range vals {
		vals[i] = T(unpack(buf, i))
	}
}
