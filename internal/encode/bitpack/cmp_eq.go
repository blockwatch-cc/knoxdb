// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

// Specialized compare kernels for bitpacked data. We assume all data is MinFOR
// converted hence only unsigned numbers are supported here. A caller may also
// want to perform pre-checks on the value's bit width to exclude obvious cases
// that cannot match like equal 256 on a less than 8 bit packing.

package bitpack

import (
	"blockwatch.cc/knoxdb/internal/bitset"
	"blockwatch.cc/knoxdb/internal/cmp"
)

type Bitset = bitset.Bitset

type CmpFunc func(buf []byte, val uint64, n int, bits *Bitset) *Bitset

var Equal = [...]CmpFunc{
	cmp_bp_0_eq, cmp_bp_1_eq, cmp_bp_2_eq, cmp_bp_3_eq,
	cmp_bp_4_eq, cmp_bp_5_eq, cmp_bp_6_eq, cmp_bp_7_eq,
	cmp_bp_8_eq, cmp_bp_9_eq, cmp_bp_10_eq, cmp_bp_11_eq,
	cmp_bp_12_eq, cmp_bp_13_eq, cmp_bp_14_eq, cmp_bp_15_eq,
	cmp_bp_16_eq,
	// cmp_bp_17_eq, cmp_bp_18_eq, cmp_bp_19_eq,
	// cmp_bp_20_eq, cmp_bp_21_eq, cmp_bp_22_eq, cmp_bp_23_eq,
	// cmp_bp_24_eq, cmp_bp_25_eq, cmp_bp_26_eq, cmp_bp_27_eq,
	// cmp_bp_28_eq, cmp_bp_29_eq, cmp_bp_30_eq, cmp_bp_31_eq,
	// cmp_bp_31_eq, cmp_bp_33_eq, cmp_bp_34_eq, cmp_bp_35_eq,
	// cmp_bp_36_eq, cmp_bp_37_eq, cmp_bp_38_eq, cmp_bp_39_eq,
	// cmp_bp_40_eq, cmp_bp_41_eq, cmp_bp_42_eq, cmp_bp_43_eq,
	// cmp_bp_44_eq, cmp_bp_45_eq, cmp_bp_46_eq, cmp_bp_47_eq,
	// cmp_bp_48_eq, cmp_bp_49_eq, cmp_bp_50_eq, cmp_bp_51_eq,
	// cmp_bp_52_eq, cmp_bp_53_eq, cmp_bp_54_eq, cmp_bp_55_eq,
	// cmp_bp_56_eq, cmp_bp_57_eq, cmp_bp_58_eq, cmp_bp_59_eq,
	// cmp_bp_60_eq, cmp_bp_61_eq, cmp_bp_62_eq, cmp_bp_63_eq,
}

func cmp_bp_0_eq(buf []byte, val uint64, n int, bits *Bitset) *Bitset {
	if val == 0 {
		return bits.One()
	}
	return bits
}

func cmp_bp_1_eq(buf []byte, val uint64, n int, bits *Bitset) *Bitset {
	// value can only be 0 or 1, so we can simply copy bitpack buffer to bitset
	// and flip bits if val == 0
	bits.SetFromBytes(buf, n)
	if val == 0 {
		bits.Neg()
	}
	return bits
}

func cmp_bp_2_eq(buf []byte, val uint64, n int, bits *Bitset) *Bitset {
	// 2bit packing [aabb ccdd] [eeff gghh]
	var (
		i    int
		k    int
		c    byte = byte(val)
		mask byte = 0x3
	)
	// process 8 values
	for n >= 8 {
		if buf[i]>>6&mask == c { // a
			bits.Set(k)
		}
		if buf[i]>>4&mask == c { // b
			bits.Set(k + 1)
		}
		if buf[i]>>2&mask == c { // c
			bits.Set(k + 2)
		}
		if buf[i]&mask == c { // d
			bits.Set(k + 3)
		}
		i++
		if buf[i]>>6&mask == c { // e
			bits.Set(k + 4)
		}
		if buf[i]>>4&mask == c { // f
			bits.Set(k + 5)
		}
		if buf[i]>>2&mask == c { // g
			bits.Set(k + 6)
		}
		if buf[i]&mask == c { // h
			bits.Set(k + 7)
		}
		i++
		n -= 8
		k += 8
	}

	// process tail (max 7 values left)
	if n > 0 {
		if buf[i]>>6&mask == c { // a
			bits.Set(k)
		}
		n--
	}
	if n > 0 {
		if buf[i]>>4&mask == c { // b
			bits.Set(k + 1)
		}
		n--
	}
	if n > 0 {
		if buf[i]>>2&mask == c { // c
			bits.Set(k + 2)
		}
		n--
	}
	if n > 0 {
		if buf[i]&mask == c { // d
			bits.Set(k + 3)
		}
		n--
		i++
	}
	if n > 0 {
		if buf[i]>>6&mask == c { // e
			bits.Set(k + 4)
		}
		n--
	}
	if n > 0 {
		if buf[i]>>4&mask == c { // f
			bits.Set(k + 5)
		}
		n--
	}
	if n > 0 {
		if buf[i]>>2&mask == c { // g
			bits.Set(k + 6)
		}
	}

	return bits
}

func cmp_bp_3_eq(buf []byte, val uint64, n int, bits *Bitset) *Bitset {
	// 3bit packed [aaab bbcc] [cddd eeef] [ffgg ghhh]
	var (
		i    int
		k    int
		c    byte = byte(val)
		mask byte = 0x7
	)

	// process 8 values per loop
	for n >= 8 {
		if buf[i]>>5&mask == c { // a
			bits.Set(k)
		}
		if buf[i]>>2&mask == c { // b
			bits.Set(k + 1)
		}
		if buf[i]<<1|buf[i+1]>>7&mask == c { // c
			bits.Set(k + 2)
		}
		i++
		if buf[i]>>4&mask == c { // d
			bits.Set(k + 3)
		}
		if buf[i]>>1&mask == c { // e
			bits.Set(k + 4)
		}
		if buf[i]<<2|buf[i+1]>>6&mask == c { // f
			bits.Set(k + 5)
		}
		i++
		if buf[i]>>3&mask == c { // g
			bits.Set(k + 6)
		}
		if buf[i]&mask == c { // h
			bits.Set(k + 7)
		}
		i++
		n -= 8
		k += 8
	}

	// process tail
	if n > 0 {
		if buf[i]>>5&mask == c { // a
			bits.Set(k)
		}
		n--
	}
	if n > 0 {
		if buf[i]>>2&mask == c { // b
			bits.Set(k + 1)
		}
		n--
	}
	if n > 0 {
		if buf[i]<<1|buf[i+1]>>7&mask == c { // c
			bits.Set(k + 2)
		}
		i++
		n--
	}
	if n > 0 {
		if buf[i]>>4&mask == c { // d
			bits.Set(k + 3)
		}
		n--
	}
	if n > 0 {
		if buf[i]>>1&mask == c { // e
			bits.Set(k + 4)
		}
		n--
	}
	if n > 0 {
		if buf[i]<<2|buf[i+1]>>6&mask == c { // f
			bits.Set(k + 5)
		}
		i++
		n--
	}
	if n > 0 {
		if buf[i]>>3&mask == c { // g
			bits.Set(k + 6)
		}
	}

	return bits
}

func cmp_bp_4_eq(buf []byte, val uint64, n int, bits *Bitset) *Bitset {
	// 4bit packing [aaaa bbbb] [cccc dddd] [eeee ffff] [gggg hhhh]
	var (
		i    int
		k    int
		c    byte = byte(val)
		mask byte = 0xF
	)
	// process 8 values
	for n >= 8 {
		if buf[i]>>4&mask == c { // a
			bits.Set(k)
		}
		if buf[i]&mask == c { // b
			bits.Set(k + 1)
		}
		i++
		if buf[i]>>4&mask == c { // c
			bits.Set(k + 2)
		}
		if buf[i]&mask == c { // d
			bits.Set(k + 3)
		}
		i++
		if buf[i]>>4&mask == c { // e
			bits.Set(k + 4)
		}
		if buf[i]&mask == c { // f
			bits.Set(k + 5)
		}
		i++
		if buf[i]>>4&mask == c { // g
			bits.Set(k + 6)
		}
		if buf[i]&mask == c { // h
			bits.Set(k + 7)
		}
		i++
		n -= 8
		k += 8
	}

	// process tail (max 7 values left)
	if n > 0 {
		if buf[i]>>4&mask == c { // a
			bits.Set(k)
		}
		n--
	}
	if n > 0 {
		if buf[i]&mask == c { // b
			bits.Set(k + 1)
		}
		i++
		n--
	}
	if n > 0 {
		if buf[i]>>4&mask == c { // c
			bits.Set(k + 2)
		}
		n--
	}
	if n > 0 {
		if buf[i]&mask == c { // d
			bits.Set(k + 3)
		}
		i++
		n--
	}
	if n > 0 {
		if buf[i]>>4&mask == c { // e
			bits.Set(k + 4)
		}
		n--
	}
	if n > 0 {
		if buf[i]&mask == c { // f
			bits.Set(k + 5)
		}
		i++
		n--
	}
	if n > 0 {
		if buf[i]>>4&mask == c { // g
			bits.Set(k + 6)
		}
	}

	return bits
}

func cmp_bp_5_eq(buf []byte, val uint64, n int, bits *Bitset) *Bitset {
	// 5bit packed [aaaa abbb] [bbcc cccd] [dddd eeee] [efff ffgg] [gggh hhhh]
	var (
		i    int
		k    int
		c    byte = byte(val)
		mask byte = 0x1f
	)

	// process 8 values per loop
	for n >= 8 {
		if buf[i]>>3&mask == c { // a
			bits.Set(k)
		}
		if buf[i]<<2|buf[i+1]>>6&mask == c { // b
			bits.Set(k + 1)
		}
		i++
		if buf[i]>>1&mask == c { // c
			bits.Set(k + 2)
		}
		if buf[i]<<4|buf[i+1]>>4&mask == c { // d
			bits.Set(k + 3)
		}
		i++
		if buf[i]<<1|buf[i+1]>>7&mask == c { // e
			bits.Set(k + 4)
		}
		i++
		if buf[i]>>2&mask == c { // f
			bits.Set(k + 5)
		}
		if buf[i]<<3|buf[i+1]>>5&mask == c { // g
			bits.Set(k + 6)
		}
		i++
		if buf[i]&mask == c { // h
			bits.Set(k + 7)
		}
		i++
		n -= 8
		k += 8
	}

	// process tail (max 7 values left)
	if n > 0 {
		if buf[i]>>3&mask == c { // a
			bits.Set(k)
		}
		n--
	}
	if n > 0 {
		if buf[i]<<2|buf[i+1]>>6&mask == c { // b
			bits.Set(k + 1)
		}
		n--
		i++
	}
	if n > 0 {
		if buf[i]>>1&mask == c { // c
			bits.Set(k + 2)
		}
		n--
	}
	if n > 0 {
		if buf[i]<<4|buf[i+1]>>4&mask == c { // d
			bits.Set(k + 3)
		}
		n--
		i++
	}
	if n > 0 {
		if buf[i]<<1|buf[i+1]>>7&mask == c { // e
			bits.Set(k + 4)
		}
		n--
		i++
	}
	if n > 0 {
		if buf[i]>>2&mask == c { // f
			bits.Set(k + 5)
		}
		n--
		i++
	}
	if n > 0 {
		if buf[i]<<3|buf[i+1]>>5&mask == c { // g
			bits.Set(k + 6)
		}
	}
	return bits
}

func cmp_bp_6_eq(buf []byte, val uint64, n int, bits *Bitset) *Bitset {
	// 6bit packed [aaaa aabb] [bbbb cccc] [ccdd dddd]
	//             [eeee eeff] [ffff gggg] [gghh hhhh]
	var (
		i    int
		k    int
		c    byte = byte(val)
		mask byte = 0x3f
	)

	// process 8 values per loop
	for n >= 4 {
		if buf[i]>>2&mask == c { // a
			bits.Set(k)
		}
		if buf[i]<<4|buf[i+1]>>4&mask == c { // b
			bits.Set(k + 1)
		}
		i++
		if buf[i]<<2|buf[i+1]>>6&mask == c { // c
			bits.Set(k + 2)
		}
		i++
		if buf[i]&mask == c { // d
			bits.Set(k + 3)
		}
		i++
		if buf[i]>>2&mask == c { // e
			bits.Set(k + 4)
		}
		if buf[i]<<4|buf[i+1]>>4&mask == c { // f
			bits.Set(k + 5)
		}
		i++
		if buf[i]<<2|buf[i+1]>>6&mask == c { // g
			bits.Set(k + 6)
		}
		i++
		if buf[i]&mask == c { // h
			bits.Set(k + 7)
		}
		i++
		n -= 8
		k += 8
	}

	// process tail (max 7 values left)
	if n > 0 {
		if buf[i]>>2&mask == c { // a
			bits.Set(k)
		}
		n--
	}
	if n > 0 {
		if buf[i]<<4|buf[i+1]>>4&mask == c { // b
			bits.Set(k + 1)
		}
		n--
		i++
	}
	if n > 0 {
		if buf[i]<<2|buf[i+1]>>6&mask == c { // c
			bits.Set(k + 2)
		}
	}
	if n > 0 {
		if buf[i]&mask == c { // d
			bits.Set(k + 3)
		}
		n--
		i++
	}
	if n > 0 {
		if buf[i]>>2&mask == c { // e
			bits.Set(k + 4)
		}
		n--
	}
	if n > 0 {
		if buf[i]<<4|buf[i+1]>>4&mask == c { // f
			bits.Set(k + 5)
		}
		n--
		i++
	}
	if n > 0 {
		if buf[i]<<2|buf[i+1]>>6&mask == c { // g
			bits.Set(k + 6)
		}
	}
	return bits
}

func cmp_bp_7_eq(buf []byte, val uint64, n int, bits *Bitset) *Bitset {
	// 7bit packed [aaaa aaab] [bbbb bbcc] [cccc cddd] [dddd eeee]
	//             [eeef ffff] [ffgg gggg] [ghhh hhhh]
	var (
		i    int
		k    int
		c    byte = byte(val)
		mask byte = 0x7f
	)

	// process 8 values per loop
	for n >= 8 {
		if buf[i]>>1&mask == c { // a
			bits.Set(k)
		}
		if buf[i]<<6|buf[i+1]>>2&mask == c { // b
			bits.Set(k + 1)
		}
		i++
		if buf[i]<<5|buf[i+1]>>3&mask == c { // c
			bits.Set(k + 2)
		}
		i++
		if buf[i]<<4|buf[i+1]>>4&mask == c { // d
			bits.Set(k + 3)
		}
		i++
		if buf[i]<<3|buf[i+1]>>5&mask == c { // e
			bits.Set(k + 4)
		}
		i++
		if buf[i]<<2|buf[i+1]>>6&mask == c { // f
			bits.Set(k + 5)
		}
		i++
		if buf[i]<<1|buf[i+1]>>7&mask == c { // g
			bits.Set(k + 6)
		}
		i++
		if buf[i]&mask == c { // h
			bits.Set(k + 7)
		}
		i++
		n -= 8
		k += 8
	}

	// process tail (max 7 values left)
	if n > 0 {
		if buf[i]>>1&mask == c { // a
			bits.Set(k)
		}
		n--
	}
	if n > 0 {
		if buf[i]<<6|buf[i+1]>>3&mask == c { // b
			bits.Set(k + 1)
		}
		n--
		i++
	}
	if n > 0 {
		if buf[i]<<5|buf[i+1]>>3&mask == c { // c
			bits.Set(k + 2)
		}
		n--
		i++
	}
	if n > 0 {
		if buf[i]<<4|buf[i+1]>>4&mask == c { // d
			bits.Set(k + 3)
		}
		n--
		i++
	}
	if n > 0 {
		if buf[i]<<3|buf[i+1]>>5&mask == c { // e
			bits.Set(k + 4)
		}
		n--
		i++
	}
	if n > 0 {
		if buf[i]<<2|buf[i+1]>>6&mask == c { // f
			bits.Set(k + 5)
		}
		n--
		i++
	}
	if n > 0 {
		if buf[i]<<1|buf[i+1]>>7&mask == c { // g
			bits.Set(k + 6)
		}
	}
	return bits
}

func cmp_bp_8_eq(buf []byte, val uint64, n int, bits *Bitset) *Bitset {
	return cmp.MatchUint8Equal(buf[:n], uint8(val), bits, nil)
}

func cmp_bp_9_eq(buf []byte, val uint64, n int, bits *Bitset) *Bitset {
	// 9bit packed [aaaa aaaa] [abbb bbbb] [bbcc cccc] [cccd dddd]
	//             [dddd eeee] [eeee efff] [ffff ffgg] [gggg gggh]
	//             [hhhh hhhh]
	var (
		i    int
		k    int
		c    uint16 = uint16(val)
		mask uint16 = 0x1FF
	)

	// process 8 values per loop
	for n >= 8 {
		if BE.Uint16(buf[i:])>>7&mask == c { // a
			bits.Set(k)
		}
		i++
		if BE.Uint16(buf[i:])>>6&mask == c { // b
			bits.Set(k + 1)
		}
		i++
		if BE.Uint16(buf[i:])>>5&mask == c { // c
			bits.Set(k + 2)
		}
		i++
		if BE.Uint16(buf[i:])>>4&mask == c { // d
			bits.Set(k + 3)
		}
		i++
		if BE.Uint16(buf[i:])>>3&mask == c { // e
			bits.Set(k + 4)
		}
		i++
		if BE.Uint16(buf[i:])>>2&mask == c { // f
			bits.Set(k + 5)
		}
		i++
		if BE.Uint16(buf[i:])>>1&mask == c { // g
			bits.Set(k + 6)
		}
		i++
		if BE.Uint16(buf[i:]) == c { // h
			bits.Set(k + 7)
		}
		i++
		n -= 8
		k += 8
	}

	// process tail (max 7 values left)
	if n > 0 {
		if BE.Uint16(buf[i:])>>7&mask == c { // a
			bits.Set(k)
		}
		i++
		n--
	}
	if n > 0 {
		if BE.Uint16(buf[i:])>>6&mask == c { // b
			bits.Set(k + 1)
		}
		i++
		n--
	}
	if n > 0 {
		if BE.Uint16(buf[i:])>>5&mask == c { // c
			bits.Set(k + 2)
		}
		n--
		i++
	}
	if n > 0 {
		if BE.Uint16(buf[i:])>>4&mask == c { // d
			bits.Set(k + 3)
		}
		i++
		n--
	}
	if n > 0 {
		if BE.Uint16(buf[i:])>>3&mask == c { // e
			bits.Set(k + 4)
		}
		i++
		n--
	}
	if n > 0 {
		if BE.Uint16(buf[i:])>>2&mask == c { // f
			bits.Set(k + 5)
		}
		i++
		n--
	}
	if n > 0 {
		if BE.Uint16(buf[i:])>>1&mask == c { // g
			bits.Set(k + 6)
		}
	}
	return bits
}

func cmp_bp_10_eq(buf []byte, val uint64, n int, bits *Bitset) *Bitset {
	// 10bit packed [aaaa aaaa] [aabb bbbb] [bbbb cccc] [cccc ccdd]
	//              [dddd dddd]
	var (
		i    int
		k    int
		c    uint16 = uint16(val)
		mask uint16 = 0x3FF
	)

	// process 8 values per loop
	for n >= 8 {
		if BE.Uint16(buf[i:])>>6&mask == c { // a
			bits.Set(k)
		}
		i++
		if BE.Uint16(buf[i:])>>4&mask == c { // b
			bits.Set(k + 1)
		}
		i++
		if BE.Uint16(buf[i:])>>2&mask == c { // c
			bits.Set(k + 2)
		}
		i++
		if BE.Uint16(buf[i:]) == c { // d
			bits.Set(k + 3)
		}
		if BE.Uint16(buf[i:])>>6&mask == c { // e
			bits.Set(k + 4)
		}
		i++
		if BE.Uint16(buf[i:])>>4&mask == c { // f
			bits.Set(k + 5)
		}
		i++
		if BE.Uint16(buf[i:])>>2&mask == c { // g
			bits.Set(k + 6)
		}
		i++
		if BE.Uint16(buf[i:]) == c { // h
			bits.Set(k + 7)
		}
		i++
		n -= 8
		k += 8
	}

	// process tail (max 3 values left)
	if n > 0 {
		if BE.Uint16(buf[i:])>>6&mask == c { // a
			bits.Set(k)
		}
		i++
		n--
	}
	if n > 0 {
		if BE.Uint16(buf[i:])>>4&mask == c { // b
			bits.Set(k + 1)
		}
		i++
		n--
	}
	if n > 0 {
		if BE.Uint16(buf[i:])>>2&mask == c { // c
			bits.Set(k + 2)
		}
		i++
		n--
	}
	if n > 0 {
		if BE.Uint16(buf[i:]) == c { // d
			bits.Set(k + 3)
		}
		n--
	}
	if n > 0 {
		if BE.Uint16(buf[i:])>>6&mask == c { // e
			bits.Set(k + 4)
		}
		i++
		n--
	}
	if n > 0 {
		if BE.Uint16(buf[i:])>>4&mask == c { // f
			bits.Set(k + 5)
		}
		i++
		n--
	}
	if n > 0 {
		if BE.Uint16(buf[i:])>>2&mask == c { // g
			bits.Set(k + 6)
		}
	}
	return bits
}

func cmp_bp_11_eq(buf []byte, val uint64, n int, bits *Bitset) *Bitset {
	// 11bit packed [aaaa aaaa] [aaab bbbb] [bbbb bbcc] [cccc cccc]
	//              [cddd dddd] [dddd eeee] [eeee eeef] [ffff ffff]
	//              [ffgg gggg] [gggg ghhh] [hhhh hhhh]
	var (
		i    int
		k    int
		c    uint16 = uint16(val)
		mask uint16 = 0x7FF
	)

	// process 8 values per loop
	for n >= 8 {
		if BE.Uint16(buf[i:])>>5&mask == c { // a
			bits.Set(k)
		}
		i++
		if BE.Uint16(buf[i:])>>2&mask == c { // b
			bits.Set(k + 1)
		}
		i++
		if BE.Uint16(buf[i:])<<1|uint16(buf[i+2]>>7)&mask == c { // c
			bits.Set(k + 2)
		}
		i += 2
		if BE.Uint16(buf[i:])>>4&mask == c { // d
			bits.Set(k + 3)
		}
		i++
		if BE.Uint16(buf[i:])>>1&mask == c { // e
			bits.Set(k + 4)
		}
		i++
		if BE.Uint16(buf[i:])<<2|uint16(buf[i+2]>>6)&mask == c { // f
			bits.Set(k + 5)
		}
		i += 2
		if BE.Uint16(buf[i:])>>3&mask == c { // g
			bits.Set(k + 6)
		}
		i++
		if BE.Uint16(buf[i:]) == c { // h
			bits.Set(k + 7)
		}
		i++
		n -= 8
		k += 8
	}

	// process tail (max 7 values left)
	if n > 0 {
		if BE.Uint16(buf[i:])>>5&mask == c { // a
			bits.Set(k)
		}
		i++
		n--
	}
	if n > 0 {
		if BE.Uint16(buf[i:])>>2&mask == c { // b
			bits.Set(k + 1)
		}
		i++
		n--
	}
	if n > 0 {
		if BE.Uint16(buf[i:])<<1|uint16(buf[i+2]>>7)&mask == c { // c
			bits.Set(k + 2)
		}
		n--
		i += 2
	}
	if n > 0 {
		if BE.Uint16(buf[i:])>>4&mask == c { // d
			bits.Set(k + 3)
		}
		i++
		n--
	}
	if n > 0 {
		if BE.Uint16(buf[i:])>>1&mask == c { // e
			bits.Set(k + 4)
		}
		i++
		n--
	}
	if n > 0 {
		if BE.Uint16(buf[i:])<<2|uint16(buf[i+2]>>6)&mask == c { // f
			bits.Set(k + 5)
		}
		i += 2
		n--
	}
	if n > 0 {
		if BE.Uint16(buf[i:])>>3&mask == c { // g
			bits.Set(k + 6)
		}
	}
	return bits
}

func cmp_bp_12_eq(buf []byte, val uint64, n int, bits *Bitset) *Bitset {
	// 12bit packed [aaaa aaaa] [aaaa bbbb] [bbbb bbbb]
	var (
		i    int
		k    int
		c    uint16 = uint16(val)
		mask uint16 = 0xFFF
	)

	// process 8 values per loop
	for n >= 8 {
		if BE.Uint16(buf[i:])>>4&mask == c { // a
			bits.Set(k)
		}
		i++
		if BE.Uint16(buf[i:])&mask == c { // b
			bits.Set(k + 1)
		}
		if BE.Uint16(buf[i:])>>4&mask == c { // c
			bits.Set(k + 2)
		}
		i++
		if BE.Uint16(buf[i:])&mask == c { // d
			bits.Set(k + 3)
		}
		i++
		if BE.Uint16(buf[i:])>>4&mask == c { // e
			bits.Set(k + 4)
		}
		i++
		if BE.Uint16(buf[i:])&mask == c { // f
			bits.Set(k + 5)
		}
		if BE.Uint16(buf[i:])>>4&mask == c { // g
			bits.Set(k + 6)
		}
		i++
		if BE.Uint16(buf[i:])&mask == c { // h
			bits.Set(k + 7)
		}
		i++
		n -= 8
		k += 8
	}

	// process tail (max 7 values left)
	if n > 0 {
		if BE.Uint16(buf[i:])>>4&mask == c { // a
			bits.Set(k)
		}
		i++
		n--
	}
	if n > 0 {
		if BE.Uint16(buf[i:])&mask == c { // b
			bits.Set(k + 1)
		}
		n--
	}
	if n > 0 {
		if BE.Uint16(buf[i:])>>4&mask == c { // c
			bits.Set(k + 2)
		}
		i++
		n--
	}
	if n > 0 {
		if BE.Uint16(buf[i:])&mask == c { // d
			bits.Set(k + 3)
		}
		i++
		n--
	}
	if n > 0 {
		if BE.Uint16(buf[i:])>>4&mask == c { // e
			bits.Set(k + 4)
		}
		i++
		n--
	}
	if n > 0 {
		if BE.Uint16(buf[i:])&mask == c { // f
			bits.Set(k + 5)
		}
		n--
	}
	if n > 0 {
		if BE.Uint16(buf[i:])>>4&mask == c { // g
			bits.Set(k + 6)
		}
	}
	return bits
}

func cmp_bp_13_eq(buf []byte, val uint64, n int, bits *Bitset) *Bitset {
	// 13bit packed [aaaa aaaa] [aaaa abbb] [bbbb bbbb] [bbcc cccc]
	//              [cccc cccd] [dddd dddd] [dddd eeee] [eeee eeee]
	//              [efff ffff] [ffff ffgg] [gggg gggg] [gggh hhhh]
	//              [hhhh hhhh]
	var (
		i    int
		k    int
		c    uint16 = uint16(val)
		mask uint16 = 0x1FFF
	)

	// process 8 values per loop
	for n >= 8 {
		if BE.Uint16(buf[i:])>>3&mask == c { // a
			bits.Set(k)
		}
		i++
		if BE.Uint16(buf[i:])<<2|uint16(buf[i+2]>>6)&mask == c { // b
			bits.Set(k + 1)
		}
		i += 2
		if BE.Uint16(buf[i:])>>1&mask == c { // c
			bits.Set(k + 2)
		}
		i++
		if BE.Uint16(buf[i:])<<4|uint16(buf[i+2]>>4)&mask == c { // d
			bits.Set(k + 3)
		}
		i += 2
		if BE.Uint16(buf[i:])<<1|uint16(buf[i+2]>>7)&mask == c { // e
			bits.Set(k + 4)
		}
		i += 2
		if BE.Uint16(buf[i:])>>2&mask == c { // f
			bits.Set(k + 5)
		}
		i++
		if BE.Uint16(buf[i:])<<3|uint16(buf[i+2]>>5)&mask == c { // g
			bits.Set(k + 6)
		}
		i += 2
		if BE.Uint16(buf[i:]) == c { // h
			bits.Set(k + 7)
		}
		i++
		n -= 8
		k += 8
	}

	// process tail (max 7 values left)
	if n > 0 {
		if BE.Uint16(buf[i:])>>3&mask == c { // a
			bits.Set(k)
		}
		i++
		n--
	}
	if n > 0 {
		if BE.Uint16(buf[i:])<<2|uint16(buf[i+2]>>6)&mask == c { // b
			bits.Set(k + 1)
		}
		i += 2
		n--
	}
	if n > 0 {
		if BE.Uint16(buf[i:])>>1&mask == c { // c
			bits.Set(k + 2)
		}
		n--
		i++
	}
	if n > 0 {
		if BE.Uint16(buf[i:])<<4|uint16(buf[i+2]>>4)&mask == c { // d
			bits.Set(k + 3)
		}
		i += 2
		n--
	}
	if n > 0 {
		if BE.Uint16(buf[i:])<<1|uint16(buf[i+2]>>7)&mask == c { // e
			bits.Set(k + 4)
		}
		i += 2
		n--
	}
	if n > 0 {
		if BE.Uint16(buf[i:])>>2&mask == c { // f
			bits.Set(k + 5)
		}
		i++
		n--
	}
	if n > 0 {
		if BE.Uint16(buf[i:])<<3|uint16(buf[i+2]>>5)&mask == c { // g
			bits.Set(k + 6)
		}
	}
	return bits
}

func cmp_bp_14_eq(buf []byte, val uint64, n int, bits *Bitset) *Bitset {
	// 14bit packed [aaaa aaaa] [aaaa aabb] [bbbb bbbb] [bbbb cccc]
	//              [cccc cccc] [ccdd dddd] [dddd dddd]
	var (
		i    int
		k    int
		c    uint16 = uint16(val)
		mask uint16 = 0x3FFF
	)

	// process 8 values per loop
	for n >= 8 {
		if BE.Uint16(buf[i:])>>2&mask == c { // a
			bits.Set(k)
		}
		i++
		if BE.Uint16(buf[i:])<<4|uint16(buf[i+2]>>4)&mask == c { // b
			bits.Set(k + 1)
		}
		i += 2
		if BE.Uint16(buf[i:])<<2|uint16(buf[i+2]>>6)&mask == c { // c
			bits.Set(k + 2)
		}
		i += 2
		if BE.Uint16(buf[i:]) == c { // d
			bits.Set(k + 3)
		}
		i++
		if BE.Uint16(buf[i:])>>2&mask == c { // e
			bits.Set(k + 4)
		}
		i++
		if BE.Uint16(buf[i:])<<4|uint16(buf[i+2]>>4)&mask == c { // f
			bits.Set(k + 5)
		}
		i += 2
		if BE.Uint16(buf[i:])<<2|uint16(buf[i+2]>>6)&mask == c { // g
			bits.Set(k + 6)
		}
		i += 2
		if BE.Uint16(buf[i:]) == c { // h
			bits.Set(k + 7)
		}
		i++
		n -= 8
		k += 8
	}

	// process tail (max 7 values left)
	if n > 0 {
		if BE.Uint16(buf[i:])>>2&mask == c { // a
			bits.Set(k)
		}
		i++
		n--
	}
	if n > 0 {
		if BE.Uint16(buf[i:])<<4|uint16(buf[i+2]>>4)&mask == c { // b
			bits.Set(k + 1)
		}
		i += 2
		n--
	}
	if n > 0 {
		if BE.Uint16(buf[i:])<<2|uint16(buf[i+2]>>6)&mask == c { // c
			bits.Set(k + 2)
		}
		i += 2
		n--
	}
	if n > 0 {
		if BE.Uint16(buf[i:]) == c { // d
			bits.Set(k + 3)
		}
		i++
		n--
	}
	if n > 0 {
		if BE.Uint16(buf[i:])>>2&mask == c { // e
			bits.Set(k + 4)
		}
		i++
		n--
	}
	if n > 0 {
		if BE.Uint16(buf[i:])<<4|uint16(buf[i+2]>>4)&mask == c { // f
			bits.Set(k + 5)
		}
		i += 2
		n--
	}
	if n > 0 {
		if BE.Uint16(buf[i:])<<2|uint16(buf[i+2]>>6)&mask == c { // g
			bits.Set(k + 6)
		}
	}
	return bits
}

func cmp_bp_15_eq(buf []byte, val uint64, n int, bits *Bitset) *Bitset {
	// 15bit packed [aaaa aaaa] [aaaa aaab] [bbbb bbbb] [bbbb bbcc]
	//              [cccc cccc] [cccc cddd] [dddd dddd] [dddd eeee]
	//              [eeee eeee] [eeef ffff] [ffff ffff] [ffgg gggg]
	//              [gggg gggg] [ghhh hhhh] [hhhh hhhh]
	var (
		i    int
		k    int
		c    uint16 = uint16(val)
		mask uint16 = 0x7FFF
	)

	// process 8 values per loop
	for n >= 8 {
		if BE.Uint16(buf[i:])>>1&mask == c { // a
			bits.Set(k)
		}
		i++
		if BE.Uint16(buf[i:])<<6|uint16(buf[i+2]>>2)&mask == c { // b
			bits.Set(k + 1)
		}
		i += 2
		if BE.Uint16(buf[i:])<<5|uint16(buf[i+2]>>3)&mask == c { // c
			bits.Set(k + 2)
		}
		i += 2
		if BE.Uint16(buf[i:])<<4|uint16(buf[i+2]>>4)&mask == c { // d
			bits.Set(k + 3)
		}
		i += 2
		if BE.Uint16(buf[i:])<<3|uint16(buf[i+2]>>5)&mask == c { // e
			bits.Set(k + 4)
		}
		i += 2
		if BE.Uint16(buf[i:])<<2|uint16(buf[i+2]>>6)&mask == c { // f
			bits.Set(k + 5)
		}
		i += 2
		if BE.Uint16(buf[i:])<<1|uint16(buf[i+2]>>7)&mask == c { // g
			bits.Set(k + 6)
		}
		i += 2
		if BE.Uint16(buf[i:]) == c { // h
			bits.Set(k + 7)
		}
		i++
		n -= 8
		k += 8
	}

	// process tail (max 7 values left)
	if n > 0 {
		if BE.Uint16(buf[i:])>>1&mask == c { // a
			bits.Set(k)
		}
		i++
		n--
	}
	if n > 0 {
		if BE.Uint16(buf[i:])<<6|uint16(buf[i+2]>>2)&mask == c { // b
			bits.Set(k + 1)
		}
		i += 2
		n--
	}
	if n > 0 {
		if BE.Uint16(buf[i:])<<5|uint16(buf[i+2]>>3)&mask == c { // c
			bits.Set(k + 2)
		}
		i += 2
		n--
	}
	if n > 0 {
		if BE.Uint16(buf[i:])<<4|uint16(buf[i+2]>>4)&mask == c { // d
			bits.Set(k + 3)
		}
		i += 2
		n--
	}
	if n > 0 {
		if BE.Uint16(buf[i:])<<3|uint16(buf[i+2]>>5)&mask == c { // e
			bits.Set(k + 4)
		}
		i += 2
		n--
	}
	if n > 0 {
		if BE.Uint16(buf[i:])<<2|uint16(buf[i+2]>>6)&mask == c { // f
			bits.Set(k + 5)
		}
		i += 2
		n--
	}
	if n > 0 {
		if BE.Uint16(buf[i:])<<1|uint16(buf[i+2]>>7)&mask == c { // g
			bits.Set(k + 6)
		}
	}
	return bits
}

func cmp_bp_16_eq(buf []byte, val uint64, n int, bits *Bitset) *Bitset {
	// 16bit packed [aaaa aaaa] [aaaa aaaa] [bbbb bbbb] [bbbb bbbb]
	var (
		i int
		k int
		c uint16 = uint16(val)
	)

	// process 48 values per loop
	for n >= 8 {
		if BE.Uint16(buf[i:]) == c { // a
			bits.Set(k)
		}
		i += 2
		if BE.Uint16(buf[i:]) == c { // b
			bits.Set(k + 1)
		}
		i += 2
		if BE.Uint16(buf[i:]) == c { // c
			bits.Set(k + 2)
		}
		i += 2
		if BE.Uint16(buf[i:]) == c { // d
			bits.Set(k + 3)
		}
		if BE.Uint16(buf[i:]) == c { // e
			bits.Set(k + 4)
		}
		i += 2
		if BE.Uint16(buf[i:]) == c { // f
			bits.Set(k + 5)
		}
		i += 2
		if BE.Uint16(buf[i:]) == c { // g
			bits.Set(k + 6)
		}
		i += 2
		if BE.Uint16(buf[i:]) == c { // h
			bits.Set(k + 7)
		}
		i += 2
		n -= 8
		k += 8
	}

	// process tail (max 7 values left)
	if n > 0 {
		if BE.Uint16(buf[i:]) == c { // a
			bits.Set(k)
		}
		i += 2
		n--
	}
	if n > 0 {
		if BE.Uint16(buf[i:]) == c { // b
			bits.Set(k + 1)
		}
		i += 2
		n--
	}
	if n > 0 {
		if BE.Uint16(buf[i:]) == c { // c
			bits.Set(k + 2)
		}
		i += 2
		n--
	}
	if n > 0 {
		if BE.Uint16(buf[i:]) == c { // d
			bits.Set(k + 3)
		}
		i += 2
		n--
	}
	if n > 0 {
		if BE.Uint16(buf[i:]) == c { // e
			bits.Set(k + 4)
		}
		i += 2
		n--
	}
	if n > 0 {
		if BE.Uint16(buf[i:]) == c { // f
			bits.Set(k + 5)
		}
		i += 2
		n--
	}
	if n > 0 {
		if BE.Uint16(buf[i:]) == c { // g
			bits.Set(k + 6)
		}
	}
	return bits
}
