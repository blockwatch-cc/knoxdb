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
	cmp_bp_16_eq, cmp_bp_17_eq, cmp_bp_18_eq, cmp_bp_19_eq,
	cmp_bp_20_eq, cmp_bp_21_eq, cmp_bp_22_eq, cmp_bp_23_eq,
	cmp_bp_24_eq, cmp_bp_25_eq, cmp_bp_26_eq, cmp_bp_27_eq,
	cmp_bp_28_eq, cmp_bp_29_eq, cmp_bp_30_eq, cmp_bp_31_eq,
	cmp_bp_32_eq, cmp_bp_33_eq, cmp_bp_34_eq, cmp_bp_35_eq,
	cmp_bp_36_eq, cmp_bp_37_eq, cmp_bp_38_eq, cmp_bp_39_eq,
	cmp_bp_40_eq, cmp_bp_41_eq, cmp_bp_42_eq, cmp_bp_43_eq,
	cmp_bp_44_eq, cmp_bp_45_eq, cmp_bp_46_eq, cmp_bp_47_eq,
	cmp_bp_48_eq, cmp_bp_49_eq, cmp_bp_50_eq, cmp_bp_51_eq,
	cmp_bp_52_eq, cmp_bp_53_eq, cmp_bp_54_eq, cmp_bp_55_eq,
	cmp_bp_56_eq, cmp_bp_57_eq, cmp_bp_58_eq, cmp_bp_59_eq,
	cmp_bp_60_eq, cmp_bp_61_eq, cmp_bp_62_eq, cmp_bp_63_eq,
}

func cmp_bp_0_eq(buf []byte, val uint64, n int, bits *Bitset) *Bitset {
	if val == 0 {
		return bits.One()
	}
	return bits
}

func cmp_bp_1_eq(buf []byte, val uint64, n int, bits *Bitset) *Bitset {
	// value can only be 0 or 1, so we can simply copy bitpack buffer to bitset
	// note: bit set is reverse order, so we must flip during set
	bits.SetFromBytes(buf, n, true)

	// flip bits if val == 0
	if val == 0 {
		bits.Neg()
	}
	return bits
}

func cmp_bp_2_eq(buf []byte, val uint64, n int, bits *Bitset) *Bitset {
	// 2 bit packing
	// [aabb ccdd] [eeff gghh]
	var (
		i    int
		k    int
		c    byte = byte(val)
		mask byte = 0x3
		b    byte
		res  []byte = bits.Bytes()
	)
	// process 8 values
	for n >= 8 {
		if buf[i]>>6&mask == c { // a
			b |= 1
		}
		if buf[i]>>4&mask == c { // b
			b |= 2
		}
		if buf[i]>>2&mask == c { // c
			b |= 4
		}
		if buf[i]&mask == c { // d
			b |= 8
		}
		i++
		if buf[i]>>6&mask == c { // e
			b |= 0x10
		}
		if buf[i]>>4&mask == c { // f
			b |= 0x20
		}
		if buf[i]>>2&mask == c { // g
			b |= 0x40
		}
		if buf[i]&mask == c { // h
			b |= 0x80
		}
		if b > 0 {
			res[k/8] = b
			b = 0
			bits.ResetCount()
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
	// 3 bit packed
	// [aaab bbcc] [cddd eeef] [ffgg ghhh]
	var (
		i    int
		k    int
		c    byte = byte(val)
		mask byte = 0x7
		b    byte
		res  []byte = bits.Bytes()
	)

	// process 8 values per loop
	for n >= 8 {
		if buf[i]>>5&mask == c { // a
			b |= 1
		}
		if buf[i]>>2&mask == c { // b
			b |= 2
		}
		if (buf[i]<<1|buf[i+1]>>7)&mask == c { // c
			b |= 4
		}
		i++
		if buf[i]>>4&mask == c { // d
			b |= 8
		}
		if buf[i]>>1&mask == c { // e
			b |= 0x10
		}
		if (buf[i]<<2|buf[i+1]>>6)&mask == c { // f
			b |= 0x20
		}
		i++
		if buf[i]>>3&mask == c { // g
			b |= 0x40
		}
		if buf[i]&mask == c { // h
			b |= 0x80
		}
		if b > 0 {
			res[k/8] = b
			b = 0
			bits.ResetCount()
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
		if (buf[i]<<1|buf[i+1]>>7)&mask == c { // c
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
		if (buf[i]<<2|buf[i+1]>>6)&mask == c { // f
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
	// 4bit packed
	// [aaaa bbbb] [cccc dddd] [eeee ffff] [gggg hhhh]
	var (
		i    int
		k    int
		c    byte = byte(val)
		mask byte = 0xF
		b    byte
		res  []byte = bits.Bytes()
	)
	// process 8 values
	for n >= 8 {
		if buf[i]>>4&mask == c { // a
			b |= 1
		}
		if buf[i]&mask == c { // b
			b |= 2
		}
		i++
		if buf[i]>>4&mask == c { // c
			b |= 4
		}
		if buf[i]&mask == c { // d
			b |= 8
		}
		i++
		if buf[i]>>4&mask == c { // e
			b |= 0x10
		}
		if buf[i]&mask == c { // f
			b |= 0x20
		}
		i++
		if buf[i]>>4&mask == c { // g
			b |= 0x40
		}
		if buf[i]&mask == c { // h
			b |= 0x80
		}
		if b > 0 {
			res[k/8] = b
			b = 0
			bits.ResetCount()
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
	// 5bit packed
	// [aaaa abbb] [bbcc cccd] [dddd eeee] [efff ffgg] [gggh hhhh]
	var (
		i    int
		k    int
		c    uint16 = uint16(val)
		mask uint16 = 0x1f
		b    byte
		res  []byte = bits.Bytes()
	)

	// process 8 values per loop
	for n >= 8 {
		if uint16(buf[i])>>3&mask == c { // a
			b |= 1
		}
		if BE.Uint16(buf[i:])>>6&mask == c { // b
			b |= 2
		}
		i++
		if uint16(buf[i])>>1&mask == c { // c
			b |= 4
		}
		if BE.Uint16(buf[i:])>>4&mask == c { // d
			b |= 8
		}
		i++
		if BE.Uint16(buf[i:])>>7&mask == c { // e
			b |= 0x10
		}
		i++
		if uint16(buf[i])>>2&mask == c { // f
			b |= 0x20
		}
		if BE.Uint16(buf[i:])>>5&mask == c { // g
			b |= 0x40
		}
		i++
		if uint16(buf[i])&mask == c { // h
			b |= 0x80
		}
		if b > 0 {
			res[k/8] = b
			b = 0
			bits.ResetCount()
		}
		i++
		n -= 8
		k += 8
	}

	// process tail (max 7 values left)
	if n > 0 {
		if uint16(buf[i])>>3&mask == c { // a
			bits.Set(k)
		}
		n--
	}
	if n > 0 {
		if u16be(buf[i:])>>6&mask == c { // b
			bits.Set(k + 1)
		}
		n--
		i++
	}
	if n > 0 {
		if uint16(buf[i])>>1&mask == c { // c
			bits.Set(k + 2)
		}
		n--
	}
	if n > 0 {
		if u16be(buf[i:])>>4&mask == c { // d
			bits.Set(k + 3)
		}
		n--
		i++
	}
	if n > 0 {
		if u16be(buf[i:])>>7&mask == c { // e
			bits.Set(k + 4)
		}
		n--
		i++
	}
	if n > 0 {
		if uint16(buf[i])>>2&mask == c { // f
			bits.Set(k + 5)
		}
		n--
	}
	if n > 0 {
		if u16be(buf[i:])>>5&mask == c { // g
			bits.Set(k + 6)
		}
	}
	return bits
}

func cmp_bp_6_eq(buf []byte, val uint64, n int, bits *Bitset) *Bitset {
	// 6bit packed
	// [aaaa aabb] [bbbb cccc] [ccdd dddd]
	// [eeee eeff] [ffff gggg] [gghh hhhh]
	var (
		i    int
		k    int
		c    uint16 = uint16(val)
		mask uint16 = 0x3f
		b    byte
		res  []byte = bits.Bytes()
	)

	// process 8 values per loop
	for n >= 4 {
		if uint16(buf[i])>>2&mask == c { // a
			b |= 1
		}
		if BE.Uint16(buf[i:])>>4&mask == c { // b
			b |= 2
		}
		i++
		if BE.Uint16(buf[i:])>>6&mask == c { // c
			b |= 4
		}
		i++
		if uint16(buf[i])&mask == c { // d
			b |= 8
		}
		i++
		if uint16(buf[i])>>2&mask == c { // e
			b |= 0x10
		}
		if BE.Uint16(buf[i:])>>4&mask == c { // f
			b |= 0x20
		}
		i++
		if BE.Uint16(buf[i:])>>6&mask == c { // g
			b |= 0x40
		}
		i++
		if uint16(buf[i])&mask == c { // h
			b |= 0x80
		}
		if b > 0 {
			res[k/8] = b
			b = 0
			bits.ResetCount()
		}
		i++
		n -= 8
		k += 8
	}

	// process tail (max 7 values left)
	if n > 0 {
		if uint16(buf[i])>>2&mask == c { // a
			bits.Set(k)
		}
		n--
	}
	if n > 0 {
		if u16be(buf[i:])>>4&mask == c { // b
			bits.Set(k + 1)
		}
		n--
		i++
	}
	if n > 0 {
		if u16be(buf[i:])>>6&mask == c { // c
			bits.Set(k + 2)
		}
	}
	if n > 0 {
		if uint16(buf[i])&mask == c { // d
			bits.Set(k + 3)
		}
		n--
		i++
	}
	if n > 0 {
		if uint16(buf[i])>>2&mask == c { // e
			bits.Set(k + 4)
		}
		n--
	}
	if n > 0 {
		if u16be(buf[i:])>>4&mask == c { // f
			bits.Set(k + 5)
		}
		n--
		i++
	}
	if n > 0 {
		if u16be(buf[i:])>>6&mask == c { // g
			bits.Set(k + 6)
		}
	}
	return bits
}

func cmp_bp_7_eq(buf []byte, val uint64, n int, bits *Bitset) *Bitset {
	// 7bit packed
	// [aaaa aaab] [bbbb bbcc] [cccc cddd] [dddd eeee]
	// [eeef ffff] [ffgg gggg] [ghhh hhhh]
	var (
		i    int
		k    int
		c    uint16 = uint16(val)
		mask uint16 = 0x7f
		b    byte
		res  []byte = bits.Bytes()
	)

	// process 8 values per loop
	for n >= 8 {
		if uint16(buf[i])>>1&mask == c { // a
			b |= 1
		}
		if BE.Uint16(buf[i:])>>2&mask == c { // b
			b |= 2
		}
		i++
		if BE.Uint16(buf[i:])>>3&mask == c { // c
			b |= 4
		}
		i++
		if BE.Uint16(buf[i:])>>4&mask == c { // d
			b |= 8
		}
		i++
		if BE.Uint16(buf[i:])>>5&mask == c { // e
			b |= 0x10
		}
		i++
		if BE.Uint16(buf[i:])>>6&mask == c { // f
			b |= 0x20
		}
		i++
		if BE.Uint16(buf[i:])>>7&mask == c { // g
			b |= 0x40
		}
		i++
		if uint16(buf[i])&mask == c { // h
			b |= 0x80
		}
		if b > 0 {
			res[k/8] = b
			b = 0
			bits.ResetCount()
		}
		i++
		n -= 8
		k += 8
	}

	// process tail (max 7 values left)
	if n > 0 {
		if uint16(buf[i])>>1&mask == c { // a
			bits.Set(k)
		}
		n--
	}
	if n > 0 {
		if u16be(buf[i:])>>2&mask == c { // b
			bits.Set(k + 1)
		}
		n--
		i++
	}
	if n > 0 {
		if u16be(buf[i:])>>3&mask == c { // c
			bits.Set(k + 2)
		}
		n--
		i++
	}
	if n > 0 {
		if u16be(buf[i:])>>4&mask == c { // d
			bits.Set(k + 3)
		}
		n--
		i++
	}
	if n > 0 {
		if u16be(buf[i:])>>5&mask == c { // e
			bits.Set(k + 4)
		}
		n--
		i++
	}
	if n > 0 {
		if u16be(buf[i:])>>6&mask == c { // f
			bits.Set(k + 5)
		}
		n--
		i++
	}
	if n > 0 {
		if u16be(buf[i:])>>7&mask == c { // g
			bits.Set(k + 6)
		}
	}
	return bits
}

func cmp_bp_8_eq(buf []byte, val uint64, n int, bits *Bitset) *Bitset {
	return cmp.MatchUint8Equal(buf[:n], uint8(val), bits, nil)
}

func cmp_bp_9_eq(buf []byte, val uint64, n int, bits *Bitset) *Bitset {
	// 9bit packed
	// [aaaa aaaa] [abbb bbbb] [bbcc cccc] [cccd dddd]
	// [dddd eeee] [eeee efff] [ffff ffgg] [gggg gggh]
	// [hhhh hhhh]
	var (
		i    int
		k    int
		c    uint16 = uint16(val)
		mask uint16 = 0x1FF
		b    byte
		res  []byte = bits.Bytes()
	)

	// process 8 values per loop
	for n >= 8 {
		if BE.Uint16(buf[i:])>>7&mask == c { // a
			b |= 1
		}
		i++
		if BE.Uint16(buf[i:])>>6&mask == c { // b
			b |= 2
		}
		i++
		if BE.Uint16(buf[i:])>>5&mask == c { // c
			b |= 4
		}
		i++
		if BE.Uint16(buf[i:])>>4&mask == c { // d
			b |= 8
		}
		i++
		if BE.Uint16(buf[i:])>>3&mask == c { // e
			b |= 0x10
		}
		i++
		if BE.Uint16(buf[i:])>>2&mask == c { // f
			b |= 0x20
		}
		i++
		if BE.Uint16(buf[i:])>>1&mask == c { // g
			b |= 0x40
		}
		i++
		if BE.Uint16(buf[i:])&mask == c { // h
			b |= 0x80
		}
		if b > 0 {
			res[k/8] = b
			b = 0
			bits.ResetCount()
		}
		i += 2
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
	// 10bit packed
	// [aaaa aaaa] [aabb bbbb] [bbbb cccc] [cccc ccdd]
	// [dddd dddd] [eeee eeee] [eeff ffff] [ffff gggg]
	// [gggg gghh] [hhhh hhhh]
	var (
		i    int
		k    int
		c    uint16 = uint16(val)
		mask uint16 = 0x3FF
		b    byte
		res  []byte = bits.Bytes()
	)

	// process 8 values per loop
	for n >= 8 {
		if BE.Uint16(buf[i:])>>6&mask == c { // a
			b |= 1
		}
		i++
		if BE.Uint16(buf[i:])>>4&mask == c { // b
			b |= 2
		}
		i++
		if BE.Uint16(buf[i:])>>2&mask == c { // c
			b |= 4
		}
		i++
		if BE.Uint16(buf[i:])&mask == c { // d
			b |= 8
		}
		i += 2
		if BE.Uint16(buf[i:])>>6&mask == c { // e
			b |= 0x10
		}
		i++
		if BE.Uint16(buf[i:])>>4&mask == c { // f
			b |= 0x20
		}
		i++
		if BE.Uint16(buf[i:])>>2&mask == c { // g
			b |= 0x40
		}
		i++
		if BE.Uint16(buf[i:])&mask == c { // h
			b |= 0x80
		}
		if b > 0 {
			res[k/8] = b
			b = 0
			bits.ResetCount()
		}
		i += 2
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
		if BE.Uint16(buf[i:])&mask == c { // d
			bits.Set(k + 3)
		}
		i += 2
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
	// 11bit packed
	// [aaaa aaaa] [aaab bbbb] [bbbb bbcc] [cccc cccc]
	// [cddd dddd] [dddd eeee] [eeee eeef] [ffff ffff]
	// [ffgg gggg] [gggg ghhh] [hhhh hhhh]
	var (
		i    int
		k    int
		c    uint32 = uint32(val)
		mask uint32 = 0x7FF
		b    byte
		res  []byte = bits.Bytes()
	)

	// process 8 values per loop
	for n >= 8 {
		if BE.Uint32(buf[i:])>>21&mask == c { // a
			b |= 1
		}
		i++
		if BE.Uint32(buf[i:])>>18&mask == c { // b
			b |= 2
		}
		i++
		if BE.Uint32(buf[i:])>>15&mask == c { // c
			b |= 4
		}
		i += 2
		if BE.Uint32(buf[i:])>>20&mask == c { // d
			b |= 8
		}
		i++
		if BE.Uint32(buf[i:])>>17&mask == c { // e
			b |= 0x10
		}
		i++
		if BE.Uint32(buf[i:])>>14&mask == c { // f
			b |= 0x20
		}
		i++
		if BE.Uint32(buf[i:])>>11&mask == c { // g
			b |= 0x40
		}
		if BE.Uint32(buf[i:])&mask == c { // h
			b |= 0x80
		}
		if b > 0 {
			res[k/8] = b
			b = 0
			bits.ResetCount()
		}
		i += 4
		n -= 8
		k += 8
	}

	// process tail (max 7 values left)
	if n > 0 {
		if u32be(buf[i:])>>21&mask == c { // a
			bits.Set(k)
		}
		i++
		n--
	}
	if n > 0 {
		if u32be(buf[i:])>>18&mask == c { // b
			bits.Set(k + 1)
		}
		i++
		n--
	}
	if n > 0 {
		if u32be(buf[i:])>>15&mask == c { // c
			bits.Set(k + 2)
		}
		n--
		i += 2
	}
	if n > 0 {
		if u32be(buf[i:])>>20&mask == c { // d
			bits.Set(k + 3)
		}
		i++
		n--
	}
	if n > 0 {
		if u32be(buf[i:])>>17&mask == c { // e
			bits.Set(k + 4)
		}
		i++
		n--
	}
	if n > 0 {
		if u32be(buf[i:])>>14&mask == c { // f
			bits.Set(k + 5)
		}
		i++
		n--
	}
	if n > 0 {
		if u32be(buf[i:])>>11&mask == c { // g
			bits.Set(k + 6)
		}
	}
	return bits
}

func cmp_bp_12_eq(buf []byte, val uint64, n int, bits *Bitset) *Bitset {
	// 12bit packed
	// [aaaa aaaa] [aaaa bbbb] [bbbb bbbb] [cccc cccc] [cccc dddd]
	// [dddd dddd] [eeee eeee] [eeee ffff] [ffff ffff] [gggg gggg]
	// [gggg hhhh] [hhhh hhhh]
	var (
		i    int
		k    int
		c    uint16 = uint16(val)
		mask uint16 = 0xFFF
		b    byte
		res  []byte = bits.Bytes()
	)

	// process 8 values per loop
	for n >= 8 {
		if BE.Uint16(buf[i:])>>4&mask == c { // a
			b |= 1
		}
		i++
		if BE.Uint16(buf[i:])&mask == c { // b
			b |= 2
		}
		i += 2
		if BE.Uint16(buf[i:])>>4&mask == c { // c
			b |= 4
		}
		i++
		if BE.Uint16(buf[i:])&mask == c { // d
			b |= 8
		}
		i += 2
		if BE.Uint16(buf[i:])>>4&mask == c { // e
			b |= 0x10
		}
		i++
		if BE.Uint16(buf[i:])&mask == c { // f
			b |= 0x20
		}
		i += 2
		if BE.Uint16(buf[i:])>>4&mask == c { // g
			b |= 0x40
		}
		i++
		if BE.Uint16(buf[i:])&mask == c { // h
			b |= 0x80
		}
		if b > 0 {
			res[k/8] = b
			b = 0
			bits.ResetCount()
		}
		i += 2
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
		i += 2
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
		i += 2
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
		i += 2
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
	// 13bit packed
	// [aaaa aaaa] [aaaa abbb] [bbbb bbbb] [bbcc cccc]
	// [cccc cccd] [dddd dddd] [dddd eeee] [eeee eeee]
	// [efff ffff] [ffff ffgg] [gggg gggg] [gggh hhhh]
	// [hhhh hhhh]
	var (
		i    int
		k    int
		c    uint32 = uint32(val)
		mask uint32 = 0x1FFF
		b    byte
		res  []byte = bits.Bytes()
	)

	// process 8 values per loop
	for n >= 8 {
		if BE.Uint32(buf[i:])>>19&mask == c { // a
			b |= 1
		}
		i++
		if BE.Uint32(buf[i:])>>14&mask == c { // b
			b |= 2
		}
		i += 2
		if BE.Uint32(buf[i:])>>17&mask == c { // c
			b |= 4
		}
		i++
		if BE.Uint32(buf[i:])>>12&mask == c { // d
			b |= 8
		}
		i += 2
		if BE.Uint32(buf[i:])>>15&mask == c { // e
			b |= 0x10
		}
		i += 2
		if BE.Uint32(buf[i:])>>18&mask == c { // f
			b |= 0x20
		}
		i++
		if BE.Uint32(buf[i:])>>13&mask == c { // g
			b |= 0x40
		}
		if BE.Uint32(buf[i:])&mask == c { // h
			b |= 0x80
		}
		if b > 0 {
			res[k/8] = b
			b = 0
			bits.ResetCount()
		}
		i += 4
		n -= 8
		k += 8
	}

	// process tail (max 7 values left)
	if n > 0 {
		if u32be(buf[i:])>>19&mask == c { // a
			bits.Set(k)
		}
		i++
		n--
	}
	if n > 0 {
		if u32be(buf[i:])>>14&mask == c { // b
			bits.Set(k + 1)
		}
		i += 2
		n--
	}
	if n > 0 {
		if u32be(buf[i:])>>17&mask == c { // c
			bits.Set(k + 2)
		}
		n--
		i++
	}
	if n > 0 {
		if u32be(buf[i:])>>12&mask == c { // d
			bits.Set(k + 3)
		}
		i += 2
		n--
	}
	if n > 0 {
		if u32be(buf[i:])>>15&mask == c { // e
			bits.Set(k + 4)
		}
		i += 2
		n--
	}
	if n > 0 {
		if u32be(buf[i:])>>18&mask == c { // f
			bits.Set(k + 5)
		}
		i++
		n--
	}
	if n > 0 {
		if u32be(buf[i:])>>13&mask == c { // g
			bits.Set(k + 6)
		}
	}
	return bits
}

func cmp_bp_14_eq(buf []byte, val uint64, n int, bits *Bitset) *Bitset {
	// 14bit packed
	// [aaaa aaaa] [aaaa aabb] [bbbb bbbb] [bbbb cccc]
	// [cccc cccc] [ccdd dddd] [dddd dddd] [eeee eeee]
	// [eeee eeff] [ffff ffff] [ffff gggg] [gggg gggg]
	// [gghh hhhh] [hhhh hhhh]
	var (
		i    int
		k    int
		c    uint32 = uint32(val)
		mask uint32 = 0x3FFF
		b    byte
		res  []byte = bits.Bytes()
	)

	// process 8 values per loop
	for n >= 8 {
		if BE.Uint32(buf[i:])>>18&mask == c { // a
			b |= 1
		}
		i++
		if BE.Uint32(buf[i:])>>12&mask == c { // b
			b |= 2
		}
		i += 2
		if BE.Uint32(buf[i:])>>14&mask == c { // c
			b |= 4
		}
		i += 2
		if BE.Uint32(buf[i:])>>16&mask == c { // d
			b |= 8
		}
		i += 2
		if BE.Uint32(buf[i:])>>18&mask == c { // e
			b |= 0x10
		}
		i++
		if BE.Uint32(buf[i:])>>12&mask == c { // f
			b |= 0x20
		}
		i += 2
		if BE.Uint32(buf[i:])>>14&mask == c { // g
			b |= 0x40
		}
		if BE.Uint32(buf[i:])&mask == c { // h
			b |= 0x80
		}
		if b > 0 {
			res[k/8] = b
			b = 0
			bits.ResetCount()
		}
		i += 4
		n -= 8
		k += 8
	}

	// process tail (max 7 values left)
	if n > 0 {
		if u32be(buf[i:])>>18&mask == c { // a
			bits.Set(k)
		}
		i++
		n--
	}
	if n > 0 {
		if u32be(buf[i:])>>12&mask == c { // b
			bits.Set(k + 1)
		}
		i += 2
		n--
	}
	if n > 0 {
		if u32be(buf[i:])>>14&mask == c { // c
			bits.Set(k + 2)
		}
		i += 2
		n--
	}
	if n > 0 {
		if u32be(buf[i:])>>16&mask == c { // d
			bits.Set(k + 3)
		}
		i += 2
		n--
	}
	if n > 0 {
		if u32be(buf[i:])>>18&mask == c { // e
			bits.Set(k + 4)
		}
		i++
		n--
	}
	if n > 0 {
		if u32be(buf[i:])>>12&mask == c { // f
			bits.Set(k + 5)
		}
		i += 2
		n--
	}
	if n > 0 {
		if u32be(buf[i:])>>14&mask == c { // g
			bits.Set(k + 6)
		}
	}
	return bits
}

func cmp_bp_15_eq(buf []byte, val uint64, n int, bits *Bitset) *Bitset {
	// 15bit packed
	// [aaaa aaaa] [aaaa aaab] [bbbb bbbb] [bbbb bbcc]
	// [cccc cccc] [cccc cddd] [dddd dddd] [dddd eeee]
	// [eeee eeee] [eeef ffff] [ffff ffff] [ffgg gggg]
	// [gggg gggg] [ghhh hhhh] [hhhh hhhh]
	var (
		i    int
		k    int
		c    uint32 = uint32(val)
		mask uint32 = 0x7FFF
		b    byte
		res  []byte = bits.Bytes()
	)

	// process 8 values per loop
	for n >= 8 {
		if BE.Uint32(buf[i:])>>17&mask == c { // a
			b |= 1
		}
		i++
		if BE.Uint32(buf[i:])>>10&mask == c { // b
			b |= 2
		}
		i += 2
		if BE.Uint32(buf[i:])>>11&mask == c { // c
			b |= 4
		}
		i += 2
		if BE.Uint32(buf[i:])>>12&mask == c { // d
			b |= 8
		}
		i += 2
		if BE.Uint32(buf[i:])>>13&mask == c { // e
			b |= 0x10
		}
		i += 2
		if BE.Uint32(buf[i:])>>14&mask == c { // f
			b |= 0x20
		}
		i += 2
		if BE.Uint32(buf[i:])>>15&mask == c { // g
			b |= 0x40
		}
		if BE.Uint32(buf[i:])&mask == c { // h
			b |= 0x80
		}
		if b > 0 {
			res[k/8] = b
			b = 0
			bits.ResetCount()
		}
		i += 4
		n -= 8
		k += 8
	}

	// process tail (max 7 values left)
	if n > 0 {
		if u32be(buf[i:])>>17&mask == c { // a
			bits.Set(k)
		}
		i++
		n--
	}
	if n > 0 {
		if u32be(buf[i:])>>10&mask == c { // b
			bits.Set(k + 1)
		}
		i += 2
		n--
	}
	if n > 0 {
		if u32be(buf[i:])>>11&mask == c { // c
			bits.Set(k + 2)
		}
		i += 2
		n--
	}
	if n > 0 {
		if u32be(buf[i:])>>12&mask == c { // d
			bits.Set(k + 3)
		}
		i += 2
		n--
	}
	if n > 0 {
		if u32be(buf[i:])>>13&mask == c { // e
			bits.Set(k + 4)
		}
		i += 2
		n--
	}
	if n > 0 {
		if u32be(buf[i:])>>14&mask == c { // f
			bits.Set(k + 5)
		}
		i += 2
		n--
	}
	if n > 0 {
		if u32be(buf[i:])>>15&mask == c { // g
			bits.Set(k + 6)
		}
	}
	return bits
}

func cmp_bp_16_eq(buf []byte, val uint64, n int, bits *Bitset) *Bitset {
	// 16bit packed
	// [aaaa aaaa] [aaaa aaaa] [bbbb bbbb] [bbbb bbbb]
	// [cccc cccc] [cccc cccc] [dddd dddd] [dddd dddd]
	// [eeee eeee] [eeee eeee] [ffff ffff] [ffff ffff]
	// [gggg gggg] [gggg gggg] [hhhh hhhh] [hhhh hhhh]
	var (
		i   int
		k   int
		c   uint16 = uint16(val)
		b   byte
		res []byte = bits.Bytes()
	)

	// process 48 values per loop
	for n >= 8 {
		if BE.Uint16(buf[i:]) == c { // a
			b |= 1
		}
		i += 2
		if BE.Uint16(buf[i:]) == c { // b
			b |= 2
		}
		i += 2
		if BE.Uint16(buf[i:]) == c { // c
			b |= 4
		}
		i += 2
		if BE.Uint16(buf[i:]) == c { // d
			b |= 8
		}
		i += 2
		if BE.Uint16(buf[i:]) == c { // e
			b |= 0x10
		}
		i += 2
		if BE.Uint16(buf[i:]) == c { // f
			b |= 0x20
		}
		i += 2
		if BE.Uint16(buf[i:]) == c { // g
			b |= 0x40
		}
		i += 2
		if BE.Uint16(buf[i:]) == c { // h
			b |= 0x80
		}
		if b > 0 {
			res[k/8] = b
			b = 0
			bits.ResetCount()
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

func cmp_bp_17_eq(buf []byte, val uint64, n int, bits *Bitset) *Bitset {
	// 17bit packed
	// [aaaa aaaa] [aaaa aaaa] [abbb bbbb] [bbbb bbbb]
	// [bbcc cccc] [cccc cccc] [cccd dddd] [dddd dddd]
	// [dddd eeee] [eeee eeee] [eeee efff] [ffff ffff]
	// [ffff ffgg] [gggg gggg] [gggg gggh] [hhhh hhhh]
	// [hhhh hhhh]
	var (
		i    int
		k    int
		c    uint32 = uint32(val)
		mask uint32 = 0x1FFFF
		b    byte
		res  []byte = bits.Bytes()
	)

	// process 8 values per loop
	for n >= 8 {
		if BE.Uint32(buf[i:])>>15&mask == c { // a
			b |= 1
		}
		i += 2
		if BE.Uint32(buf[i:])>>14&mask == c { // b
			b |= 2
		}
		i += 2
		if BE.Uint32(buf[i:])>>13&mask == c { // c
			b |= 4
		}
		i += 2
		if BE.Uint32(buf[i:])>>12&mask == c { // d
			b |= 8
		}
		i += 2
		if BE.Uint32(buf[i:])>>11&mask == c { // e
			b |= 0x10
		}
		i += 2
		if BE.Uint32(buf[i:])>>10&mask == c { // f
			b |= 0x20
		}
		i += 2
		if BE.Uint32(buf[i:])>>9&mask == c { // g
			b |= 0x40
		}
		i += 1
		if BE.Uint32(buf[i:])&mask == c { // h
			b |= 0x80
		}
		if b > 0 {
			res[k/8] = b
			b = 0
			bits.ResetCount()
		}
		i += 4
		n -= 8
		k += 8
	}

	// process tail (max 7 values left)
	if n > 0 {
		if u32be(buf[i:])>>15&mask == c { // a
			bits.Set(k)
		}
		i += 2
		n--
	}
	if n > 0 {
		if u32be(buf[i:])>>14&mask == c { // b
			bits.Set(k + 1)
		}
		i += 2
		n--
	}
	if n > 0 {
		if u32be(buf[i:])>>13&mask == c { // c
			bits.Set(k + 2)
		}
		i += 2
		n--
	}
	if n > 0 {
		if u32be(buf[i:])>>12&mask == c { // d
			bits.Set(k + 3)
		}
		i += 2
		n--
	}
	if n > 0 {
		if u32be(buf[i:])>>11&mask == c { // e
			bits.Set(k + 4)
		}
		i += 2
		n--
	}
	if n > 0 {
		if u32be(buf[i:])>>10&mask == c { // f
			bits.Set(k + 5)
		}
		i += 2
		n--
	}
	if n > 0 {
		if u32be(buf[i:])>>9&mask == c { // g
			bits.Set(k + 6)
		}
	}
	return bits
}

func cmp_bp_18_eq(buf []byte, val uint64, n int, bits *Bitset) *Bitset {
	// 18bit packed
	// [aaaa aaaa] [aaaa aaaa] [aabb bbbb] [bbbb bbbb]
	// [bbbb cccc] [cccc cccc] [cccc ccdd] [dddd dddd]
	// [dddd dddd] [eeee eeee] [eeee eeee] [eeff ffff]
	// [ffff ffff] [ffff gggg] [gggg gggg] [gggg gghh]
	// [hhhh hhhh] [hhhh hhhh]
	var (
		i    int
		k    int
		c    uint32 = uint32(val)
		mask uint32 = 0x3FFFF
		b    byte
		res  []byte = bits.Bytes()
	)

	// process 8 values per loop
	for n >= 8 {
		if BE.Uint32(buf[i:])>>14&mask == c { // a
			b |= 1
		}
		i += 2
		if BE.Uint32(buf[i:])>>12&mask == c { // b
			b |= 2
		}
		i += 2
		if BE.Uint32(buf[i:])>>10&mask == c { // c
			b |= 4
		}
		i += 2
		if BE.Uint32(buf[i:])>>8&mask == c { // d
			b |= 8
		}
		i += 3
		if BE.Uint32(buf[i:])>>14&mask == c { // e
			b |= 0x10
		}
		i += 2
		if BE.Uint32(buf[i:])>>12&mask == c { // f
			b |= 0x20
		}
		i += 2
		if BE.Uint32(buf[i:])>>10&mask == c { // g
			b |= 0x40
		}
		i += 1
		if BE.Uint32(buf[i:])&mask == c { // h
			b |= 0x80
		}
		if b > 0 {
			res[k/8] = b
			b = 0
			bits.ResetCount()
		}
		i += 4
		n -= 8
		k += 8
	}

	// process tail (max 7 values left)
	if n > 0 {
		if u32be(buf[i:])>>14&mask == c { // a
			bits.Set(k)
		}
		i += 2
		n--
	}
	if n > 0 {
		if u32be(buf[i:])>>12&mask == c { // b
			bits.Set(k + 1)
		}
		i += 2
		n--
	}
	if n > 0 {
		if u32be(buf[i:])>>10&mask == c { // c
			bits.Set(k + 2)
		}
		i += 2
		n--
	}
	if n > 0 {
		if u32be(buf[i:])>>8&mask == c { // d
			bits.Set(k + 3)
		}
		i += 3
		n--
	}
	if n > 0 {
		if u32be(buf[i:])>>14&mask == c { // e
			bits.Set(k + 4)
		}
		i += 2
		n--
	}
	if n > 0 {
		if u32be(buf[i:])>>12&mask == c { // f
			bits.Set(k + 5)
		}
		i += 2
		n--
	}
	if n > 0 {
		if u32be(buf[i:])>>10&mask == c { // g
			bits.Set(k + 6)
		}
	}
	return bits
}

func cmp_bp_19_eq(buf []byte, val uint64, n int, bits *Bitset) *Bitset {
	// 19bit packed
	// [aaaa aaaa] [aaaa aaaa] [aaab bbbb] [bbbb bbbb]
	// [bbbb bbcc] [cccc cccc] [cccc cccc] [cddd dddd]
	// [dddd dddd] [dddd eeee] [eeee eeee] [eeee eeef]
	// [ffff ffff] [ffff ffff] [ffgg gggg] [gggg gggg]
	// [gggg ghhh] [hhhh hhhh] [hhhh hhhh]
	var (
		i    int
		k    int
		c    uint32 = uint32(val)
		mask uint32 = 0x7FFFF
		b    byte
		res  []byte = bits.Bytes()
	)

	// process 8 values per loop
	for n >= 8 {
		if BE.Uint32(buf[i:])>>13&mask == c { // a
			b |= 1
		}
		i += 2
		if BE.Uint32(buf[i:])>>10&mask == c { // b
			b |= 2
		}
		i += 2
		if BE.Uint32(buf[i:])>>7&mask == c { // c
			b |= 4
		}
		i += 3
		if BE.Uint32(buf[i:])>>12&mask == c { // d
			b |= 8
		}
		i += 2
		if BE.Uint32(buf[i:])>>9&mask == c { // e
			b |= 0x10
		}
		i += 2
		if BE.Uint32(buf[i:])>>6&mask == c { // f
			b |= 0x20
		}
		i += 3
		if BE.Uint32(buf[i:])>>11&mask == c { // g
			b |= 0x40
		}
		i += 1
		if BE.Uint32(buf[i:])&mask == c { // h
			b |= 0x80
		}
		if b > 0 {
			res[k/8] = b
			b = 0
			bits.ResetCount()
		}
		i += 4
		n -= 8
		k += 8
	}

	// process tail (max 7 values left)
	if n > 0 {
		if u32be(buf[i:])>>13&mask == c { // a
			bits.Set(k)
		}
		i += 2
		n--
	}
	if n > 0 {
		if u32be(buf[i:])>>10&mask == c { // b
			bits.Set(k + 1)
		}
		i += 2
		n--
	}
	if n > 0 {
		if u32be(buf[i:])>>7&mask == c { // c
			bits.Set(k + 2)
		}
		i += 3
		n--
	}
	if n > 0 {
		if u32be(buf[i:])>>12&mask == c { // d
			bits.Set(k + 3)
		}
		i += 2
		n--
	}
	if n > 0 {
		if u32be(buf[i:])>>9&mask == c { // e
			bits.Set(k + 4)
		}
		i += 2
		n--
	}
	if n > 0 {
		if u32be(buf[i:])>>6&mask == c { // f
			bits.Set(k + 5)
		}
		i += 3
		n--
	}
	if n > 0 {
		if u32be(buf[i:])>>11&mask == c { // g
			bits.Set(k + 6)
		}
	}
	return bits
}

func cmp_bp_20_eq(buf []byte, val uint64, n int, bits *Bitset) *Bitset {
	// 20bit packed
	// [aaaa aaaa] [aaaa aaaa] [aaaa bbbb] [bbbb bbbb]
	// [bbbb bbbb] [cccc cccc] [cccc cccc] [cccc dddd]
	// [dddd dddd] [dddd dddd] [eeee eeee] [eeee eeee]
	// [eeee ffff] [ffff ffff] [ffff ffff] [gggg gggg]
	// [gggg gggg] [gggg hhhh] [hhhh hhhh] [hhhh hhhh]
	var (
		i    int
		k    int
		c    uint32 = uint32(val)
		mask uint32 = 0xFFFFF
		b    byte
		res  []byte = bits.Bytes()
	)

	// process 8 values per loop
	for n >= 8 {
		if BE.Uint32(buf[i:])>>12&mask == c { // a
			b |= 1
		}
		i += 2
		if BE.Uint32(buf[i:])>>8&mask == c { // b
			b |= 2
		}
		i += 2
		if BE.Uint32(buf[i:])>>4&mask == c { // c
			b |= 4
		}
		i += 2
		if BE.Uint32(buf[i:])&mask == c { // d
			b |= 8
		}
		i += 4
		if BE.Uint32(buf[i:])>>12&mask == c { // e
			b |= 0x10
		}
		i += 2
		if BE.Uint32(buf[i:])>>8&mask == c { // f
			b |= 0x20
		}
		i += 2
		if BE.Uint32(buf[i:])>>4&mask == c { // g
			b |= 0x40
		}
		i += 2
		if BE.Uint32(buf[i:])&mask == c { // h
			b |= 0x80
		}
		if b > 0 {
			res[k/8] = b
			b = 0
			bits.ResetCount()
		}
		i += 4
		n -= 8
		k += 8
	}

	// process tail (max 7 values left)
	if n > 0 {
		if u32be(buf[i:])>>12&mask == c { // a
			bits.Set(k)
		}
		i += 2
		n--
	}
	if n > 0 {
		if u32be(buf[i:])>>8&mask == c { // b
			bits.Set(k + 1)
		}
		i += 2
		n--
	}
	if n > 0 {
		if u32be(buf[i:])>>4&mask == c { // c
			bits.Set(k + 2)
		}
		i += 2
		n--
	}
	if n > 0 {
		if u32be(buf[i:])&mask == c { // d
			bits.Set(k + 3)
		}
		i += 4
		n--
	}
	if n > 0 {
		if u32be(buf[i:])>>12&mask == c { // e
			bits.Set(k + 4)
		}
		i += 2
		n--
	}
	if n > 0 {
		if u32be(buf[i:])>>8&mask == c { // f
			bits.Set(k + 5)
		}
		i += 2
		n--
	}
	if n > 0 {
		if u32be(buf[i:])>>4&mask == c { // g
			bits.Set(k + 6)
		}
	}
	return bits
}

func cmp_bp_21_eq(buf []byte, val uint64, n int, bits *Bitset) *Bitset {
	// 21bit packed
	// [aaaa aaaa] [aaaa aaaa] [aaaa abbb] [bbbb bbbb]
	// [bbbb bbbb] [bbcc cccc] [cccc cccc] [cccc cccd]
	// [dddd dddd] [dddd dddd] [dddd eeee] [eeee eeee]
	// [eeee eeee] [efff ffff] [ffff ffff] [ffff ffgg]
	// [gggg gggg] [gggg gggg] [gggh hhhh] [hhhh hhhh]
	// [hhhh hhhh]
	var (
		i    int
		k    int
		c    uint32 = uint32(val)
		mask uint32 = 0x1FFFFF
		b    byte
		res  []byte = bits.Bytes()
	)

	// process 8 values per loop
	for n >= 8 {
		if BE.Uint32(buf[i:])>>11&mask == c { // a
			b |= 1
		}
		i += 2
		if BE.Uint32(buf[i:])>>6&mask == c { // b
			b |= 2
		}
		i += 2
		if BE.Uint32(buf[i:])>>1&mask == c { // c
			b |= 4
		}
		i += 3
		if BE.Uint32(buf[i:])>>4&mask == c { // d
			b |= 8
		}
		i += 3
		if BE.Uint32(buf[i:])>>7&mask == c { // e
			b |= 0x10
		}
		i += 2
		if BE.Uint32(buf[i:])>>2&mask == c { // f
			b |= 0x20
		}
		i += 3
		if BE.Uint32(buf[i:])>>5&mask == c { // g
			b |= 0x40
		}
		i += 2
		if BE.Uint32(buf[i:])&mask == c { // h
			b |= 0x80
		}
		if b > 0 {
			res[k/8] = b
			b = 0
			bits.ResetCount()
		}
		i += 4
		n -= 8
		k += 8
	}

	// process tail (max 7 values left)
	if n > 0 {
		if u32be(buf[i:])>>11&mask == c { // a
			bits.Set(k)
		}
		i += 2
		n--
	}
	if n > 0 {
		if u32be(buf[i:])>>6&mask == c { // b
			bits.Set(k + 1)
		}
		i += 2
		n--
	}
	if n > 0 {
		if u32be(buf[i:])>>1&mask == c { // c
			bits.Set(k + 2)
		}
		i += 3
		n--
	}
	if n > 0 {
		if u32be(buf[i:])>>4&mask == c { // d
			bits.Set(k + 3)
		}
		i += 3
		n--
	}
	if n > 0 {
		if u32be(buf[i:])>>7&mask == c { // e
			bits.Set(k + 4)
		}
		i += 2
		n--
	}
	if n > 0 {
		if u32be(buf[i:])>>2&mask == c { // f
			bits.Set(k + 5)
		}
		i += 3
		n--
	}
	if n > 0 {
		if u32be(buf[i:])>>5&mask == c { // g
			bits.Set(k + 6)
		}
	}
	return bits
}

func cmp_bp_22_eq(buf []byte, val uint64, n int, bits *Bitset) *Bitset {
	// 22bit packed
	// [aaaa aaaa] [aaaa aaaa] [aaaa aabb] [bbbb bbbb]
	// [bbbb bbbb] [bbbb cccc] [cccc cccc] [cccc cccc]
	// [ccdd dddd] [dddd dddd] [dddd dddd] [eeee eeee]
	// [eeee eeee] [eeee eeff] [ffff ffff] [ffff ffff]
	// [ffff gggg] [gggg gggg] [gggg gggg] [gghh hhhh]
	// [hhhh hhhh] [hhhh hhhh]
	var (
		i    int
		k    int
		c    uint32 = uint32(val)
		mask uint32 = 0x3FFFFF
		b    byte
		res  []byte = bits.Bytes()
	)

	// process 8 values per loop
	for n >= 8 {
		if BE.Uint32(buf[i:])>>10&mask == c { // a
			b |= 1
		}
		i += 2
		if BE.Uint32(buf[i:])>>4&mask == c { // b
			b |= 2
		}
		i += 3
		if BE.Uint32(buf[i:])>>6&mask == c { // c
			b |= 4
		}
		i += 2
		if BE.Uint32(buf[i:])&mask == c { // d
			b |= 8
		}
		i += 4
		if BE.Uint32(buf[i:])>>10&mask == c { // e
			b |= 0x10
		}
		i += 2
		if BE.Uint32(buf[i:])>>4&mask == c { // f
			b |= 0x20
		}
		i += 3
		if BE.Uint32(buf[i:])>>6&mask == c { // g
			b |= 0x40
		}
		i += 2
		if BE.Uint32(buf[i:])&mask == c { // h
			b |= 0x80
		}
		if b > 0 {
			res[k/8] = b
			b = 0
			bits.ResetCount()
		}
		i += 4
		n -= 8
		k += 8
	}

	// process tail (max 7 values left)
	if n > 0 {
		if u32be(buf[i:])>>10&mask == c { // a
			bits.Set(k)
		}
		i += 2
		n--
	}
	if n > 0 {
		if u32be(buf[i:])>>4&mask == c { // b
			bits.Set(k + 1)
		}
		i += 3
		n--
	}
	if n > 0 {
		if u32be(buf[i:])>>6&mask == c { // c
			bits.Set(k + 2)
		}
		i += 2
		n--
	}
	if n > 0 {
		if u32be(buf[i:])&mask == c { // d
			bits.Set(k + 3)
		}
		i += 4
		n--
	}
	if n > 0 {
		if u32be(buf[i:])>>10&mask == c { // e
			bits.Set(k + 4)
		}
		i += 2
		n--
	}
	if n > 0 {
		if u32be(buf[i:])>>4&mask == c { // f
			bits.Set(k + 5)
		}
		i += 3
		n--
	}
	if n > 0 {
		if u32be(buf[i:])>>6&mask == c { // g
			bits.Set(k + 6)
		}
	}
	return bits
}

func cmp_bp_23_eq(buf []byte, val uint64, n int, bits *Bitset) *Bitset {
	// 23bit packed
	// [aaaa aaaa] [aaaa aaaa] [aaaa aaab] [bbbb bbbb]
	// [bbbb bbbb] [bbbb bbcc] [cccc cccc] [cccc cccc]
	// [cccc cddd] [dddd dddd] [dddd dddd] [dddd eeee]
	// [eeee eeee] [eeee eeee] [eeef ffff] [ffff ffff]
	// [ffff ffff] [ffgg gggg] [gggg gggg] [gggg gggg]
	// [ghhh hhhh] [hhhh hhhh] [hhhh hhhh]
	var (
		i    int
		k    int
		c    uint32 = uint32(val)
		mask uint32 = 0x7FFFFF
		b    byte
		res  []byte = bits.Bytes()
	)

	// process 8 values per loop
	for n >= 8 {
		if BE.Uint32(buf[i:])>>9&mask == c { // a
			b |= 1
		}
		i += 2
		if BE.Uint32(buf[i:])>>2&mask == c { // b
			b |= 2
		}
		i += 3
		if BE.Uint32(buf[i:])>>3&mask == c { // c
			b |= 4
		}
		i += 3
		if BE.Uint32(buf[i:])>>4&mask == c { // d
			b |= 8
		}
		i += 3
		if BE.Uint32(buf[i:])>>5&mask == c { // e
			b |= 0x10
		}
		i += 3
		if BE.Uint32(buf[i:])>>6&mask == c { // f
			b |= 0x20
		}
		i += 3
		if BE.Uint32(buf[i:])>>7&mask == c { // g
			b |= 0x40
		}
		i += 2
		if BE.Uint32(buf[i:])&mask == c { // h
			b |= 0x80
		}
		if b > 0 {
			res[k/8] = b
			b = 0
			bits.ResetCount()
		}
		i += 4
		n -= 8
		k += 8
	}

	// process tail (max 7 values left)
	if n > 0 {
		if u32be(buf[i:])>>9&mask == c { // a
			bits.Set(k)
		}
		i += 2
		n--
	}
	if n > 0 {
		if u32be(buf[i:])>>2&mask == c { // b
			bits.Set(k + 1)
		}
		i += 3
		n--
	}
	if n > 0 {
		if u32be(buf[i:])>>3&mask == c { // c
			bits.Set(k + 2)
		}
		i += 3
		n--
	}
	if n > 0 {
		if u32be(buf[i:])>>4&mask == c { // d
			bits.Set(k + 3)
		}
		i += 3
		n--
	}
	if n > 0 {
		if u32be(buf[i:])>>5&mask == c { // e
			bits.Set(k + 4)
		}
		i += 3
		n--
	}
	if n > 0 {
		if u32be(buf[i:])>>6&mask == c { // f
			bits.Set(k + 5)
		}
		i += 3
		n--
	}
	if n > 0 {
		if u32be(buf[i:])>>7&mask == c { // g
			bits.Set(k + 6)
		}
	}
	return bits
}

func cmp_bp_24_eq(buf []byte, val uint64, n int, bits *Bitset) *Bitset {
	// 24bit packed
	// [aaaa aaaa] [aaaa aaaa] [aaaa aaaa] [bbbb bbbb]
	// [bbbb bbbb] [bbbb bbbb] [cccc cccc] [cccc cccc]
	// [cccc cccc] [dddd dddd] [dddd dddd] [dddd dddd]
	// [eeee eeee] [eeee eeee] [eeee eeee] [ffff ffff]
	// [ffff ffff] [ffff ffff] [gggg gggg] [gggg gggg]
	// [gggg gggg] [hhhh hhhh] [hhhh hhhh] [hhhh hhhh]
	var (
		i    int
		k    int
		c    uint32 = uint32(val)
		mask uint32 = 0xFFFFFF
		b    byte
		res  []byte = bits.Bytes()
	)

	// process 8 values per loop
	for n >= 8 {
		if BE.Uint32(buf[i:])>>8&mask == c { // a
			b |= 1
		}
		i += 2
		if BE.Uint32(buf[i:])&mask == c { // b
			b |= 2
		}
		i += 4
		if BE.Uint32(buf[i:])>>8&mask == c { // c
			b |= 4
		}
		i += 2
		if BE.Uint32(buf[i:])&mask == c { // d
			b |= 8
		}
		i += 4
		if BE.Uint32(buf[i:])>>8&mask == c { // e
			b |= 0x10
		}
		i += 2
		if BE.Uint32(buf[i:])&mask == c { // f
			b |= 0x20
		}
		i += 4
		if BE.Uint32(buf[i:])>>8&mask == c { // g
			b |= 0x40
		}
		i += 2
		if BE.Uint32(buf[i:])&mask == c { // h
			b |= 0x80
		}
		if b > 0 {
			res[k/8] = b
			b = 0
			bits.ResetCount()
		}
		i += 4
		n -= 8
		k += 8
	}

	// process tail (max 7 values left)
	if n > 0 {
		if u32be(buf[i:])>>8&mask == c { // a
			bits.Set(k)
		}
		i += 2
		n--
	}
	if n > 0 {
		if u32be(buf[i:])&mask == c { // b
			bits.Set(k + 1)
		}
		i += 4
		n--
	}
	if n > 0 {
		if u32be(buf[i:])>>8&mask == c { // c
			bits.Set(k + 2)
		}
		i += 2
		n--
	}
	if n > 0 {
		if u32be(buf[i:])&mask == c { // d
			bits.Set(k + 3)
		}
		i += 4
		n--
	}
	if n > 0 {
		if u32be(buf[i:])>>8&mask == c { // e
			bits.Set(k + 4)
		}
		i += 2
		n--
	}
	if n > 0 {
		if u32be(buf[i:])&mask == c { // f
			bits.Set(k + 5)
		}
		i += 4
		n--
	}
	if n > 0 {
		if u32be(buf[i:])>>8&mask == c { // g
			bits.Set(k + 6)
		}
	}
	return bits
}

func cmp_bp_25_eq(buf []byte, val uint64, n int, bits *Bitset) *Bitset {
	// 25bit packed
	// [aaaa aaaa] [aaaa aaaa] [aaaa aaaa] [abbb bbbb]
	// [bbbb bbbb] [bbbb bbbb] [bbcc cccc] [cccc cccc]
	// [cccc cccc] [cccd dddd] [dddd dddd] [dddd dddd]
	// [dddd eeee] [eeee eeee] [eeee eeee] [eeee efff]
	// [ffff ffff] [ffff ffff] [ffff ffgg] [gggg gggg]
	// [gggg gggg] [gggg gggh] [hhhh hhhh] [hhhh hhhh]
	// [hhhh hhhh]
	var (
		i    int
		k    int
		c    uint32 = uint32(val)
		mask uint32 = 0x1FFFFFF
		b    byte
		res  []byte = bits.Bytes()
	)

	// process 8 values per loop
	for n >= 8 {
		if BE.Uint32(buf[i:])>>7&mask == c { // a
			b |= 1
		}
		i += 3
		if BE.Uint32(buf[i:])>>6&mask == c { // b
			b |= 2
		}
		i += 3
		if BE.Uint32(buf[i:])>>5&mask == c { // c
			b |= 4
		}
		i += 3
		if BE.Uint32(buf[i:])>>4&mask == c { // d
			b |= 8
		}
		i += 3
		if BE.Uint32(buf[i:])>>3&mask == c { // e
			b |= 0x10
		}
		i += 3
		if BE.Uint32(buf[i:])>>2&mask == c { // f
			b |= 0x20
		}
		i += 3
		if BE.Uint32(buf[i:])>>1&mask == c { // g
			b |= 0x40
		}
		i += 3
		if BE.Uint32(buf[i:])&mask == c { // h
			b |= 0x80
		}
		if b > 0 {
			res[k/8] = b
			b = 0
			bits.ResetCount()
		}
		i += 4
		n -= 8
		k += 8
	}

	// process tail (max 7 values left)
	if n > 0 {
		if BE.Uint32(buf[i:])>>7&mask == c { // a
			bits.Set(k)
		}
		i += 3
		n--
	}
	if n > 0 {
		if BE.Uint32(buf[i:])>>6&mask == c { // b
			bits.Set(k + 1)
		}
		i += 3
		n--
	}
	if n > 0 {
		if BE.Uint32(buf[i:])>>5&mask == c { // c
			bits.Set(k + 2)
		}
		i += 3
		n--
	}
	if n > 0 {
		if BE.Uint32(buf[i:])>>4&mask == c { // d
			bits.Set(k + 3)
		}
		i += 3
		n--
	}
	if n > 0 {
		if BE.Uint32(buf[i:])>>3&mask == c { // e
			bits.Set(k + 4)
		}
		i += 3
		n--
	}
	if n > 0 {
		if BE.Uint32(buf[i:])>>2&mask == c { // f
			bits.Set(k + 5)
		}
		i += 3
		n--
	}
	if n > 0 {
		if BE.Uint32(buf[i:])>>1&mask == c { // g
			bits.Set(k + 6)
		}
	}
	return bits
}

func cmp_bp_26_eq(buf []byte, val uint64, n int, bits *Bitset) *Bitset {
	// 26 bit packed
	// [aaaa aaaa] [aaaa aaaa] [aaaa aaaa] [aabb bbbb]
	// [bbbb bbbb] [bbbb bbbb] [bbbb cccc] [cccc cccc]
	// [cccc cccc] [cccc ccdd] [dddd dddd] [dddd dddd]
	// [dddd dddd] [eeee eeee] [eeee eeee] [eeee eeee]
	// [eeff ffff] [ffff ffff] [ffff ffff] [ffff gggg]
	// [gggg gggg] [gggg gggg] [gggg gghh] [hhhh hhhh]
	// [hhhh hhhh] [hhhh hhhh]
	var (
		i    int
		k    int
		c    uint32 = uint32(val)
		mask uint32 = 0x3FFFFFF
		b    byte
		res  []byte = bits.Bytes()
	)

	// process 8 values per loop
	for n >= 8 {
		if BE.Uint32(buf[i:])>>6&mask == c { // a
			b |= 1
		}
		i += 3
		if BE.Uint32(buf[i:])>>4&mask == c { // b
			b |= 2
		}
		i += 3
		if BE.Uint32(buf[i:])>>2&mask == c { // c
			b |= 4
		}
		i += 3
		if BE.Uint32(buf[i:])&mask == c { // d
			b |= 8
		}
		i += 4
		if BE.Uint32(buf[i:])>>6&mask == c { // e
			b |= 0x10
		}
		i += 3
		if BE.Uint32(buf[i:])>>4&mask == c { // f
			b |= 0x20
		}
		i += 3
		if BE.Uint32(buf[i:])>>2&mask == c { // g
			b |= 0x40
		}
		i += 3
		if BE.Uint32(buf[i:])&mask == c { // h
			b |= 0x80
		}
		if b > 0 {
			res[k/8] = b
			b = 0
			bits.ResetCount()
		}
		i += 4
		n -= 8
		k += 8
	}

	// process tail (max 7 values left)
	if n > 0 {
		if BE.Uint32(buf[i:])>>6&mask == c { // a
			bits.Set(k)
		}
		i += 3
		n--
	}
	if n > 0 {
		if BE.Uint32(buf[i:])>>4&mask == c { // b
			bits.Set(k + 1)
		}
		i += 3
		n--
	}
	if n > 0 {
		if BE.Uint32(buf[i:])>>2&mask == c { // c
			bits.Set(k + 2)
		}
		i += 3
		n--
	}
	if n > 0 {
		if BE.Uint32(buf[i:])&mask == c { // d
			bits.Set(k + 3)
		}
		i += 4
		n--
	}
	if n > 0 {
		if BE.Uint32(buf[i:])>>6&mask == c { // e
			bits.Set(k + 4)
		}
		i += 3
		n--
	}
	if n > 0 {
		if BE.Uint32(buf[i:])>>4&mask == c { // f
			bits.Set(k + 5)
		}
		i += 3
		n--
	}
	if n > 0 {
		if BE.Uint32(buf[i:])>>2&mask == c { // g
			bits.Set(k + 6)
		}
	}
	return bits
}

func cmp_bp_27_eq(buf []byte, val uint64, n int, bits *Bitset) *Bitset {
	// 27 bit packed
	// [aaaa aaaa] [aaaa aaaa] [aaaa aaaa] [aaab bbbb]
	// [bbbb bbbb] [bbbb bbbb] [bbbb bbcc] [cccc cccc]
	// [cccc cccc] [cccc cccc] [cddd dddd] [dddd dddd]
	// [dddd dddd] [dddd eeee] [eeee eeee] [eeee eeee]
	// [eeee eeef] [ffff ffff] [ffff ffff] [ffff ffff]
	// [ffgg gggg] [gggg gggg] [gggg gggg] [gggg ghhh]
	// [hhhh hhhh] [hhhh hhhh] [hhhh hhhh]
	var (
		i    int
		k    int
		c    uint64 = val
		mask uint64 = 0x7FFFFFF
		b    byte
		res  []byte = bits.Bytes()
	)

	// process 8 values per loop
	for n >= 8 {
		if BE.Uint64(buf[i:])>>37&mask == c { // a
			b |= 1
		}
		i += 3
		if BE.Uint64(buf[i:])>>34&mask == c { // b
			b |= 2
		}
		i += 3
		if BE.Uint64(buf[i:])>>31&mask == c { // c
			b |= 4
		}
		i += 4
		if BE.Uint64(buf[i:])>>36&mask == c { // d
			b |= 8
		}
		i += 3
		if BE.Uint64(buf[i:])>>33&mask == c { // e
			b |= 0x10
		}
		i += 3
		if BE.Uint64(buf[i:])>>30&mask == c { // f
			b |= 0x20
		}
		i += 3
		if BE.Uint64(buf[i:])>>27&mask == c { // g
			b |= 0x40
		}
		if BE.Uint64(buf[i:])&mask == c { // h
			b |= 0x80
		}
		if b > 0 {
			res[k/8] = b
			b = 0
			bits.ResetCount()
		}
		i += 8
		n -= 8
		k += 8
	}

	// process tail (max 7 values left)
	if n > 0 {
		if u64be(buf[i:])>>37&mask == c { // a
			bits.Set(k)
		}
		i += 3
		n--
	}
	if n > 0 {
		if u64be(buf[i:])>>34&mask == c { // b
			bits.Set(k + 1)
		}
		i += 3
		n--
	}
	if n > 0 {
		if u64be(buf[i:])>>31&mask == c { // c
			bits.Set(k + 2)
		}
		i += 4
		n--
	}
	if n > 0 {
		if u64be(buf[i:])>>36&mask == c { // d
			bits.Set(k + 3)
		}
		i += 3
		n--
	}
	if n > 0 {
		if u64be(buf[i:])>>33&mask == c { // e
			bits.Set(k + 4)
		}
		i += 3
		n--
	}
	if n > 0 {
		if u64be(buf[i:])>>30&mask == c { // f
			bits.Set(k + 5)
		}
		i += 3
		n--
	}
	if n > 0 {
		if u64be(buf[i:])>>27&mask == c { // g
			bits.Set(k + 6)
		}
	}
	return bits
}

func cmp_bp_28_eq(buf []byte, val uint64, n int, bits *Bitset) *Bitset {
	// 28 bit packed
	// [aaaaaaaa] [aaaaaaaa] [aaaaaaaa] [aaaabbbb]
	// [bbbbbbbb] [bbbbbbbb] [bbbbbbbb] [cccccccc]
	// [cccccccc] [cccccccc] [ccccdddd] [dddddddd]
	// [dddddddd] [dddddddd] [eeeeeeee] [eeeeeeee]
	// [eeeeeeee] [eeeeffff] [ffffffff] [ffffffff]
	// [ffffffff] [gggggggg] [gggggggg] [gggggggg]
	// [gggghhhh] [hhhhhhhh] [hhhhhhhh] [hhhhhhhh]
	//
	var (
		i    int
		k    int
		c    uint32 = uint32(val)
		mask uint32 = 0xFFFFFFF
		b    byte
		res  []byte = bits.Bytes()
	)

	// process 8 values per loop
	for n >= 8 {
		if BE.Uint32(buf[i:])>>4&mask == c { // a
			b |= 1
		}
		i += 3
		if BE.Uint32(buf[i:])&mask == c { // b
			b |= 2
		}
		i += 4
		if BE.Uint32(buf[i:])>>4&mask == c { // c
			b |= 4
		}
		i += 3
		if BE.Uint32(buf[i:])&mask == c { // d
			b |= 8
		}
		i += 4
		if BE.Uint32(buf[i:])>>4&mask == c { // e
			b |= 0x10
		}
		i += 3
		if BE.Uint32(buf[i:])&mask == c { // f
			b |= 0x20
		}
		i += 4
		if BE.Uint32(buf[i:])>>4&mask == c { // g
			b |= 0x40
		}
		i += 3
		if BE.Uint32(buf[i:])&mask == c { // h
			b |= 0x80
		}
		if b > 0 {
			res[k/8] = b
			b = 0
			bits.ResetCount()
		}
		i += 4
		n -= 8
		k += 8
	}

	// process tail (max 7 values left)
	if n > 0 {
		if BE.Uint32(buf[i:])>>4&mask == c { // a
			bits.Set(k)
		}
		i += 3
		n--
	}
	if n > 0 {
		if BE.Uint32(buf[i:])&mask == c { // b
			bits.Set(k + 1)
		}
		i += 4
		n--
	}
	if n > 0 {
		if BE.Uint32(buf[i:])>>4&mask == c { // c
			bits.Set(k + 2)
		}
		i += 3
		n--
	}
	if n > 0 {
		if BE.Uint32(buf[i:])&mask == c { // d
			bits.Set(k + 3)
		}
		i += 4
		n--
	}
	if n > 0 {
		if BE.Uint32(buf[i:])>>4&mask == c { // e
			bits.Set(k + 4)
		}
		i += 3
		n--
	}
	if n > 0 {
		if BE.Uint32(buf[i:])&mask == c { // f
			bits.Set(k + 5)
		}
		i += 4
		n--
	}
	if n > 0 {
		if BE.Uint32(buf[i:])>>4&mask == c { // g
			bits.Set(k + 6)
		}
	}
	return bits
}

func cmp_bp_29_eq(buf []byte, val uint64, n int, bits *Bitset) *Bitset {
	// 29 bit packed
	// [aaaaaaaa] [aaaaaaaa] [aaaaaaaa] [aaaaabbb]
	// [bbbbbbbb] [bbbbbbbb] [bbbbbbbb] [bbcccccc]
	// [cccccccc] [cccccccc] [cccccccd] [dddddddd]
	// [dddddddd] [dddddddd] [ddddeeee] [eeeeeeee]
	// [eeeeeeee] [eeeeeeee] [efffffff] [ffffffff]
	// [ffffffff] [ffffffgg] [gggggggg] [gggggggg]
	// [gggggggg] [ggghhhhh] [hhhhhhhh] [hhhhhhhh]
	// [hhhhhhhh]
	var (
		i    int
		k    int
		c    uint64 = val
		mask uint64 = 0x1FFFFFFF
		b    byte
		res  []byte = bits.Bytes()
	)

	// process 8 values per loop
	for n >= 8 {
		if uint64(BE.Uint32(buf[i:]))>>3&mask == c { // a
			b |= 1
		}
		i += 3
		if BE.Uint64(buf[i:])>>30&mask == c { // b
			b |= 2
		}
		i += 4
		if uint64(BE.Uint32(buf[i:]))>>1&mask == c { // c
			b |= 4
		}
		i += 3
		if BE.Uint64(buf[i:])>>28&mask == c { // d
			b |= 8
		}
		i += 4
		if BE.Uint64(buf[i:])>>31&mask == c { // e
			b |= 0x10
		}
		i += 4
		if uint64(BE.Uint32(buf[i:]))>>2&mask == c { // f
			b |= 0x20
		}
		i += 3
		if BE.Uint64(buf[i:])>>29&mask == c { // g
			b |= 0x40
		}
		i += 4
		if uint64(BE.Uint32(buf[i:]))&mask == c { // h
			b |= 0x80
		}
		if b > 0 {
			res[k/8] = b
			b = 0
			bits.ResetCount()
		}
		i += 4
		n -= 8
		k += 8
	}

	// process tail (max 7 values left)
	if n > 0 {
		if uint64(BE.Uint32(buf[i:]))>>3&mask == c { // a
			bits.Set(k)
		}
		i += 3
		n--
	}
	if n > 0 {
		if u64be(buf[i:])>>30&mask == c { // b
			bits.Set(k + 1)
		}
		i += 4
		n--
	}
	if n > 0 {
		if uint64(BE.Uint32(buf[i:]))>>1&mask == c { // c
			bits.Set(k + 2)
		}
		i += 3
		n--
	}
	if n > 0 {
		if u64be(buf[i:])>>28&mask == c { // d
			bits.Set(k + 3)
		}
		i += 4
		n--
	}
	if n > 0 {
		if u64be(buf[i:])>>31&mask == c { // e
			bits.Set(k + 4)
		}
		i += 4
		n--
	}
	if n > 0 {
		if uint64(BE.Uint32(buf[i:]))>>2&mask == c { // f
			bits.Set(k + 5)
		}
		i += 3
		n--
	}
	if n > 0 {
		if u64be(buf[i:])>>29&mask == c { // g
			bits.Set(k + 6)
		}
	}
	return bits
}

func cmp_bp_30_eq(buf []byte, val uint64, n int, bits *Bitset) *Bitset {
	// 30 bit packed
	// [aaaaaaaa] [aaaaaaaa] [aaaaaaaa] [aaaaaabb]
	// [bbbbbbbb] [bbbbbbbb] [bbbbbbbb] [bbbbcccc]
	// [cccccccc] [cccccccc] [cccccccc] [ccdddddd]
	// [dddddddd] [dddddddd] [dddddddd] [eeeeeeee]
	// [eeeeeeee] [eeeeeeee] [eeeeeeff] [ffffffff]
	// [ffffffff] [ffffffff] [ffffgggg] [gggggggg]
	// [gggggggg] [gggggggg] [gghhhhhh] [hhhhhhhh]
	// [hhhhhhhh] [hhhhhhhh]
	var (
		i    int
		k    int
		c    uint64 = val
		mask uint64 = 0x3FFFFFFF
		b    byte
		res  []byte = bits.Bytes()
	)

	// process 8 values per loop
	for n >= 8 {
		if uint64(BE.Uint32(buf[i:]))>>2&mask == c { // a
			b |= 1
		}
		i += 3
		if BE.Uint64(buf[i:])>>28&mask == c { // b
			b |= 2
		}
		i += 4
		if BE.Uint64(buf[i:])>>30&mask == c { // c
			b |= 4
		}
		i += 4
		if uint64(BE.Uint32(buf[i:]))&mask == c { // d
			b |= 8
		}
		i += 4
		if uint64(BE.Uint32(buf[i:]))>>2&mask == c { // e
			b |= 0x10
		}
		i += 3
		if BE.Uint64(buf[i:])>>28&mask == c { // f
			b |= 0x20
		}
		i += 4
		if BE.Uint64(buf[i:])>>30&mask == c { // g
			b |= 0x40
		}
		i += 4
		if uint64(BE.Uint32(buf[i:]))&mask == c { // h
			b |= 0x80
		}
		if b > 0 {
			res[k/8] = b
			b = 0
			bits.ResetCount()
		}
		i += 4
		n -= 8
		k += 8
	}

	// process tail (max 7 values left)
	if n > 0 {
		if uint64(BE.Uint32(buf[i:]))>>2&mask == c { // a
			bits.Set(k)
		}
		i += 3
		n--
	}
	if n > 0 {
		if u64be(buf[i:])>>28&mask == c { // b
			bits.Set(k + 1)
		}
		i += 4
		n--
	}
	if n > 0 {
		if u64be(buf[i:])>>30&mask == c { // c
			bits.Set(k + 2)
		}
		i += 4
		n--
	}
	if n > 0 {
		if uint64(BE.Uint32(buf[i:]))&mask == c { // d
			bits.Set(k + 3)
		}
		i += 4
		n--
	}
	if n > 0 {
		if uint64(BE.Uint32(buf[i:]))>>2&mask == c { // e
			bits.Set(k + 4)
		}
		i += 3
		n--
	}
	if n > 0 {
		if u64be(buf[i:])>>28&mask == c { // f
			bits.Set(k + 5)
		}
		i += 4
		n--
	}
	if n > 0 {
		if u64be(buf[i:])>>30&mask == c { // g
			bits.Set(k + 6)
		}
	}
	return bits
}

func cmp_bp_31_eq(buf []byte, val uint64, n int, bits *Bitset) *Bitset {
	// 31 bit packed
	// [aaaa aaaa] [aaaa aaaa] [aaaa aaaa] [aaaa aaab]
	// [bbbb bbbb] [bbbb bbbb] [bbbb bbbb] [bbbb bbcc]
	// [cccc cccc] [cccc cccc] [cccc cccc] [cccc cddd]
	// [dddd dddd] [dddd dddd] [dddd dddd] [dddd eeee]
	// [eeee eeee] [eeee eeee] [eeee eeee] [eeef ffff]
	// [ffff ffff] [ffff ffff] [ffff ffff] [ffgg gggg]
	// [gggg gggg] [gggg gggg] [gggg gggg] [ghhh hhhh]
	// [hhhh hhhh] [hhhh hhhh] [hhhh hhhh]
	var (
		i    int
		k    int
		c    uint64 = val
		mask uint64 = 0x7FFFFFFF
		b    byte
		res  []byte = bits.Bytes()
	)

	// process 8 values per loop
	for n >= 8 {
		if uint64(BE.Uint32(buf[i:]))>>1&mask == c { // a
			b |= 1
		}
		i += 3
		if BE.Uint64(buf[i:])>>26&mask == c { // b
			b |= 2
		}
		i += 4
		if BE.Uint64(buf[i:])>>27&mask == c { // c
			b |= 4
		}
		i += 4
		if BE.Uint64(buf[i:])>>28&mask == c { // d
			b |= 8
		}
		i += 4
		if BE.Uint64(buf[i:])>>29&mask == c { // e
			b |= 0x10
		}
		i += 4
		if BE.Uint64(buf[i:])>>30&mask == c { // f
			b |= 0x20
		}
		i += 4
		if BE.Uint64(buf[i:])>>31&mask == c { // g
			b |= 0x40
		}
		i += 4
		if uint64(BE.Uint32(buf[i:]))&mask == c { // h
			b |= 0x80
		}
		if b > 0 {
			res[k/8] = b
			b = 0
			bits.ResetCount()
		}
		i += 4
		n -= 8
		k += 8
	}

	// process tail (max 7 values left)
	if n > 0 {
		if uint64(BE.Uint32(buf[i:]))>>1&mask == c { // a
			bits.Set(k)
		}
		i += 3
		n--
	}
	if n > 0 {
		if u64be(buf[i:])>>26&mask == c { // b
			bits.Set(k + 1)
		}
		i += 4
		n--
	}
	if n > 0 {
		if u64be(buf[i:])>>27&mask == c { // c
			bits.Set(k + 2)
		}
		i += 4
		n--
	}
	if n > 0 {
		if u64be(buf[i:])>>28&mask == c { // d
			bits.Set(k + 3)
		}
		i += 4
		n--
	}
	if n > 0 {
		if u64be(buf[i:])>>29&mask == c { // e
			bits.Set(k + 4)
		}
		i += 4
		n--
	}
	if n > 0 {
		if u64be(buf[i:])>>30&mask == c { // f
			bits.Set(k + 5)
		}
		i += 4
		n--
	}
	if n > 0 {
		if u64be(buf[i:])>>31&mask == c { // g
			bits.Set(k + 6)
		}
	}
	return bits
}

func cmp_bp_32_eq(buf []byte, val uint64, n int, bits *Bitset) *Bitset {
	// 32 bit packed
	// [aaaaaaaa] [aaaaaaaa] [aaaaaaaa] [aaaaaaaa]
	// [bbbbbbbb] [bbbbbbbb] [bbbbbbbb] [bbbbbbbb]
	// [cccccccc] [cccccccc] [cccccccc] [cccccccc]
	// [dddddddd] [dddddddd] [dddddddd] [dddddddd]
	// [eeeeeeee] [eeeeeeee] [eeeeeeee] [eeeeeeee]
	// [ffffffff] [ffffffff] [ffffffff] [ffffffff]
	// [gggggggg] [gggggggg] [gggggggg] [gggggggg]
	// [hhhhhhhh] [hhhhhhhh] [hhhhhhhh] [hhhhhhhh]
	//
	return bits
}

func cmp_bp_33_eq(buf []byte, val uint64, n int, bits *Bitset) *Bitset {
	// 33 bit packed
	// [aaaaaaaa] [aaaaaaaa] [aaaaaaaa] [aaaaaaaa]
	// [abbbbbbb] [bbbbbbbb] [bbbbbbbb] [bbbbbbbb]
	// [bbcccccc] [cccccccc] [cccccccc] [cccccccc]
	// [cccddddd] [dddddddd] [dddddddd] [dddddddd]
	// [ddddeeee] [eeeeeeee] [eeeeeeee] [eeeeeeee]
	// [eeeeefff] [ffffffff] [ffffffff] [ffffffff]
	// [ffffffgg] [gggggggg] [gggggggg] [gggggggg]
	// [gggggggh] [hhhhhhhh] [hhhhhhhh] [hhhhhhhh]
	// [hhhhhhhh]
	return bits
}

func cmp_bp_34_eq(buf []byte, val uint64, n int, bits *Bitset) *Bitset {
	// 34 bit packed
	// [aaaaaaaa] [aaaaaaaa] [aaaaaaaa] [aaaaaaaa]
	// [aabbbbbb] [bbbbbbbb] [bbbbbbbb] [bbbbbbbb]
	// [bbbbcccc] [cccccccc] [cccccccc] [cccccccc]
	// [ccccccdd] [dddddddd] [dddddddd] [dddddddd]
	// [dddddddd] [eeeeeeee] [eeeeeeee] [eeeeeeee]
	// [eeeeeeee] [eeffffff] [ffffffff] [ffffffff]
	// [ffffffff] [ffffgggg] [gggggggg] [gggggggg]
	// [gggggggg] [gggggghh] [hhhhhhhh] [hhhhhhhh]
	// [hhhhhhhh] [hhhhhhhh]
	return bits
}

func cmp_bp_35_eq(buf []byte, val uint64, n int, bits *Bitset) *Bitset {
	// 35 bit packed
	// [aaaaaaaa] [aaaaaaaa] [aaaaaaaa] [aaaaaaaa]
	// [aaabbbbb] [bbbbbbbb] [bbbbbbbb] [bbbbbbbb]
	// [bbbbbbcc] [cccccccc] [cccccccc] [cccccccc]
	// [cccccccc] [cddddddd] [dddddddd] [dddddddd]
	// [dddddddd] [ddddeeee] [eeeeeeee] [eeeeeeee]
	// [eeeeeeee] [eeeeeeef] [ffffffff] [ffffffff]
	// [ffffffff] [ffffffff] [ffgggggg] [gggggggg]
	// [gggggggg] [gggggggg] [ggggghhh] [hhhhhhhh]
	// [hhhhhhhh] [hhhhhhhh] [hhhhhhhh]
	return bits
}

func cmp_bp_36_eq(buf []byte, val uint64, n int, bits *Bitset) *Bitset {
	// 36 bit packed
	// [aaaaaaaa] [aaaaaaaa] [aaaaaaaa] [aaaaaaaa]
	// [aaaabbbb] [bbbbbbbb] [bbbbbbbb] [bbbbbbbb]
	// [bbbbbbbb] [cccccccc] [cccccccc] [cccccccc]
	// [cccccccc] [ccccdddd] [dddddddd] [dddddddd]
	// [dddddddd] [dddddddd] [eeeeeeee] [eeeeeeee]
	// [eeeeeeee] [eeeeeeee] [eeeeffff] [ffffffff]
	// [ffffffff] [ffffffff] [ffffffff] [gggggggg]
	// [gggggggg] [gggggggg] [gggggggg] [gggghhhh]
	// [hhhhhhhh] [hhhhhhhh] [hhhhhhhh] [hhhhhhhh]
	//
	return bits
}

func cmp_bp_37_eq(buf []byte, val uint64, n int, bits *Bitset) *Bitset {
	// 37 bit packed
	// [aaaaaaaa] [aaaaaaaa] [aaaaaaaa] [aaaaaaaa]
	// [aaaaabbb] [bbbbbbbb] [bbbbbbbb] [bbbbbbbb]
	// [bbbbbbbb] [bbcccccc] [cccccccc] [cccccccc]
	// [cccccccc] [cccccccd] [dddddddd] [dddddddd]
	// [dddddddd] [dddddddd] [ddddeeee] [eeeeeeee]
	// [eeeeeeee] [eeeeeeee] [eeeeeeee] [efffffff]
	// [ffffffff] [ffffffff] [ffffffff] [ffffffgg]
	// [gggggggg] [gggggggg] [gggggggg] [gggggggg]
	// [ggghhhhh] [hhhhhhhh] [hhhhhhhh] [hhhhhhhh]
	// [hhhhhhhh]
	return bits
}

func cmp_bp_38_eq(buf []byte, val uint64, n int, bits *Bitset) *Bitset {
	// 38 bit packed
	// [aaaaaaaa] [aaaaaaaa] [aaaaaaaa] [aaaaaaaa]
	// [aaaaaabb] [bbbbbbbb] [bbbbbbbb] [bbbbbbbb]
	// [bbbbbbbb] [bbbbcccc] [cccccccc] [cccccccc]
	// [cccccccc] [cccccccc] [ccdddddd] [dddddddd]
	// [dddddddd] [dddddddd] [dddddddd] [eeeeeeee]
	// [eeeeeeee] [eeeeeeee] [eeeeeeee] [eeeeeeff]
	// [ffffffff] [ffffffff] [ffffffff] [ffffffff]
	// [ffffgggg] [gggggggg] [gggggggg] [gggggggg]
	// [gggggggg] [gghhhhhh] [hhhhhhhh] [hhhhhhhh]
	// [hhhhhhhh] [hhhhhhhh]
	return bits
}

func cmp_bp_39_eq(buf []byte, val uint64, n int, bits *Bitset) *Bitset {
	// 39 bit packed
	// [aaaaaaaa] [aaaaaaaa] [aaaaaaaa] [aaaaaaaa]
	// [aaaaaaab] [bbbbbbbb] [bbbbbbbb] [bbbbbbbb]
	// [bbbbbbbb] [bbbbbbcc] [cccccccc] [cccccccc]
	// [cccccccc] [cccccccc] [cccccddd] [dddddddd]
	// [dddddddd] [dddddddd] [dddddddd] [ddddeeee]
	// [eeeeeeee] [eeeeeeee] [eeeeeeee] [eeeeeeee]
	// [eeefffff] [ffffffff] [ffffffff] [ffffffff]
	// [ffffffff] [ffgggggg] [gggggggg] [gggggggg]
	// [gggggggg] [gggggggg] [ghhhhhhh] [hhhhhhhh]
	// [hhhhhhhh] [hhhhhhhh] [hhhhhhhh]
	return bits
}

func cmp_bp_40_eq(buf []byte, val uint64, n int, bits *Bitset) *Bitset {
	// 40 bit packed
	// [aaaaaaaa] [aaaaaaaa] [aaaaaaaa] [aaaaaaaa]
	// [aaaaaaaa] [bbbbbbbb] [bbbbbbbb] [bbbbbbbb]
	// [bbbbbbbb] [bbbbbbbb] [cccccccc] [cccccccc]
	// [cccccccc] [cccccccc] [cccccccc] [dddddddd]
	// [dddddddd] [dddddddd] [dddddddd] [dddddddd]
	// [eeeeeeee] [eeeeeeee] [eeeeeeee] [eeeeeeee]
	// [eeeeeeee] [ffffffff] [ffffffff] [ffffffff]
	// [ffffffff] [ffffffff] [gggggggg] [gggggggg]
	// [gggggggg] [gggggggg] [gggggggg] [hhhhhhhh]
	// [hhhhhhhh] [hhhhhhhh] [hhhhhhhh] [hhhhhhhh]
	//
	return bits
}

func cmp_bp_41_eq(buf []byte, val uint64, n int, bits *Bitset) *Bitset {
	// 41 bit packed
	// [aaaaaaaa] [aaaaaaaa] [aaaaaaaa] [aaaaaaaa]
	// [aaaaaaaa] [abbbbbbb] [bbbbbbbb] [bbbbbbbb]
	// [bbbbbbbb] [bbbbbbbb] [bbcccccc] [cccccccc]
	// [cccccccc] [cccccccc] [cccccccc] [cccddddd]
	// [dddddddd] [dddddddd] [dddddddd] [dddddddd]
	// [ddddeeee] [eeeeeeee] [eeeeeeee] [eeeeeeee]
	// [eeeeeeee] [eeeeefff] [ffffffff] [ffffffff]
	// [ffffffff] [ffffffff] [ffffffgg] [gggggggg]
	// [gggggggg] [gggggggg] [gggggggg] [gggggggh]
	// [hhhhhhhh] [hhhhhhhh] [hhhhhhhh] [hhhhhhhh]
	// [hhhhhhhh]
	return bits
}

func cmp_bp_42_eq(buf []byte, val uint64, n int, bits *Bitset) *Bitset {
	// 42 bit packed
	// [aaaaaaaa] [aaaaaaaa] [aaaaaaaa] [aaaaaaaa]
	// [aaaaaaaa] [aabbbbbb] [bbbbbbbb] [bbbbbbbb]
	// [bbbbbbbb] [bbbbbbbb] [bbbbcccc] [cccccccc]
	// [cccccccc] [cccccccc] [cccccccc] [ccccccdd]
	// [dddddddd] [dddddddd] [dddddddd] [dddddddd]
	// [dddddddd] [eeeeeeee] [eeeeeeee] [eeeeeeee]
	// [eeeeeeee] [eeeeeeee] [eeffffff] [ffffffff]
	// [ffffffff] [ffffffff] [ffffffff] [ffffgggg]
	// [gggggggg] [gggggggg] [gggggggg] [gggggggg]
	// [gggggghh] [hhhhhhhh] [hhhhhhhh] [hhhhhhhh]
	// [hhhhhhhh] [hhhhhhhh]
	return bits
}

func cmp_bp_43_eq(buf []byte, val uint64, n int, bits *Bitset) *Bitset {
	// 43 bit packed
	// [aaaaaaaa] [aaaaaaaa] [aaaaaaaa] [aaaaaaaa]
	// [aaaaaaaa] [aaabbbbb] [bbbbbbbb] [bbbbbbbb]
	// [bbbbbbbb] [bbbbbbbb] [bbbbbbcc] [cccccccc]
	// [cccccccc] [cccccccc] [cccccccc] [cccccccc]
	// [cddddddd] [dddddddd] [dddddddd] [dddddddd]
	// [dddddddd] [ddddeeee] [eeeeeeee] [eeeeeeee]
	// [eeeeeeee] [eeeeeeee] [eeeeeeef] [ffffffff]
	// [ffffffff] [ffffffff] [ffffffff] [ffffffff]
	// [ffgggggg] [gggggggg] [gggggggg] [gggggggg]
	// [gggggggg] [ggggghhh] [hhhhhhhh] [hhhhhhhh]
	// [hhhhhhhh] [hhhhhhhh] [hhhhhhhh]
	return bits
}

func cmp_bp_44_eq(buf []byte, val uint64, n int, bits *Bitset) *Bitset {
	// 44 bit packed
	// [aaaaaaaa] [aaaaaaaa] [aaaaaaaa] [aaaaaaaa]
	// [aaaaaaaa] [aaaabbbb] [bbbbbbbb] [bbbbbbbb]
	// [bbbbbbbb] [bbbbbbbb] [bbbbbbbb] [cccccccc]
	// [cccccccc] [cccccccc] [cccccccc] [cccccccc]
	// [ccccdddd] [dddddddd] [dddddddd] [dddddddd]
	// [dddddddd] [dddddddd] [eeeeeeee] [eeeeeeee]
	// [eeeeeeee] [eeeeeeee] [eeeeeeee] [eeeeffff]
	// [ffffffff] [ffffffff] [ffffffff] [ffffffff]
	// [ffffffff] [gggggggg] [gggggggg] [gggggggg]
	// [gggggggg] [gggggggg] [gggghhhh] [hhhhhhhh]
	// [hhhhhhhh] [hhhhhhhh] [hhhhhhhh] [hhhhhhhh]
	//
	return bits
}

func cmp_bp_45_eq(buf []byte, val uint64, n int, bits *Bitset) *Bitset {
	// 45 bit packed
	// [aaaaaaaa] [aaaaaaaa] [aaaaaaaa] [aaaaaaaa]
	// [aaaaaaaa] [aaaaabbb] [bbbbbbbb] [bbbbbbbb]
	// [bbbbbbbb] [bbbbbbbb] [bbbbbbbb] [bbcccccc]
	// [cccccccc] [cccccccc] [cccccccc] [cccccccc]
	// [cccccccd] [dddddddd] [dddddddd] [dddddddd]
	// [dddddddd] [dddddddd] [ddddeeee] [eeeeeeee]
	// [eeeeeeee] [eeeeeeee] [eeeeeeee] [eeeeeeee]
	// [efffffff] [ffffffff] [ffffffff] [ffffffff]
	// [ffffffff] [ffffffgg] [gggggggg] [gggggggg]
	// [gggggggg] [gggggggg] [gggggggg] [ggghhhhh]
	// [hhhhhhhh] [hhhhhhhh] [hhhhhhhh] [hhhhhhhh]
	// [hhhhhhhh]
	return bits
}

func cmp_bp_46_eq(buf []byte, val uint64, n int, bits *Bitset) *Bitset {
	// 46 bit packed
	// [aaaaaaaa] [aaaaaaaa] [aaaaaaaa] [aaaaaaaa]
	// [aaaaaaaa] [aaaaaabb] [bbbbbbbb] [bbbbbbbb]
	// [bbbbbbbb] [bbbbbbbb] [bbbbbbbb] [bbbbcccc]
	// [cccccccc] [cccccccc] [cccccccc] [cccccccc]
	// [cccccccc] [ccdddddd] [dddddddd] [dddddddd]
	// [dddddddd] [dddddddd] [dddddddd] [eeeeeeee]
	// [eeeeeeee] [eeeeeeee] [eeeeeeee] [eeeeeeee]
	// [eeeeeeff] [ffffffff] [ffffffff] [ffffffff]
	// [ffffffff] [ffffffff] [ffffgggg] [gggggggg]
	// [gggggggg] [gggggggg] [gggggggg] [gggggggg]
	// [gghhhhhh] [hhhhhhhh] [hhhhhhhh] [hhhhhhhh]
	// [hhhhhhhh] [hhhhhhhh]
	return bits
}

func cmp_bp_47_eq(buf []byte, val uint64, n int, bits *Bitset) *Bitset {
	// 47 bit packed
	// [aaaaaaaa] [aaaaaaaa] [aaaaaaaa] [aaaaaaaa]
	// [aaaaaaaa] [aaaaaaab] [bbbbbbbb] [bbbbbbbb]
	// [bbbbbbbb] [bbbbbbbb] [bbbbbbbb] [bbbbbbcc]
	// [cccccccc] [cccccccc] [cccccccc] [cccccccc]
	// [cccccccc] [cccccddd] [dddddddd] [dddddddd]
	// [dddddddd] [dddddddd] [dddddddd] [ddddeeee]
	// [eeeeeeee] [eeeeeeee] [eeeeeeee] [eeeeeeee]
	// [eeeeeeee] [eeefffff] [ffffffff] [ffffffff]
	// [ffffffff] [ffffffff] [ffffffff] [ffgggggg]
	// [gggggggg] [gggggggg] [gggggggg] [gggggggg]
	// [gggggggg] [ghhhhhhh] [hhhhhhhh] [hhhhhhhh]
	// [hhhhhhhh] [hhhhhhhh] [hhhhhhhh]
	return bits
}

func cmp_bp_48_eq(buf []byte, val uint64, n int, bits *Bitset) *Bitset {
	// 48 bit packed
	// [aaaaaaaa] [aaaaaaaa] [aaaaaaaa] [aaaaaaaa]
	// [aaaaaaaa] [aaaaaaaa] [bbbbbbbb] [bbbbbbbb]
	// [bbbbbbbb] [bbbbbbbb] [bbbbbbbb] [bbbbbbbb]
	// [cccccccc] [cccccccc] [cccccccc] [cccccccc]
	// [cccccccc] [cccccccc] [dddddddd] [dddddddd]
	// [dddddddd] [dddddddd] [dddddddd] [dddddddd]
	// [eeeeeeee] [eeeeeeee] [eeeeeeee] [eeeeeeee]
	// [eeeeeeee] [eeeeeeee] [ffffffff] [ffffffff]
	// [ffffffff] [ffffffff] [ffffffff] [ffffffff]
	// [gggggggg] [gggggggg] [gggggggg] [gggggggg]
	// [gggggggg] [gggggggg] [hhhhhhhh] [hhhhhhhh]
	// [hhhhhhhh] [hhhhhhhh] [hhhhhhhh] [hhhhhhhh]
	//
	return bits
}

func cmp_bp_49_eq(buf []byte, val uint64, n int, bits *Bitset) *Bitset {
	// 49 bit packed
	// [aaaaaaaa] [aaaaaaaa] [aaaaaaaa] [aaaaaaaa]
	// [aaaaaaaa] [aaaaaaaa] [abbbbbbb] [bbbbbbbb]
	// [bbbbbbbb] [bbbbbbbb] [bbbbbbbb] [bbbbbbbb]
	// [bbcccccc] [cccccccc] [cccccccc] [cccccccc]
	// [cccccccc] [cccccccc] [cccddddd] [dddddddd]
	// [dddddddd] [dddddddd] [dddddddd] [dddddddd]
	// [ddddeeee] [eeeeeeee] [eeeeeeee] [eeeeeeee]
	// [eeeeeeee] [eeeeeeee] [eeeeefff] [ffffffff]
	// [ffffffff] [ffffffff] [ffffffff] [ffffffff]
	// [ffffffgg] [gggggggg] [gggggggg] [gggggggg]
	// [gggggggg] [gggggggg] [gggggggh] [hhhhhhhh]
	// [hhhhhhhh] [hhhhhhhh] [hhhhhhhh] [hhhhhhhh]
	// [hhhhhhhh]
	return bits
}

func cmp_bp_50_eq(buf []byte, val uint64, n int, bits *Bitset) *Bitset {
	// 50 bit packed
	// [aaaaaaaa] [aaaaaaaa] [aaaaaaaa] [aaaaaaaa]
	// [aaaaaaaa] [aaaaaaaa] [aabbbbbb] [bbbbbbbb]
	// [bbbbbbbb] [bbbbbbbb] [bbbbbbbb] [bbbbbbbb]
	// [bbbbcccc] [cccccccc] [cccccccc] [cccccccc]
	// [cccccccc] [cccccccc] [ccccccdd] [dddddddd]
	// [dddddddd] [dddddddd] [dddddddd] [dddddddd]
	// [dddddddd] [eeeeeeee] [eeeeeeee] [eeeeeeee]
	// [eeeeeeee] [eeeeeeee] [eeeeeeee] [eeffffff]
	// [ffffffff] [ffffffff] [ffffffff] [ffffffff]
	// [ffffffff] [ffffgggg] [gggggggg] [gggggggg]
	// [gggggggg] [gggggggg] [gggggggg] [gggggghh]
	// [hhhhhhhh] [hhhhhhhh] [hhhhhhhh] [hhhhhhhh]
	// [hhhhhhhh] [hhhhhhhh]
	return bits
}

func cmp_bp_51_eq(buf []byte, val uint64, n int, bits *Bitset) *Bitset {
	// 51 bit packed
	// [aaaaaaaa] [aaaaaaaa] [aaaaaaaa] [aaaaaaaa]
	// [aaaaaaaa] [aaaaaaaa] [aaabbbbb] [bbbbbbbb]
	// [bbbbbbbb] [bbbbbbbb] [bbbbbbbb] [bbbbbbbb]
	// [bbbbbbcc] [cccccccc] [cccccccc] [cccccccc]
	// [cccccccc] [cccccccc] [cccccccc] [cddddddd]
	// [dddddddd] [dddddddd] [dddddddd] [dddddddd]
	// [dddddddd] [ddddeeee] [eeeeeeee] [eeeeeeee]
	// [eeeeeeee] [eeeeeeee] [eeeeeeee] [eeeeeeef]
	// [ffffffff] [ffffffff] [ffffffff] [ffffffff]
	// [ffffffff] [ffffffff] [ffgggggg] [gggggggg]
	// [gggggggg] [gggggggg] [gggggggg] [gggggggg]
	// [ggggghhh] [hhhhhhhh] [hhhhhhhh] [hhhhhhhh]
	// [hhhhhhhh] [hhhhhhhh] [hhhhhhhh]
	return bits
}

func cmp_bp_52_eq(buf []byte, val uint64, n int, bits *Bitset) *Bitset {
	// 52 bit packed
	// [aaaaaaaa] [aaaaaaaa] [aaaaaaaa] [aaaaaaaa]
	// [aaaaaaaa] [aaaaaaaa] [aaaabbbb] [bbbbbbbb]
	// [bbbbbbbb] [bbbbbbbb] [bbbbbbbb] [bbbbbbbb]
	// [bbbbbbbb] [cccccccc] [cccccccc] [cccccccc]
	// [cccccccc] [cccccccc] [cccccccc] [ccccdddd]
	// [dddddddd] [dddddddd] [dddddddd] [dddddddd]
	// [dddddddd] [dddddddd] [eeeeeeee] [eeeeeeee]
	// [eeeeeeee] [eeeeeeee] [eeeeeeee] [eeeeeeee]
	// [eeeeffff] [ffffffff] [ffffffff] [ffffffff]
	// [ffffffff] [ffffffff] [ffffffff] [gggggggg]
	// [gggggggg] [gggggggg] [gggggggg] [gggggggg]
	// [gggggggg] [gggghhhh] [hhhhhhhh] [hhhhhhhh]
	// [hhhhhhhh] [hhhhhhhh] [hhhhhhhh] [hhhhhhhh]
	//
	return bits
}

func cmp_bp_53_eq(buf []byte, val uint64, n int, bits *Bitset) *Bitset {
	// 53 bit packed
	// [aaaaaaaa] [aaaaaaaa] [aaaaaaaa] [aaaaaaaa]
	// [aaaaaaaa] [aaaaaaaa] [aaaaabbb] [bbbbbbbb]
	// [bbbbbbbb] [bbbbbbbb] [bbbbbbbb] [bbbbbbbb]
	// [bbbbbbbb] [bbcccccc] [cccccccc] [cccccccc]
	// [cccccccc] [cccccccc] [cccccccc] [cccccccd]
	// [dddddddd] [dddddddd] [dddddddd] [dddddddd]
	// [dddddddd] [dddddddd] [ddddeeee] [eeeeeeee]
	// [eeeeeeee] [eeeeeeee] [eeeeeeee] [eeeeeeee]
	// [eeeeeeee] [efffffff] [ffffffff] [ffffffff]
	// [ffffffff] [ffffffff] [ffffffff] [ffffffgg]
	// [gggggggg] [gggggggg] [gggggggg] [gggggggg]
	// [gggggggg] [gggggggg] [ggghhhhh] [hhhhhhhh]
	// [hhhhhhhh] [hhhhhhhh] [hhhhhhhh] [hhhhhhhh]
	// [hhhhhhhh]
	return bits
}

func cmp_bp_54_eq(buf []byte, val uint64, n int, bits *Bitset) *Bitset {
	// 54 bit packed
	// [aaaaaaaa] [aaaaaaaa] [aaaaaaaa] [aaaaaaaa]
	// [aaaaaaaa] [aaaaaaaa] [aaaaaabb] [bbbbbbbb]
	// [bbbbbbbb] [bbbbbbbb] [bbbbbbbb] [bbbbbbbb]
	// [bbbbbbbb] [bbbbcccc] [cccccccc] [cccccccc]
	// [cccccccc] [cccccccc] [cccccccc] [cccccccc]
	// [ccdddddd] [dddddddd] [dddddddd] [dddddddd]
	// [dddddddd] [dddddddd] [dddddddd] [eeeeeeee]
	// [eeeeeeee] [eeeeeeee] [eeeeeeee] [eeeeeeee]
	// [eeeeeeee] [eeeeeeff] [ffffffff] [ffffffff]
	// [ffffffff] [ffffffff] [ffffffff] [ffffffff]
	// [ffffgggg] [gggggggg] [gggggggg] [gggggggg]
	// [gggggggg] [gggggggg] [gggggggg] [gghhhhhh]
	// [hhhhhhhh] [hhhhhhhh] [hhhhhhhh] [hhhhhhhh]
	// [hhhhhhhh] [hhhhhhhh]
	return bits
}

func cmp_bp_55_eq(buf []byte, val uint64, n int, bits *Bitset) *Bitset {
	// 55 bit packed
	// [aaaaaaaa] [aaaaaaaa] [aaaaaaaa] [aaaaaaaa]
	// [aaaaaaaa] [aaaaaaaa] [aaaaaaab] [bbbbbbbb]
	// [bbbbbbbb] [bbbbbbbb] [bbbbbbbb] [bbbbbbbb]
	// [bbbbbbbb] [bbbbbbcc] [cccccccc] [cccccccc]
	// [cccccccc] [cccccccc] [cccccccc] [cccccccc]
	// [cccccddd] [dddddddd] [dddddddd] [dddddddd]
	// [dddddddd] [dddddddd] [dddddddd] [ddddeeee]
	// [eeeeeeee] [eeeeeeee] [eeeeeeee] [eeeeeeee]
	// [eeeeeeee] [eeeeeeee] [eeefffff] [ffffffff]
	// [ffffffff] [ffffffff] [ffffffff] [ffffffff]
	// [ffffffff] [ffgggggg] [gggggggg] [gggggggg]
	// [gggggggg] [gggggggg] [gggggggg] [gggggggg]
	// [ghhhhhhh] [hhhhhhhh] [hhhhhhhh] [hhhhhhhh]
	// [hhhhhhhh] [hhhhhhhh] [hhhhhhhh]
	return bits
}

func cmp_bp_56_eq(buf []byte, val uint64, n int, bits *Bitset) *Bitset {
	// 56 bit packed
	// [aaaaaaaa] [aaaaaaaa] [aaaaaaaa] [aaaaaaaa]
	// [aaaaaaaa] [aaaaaaaa] [aaaaaaaa] [bbbbbbbb]
	// [bbbbbbbb] [bbbbbbbb] [bbbbbbbb] [bbbbbbbb]
	// [bbbbbbbb] [bbbbbbbb] [cccccccc] [cccccccc]
	// [cccccccc] [cccccccc] [cccccccc] [cccccccc]
	// [cccccccc] [dddddddd] [dddddddd] [dddddddd]
	// [dddddddd] [dddddddd] [dddddddd] [dddddddd]
	// [eeeeeeee] [eeeeeeee] [eeeeeeee] [eeeeeeee]
	// [eeeeeeee] [eeeeeeee] [eeeeeeee] [ffffffff]
	// [ffffffff] [ffffffff] [ffffffff] [ffffffff]
	// [ffffffff] [ffffffff] [gggggggg] [gggggggg]
	// [gggggggg] [gggggggg] [gggggggg] [gggggggg]
	// [gggggggg] [hhhhhhhh] [hhhhhhhh] [hhhhhhhh]
	// [hhhhhhhh] [hhhhhhhh] [hhhhhhhh] [hhhhhhhh]
	//
	return bits
}

func cmp_bp_57_eq(buf []byte, val uint64, n int, bits *Bitset) *Bitset {
	// 57 bit packed
	// [aaaaaaaa] [aaaaaaaa] [aaaaaaaa] [aaaaaaaa]
	// [aaaaaaaa] [aaaaaaaa] [aaaaaaaa] [abbbbbbb]
	// [bbbbbbbb] [bbbbbbbb] [bbbbbbbb] [bbbbbbbb]
	// [bbbbbbbb] [bbbbbbbb] [bbcccccc] [cccccccc]
	// [cccccccc] [cccccccc] [cccccccc] [cccccccc]
	// [cccccccc] [cccddddd] [dddddddd] [dddddddd]
	// [dddddddd] [dddddddd] [dddddddd] [dddddddd]
	// [ddddeeee] [eeeeeeee] [eeeeeeee] [eeeeeeee]
	// [eeeeeeee] [eeeeeeee] [eeeeeeee] [eeeeefff]
	// [ffffffff] [ffffffff] [ffffffff] [ffffffff]
	// [ffffffff] [ffffffff] [ffffffgg] [gggggggg]
	// [gggggggg] [gggggggg] [gggggggg] [gggggggg]
	// [gggggggg] [gggggggh] [hhhhhhhh] [hhhhhhhh]
	// [hhhhhhhh] [hhhhhhhh] [hhhhhhhh] [hhhhhhhh]
	// [hhhhhhhh]
	return bits
}

func cmp_bp_58_eq(buf []byte, val uint64, n int, bits *Bitset) *Bitset {
	// 58 bit packed
	// [aaaaaaaa] [aaaaaaaa] [aaaaaaaa] [aaaaaaaa]
	// [aaaaaaaa] [aaaaaaaa] [aaaaaaaa] [aabbbbbb]
	// [bbbbbbbb] [bbbbbbbb] [bbbbbbbb] [bbbbbbbb]
	// [bbbbbbbb] [bbbbbbbb] [bbbbcccc] [cccccccc]
	// [cccccccc] [cccccccc] [cccccccc] [cccccccc]
	// [cccccccc] [ccccccdd] [dddddddd] [dddddddd]
	// [dddddddd] [dddddddd] [dddddddd] [dddddddd]
	// [dddddddd] [eeeeeeee] [eeeeeeee] [eeeeeeee]
	// [eeeeeeee] [eeeeeeee] [eeeeeeee] [eeeeeeee]
	// [eeffffff] [ffffffff] [ffffffff] [ffffffff]
	// [ffffffff] [ffffffff] [ffffffff] [ffffgggg]
	// [gggggggg] [gggggggg] [gggggggg] [gggggggg]
	// [gggggggg] [gggggggg] [gggggghh] [hhhhhhhh]
	// [hhhhhhhh] [hhhhhhhh] [hhhhhhhh] [hhhhhhhh]
	// [hhhhhhhh] [hhhhhhhh]
	return bits
}

func cmp_bp_59_eq(buf []byte, val uint64, n int, bits *Bitset) *Bitset {
	// 59 bit packed
	// [aaaaaaaa] [aaaaaaaa] [aaaaaaaa] [aaaaaaaa]
	// [aaaaaaaa] [aaaaaaaa] [aaaaaaaa] [aaabbbbb]
	// [bbbbbbbb] [bbbbbbbb] [bbbbbbbb] [bbbbbbbb]
	// [bbbbbbbb] [bbbbbbbb] [bbbbbbcc] [cccccccc]
	// [cccccccc] [cccccccc] [cccccccc] [cccccccc]
	// [cccccccc] [cccccccc] [cddddddd] [dddddddd]
	// [dddddddd] [dddddddd] [dddddddd] [dddddddd]
	// [dddddddd] [ddddeeee] [eeeeeeee] [eeeeeeee]
	// [eeeeeeee] [eeeeeeee] [eeeeeeee] [eeeeeeee]
	// [eeeeeeef] [ffffffff] [ffffffff] [ffffffff]
	// [ffffffff] [ffffffff] [ffffffff] [ffffffff]
	// [ffgggggg] [gggggggg] [gggggggg] [gggggggg]
	// [gggggggg] [gggggggg] [gggggggg] [ggggghhh]
	// [hhhhhhhh] [hhhhhhhh] [hhhhhhhh] [hhhhhhhh]
	// [hhhhhhhh] [hhhhhhhh] [hhhhhhhh]
	return bits
}

func cmp_bp_60_eq(buf []byte, val uint64, n int, bits *Bitset) *Bitset {
	// 60 bit packed
	// [aaaaaaaa] [aaaaaaaa] [aaaaaaaa] [aaaaaaaa]
	// [aaaaaaaa] [aaaaaaaa] [aaaaaaaa] [aaaabbbb]
	// [bbbbbbbb] [bbbbbbbb] [bbbbbbbb] [bbbbbbbb]
	// [bbbbbbbb] [bbbbbbbb] [bbbbbbbb] [cccccccc]
	// [cccccccc] [cccccccc] [cccccccc] [cccccccc]
	// [cccccccc] [cccccccc] [ccccdddd] [dddddddd]
	// [dddddddd] [dddddddd] [dddddddd] [dddddddd]
	// [dddddddd] [dddddddd] [eeeeeeee] [eeeeeeee]
	// [eeeeeeee] [eeeeeeee] [eeeeeeee] [eeeeeeee]
	// [eeeeeeee] [eeeeffff] [ffffffff] [ffffffff]
	// [ffffffff] [ffffffff] [ffffffff] [ffffffff]
	// [ffffffff] [gggggggg] [gggggggg] [gggggggg]
	// [gggggggg] [gggggggg] [gggggggg] [gggggggg]
	// [gggghhhh] [hhhhhhhh] [hhhhhhhh] [hhhhhhhh]
	// [hhhhhhhh] [hhhhhhhh] [hhhhhhhh] [hhhhhhhh]
	//
	return bits
}

func cmp_bp_61_eq(buf []byte, val uint64, n int, bits *Bitset) *Bitset {
	// 61 bit packed
	// [aaaaaaaa] [aaaaaaaa] [aaaaaaaa] [aaaaaaaa]
	// [aaaaaaaa] [aaaaaaaa] [aaaaaaaa] [aaaaabbb]
	// [bbbbbbbb] [bbbbbbbb] [bbbbbbbb] [bbbbbbbb]
	// [bbbbbbbb] [bbbbbbbb] [bbbbbbbb] [bbcccccc]
	// [cccccccc] [cccccccc] [cccccccc] [cccccccc]
	// [cccccccc] [cccccccc] [cccccccd] [dddddddd]
	// [dddddddd] [dddddddd] [dddddddd] [dddddddd]
	// [dddddddd] [dddddddd] [ddddeeee] [eeeeeeee]
	// [eeeeeeee] [eeeeeeee] [eeeeeeee] [eeeeeeee]
	// [eeeeeeee] [eeeeeeee] [efffffff] [ffffffff]
	// [ffffffff] [ffffffff] [ffffffff] [ffffffff]
	// [ffffffff] [ffffffgg] [gggggggg] [gggggggg]
	// [gggggggg] [gggggggg] [gggggggg] [gggggggg]
	// [gggggggg] [ggghhhhh] [hhhhhhhh] [hhhhhhhh]
	// [hhhhhhhh] [hhhhhhhh] [hhhhhhhh] [hhhhhhhh]
	// [hhhhhhhh]
	return bits
}

func cmp_bp_62_eq(buf []byte, val uint64, n int, bits *Bitset) *Bitset {
	// 62 bit packed
	// [aaaaaaaa] [aaaaaaaa] [aaaaaaaa] [aaaaaaaa]
	// [aaaaaaaa] [aaaaaaaa] [aaaaaaaa] [aaaaaabb]
	// [bbbbbbbb] [bbbbbbbb] [bbbbbbbb] [bbbbbbbb]
	// [bbbbbbbb] [bbbbbbbb] [bbbbbbbb] [bbbbcccc]
	// [cccccccc] [cccccccc] [cccccccc] [cccccccc]
	// [cccccccc] [cccccccc] [cccccccc] [ccdddddd]
	// [dddddddd] [dddddddd] [dddddddd] [dddddddd]
	// [dddddddd] [dddddddd] [dddddddd] [eeeeeeee]
	// [eeeeeeee] [eeeeeeee] [eeeeeeee] [eeeeeeee]
	// [eeeeeeee] [eeeeeeee] [eeeeeeff] [ffffffff]
	// [ffffffff] [ffffffff] [ffffffff] [ffffffff]
	// [ffffffff] [ffffffff] [ffffgggg] [gggggggg]
	// [gggggggg] [gggggggg] [gggggggg] [gggggggg]
	// [gggggggg] [gggggggg] [gghhhhhh] [hhhhhhhh]
	// [hhhhhhhh] [hhhhhhhh] [hhhhhhhh] [hhhhhhhh]
	// [hhhhhhhh] [hhhhhhhh]
	return bits
}

func cmp_bp_63_eq(buf []byte, val uint64, n int, bits *Bitset) *Bitset {
	// 63 bit packed
	// [aaaaaaaa] [aaaaaaaa] [aaaaaaaa] [aaaaaaaa]
	// [aaaaaaaa] [aaaaaaaa] [aaaaaaaa] [aaaaaaab]
	// [bbbbbbbb] [bbbbbbbb] [bbbbbbbb] [bbbbbbbb]
	// [bbbbbbbb] [bbbbbbbb] [bbbbbbbb] [bbbbbbcc]
	// [cccccccc] [cccccccc] [cccccccc] [cccccccc]
	// [cccccccc] [cccccccc] [cccccccc] [cccccddd]
	// [dddddddd] [dddddddd] [dddddddd] [dddddddd]
	// [dddddddd] [dddddddd] [dddddddd] [ddddeeee]
	// [eeeeeeee] [eeeeeeee] [eeeeeeee] [eeeeeeee]
	// [eeeeeeee] [eeeeeeee] [eeeeeeee] [eeefffff]
	// [ffffffff] [ffffffff] [ffffffff] [ffffffff]
	// [ffffffff] [ffffffff] [ffffffff] [ffgggggg]
	// [gggggggg] [gggggggg] [gggggggg] [gggggggg]
	// [gggggggg] [gggggggg] [gggggggg] [ghhhhhhh]
	// [hhhhhhhh] [hhhhhhhh] [hhhhhhhh] [hhhhhhhh]
	// [hhhhhhhh] [hhhhhhhh] [hhhhhhhh]
	return bits
}
