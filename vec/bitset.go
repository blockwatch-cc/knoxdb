// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package vec

import (
	"sync"
)

const defaultBitSetSize = 16 // defaultPackSizeLog2 (64k)

var bitSetPool = &sync.Pool{
	New: func() interface{} { return makeBitSet(1 << defaultBitSetSize) },
}

type BitSet struct {
	buf       []byte
	cnt       int64
	size      int
	isReverse bool
}

// NewBitSet allocates a new BitSet with a custom size and default capacity or
// 2<<16 bits (8kB). Call Close() to return the bitset after use. For efficiency
// an internal pool guarantees that bitsets of default capacity are reused. Changing
// the capacity with Grow() may make the Bitset uneligible for recycling.
func NewBitSet(size int) *BitSet {
	var s *BitSet
	if size <= 1<<defaultBitSetSize {
		s = bitSetPool.Get().(*BitSet)
		s.Grow(size)
	} else {
		s = makeBitSet(size)
	}
	return s
}

// NewSmallBitSet allocates a new bitset of arbitrary small size and capacity
// without using a buffer pool. Use this function when your bitsets are always
// much smaller than the default capacity.
func NewSmallBitSet(size int) *BitSet {
	return makeBitSet(size)
}

// NewBitSetFromBytes allocates a new bitset of size bits and copies the contents
// of `buf`. Buf may be nil and size must be >= zero.
func NewBitSetFromBytes(buf []byte, size int) *BitSet {
	s := &BitSet{
		buf:  make([]byte, bitFieldLen(size)),
		cnt:  -1,
		size: size,
	}
	copy(s.buf, buf)
	// ensure the last byte is masked
	if size%8 > 0 {
		s.buf[len(s.buf)-1] &= bytemask(size)
	}
	return s
}

// NewBitSetFromSlice allocates a new bitset and initializes it from boolean values
// in bools. If bools is nil, the bitset is initially empty.
func NewBitSetFromSlice(bools []bool) *BitSet {
	s := &BitSet{
		buf:  make([]byte, bitFieldLen(len(bools))),
		cnt:  0,
		size: len(bools),
	}
	for i := range bools {
		if !bools[i] {
			continue
		}
		s.buf[i>>3] |= bitmask(i)
		s.cnt++
	}
	return s
}

func (s *BitSet) SetFromBytes(buf []byte, size int) *BitSet {
	if s.size > size {
		s.Zero()
	}
	if cap(s.buf) < len(buf) {
		s.buf = make([]byte, len(buf))
	}
	s.size = size
	s.buf = s.buf[:len(buf)]
	copy(s.buf, buf)
	s.cnt = -1
	s.isReverse = false
	return s
}

func makeBitSet(size int) *BitSet {
	return &BitSet{
		buf:  make([]byte, bitFieldLen(size)),
		cnt:  0,
		size: size,
	}
}

func (s *BitSet) Clone() *BitSet {
	clone := NewBitSet(s.size)
	copy(clone.buf, s.buf)
	clone.cnt = s.cnt
	clone.isReverse = s.isReverse
	return clone
}

// Grow resizes the bitset to a new size, either growing or shrinking it.
// Content remains unchanged on grow, when shrinking trailing bits are clipped.
//
// FIXME: does not work with reversed bitset
func (s *BitSet) Grow(size int) *BitSet {
	if size < 0 {
		return s
	}
	sz := bitFieldLen(size)
	if s.buf == nil || cap(s.buf) < sz {
		buf := make([]byte, sz)
		copy(buf, s.buf)
		s.buf = buf
	} else {
		if size > 0 && size < s.size {
			// clear trailing bytes
			if len(s.buf) > sz {
				s.buf[sz] = 0
				for bp := 1; sz+bp < len(s.buf); bp *= 2 {
					copy(s.buf[sz+bp:], s.buf[sz:sz+bp])
				}
			}
			// clear trailing bits
			s.buf[sz-1] &= bytemask(size)
			s.cnt = -1
		}
		s.buf = s.buf[:sz]
	}
	s.size = size
	return s
}

// Reset clears the bitset contents and sets its size to zero.
func (s *BitSet) Reset() {
	if len(s.buf) > 0 {
		s.buf[0] = 0
		for bp := 1; bp < len(s.buf); bp *= 2 {
			copy(s.buf[bp:], s.buf[:bp])
		}
	}
	s.size = 0
	s.cnt = 0
	s.buf = s.buf[:0]
	s.isReverse = false
}

// Close clears the bitset contents, sets its size to zero and returns it
// to the internal buffer pool. Using the bitset after calling Close is
// illegal.
func (s *BitSet) Close() {
	s.Reset()
	if cap(s.buf) == 1<<defaultBitSetSize {
		bitSetPool.Put(s)
	}
}

func (s *BitSet) And(r *BitSet) (*BitSet, int) {
	if r.Count() == 0 {
		s.Zero()
		return s, 0
	}
	any := bitsetAnd(s.Bytes(), r.Bytes(), min(s.size, r.size))
	return s, any
}

func (s *BitSet) AndNot(r *BitSet) *BitSet {
	bitsetAndNot(s.Bytes(), r.Bytes(), min(s.size, r.size))
	return s
}

func (s *BitSet) Or(r *BitSet) *BitSet {
	if s.cnt == 0 {
		copy(s.buf, r.buf)
		s.cnt = r.cnt
		return s
	}
	bitsetOr(s.Bytes(), r.Bytes(), min(s.size, r.size))
	return s
}

func (s *BitSet) Xor(r *BitSet) *BitSet {
	bitsetXor(s.Bytes(), r.Bytes(), min(s.size, r.size))
	return s
}

func (s *BitSet) Neg() *BitSet {
	bitsetNeg(s.Bytes(), s.size)
	return s
}

func (s *BitSet) One() *BitSet {
	if s.size == 0 {
		return s
	}
	s.cnt = int64(s.size)
	s.buf[0] = 0xff
	for bp := 1; bp < len(s.buf); bp *= 2 {
		copy(s.buf[bp:], s.buf[:bp])
	}
	s.buf[len(s.buf)-1] = bytemask(s.size)
	return s
}

func (s *BitSet) Zero() *BitSet {
	s.isReverse = false
	if s.size == 0 || s.cnt == 0 {
		return s
	}
	s.cnt = 0
	s.buf[0] = 0
	for bp := 1; bp < len(s.buf); bp *= 2 {
		copy(s.buf[bp:], s.buf[:bp])
	}
	return s
}

func (s *BitSet) Fill(b byte) *BitSet {
	s.buf[0] = b
	for bp := 1; bp < len(s.buf); bp *= 2 {
		copy(s.buf[bp:], s.buf[:bp])
	}
	if s.isReverse {
		s.buf[0] &= bitsetReverseLut256[bytemask(s.size)]
	} else {
		s.buf[len(s.buf)-1] &= bytemask(s.size)
	}
	s.cnt = -1
	return s
}

func (s *BitSet) Set(i int) *BitSet {
	if i < 0 || i >= s.size {
		return s
	}
	if s.isReverse {
		pad := int(7 - uint(s.size-1)&0x7)
		i = s.size - i + pad - 1
	}
	mask := bitmask(i)
	if s.cnt >= 0 && s.buf[i>>3]&mask == 0 {
		s.cnt++
	}
	s.buf[i>>3] |= mask
	return s
}

func (s *BitSet) setbit(i int) {
	if s.isReverse {
		pad := int(7 - uint(s.size-1)&0x7)
		i = s.size - i + pad - 1
	}
	mask := bitmask(i)
	s.buf[i>>3] |= mask
}

func (s *BitSet) Clear(i int) *BitSet {
	if i < 0 || i >= s.size {
		return s
	}
	if s.isReverse {
		pad := int(7 - uint(s.size-1)&0x7)
		i = s.size - i + pad - 1
	}
	mask := bitmask(i)
	if s.cnt > 0 && s.buf[i>>3]&mask > 0 {
		s.cnt--
	}
	s.buf[i>>3] &^= mask
	return s
}

func (s *BitSet) clearbit(i int) {
	if s.isReverse {
		pad := int(7 - uint(s.size-1)&0x7)
		i = s.size - i + pad - 1
	}
	mask := bitmask(i)
	s.buf[i>>3] &^= mask
}

func (s *BitSet) IsSet(i int) bool {
	if i < 0 || i >= s.size {
		return false
	}
	if s.isReverse {
		pad := int(7 - uint(s.size-1)&0x7)
		i = s.size - i + pad - 1
	}
	mask := bitmask(i)
	return (s.buf[i>>3] & mask) > 0
}

// Insert inserts srcLen values from position srcPos in bitset src into the
// bitset at position dstPos and moves all values following dstPos behind the
// newly inserted bits
//
// FIXME: fast path incompatible with reversed
func (s *BitSet) Insert(src *BitSet, srcPos, srcLen, dstPos int) *BitSet {
	if srcLen <= 0 {
		return s
	}

	// append when dst is < 0
	if dstPos < 0 {
		dstPos = s.size
	}
	// clamp srcLen
	if srcPos+srcLen > src.size {
		srcLen = src.size - srcPos
	}

	// keep a copy of trailing bits to move
	var (
		cp  []byte
		cpb []bool
	)
	if dstPos&0x7+srcLen&0x7 == 0 {
		// fast path
		cp = make([]byte, len(s.buf)-dstPos>>3)
		copy(cp, s.buf[dstPos>>3:])
	} else {
		// slow path
		cpb = s.SubSlice(dstPos, -1)
	}

	// grow bitset, restore counter for fast-path
	cnt := s.cnt
	s.Grow(s.size + srcLen)
	s.cnt = cnt

	// insert
	if srcPos&0x7+dstPos&0x7+srcLen&0x7 == 0 {
		// fast path
		copy(s.buf[dstPos>>3:], src.buf[srcPos>>3:(srcPos+srcLen)>>3])
		s.cnt = -1
	} else {
		// slow path
		for i, v := range src.SubSlice(srcPos, srcLen) {
			if !v {
				s.clearbit(i + dstPos)
			} else {
				s.setbit(i + dstPos)
				if s.cnt >= 0 {
					s.cnt++
				}
			}
		}
	}

	// patch trailing bits
	if dstPos&0x7+srcLen&0x7 == 0 {
		// fast path
		copy(s.buf[(dstPos+srcLen)>>3:], cp)
	} else {
		// slow path
		for i, v := range cpb {
			if !v {
				s.clearbit(i + dstPos + srcLen)
			} else {
				s.setbit(i + dstPos + srcLen)
			}
		}
	}

	return s
}

// Replace replaces srcLen values at position dstPos with values from src
// bewteen position srcPos and srcPos + srcLen.
//
// FIXME: fast path incompatible with reversed
func (s *BitSet) Replace(src *BitSet, srcPos, srcLen, dstPos int) *BitSet {
	// skip when arguments are out of range
	if srcLen <= 0 || srcPos < 0 || dstPos < 0 || dstPos > s.size {
		return s
	}

	// clamp srcLen
	if srcLen > src.size-srcPos {
		srcLen = src.size - srcPos
	}
	if srcLen > s.size-dstPos {
		srcLen = s.size - dstPos
	}

	// replace
	if srcPos&0x7+dstPos&0x7+srcLen&0x7 == 0 {
		// fast path
		copy(s.buf[dstPos>>3:], src.buf[srcPos>>3:(srcPos+srcLen)>>3])
		s.cnt = -1
	} else {
		// slow path
		for i, v := range src.SubSlice(srcPos, srcLen) {
			if !v {
				s.clearbit(i + dstPos)
			} else {
				s.setbit(i + dstPos)
				if s.cnt >= 0 {
					s.cnt++
				}
			}
		}
	}

	return s
}

// Append grows the bitset by srcLen and appends srcLen values from
// src starting at position srcPos.
//
// FIXME: fast path incompatible with reversed
func (s *BitSet) Append(src *BitSet, srcPos, srcLen int) *BitSet {
	if srcLen <= 0 {
		return s
	}
	// clamp srcLen
	if srcPos+srcLen > src.size {
		srcLen = src.size - srcPos
	}

	end := s.size
	cnt := s.cnt
	s.Grow(s.size + srcLen)
	s.cnt = cnt

	if end&0x7+srcPos&0x7+srcLen&0x7 == 0 {
		// fast path
		copy(s.buf[end>>3:], src.buf[srcPos>>3:(srcPos+srcLen)>>3])
		s.cnt = -1
	} else {
		// slow path
		for i := 0; i < srcLen; i++ {
			if !src.IsSet(srcPos + i) {
				continue
			}
			s.setbit(end + i)
			if s.cnt >= 0 {
				s.cnt++
			}
		}
	}
	return s
}

func (s *BitSet) Delete(pos, n int) *BitSet {
	if pos >= s.size {
		return s
	}
	if pos < 0 {
		pos = 0
	}
	if n < 0 || pos+n > s.size {
		n = s.size - pos
	}

	if pos&0x7+n&0x7 == 0 {
		// fast path
		copy(s.buf[pos>>3:], s.buf[(pos+n)>>3:])
	} else {
		// slow path
		for i, v := range s.SubSlice(pos+n, -1) {
			if v {
				s.setbit(pos + i)
			} else {
				s.clearbit(pos + i)
			}
		}
	}

	// shrink and reset counter
	s.Grow(s.size - n)
	return s
}

func (s *BitSet) Swap(i, j int) {
	if uint(i) >= uint(s.size) || uint(j) >= uint(s.size) {
		return
	}
	bi, bj := s.IsSet(i), s.IsSet(j)
	if bi {
		s.setbit(j)
	} else {
		s.clearbit(j)
	}
	if bj {
		s.setbit(i)
	} else {
		s.clearbit(i)
	}
}

func (s *BitSet) Reverse() *BitSet {
	bitsetReverse(s.buf)
	s.isReverse = !s.isReverse
	return s
}

func (s BitSet) Bytes() []byte {
	return s.buf
}

func (s *BitSet) Count() int64 {
	if s.cnt < 0 {
		if s.isReverse {
			// leading padding is filled with zero bits
			s.cnt = bitsetPopCount(s.buf, len(s.buf)*8)
		} else {
			s.cnt = bitsetPopCount(s.buf, s.size)
		}
	}
	return s.cnt
}

func (s BitSet) Len() int {
	return s.size
}

func (s BitSet) Cap() int {
	return cap(s.buf) * 8
}

func (s BitSet) HeapSize() int {
	return cap(s.buf) + 24 + 16 + 1
}

func (s BitSet) EncodedSize() int {
	sz := s.size / 8
	if s.size&7 > 0 {
		sz++
	}
	return sz
}

// Run returns the index and length of the next consecutive
// run of 1s in the bit vector starting at index. When no more
// 1s exist after index, -1 and a length of 0 is returned.
func (b BitSet) Run(index int) (int, int) {
	if b.isReverse {
		if b.size == 0 || index < 0 || index > b.size {
			return -1, 0
		}
		pad := int(7 - uint(b.size-1)&0x7)
		index = b.size - index + pad - 1 // skip padding
		start, length := bitsetRun(b.buf, index, len(b.buf)*8)
		if start < 0 {
			return -1, 0
		}
		start = b.size - start + pad - 1 // reverse adjust
		return start, length
	}
	return bitsetRun(b.buf, index, b.size)
}

// Indexes returns a slice of indexes for one bits in the bitset.
func (s BitSet) Indexes(slice []int) []int {
	cnt := int(s.Count())
	if slice == nil || cap(slice) < cnt {
		slice = make([]int, cnt)
	} else {
		slice = slice[:cnt]
	}
	var j int
	for i, l := 0, s.size-s.size%8; i < l; i += 8 {
		b := s.buf[i>>3]
		for l := 0; b > 0; b, l = b<<1, l+1 {
			if b&0x80 == 0 {
				continue
			}
			slice[j] = i + l
			j++
		}
	}
	for i := s.size & ^0x7; i < s.size; i++ {
		if s.buf[i>>3]&bitmask(i) == 0 {
			continue
		}
		slice[j] = i
		j++
	}
	return slice
}

// Slice returns a boolean slice containing all values
func (s BitSet) Slice() []bool {
	res := make([]bool, s.size)
	for i, l := 0, s.size-s.size%8; i < l; i += 8 {
		b := s.buf[i>>3]
		res[i] = b&0x80 > 0
		res[i+1] = b&0x40 > 0
		res[i+2] = b&0x20 > 0
		res[i+3] = b&0x10 > 0
		res[i+4] = b&0x08 > 0
		res[i+5] = b&0x04 > 0
		res[i+6] = b&0x02 > 0
		res[i+7] = b&0x01 > 0
	}
	// tail
	for i := s.size & ^0x7; i < s.size; i++ {
		res[i] = s.buf[i>>3]&bitmask(i) > 0
	}
	return res
}

func (s BitSet) SubSlice(start, n int) []bool {
	if start >= s.size {
		return nil
	}
	if start < 0 {
		start = 0
	}
	if n < 0 {
		n = s.size - start
	} else if start+n > s.size {
		n = s.size - start
	}
	res := make([]bool, n)
	var j int
	// head
	for i := start; i < start+n && i%8 > 0; i, j = i+1, j+1 {
		res[j] = s.buf[i>>3]&bitmask(i) > 0
	}
	// fast inner loop
	for i := start + j; i < (start+n) & ^0x7; i, j = i+8, j+8 {
		b := s.buf[i>>3]
		res[j] = b&0x80 > 0
		res[j+1] = b&0x40 > 0
		res[j+2] = b&0x20 > 0
		res[j+3] = b&0x10 > 0
		res[j+4] = b&0x08 > 0
		res[j+5] = b&0x04 > 0
		res[j+6] = b&0x02 > 0
		res[j+7] = b&0x01 > 0
	}
	// tail
	for i := start + j; i < start+n; i, j = i+1, j+1 {
		res[j] = s.buf[i>>3]&bitmask(i) > 0
	}
	return res
}
