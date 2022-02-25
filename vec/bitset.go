// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package vec

import (
	"encoding/hex"
	"sync"
)

const defaultBitsetSize = 16 // 8kB

var bitsetPool = &sync.Pool{
	New: func() interface{} { return makeBitset(1 << defaultBitsetSize) },
}

type Bitset struct {
	buf  []byte
	cnt  int
	size int
}

// NewBitset allocates a new Bitset with a custom size and default capacity or
// 2<<16 bits (8kB). Call Close() to return the bitset after use. For efficiency
// an internal pool guarantees that bitsets of default capacity are reused. Changing
// the capacity with Grow() may make the Bitset uneligible for recycling.
func NewBitset(size int) *Bitset {
	var s *Bitset
	if size <= 1<<defaultBitsetSize {
		s = bitsetPool.Get().(*Bitset)
		s.Grow(size)
	} else {
		s = makeBitset(size)
	}
	return s
}

// NewCustomBitset allocates a new bitset of arbitrary small size and capacity
// without using a buffer pool. Use this function when your bitsets are always
// much smaller than the default capacity.
func NewCustomBitset(size int) *Bitset {
	return makeBitset(size)
}

// NewBitsetFromBytes allocates a new bitset of size bits and copies the contents
// of `buf`. Buf may be nil and size must be >= zero.
func NewBitsetFromBytes(buf []byte, size int) *Bitset {
	s := &Bitset{
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

// NewBitsetFromSlice allocates a new bitset and initializes it from decoding
// a hex string. If s is empty or not a valid hex string, the bitset is initially
// empty.
func NewBitsetFromString(s string, size int) *Bitset {
	buf, _ := hex.DecodeString(s)
	for i := range buf {
		buf[i] = bitsetReverseLut256[buf[i]]
	}
	return NewBitsetFromBytes(buf, size)
}

// NewBitsetFromSlice allocates a new bitset and initializes it from boolean values
// in bools. If bools is nil, the bitset is initially empty.
func NewBitsetFromSlice(bools []bool) *Bitset {
	s := &Bitset{
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

// NewBitsetFromIndexes allocates a new bitset and initializes it from
// integer positions representing one bits. If indeces is nil, the bitset
// is initially empty.
func NewBitsetFromIndexes(indexes []int, size int) *Bitset {
	s := makeBitset(size)
	for i := range indexes {
		s.Set(indexes[i])
	}
	s.cnt = len(indexes)
	return s
}

func (s *Bitset) SetFromBytes(buf []byte, size int) *Bitset {
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
	return s
}

func makeBitset(size int) *Bitset {
	return &Bitset{
		buf:  make([]byte, bitFieldLen(size)),
		cnt:  0,
		size: size,
	}
}

func (s *Bitset) Clone() *Bitset {
	clone := NewBitset(s.size)
	copy(clone.buf, s.buf)
	clone.cnt = s.cnt
	return clone
}

func (s *Bitset) Copy(b *Bitset) *Bitset {
	if s.size > b.size {
		s.Zero()
	}
	if cap(s.buf) < len(b.buf) {
		s.buf = make([]byte, len(b.buf))
	}
	s.size = b.size
	s.buf = s.buf[:len(b.buf)]
	copy(s.buf, b.buf)
	s.cnt = b.cnt
	return s
}

// Grow resizes the bitset to a new size, either growing or shrinking it.
// Content remains unchanged on grow, when shrinking trailing bits are clipped.
func (s *Bitset) Grow(size int) *Bitset {
	if size < 0 {
		return s
	}
	sz := bitFieldLen(size)
	if s.buf == nil || cap(s.buf) < sz {
		buf := make([]byte, sz, (sz>>defaultBitsetSize+1)<<defaultBitsetSize)
		copy(buf, s.buf)
		s.buf = buf
	} else {
		if size < s.size {
			// clear trailing bytes
			if len(s.buf) > sz {
				s.buf[sz] = 0
				for bp := 1; sz+bp < len(s.buf); bp *= 2 {
					copy(s.buf[sz+bp:], s.buf[sz:sz+bp])
				}
			}
			// clear trailing bits
			if sz > 0 {
				s.buf[sz-1] &= bytemask(size)
			}
			s.cnt = -1
		}
		s.buf = s.buf[:sz]
	}
	s.size = size
	return s
}

// Reset clears the bitset contents and sets its size to zero.
func (s *Bitset) Reset() {
	if len(s.buf) > 0 {
		s.buf[0] = 0
		for bp := 1; bp < len(s.buf); bp *= 2 {
			copy(s.buf[bp:], s.buf[:bp])
		}
	}
	s.size = 0
	s.cnt = 0
	s.buf = s.buf[:0]
}

// Close clears the bitset contents, sets its size to zero and returns it
// to the internal buffer pool. Using the bitset after calling Close is
// illegal.
func (s *Bitset) Close() {
	s.Reset()
	if cap(s.buf) == 1<<defaultBitsetSize {
		bitsetPool.Put(s)
	}
}

func (s *Bitset) And(r *Bitset) *Bitset {
	if s.size == r.size && s.size > 0 {
		if s.cnt == 0 {
			return s
		}
		if r.cnt == 0 {
			s.Zero()
			return s
		}
		bitsetAnd(s.Bytes(), r.Bytes(), s.size)
		s.cnt = -1
	}
	return s
}

func (s *Bitset) AndFlag(r *Bitset) (*Bitset, bool, bool) {
	if s.size == 0 {
		return s, false, true
	}
	if s.size != r.size {
		switch s.cnt {
		case 0:
			return s, false, false
		case s.size:
			return s, true, true
		default:
			return s, true, false
		}
	}
	if s.cnt == 0 {
		return s, false, false
	}
	if r.cnt == 0 {
		s.Zero()
		return s, false, false
	}
	any, all := bitsetAndFlag(s.Bytes(), r.Bytes(), s.size)
	s.cnt = -1
	if !any {
		s.cnt = 0
	} else if all {
		s.cnt = s.size
	}
	return s, any, all
}

func (s *Bitset) AndNot(r *Bitset) *Bitset {
	if s.size == r.size && s.size > 0 {
		if s.size == 0 || s.cnt == 0 {
			return s
		}
		bitsetAndNot(s.Bytes(), r.Bytes(), s.size)
		s.cnt = -1
	}
	return s
}

func (s *Bitset) Or(r *Bitset) *Bitset {
	if s.size == r.size && s.size > 0 {
		if s.cnt == s.size {
			return s
		}
		if r.cnt == r.size {
			s.One()
			return s
		}
		bitsetOr(s.Bytes(), r.Bytes(), s.size)
		s.cnt = -1
	}
	return s
}

func (s *Bitset) OrFlag(r *Bitset) (*Bitset, bool, bool) {
	if s.size == 0 {
		return s, false, true
	}
	if s.size != r.size {
		switch s.cnt {
		case 0:
			return s, false, false
		case s.size:
			return s, true, true
		default:
			return s, true, false
		}
	}
	if s.cnt == s.size {
		return s, true, true
	}
	if r.cnt == r.size {
		s.One()
		return s, true, true
	}
	any, all := bitsetOrFlag(s.Bytes(), r.Bytes(), s.size)
	s.cnt = -1
	if !any {
		s.cnt = 0
	} else if all {
		s.cnt = s.size
	}
	return s, any, all
}

func (s *Bitset) Xor(r *Bitset) *Bitset {
	if s.size == r.size && s.size > 0 {
		bitsetXor(s.Bytes(), r.Bytes(), s.size)
		s.cnt = -1
	}
	return s
}

func (s *Bitset) Neg() *Bitset {
	if s.size == 0 {
		return s
	}
	bitsetNeg(s.Bytes(), s.size)
	if s.cnt >= 0 {
		s.cnt = s.size - s.cnt
	}
	return s
}

func (s *Bitset) One() *Bitset {
	if s.size == 0 {
		return s
	}
	s.cnt = s.size
	s.buf[0] = 0xff
	for bp := 1; bp < len(s.buf); bp *= 2 {
		copy(s.buf[bp:], s.buf[:bp])
	}
	s.buf[len(s.buf)-1] = bytemask(s.size)
	return s
}

func (s *Bitset) Zero() *Bitset {
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

func (s *Bitset) Fill(b byte) *Bitset {
	s.buf[0] = b
	for bp := 1; bp < len(s.buf); bp *= 2 {
		copy(s.buf[bp:], s.buf[:bp])
	}
	s.buf[len(s.buf)-1] &= bytemask(s.size)
	s.cnt = -1
	return s
}

func (s *Bitset) Set(i int) *Bitset {
	if i < 0 || i >= s.size {
		return s
	}
	mask := bitmask(i)
	if s.cnt >= 0 && s.buf[i>>3]&mask == 0 {
		s.cnt++
	}
	s.buf[i>>3] |= mask
	return s
}

func (s *Bitset) setbit(i int) {
	mask := bitmask(i)
	s.buf[i>>3] |= mask
}

func (s *Bitset) Clear(i int) *Bitset {
	if i < 0 || i >= s.size {
		return s
	}
	mask := bitmask(i)
	if s.cnt > 0 && s.buf[i>>3]&mask > 0 {
		s.cnt--
	}
	s.buf[i>>3] &^= mask
	return s
}

func (s *Bitset) clearbit(i int) {
	mask := bitmask(i)
	s.buf[i>>3] &^= mask
}

func (s *Bitset) IsSet(i int) bool {
	if i < 0 || i >= s.size {
		return false
	}
	mask := bitmask(i)
	return (s.buf[i>>3] & mask) > 0
}

// Insert inserts srcLen values from position srcPos in bitset src into the
// bitset at position dstPos and moves all values following dstPos behind the
// newly inserted bits
func (s *Bitset) Insert(src *Bitset, srcPos, srcLen, dstPos int) *Bitset {
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
		var cnt int
		for i, v := range src.SubSlice(srcPos, srcLen) {
			if !v {
				s.clearbit(i + dstPos)
			} else {
				s.setbit(i + dstPos)
				cnt++
			}
		}
		if s.cnt >= 0 {
			s.cnt += cnt
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
func (s *Bitset) Replace(src *Bitset, srcPos, srcLen, dstPos int) *Bitset {
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
func (s *Bitset) Append(src *Bitset, srcPos, srcLen int) *Bitset {
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

func (s *Bitset) Delete(pos, n int) *Bitset {
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

func (s *Bitset) Swap(i, j int) {
	if uint(i) >= uint(s.size) || uint(j) >= uint(s.size) {
		return
	}
	m_i := bitmask(i)
	m_j := bitmask(j)
	n_i := i >> 3
	n_j := j >> 3
	b_i := (s.buf[n_i] & m_i) > 0
	b_j := (s.buf[n_j] & m_j) > 0
	switch true {
	case b_i == b_j:
		return
	case b_i:
		s.buf[n_i] &^= m_i
		s.buf[n_j] |= m_j
	case b_j:
		s.buf[n_i] |= m_i
		s.buf[n_j] &^= m_j
	}
}

func (s *Bitset) Bytes() []byte {
	if s == nil {
		return nil
	}
	return s.buf
}

func (s *Bitset) String() string {
	src := s.Bytes()
	dst := make([]byte, len(src))
	for i := range src {
		dst[i] = bitsetReverseLut256[src[i]]
	}
	return hex.EncodeToString(dst)
}

func (s *Bitset) Count() int {
	if s.cnt < 0 {
		s.cnt = int(bitsetPopCount(s.buf, s.size))
	}
	return s.cnt
}

func (s *Bitset) ResetCount(n ...int) {
	s.cnt = -1
	if n != nil {
		s.cnt = n[0]
	}
}

func (s Bitset) Len() int {
	return s.size
}

func (s Bitset) Cap() int {
	return cap(s.buf) * 8
}

func (s Bitset) HeapSize() int {
	return cap(s.buf) + 24 + 16 + 1
}

func (s Bitset) EncodedSize() int {
	sz := s.size / 8
	if s.size&7 > 0 {
		sz++
	}
	return sz
}

// Run returns the index and length of the next consecutive
// run of 1s in the bit vector starting at index. When no more
// 1s exist after index, -1 and a length of 0 is returned.
func (b Bitset) Run(index int) (int, int) {
	return bitsetRun(b.buf, index, b.size)
}

// Indexes returns a slice of indexes for one bits in the bitset.
func (s Bitset) Indexes(slice []int) []int {
	cnt := s.cnt
	switch {
	case cnt == 0:
		return slice[:0]
	case cnt < 0:
		cnt = s.size
	}
	if slice == nil || cap(slice) < cnt {
		slice = make([]int, cnt)
	} else {
		slice = slice[:cnt]
	}
	var j int
	for i, l := 0, s.size-s.size%8; i < l; i += 8 {
		b := s.buf[i>>3]
		for l := 0; b > 0; b, l = b>>1, l+1 {
			if b&0x01 == 0 {
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
	return slice[:j]
}

// IndexesU32 returns a slice positions as uint32 for one bits in the bitset.
func (s *Bitset) IndexesU32(slice []uint32) []uint32 {
	cnt := s.cnt
	switch {
	case cnt == 0:
		return slice[:0]
	case cnt < 0:
		cnt = s.size
	}
	// ensure slice dimension is multiple of 8, we need this for our
	// index lookup algo which always writes multiples of 8 entries
	cnt = roundUpPow2(cnt, 8)
	if slice == nil || cap(slice) < cnt {
		slice = make([]uint32, cnt)
	} else {
		slice = slice[:cnt]
	}
	n := bitsetIndexes(s.buf, s.size, slice)
	return slice[:n]
}

// Slice returns a boolean slice containing all values
func (s Bitset) Slice() []bool {
	res := make([]bool, s.size)
	for i, l := 0, s.size-s.size%8; i < l; i += 8 {
		b := s.buf[i>>3]
		res[i] = b&0x01 > 0
		res[i+1] = b&0x02 > 0
		res[i+2] = b&0x04 > 0
		res[i+3] = b&0x08 > 0
		res[i+4] = b&0x10 > 0
		res[i+5] = b&0x20 > 0
		res[i+6] = b&0x40 > 0
		res[i+7] = b&0x80 > 0
	}
	// tail
	for i := s.size & ^0x7; i < s.size; i++ {
		res[i] = s.buf[i>>3]&bitmask(i) > 0
	}
	return res
}

func (s Bitset) SubSlice(start, n int) []bool {
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
		res[j] = b&0x01 > 0
		res[j+1] = b&0x02 > 0
		res[j+2] = b&0x04 > 0
		res[j+3] = b&0x08 > 0
		res[j+4] = b&0x10 > 0
		res[j+5] = b&0x20 > 0
		res[j+6] = b&0x40 > 0
		res[j+7] = b&0x80 > 0
	}
	// tail
	for i := start + j; i < start+n; i, j = i+1, j+1 {
		res[j] = s.buf[i>>3]&bitmask(i) > 0
	}
	return res
}

func (s Bitset) MarshalBinary() ([]byte, error) {
	return s.Bytes(), nil
}

func (s *Bitset) UnmarshalBinary(data []byte) error {
	s.buf = make([]byte, len(data))
	copy(s.buf, data)
	s.cnt = -1
	s.size = len(data) * 8
	return nil
}

func (s Bitset) MarshalText() ([]byte, error) {
	str := hex.EncodeToString(s.Bytes())
	return []byte(str), nil
}

func (s *Bitset) UnmarshalText(data []byte) error {
	buf, err := hex.DecodeString(string(data))
	if err != nil {
		return err
	}
	s.buf = buf
	s.cnt = -1
	s.size = len(buf) * 8
	return nil
}
