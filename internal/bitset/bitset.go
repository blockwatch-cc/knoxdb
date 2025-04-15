// Copyright (c) 2023-2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package bitset

import (
	"encoding/hex"
	"io"
	"math/bits"
	"sync"

	"blockwatch.cc/knoxdb/internal/arena"
)

var bitsetPool = sync.Pool{
	New: func() any { return &Bitset{} },
}

type Bitset struct {
	buf     []byte
	cnt     int
	size    int
	noclose bool
}

// NewBitset allocates a new Bitset with a custom size and default capacity similar
// to the next power of 2. Call Close() to return the bitset after use.
func NewBitset(size int) *Bitset {
	sz := bitFieldLen(size)
	s := bitsetPool.Get().(*Bitset)
	s.buf = arena.AllocBytes(sz)[:sz]
	clear(s.buf)
	s.cnt = 0
	s.size = size
	return s
}

// FromBuffer references a pre-allocated byte slice.
func FromBuffer(buf []byte, sz int) *Bitset {
	if sz == 0 {
		sz = len(buf) << 3
	}
	buf = buf[:(sz+7)>>3]
	if sz%8 > 0 {
		buf[len(buf)-1] &= bytemask(sz)
	}
	return &Bitset{
		buf:     buf,
		cnt:     -1,
		size:    sz,
		noclose: true,
	}
}

func (s *Bitset) Count() int {
	if s.cnt < 0 {
		s.cnt = int(bitsetPopCount(s.buf, s.size))
	}
	return s.cnt
}

// MinMax returns the indices of the first and last bit set or -1 when no bits are set.
func (s Bitset) MinMax() (int, int) {
	if s.None() {
		return -1, -1
	}
	if s.All() {
		return 0, s.size
	}
	return bitsetMinMax(s.buf, s.size)
}

// All returns true if all bits are set, false otherwise. Returns true for
// empty sets.
func (s *Bitset) All() bool {
	return s.Count() == s.size
}

// None returns true if no bit is set, false otherwise. Returns true for
// empty sets.
func (s *Bitset) None() bool {
	if s.cnt >= 0 {
		return s.cnt == 0
	}
	if s != nil && s.buf != nil {
		for _, word := range s.buf {
			if word > 0 {
				return false
			}
		}
	}
	return true
}

// Any returns true if any bit is set, false otherwise
func (s *Bitset) Any() bool {
	return !s.None()
}

func (s *Bitset) ReadFrom(r io.Reader) (int64, error) {
	n, err := io.ReadFull(r, s.buf)
	return int64(n), err
}

func (s *Bitset) Clone() *Bitset {
	clone := NewBitset(s.size)
	copy(clone.buf, s.buf)
	clone.cnt = s.cnt
	return clone
}

func (s *Bitset) Copy(b *Bitset) *Bitset {
	if s.size > b.size {
		clear(s.buf[b.size>>3:])
	}
	if cap(s.buf) < len(b.buf) {
		if !s.noclose {
			arena.Free(s.buf)
			s.noclose = false
		}
		s.buf = arena.AllocBytes(len(b.buf))[:len(b.buf)]
	}
	s.size = b.size
	s.buf = s.buf[:len(b.buf)]
	copy(s.buf, b.buf)
	s.cnt = b.cnt
	// ensure the last byte is masked
	if s.size%8 > 0 {
		s.buf[len(s.buf)-1] &= bytemask(s.size)
	}
	return s
}

// Resize resizes the bitset to a new size, either growing or shrinking it.
// Content remains unchanged on grow, when shrinking trailing bits are clipped.
func (s *Bitset) Resize(size int) *Bitset {
	if s == nil {
		return NewBitset(size)
	}
	if size < 0 {
		return s
	}
	sz := bitFieldLen(size)
	if s.buf == nil || cap(s.buf) < sz {
		buf := arena.AllocBytes(sz)[:sz]
		copy(buf, s.buf)
		if !s.noclose {
			arena.Free(s.buf)
			s.noclose = false
		}
		s.buf = buf
	} else if size < s.size && s.cnt != 0 {
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
	s.size = size
	return s
}

// Grow increases the bitset to a new size.
func (s *Bitset) Grow(size int) *Bitset {
	return s.Resize(s.size + size)
}

// Reset clears the bitset contents and sets its size to zero.
func (s *Bitset) Reset() *Bitset {
	if len(s.buf) > 0 && s.cnt != 0 {
		clear(s.buf)
	}
	s.size = 0
	s.cnt = 0
	s.buf = s.buf[:0]
	return s
}

// Close resets size to zero and returns the internal buffer back to
// the allocator. For efficiency the contents is not cleared and should be
// on allocation. Using the bitset after calling Close is illegal.
func (s *Bitset) Close() {
	if s == nil {
		return
	}
	if !s.noclose {
		arena.Free(s.buf)
		s.noclose = false
	}
	s.buf = nil
	s.cnt = 0
	s.size = 0
	bitsetPool.Put(s)
}

func (s *Bitset) And(r *Bitset) *Bitset {
	if s.size == r.size && s.size > 0 {
		if s.cnt == 0 {
			return s
		}
		if r.cnt == 0 {
			return s.Zero()
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
		return s.Zero(), false, false
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
	clear(s.buf)
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
	s.buf[i>>3] |= bitmask[i%8]
	s.cnt = -1
	return s
}

func (s *Bitset) SetFromBytes(buf []byte, size int, reverse bool) *Bitset {
	l := bitFieldLen(size)
	if cap(s.buf) < l {
		if !s.noclose {
			arena.Free(s.buf)
			s.noclose = false
		}
		s.buf = arena.AllocBytes(l)[:l]
	} else if s.size > size && s.cnt >= 0 {
		s.cnt = -1
		clear(s.buf[size>>3:])
	}
	s.size = size
	s.buf = s.buf[:l]
	copy(s.buf, buf)
	if reverse {
		for i, v := range s.buf {
			s.buf[i] = reverseLut256[v]
		}
	}
	s.cnt = -1
	// ensure the last byte is masked
	if size%8 > 0 {
		s.buf[l-1] &= bytemask(size)
	}
	return s
}

func (s *Bitset) SetRange(start, end int) *Bitset {
	x, y := start/8, end/8

	// short range within same byte
	if x == y {
		var b byte
		for i := start % 8; i <= end%8; i++ {
			b |= 1 << i
		}
		s.buf[x] |= b
		s.cnt = -1
		return s
	}

	// long range across bytes

	// write start byte
	if start%8 > 0 {
		// mask start byte
		s.buf[x] |= 255 << (start % 8)
		x++
	}

	// write end byte
	if end%8 != 7 {
		// mask end byte
		s.buf[y] |= 2<<(end%8) - 1
		y--
	}

	// write intermediate bytes if any
	for i := x; i <= y; i++ {
		s.buf[i] = 0xff
	}

	// reset count
	s.cnt = -1

	return s
}

func (s *Bitset) setbit(i int) {
	s.buf[i>>3] |= bitmask[i%8]
}

func (s *Bitset) Clear(i int) *Bitset {
	if i < 0 || i >= s.size {
		return s
	}
	mask := bitmask[i%8]
	if s.cnt > 0 && s.buf[i>>3]&mask > 0 {
		s.cnt--
	}
	s.buf[i>>3] &^= mask
	return s
}

func (s *Bitset) clearbit(i int) {
	s.buf[i>>3] &^= bitmask[i%8]
}

func (s *Bitset) IsSet(i int) bool {
	if i < 0 || i >= s.size {
		return false
	}
	return (s.buf[i>>3] & bitmask[i%8]) > 0
}

// Append grows bitset by 1 and sets the trailing bit to val
func (s *Bitset) Append(val bool) *Bitset {
	s.Grow(1)
	if val {
		s.setbit(s.size - 1)
	}
	return s
}

// AppendFrom grows the bitset by srcLen and appends srcLen values from
// src starting at position srcPos.
func (s *Bitset) AppendFrom(src *Bitset, srcPos, srcLen int) *Bitset {
	if srcLen <= 0 {
		return s
	}
	// clamp srcLen
	if srcPos+srcLen > src.size {
		srcLen = src.size - srcPos
	}

	end := s.size
	cnt := s.cnt
	s.Resize(s.size + srcLen)
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
	s.Resize(s.size - n)
	return s
}

func (s *Bitset) Bytes() []byte {
	if s == nil {
		return nil
	}
	return s.buf[:(s.size+7)>>3]
}

func (s *Bitset) String() string {
	src := s.Bytes()
	dst := make([]byte, len(src))
	for i := range src {
		dst[i] = reverseLut256[src[i]]
	}
	return hex.EncodeToString(dst)
}

func (s *Bitset) ResetCount(n int) {
	s.cnt = n
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

// Iterate returns multiple next bits set starting at the specified index
// and up to cap(buf). Use this method when low memory is a priority.
// Iterate returns a zero length slice when no more set bits are found.
//
//	buf := make([]int, 256) // alloc once and reuse
//	n := int(0)
//	n, buffer = bitmap.Iterate(n, buf)
//	for ; len(buf) > 0; n, buf = bitmap.Iterate(n, buf) {
//	 for k := range buf {
//	  do something with buf[k]
//	 }
//	 n += 1
//	}
//
// It is possible to retrieve all set bits as follow:
//
//	indices := make([]int, bitmap.Count())
//	bitmap.Iterate(0, indices)
//
// However, a faster method is [Bitset.Indexes] with a pre-allocated result.
func (s *Bitset) Iterate(i int, buf []int) (int, []int) {
	capacity := cap(buf)
	result := buf[:capacity]

	x := i >> 3
	if x >= len(s.buf) || capacity == 0 {
		return 0, result[:0]
	}

	// process first (partial) word
	word := s.buf[x] >> (i & 7)

	size := 0
	for word != 0 {
		result[size] = i + bits.TrailingZeros8(word)

		size++
		if size == capacity {
			return result[size-1], result[:size]
		}

		// clear the rightmost set bit
		word &= word - 1
	}

	// process the following full words
	// x < len(b.set), no out-of-bounds panic in following slice expression
	x++
	for idx, word := range s.buf[x:] {
		for word != 0 {
			result[size] = (x+idx)<<3 + bits.TrailingZeros8(word)

			size++
			if size == capacity {
				return result[size-1], result[:size]
			}

			// clear the rightmost set bit
			word &= word - 1
		}
	}

	if size > 0 {
		return result[size-1], result[:size]
	}
	return 0, result[:0]
}

// Indexes returns a slice positions as uint32 for one bits in the bitset.
func (s Bitset) Indexes(result []uint32) []uint32 {
	cnt := s.cnt
	switch {
	case cnt == 0:
		return result[:0]
	case cnt < 0:
		cnt = s.size
	}
	// ensure slice dimension is multiple of 8, we need this for our
	// index lookup algo which always writes multiples of 8 entries
	cnt = roundUpPow2(cnt, 8)
	if result == nil || cap(result) < cnt {
		result = make([]uint32, cnt)
	} else {
		result = result[:cnt]
	}
	n := bitsetIndexes(s.buf, s.size, result)
	return result[:n]
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
		res[i] = s.buf[i>>3]&bitmask[i%8] > 0
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
		res[j] = s.buf[i>>3]&bitmask[i%8] > 0
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
		res[j] = s.buf[i>>3]&bitmask[i%8] > 0
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
	return []byte(s.String()), nil
}

func (s *Bitset) UnmarshalText(data []byte) error {
	buf, err := hex.DecodeString(string(data))
	if err != nil {
		return err
	}
	for i := range buf {
		buf[i] = reverseLut256[buf[i]]
	}
	s.buf = buf
	s.cnt = -1
	s.size = len(buf) * 8
	return nil
}
