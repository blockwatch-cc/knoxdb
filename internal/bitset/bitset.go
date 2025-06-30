// Copyright (c) 2023-2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package bitset

import (
	"encoding/binary"
	"encoding/hex"
	"io"
	"sync"

	"blockwatch.cc/knoxdb/internal/arena"
	"golang.org/x/exp/constraints"
)

// ensure we implement required interfaces
var _ BitmapAccessor = (*Bitset)(nil)

var bitsetPool = sync.Pool{
	New: func() any { return &Bitset{} },
}

type Bitset struct {
	buf     []byte
	cnt     int
	size    int
	noclose bool
}

// New allocates a new Bitset with a custom size and default capacity similar
// to the next power of 2. Call Close() to return the bitset after use.
func New(size int) *Bitset {
	sz := bitFieldLen(size)
	s := bitsetPool.Get().(*Bitset)
	s.buf = arena.AllocBytes(sz)[:sz]
	clear(s.buf)
	s.cnt = 0
	s.size = size
	return s
}

// NewFromBytes references a pre-allocated byte slice.
func NewFromBytes(buf []byte, sz int) *Bitset {
	if sz == 0 {
		sz = len(buf) << 3
	}
	buf = buf[:(sz+7)>>3]
	if sz&7 > 0 {
		buf[len(buf)-1] &= bytemask(sz)
	}
	s := bitsetPool.Get().(*Bitset)
	s.buf = buf
	s.cnt = -1
	s.size = sz
	s.noclose = true
	return s
}

// NewFromIndexes creates a new bitset and sets indexed positions.
func NewFromIndexes[T constraints.Integer](idxs []T) *Bitset {
	if len(idxs) == 0 {
		return New(0)
	}
	s := New(int(idxs[len(idxs)-1]) + 1)
	for _, i := range idxs {
		s.setbit(int(i))
	}
	s.cnt = len(idxs)
	return s
}

// Clear clears the bitset contents and sets its size to zero.
func (s *Bitset) Clear() {
	if len(s.buf) > 0 && s.cnt != 0 {
		clear(s.buf)
	}
	s.size = 0
	s.cnt = 0
	s.buf = s.buf[:0]
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

func (s *Bitset) Set(i int) {
	if i < 0 || i >= s.size {
		return
	}
	s.buf[i>>3] |= bitmask[i&7]
	s.cnt = -1
}

func (s *Bitset) Unset(i int) {
	if i < 0 || i >= s.size {
		return
	}
	mask := bitmask[i&7]
	if s.cnt > 0 && s.buf[i>>3]&mask > 0 {
		s.cnt--
	}
	s.buf[i>>3] &^= mask
}

func (s *Bitset) SetFromBytes(buf []byte, size int, reverse bool) {
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
	if size&7 > 0 {
		s.buf[l-1] &= bytemask(size)
	}
}

// Sets al bits in range. Start and end indices form a closed interval
// [start, end], i.e. boundaries are inclusive.
func (s *Bitset) SetRange(start, end int) {
	if start > s.size {
		return
	}
	// sanitize bounds
	start = max(0, start)
	end = min(s.size-1, end)
	x, y := start>>3, end>>3

	// short range within same byte
	if x == y {
		var b byte
		for i := start & 7; i <= end&7; i++ {
			b |= 1 << i
		}
		s.buf[x] |= b
		s.cnt = -1
		return
	}

	// long range across bytes

	// write start byte
	if start&7 > 0 {
		// mask start byte
		s.buf[x] |= 255 << (start & 7)
		x++
	}

	// write end byte
	if end&7 != 7 {
		// mask end byte
		s.buf[y] |= 2<<(end&7) - 1
		y--
	}

	// write intermediate bytes if any
	for i := x; i <= y; i++ {
		s.buf[i] = 0xff
	}

	// reset count
	s.cnt = -1
}

func (s *Bitset) SetIndexes(idxs []int) *Bitset {
	for _, i := range idxs {
		s.setbit(i)
	}
	s.cnt = -1
	return s
}

func (s *Bitset) Get(i int) bool {
	return s.Contains(i)
}

func (s *Bitset) Cmp(i, j int) int {
	x := (s.buf[i>>3] & bitmask[i&7]) > 0
	y := (s.buf[j>>3] & bitmask[j&7]) > 0
	switch {
	case x == y:
		return 0
	case !x && y:
		return -1
	default:
		return 1
	}
}

func (s *Bitset) Contains(i int) bool {
	if i < 0 || i >= s.size {
		return false
	}
	return (s.buf[i>>3] & bitmask[i&7]) > 0
}

// Returns true when any bit in range is set. Start and end indices form
// a closed interval [start, end], i.e. boundaries are inclusive.
func (s *Bitset) ContainsRange(start, end int) bool {
	if start >= s.size {
		return false
	}
	x, y := start/8, min(end/8, len(s.buf)-1)

	// short range within same byte
	if x == y {
		return s.buf[x]&bytemask(start)&bytemask(end) > 0
	}

	// long range across bytes

	// check masked start byte
	if start&7 > 0 {
		if s.buf[x]&255<<(start&7) > 0 {
			return true
		}
		x++
	}

	// check masked end byte
	if end&7 != 7 {
		if s.buf[y]&(2<<(end&7)-1) > 0 {
			return true
		}
		y--
	}

	// handle two byte range with masks
	if x >= y {
		return false
	}

	// check intermediate bytes
	for x+7 < y {
		if binary.LittleEndian.Uint64(s.buf[x:]) > 0 {
			return true
		}
		x += 8
	}
	for x <= y {
		if s.buf[x] > 0 {
			return true
		}
		x++
	}

	return false
}

func (s *Bitset) Count() int {
	if s.cnt < 0 {
		s.cnt = int(bitsetPopCount(s.buf, s.size))
	}
	return s.cnt
}

// MinMax returns the indices of the first and last bit set or -1 when no bits are set.
func (s *Bitset) MinMax() (int, int) {
	if s.None() {
		return -1, -1
	}
	if s.All() {
		return 0, s.size
	}
	return bitsetMinMax(s.buf, s.size)
}

// All returns true if all bits are set, false otherwise. Returns false for
// empty sets.
func (s *Bitset) All() bool {
	return s.size > 0 && s.Count() == s.size
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

func (s *Bitset) Clone() *Bitset {
	clone := New(s.size)
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
	if s.size&7 > 0 {
		s.buf[len(s.buf)-1] &= bytemask(s.size)
	}
	return s
}

// Resize resizes the bitset to a new size, either growing or shrinking it.
// Content remains unchanged on grow, when shrinking trailing bits are clipped.
func (s *Bitset) Resize(size int) *Bitset {
	if s == nil {
		return New(size)
	}
	if size < 0 {
		return s
	}
	sz := bitFieldLen(size)
	if s.buf == nil || cap(s.buf) < sz {
		buf := arena.AllocBytes(sz)[:sz]
		n := copy(buf, s.buf)
		clear(buf[n:])
		if !s.noclose {
			arena.Free(s.buf)
			s.noclose = false
		}
		s.buf = buf
	} else if size < s.size && s.cnt != 0 {
		// clear trailing bytes
		if len(s.buf) > sz {
			clear(s.buf[sz:])
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

// Append grows bitset by 1 and sets the trailing bit to val
func (s *Bitset) Append(val bool) {
	s.Grow(1)
	if val {
		s.setbit(s.size - 1)
	}
}

// AppendTo appends selected values to dst.
func (s *Bitset) AppendTo(dst *Bitset, sel []uint32) {
	if sel == nil {
		dst.AppendRange(s, 0, s.size)
	} else {
		dst.Grow(len(sel))
		for i, v := range sel {
			if s.Contains(int(v)) {
				dst.Set(i)
			}
		}
	}
}

// AppendRange appends n values from src[i:j] growing s. Range
// indices form a half open interval [i,j) similar to Go slices.
func (s *Bitset) AppendRange(src *Bitset, i, j int) *Bitset {
	// clamp srcLen
	j = min(j, src.size)

	// sanity check
	n := j - i
	if n <= 0 {
		return s
	}

	end := s.size
	s.Resize(s.size + n)

	if end&7+i&7+n&7 == 0 {
		// fast path
		copy(s.buf[end>>3:], src.buf[i>>3:j>>3])
		s.cnt = -1
	} else {
		// slow path
		var (
			tmp  [128]int
			last = i - 1
			cnt  int
			done bool
		)
		for {
			idxs, ok := src.Iterate(last, tmp[:])
			if done || !ok || idxs[0] > j {
				break
			}
			for _, idx := range idxs {
				if idx >= j {
					done = true
					break
				}
				s.setbit(end + idx - i)
				cnt++
			}
			last = idxs[len(idxs)-1]
		}
		if s.cnt >= 0 {
			s.cnt += cnt
		}
	}
	return s
}

// Delete removes n values in range s[i:j] shrinking s and moving
// tail values. Range indices form a half open interval [i,j) similar
// to Go slices. Note deletion changes indices of bits after the
// deleted range.
func (s *Bitset) Delete(i, j int) {
	// clamp
	i = max(0, i)
	j = min(j, s.size)

	if i&7+j&7 == 0 {
		// fast path
		copy(s.buf[i>>3:], s.buf[j>>3:])
	} else {
		// slow path
		for k, v := range s.Slice(j, s.size) {
			if v {
				s.setbit(i + k)
			} else {
				s.clearbit(i + k)
			}
		}
	}

	// shrink and reset counter
	s.Resize(s.size - j + i)
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

func (s *Bitset) Len() int {
	return s.size
}

func (s *Bitset) Cap() int {
	return cap(s.buf) * 8
}

func (s *Bitset) Size() int {
	return cap(s.buf) + 24 + 16 + 1
}

func (s *Bitset) ReadFrom(r io.Reader) (int64, error) {
	n, err := io.ReadFull(r, s.buf)
	return int64(n), err
}

// Slice returns a boolean slices for bits between [i:j]. Indices
// form a half optn interval [i,j) like for Go slices.
func (s *Bitset) Slice(i, j int) []bool {
	i = max(i, 0)
	j = min(j, s.size)

	var (
		n = j - i
		k int
	)
	if n == 0 {
		return nil
	}
	res := make([]bool, n)

	// head
	if i&7 > 0 {
		if word := s.buf[i>>3] >> (i & 7); word > 0 {
			for i&7 > 0 && n > 0 {
				if word&1 > 0 {
					res[k] = true
				}
				word >>= 1
				i++
				k++
				n--
			}
		} else {
			k = 8 - i&7
			i += 8 - i&7
			n -= k
		}
		if n <= 0 {
			return res
		}
	}

	// inner loop
	for n >= 8 {
		word := s.buf[i>>3]
		res[k] = word&0x01 > 0
		res[k+1] = word&0x02 > 0
		res[k+2] = word&0x04 > 0
		res[k+3] = word&0x08 > 0
		res[k+4] = word&0x10 > 0
		res[k+5] = word&0x20 > 0
		res[k+6] = word&0x40 > 0
		res[k+7] = word&0x80 > 0
		k += 8
		i += 8
		n -= 8
	}

	// tail
	if n > 0 {
		if word := s.buf[i>>3]; word > 0 {
			for n > 0 {
				if word&1 > 0 {
					res[k] = true
				}
				word >>= 1
				k++
				n--
			}
		}
	}
	return res
}

func (s *Bitset) MarshalBinary() ([]byte, error) {
	return s.Bytes(), nil
}

func (s *Bitset) UnmarshalBinary(data []byte) error {
	s.buf = make([]byte, len(data))
	copy(s.buf, data)
	s.cnt = -1
	s.size = len(data) * 8
	return nil
}

func (s *Bitset) MarshalText() ([]byte, error) {
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

func (s *Bitset) setbit(i int) {
	s.buf[i>>3] |= bitmask[i&7]
}

func (s *Bitset) clearbit(i int) {
	s.buf[i>>3] &^= bitmask[i&7]
}
