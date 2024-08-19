// Copyright (c) 2023 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package bitset

func (s *Bitset) Reverse() *ReverseBitset {
	r := &ReverseBitset{
		buf:  make([]byte, len(s.buf)),
		cnt:  -1,
		size: s.size,
	}
	copy(r.buf, s.buf)
	bitsetReverse(r.buf)
	return r
}

type ReverseBitset struct {
	buf  []byte
	cnt  int
	size int
}

func (r *ReverseBitset) Close() {
	if len(r.buf) > 0 {
		r.buf[0] = 0
		for bp := 1; bp < len(r.buf); bp *= 2 {
			copy(r.buf[bp:], r.buf[:bp])
		}
	}
	r.size = 0
	r.cnt = 0
	r.buf = r.buf[:0]
}

func (r *ReverseBitset) Bytes() []byte {
	if r == nil {
		return nil
	}
	return r.buf
}

func (r *ReverseBitset) Count() int {
	if r.cnt < 0 {
		r.cnt = int(bitsetPopCount(r.buf, r.size))
	}
	return r.cnt
}

func (r ReverseBitset) Len() int {
	return r.size
}

func (r ReverseBitset) Cap() int {
	return cap(r.buf) * 8
}

// Runs through an reversed bitset. You have to reverse it yourself
// with Reverse function.
// returns the index and length of the next consecutive
// run of 1s in the bit vector starting at index. When no more
// 1s exist after index, -1 and a length of 0 is returned.
func (b ReverseBitset) Run(index int) (int, int) {
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
