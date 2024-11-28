// Copyright (c) 2018-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package dedup

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"blockwatch.cc/knoxdb/vec"
)

type FixedByteArray struct {
	sz  int    // item size
	n   int    // item count
	buf []byte // data slice
}

func newFixedByteArray(sz, n int) *FixedByteArray {
	return &FixedByteArray{
		sz:  sz,
		n:   n,
		buf: make([]byte, 0, sz*n),
	}
}

func makeFixedByteArray(sz int, data [][]byte) *FixedByteArray {
	a := &FixedByteArray{
		sz:  sz,
		n:   len(data),
		buf: make([]byte, sz*len(data), sz*len(data)),
	}
	for i, v := range data {
		copy(a.buf[i*sz:(i+1)*sz], v)
	}
	return a
}

func (a *FixedByteArray) Len() int {
	return a.n
}

func (a *FixedByteArray) Cap() int {
	if a.sz == 0 {
		return 0
	}
	return cap(a.buf) / a.sz
}

func (a *FixedByteArray) Elem(index int) []byte {
	if len(a.buf) == 0 {
		return []byte{}
	}
	return a.buf[index*a.sz : (index+1)*a.sz]
}

func (a *FixedByteArray) Grow(int) ByteArray {
	panic("fixed: Grow unsupported")
}

func (a *FixedByteArray) Set(int, []byte) {
	panic("fixed: Set unsupported")
}

func (a *FixedByteArray) SetZeroCopy(int, []byte) {
	panic("fixed: Set unsupported")
}

func (a *FixedByteArray) Append(...[]byte) ByteArray {
	panic("fixed: Append unsupported")
}

func (a *FixedByteArray) AppendZeroCopy(...[]byte) ByteArray {
	panic("fixed: Append unsupported")
}

func (a *FixedByteArray) AppendFrom(ByteArray) ByteArray {
	panic("fixed: AppendFrom unsupported")
}

func (a *FixedByteArray) Insert(int, ...[]byte) ByteArray {
	panic("fixed: Insert unsupported")
}

func (a *FixedByteArray) InsertFrom(int, ByteArray) ByteArray {
	panic("fixed: InsertFrom unsupported")
}

func (a *FixedByteArray) Copy(ByteArray, int, int, int) ByteArray {
	panic("fixed: Copy unsupported")
}

func (a *FixedByteArray) Delete(int, int) ByteArray {
	panic("fixed: Delete unsupported")
}

func (a *FixedByteArray) Clear() {
	a.buf = a.buf[:0]
	a.sz = 0
	a.n = 0
}

func (a *FixedByteArray) Release() {
	a.Clear()
	a.buf = nil
}

func (a *FixedByteArray) Slice() [][]byte {
	return toSlice(a)
}

func (a *FixedByteArray) Subslice(start, end int) [][]byte {
	return toSubSlice(a, start, end)
}

func (a *FixedByteArray) MinMax() ([]byte, []byte) {
	return minMax(a)
}

func (a *FixedByteArray) MaxEncodedSize() int {
	return 1 + 4 + len(a.buf)
}

func (a *FixedByteArray) HeapSize() int {
	return fixedByteArraySz + len(a.buf)
}

func (a *FixedByteArray) WriteTo(w io.Writer) (int64, error) {
	count := 1
	w.Write([]byte{bytesFixedFormat << 4})

	// write element count
	var num [binary.MaxVarintLen64]byte
	l := binary.PutUvarint(num[:], uint64(a.n))
	w.Write(num[:l])
	count += l

	// write data
	w.Write(a.buf)
	count += len(a.buf)
	return int64(count), nil
}

func (a *FixedByteArray) Decode(buf []byte) error {
	if len(buf) == 0 {
		return nil
	}

	// check the encoding type
	if buf[0] != byte(bytesFixedFormat<<4) {
		return fmt.Errorf("fixed: reading header: %w", errUnexpectedFormat)
	}

	// skip the encoding type
	buf = buf[1:]

	// read element count
	count, n := binary.Uvarint(buf)
	if n <= 0 {
		return fmt.Errorf("fixed: reading element count: %w", errInvalidLength)
	}
	buf = buf[n:]
	a.n = int(count)
	a.sz = 0
	if a.n > 0 {
		a.sz = len(buf) / a.n
	}

	// copy the rest of our input buffer to avoid referencing memory
	if cap(a.buf) < len(buf) {
		a.buf = make([]byte, 0, len(buf))
	}
	a.buf = a.buf[:len(buf)]
	copy(a.buf, buf)
	return nil
}

func (a *FixedByteArray) Materialize() ByteArray {
	// copy to avoid referencing memory
	ss := a.Slice()
	for i, v := range ss {
		buf := make([]byte, len(v))
		copy(buf, v)
		ss[i] = buf
	}
	return newNativeByteArrayFromBytes(ss)
}

func (a *FixedByteArray) IsMaterialized() bool {
	return false
}

func (a *FixedByteArray) Optimize() ByteArray {
	return a
}

func (a *FixedByteArray) IsOptimized() bool {
	return true
}

func (a *FixedByteArray) Less(i, j int) bool {
	return bytes.Compare(a.Elem(i), a.Elem(j)) < 0
}

func (a *FixedByteArray) Swap(i, j int) {
	l, r := i*a.sz, j*a.sz
	for k := 0; k < a.sz; k++ {
		a.buf[l+k], a.buf[r+k] = a.buf[r+k], a.buf[l+k]
	}
}

func (a *FixedByteArray) MatchEqual(val []byte, bits, mask *vec.Bitset) *vec.Bitset {
	return matchEqual(a, val, bits, mask)
}

func (a *FixedByteArray) MatchNotEqual(val []byte, bits, mask *vec.Bitset) *vec.Bitset {
	return matchNotEqual(a, val, bits, mask)
}

func (a *FixedByteArray) MatchLessThan(val []byte, bits, mask *vec.Bitset) *vec.Bitset {
	return matchLessThan(a, val, bits, mask)
}

func (a *FixedByteArray) MatchLessThanEqual(val []byte, bits, mask *vec.Bitset) *vec.Bitset {
	return matchLessThanEqual(a, val, bits, mask)
}

func (a *FixedByteArray) MatchGreaterThan(val []byte, bits, mask *vec.Bitset) *vec.Bitset {
	return matchGreaterThan(a, val, bits, mask)
}

func (a *FixedByteArray) MatchGreaterThanEqual(val []byte, bits, mask *vec.Bitset) *vec.Bitset {
	return matchGreaterThanEqual(a, val, bits, mask)
}

func (a *FixedByteArray) MatchBetween(from, to []byte, bits, mask *vec.Bitset) *vec.Bitset {
	return matchBetween(a, from, to, bits, mask)
}
