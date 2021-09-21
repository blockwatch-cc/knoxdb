// Copyright (c) 2018-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package dedup

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"blockwatch.cc/knoxdb/encoding/compress"
	"blockwatch.cc/knoxdb/vec"
)

type DictByteArray struct {
	dict []byte
	offs []int32 // dict entry offsets
	size []int32 // dict entry sizes
	ptr  []byte  // dict pointers
	log2 int     // log2 ptr len
	n    int     // number of items
}

func newDictByteArray(sz, card, n int) *DictByteArray {
	return &DictByteArray{
		dict: make([]byte, 0, sz),
		offs: make([]int32, 0, card),
		size: make([]int32, 0, card),
		ptr:  make([]byte, log2up(card)*n/8+1),
		log2: log2up(card),
		n:    n,
	}
}

func makeDictByteArray(sz, card int, data [][]byte, dupmap []int) *DictByteArray {
	a := &DictByteArray{
		dict: make([]byte, 0, sz),
		offs: make([]int32, 0, card),
		size: make([]int32, 0, card),
		ptr:  make([]byte, log2up(card)*len(data)/8+1),
		log2: log2up(card),
		n:    len(data),
	}
	for i, v := range data {
		k := dupmap[i]
		if k < 0 {
			// append non duplicate to dict
			pack(a.ptr, i, len(a.offs), a.log2)
			a.offs = append(a.offs, int32(len(a.dict)))
			a.size = append(a.size, int32(len(v)))
			a.dict = append(a.dict, v...)
		} else if k > 0 {
			// reference as duplicate, only write when >0
			pack(a.ptr, i, k, a.log2)
		}
	}
	return a
}

func (a *DictByteArray) Len() int {
	return a.n
}

func (a *DictByteArray) Cap() int {
	return cap(a.ptr) * 8 / a.log2
}

func (a *DictByteArray) Elem(index int) []byte {
	ptr := unpack(a.ptr, index, a.log2)
	return a.dict[a.offs[ptr] : a.offs[ptr]+a.size[ptr]]
}

func (a *DictByteArray) Set(index int, buf []byte) {
	// unsupported
	panic("dict: Set unsupported")
}

func (a *DictByteArray) Append(...[]byte) ByteArray {
	// unsupported
	panic("dict: Append unsupported")
	return nil
}

func (a *DictByteArray) AppendFrom(src ByteArray) ByteArray {
	// unsupported
	panic("dict: AppendFrom unsupported")
	return nil
}

func (a *DictByteArray) Insert(index int, buf ...[]byte) ByteArray {
	// unsupported
	panic("dict: Insert unsupported")
	return a
}

func (a *DictByteArray) InsertFrom(index int, src ByteArray) ByteArray {
	// unsupported
	panic("dict: InsertFrom unsupported")
	return a
}

func (a *DictByteArray) Copy(src ByteArray, dstPos, srcPos, n int) ByteArray {
	// unsupported
	panic("dict: Copy unsupported")
	return a
}

func (a *DictByteArray) Delete(index, n int) ByteArray {
	// unsupported
	panic("dict: Delete unsupported")
	return a
}

func (a *DictByteArray) Clear() {
	a.dict = a.dict[:0]
	a.offs = a.offs[:0]
	a.size = a.size[:0]
	a.ptr = a.ptr[:0]
	a.log2 = 0
	a.n = 0
}

func (a *DictByteArray) Release() {
	a.Clear()
	a.dict = nil
	a.offs = nil
	a.size = nil
	a.ptr = nil
}

func (a *DictByteArray) Slice() [][]byte {
	return toSlice(a)
}

func (a *DictByteArray) Subslice(start, end int) [][]byte {
	return toSubSlice(a, start, end)
}

func (a *DictByteArray) MinMax() ([]byte, []byte) {
	return minMax(a)
}

func (a *DictByteArray) MaxEncodedSize() int {
	return 1 + 3*4 + len(a.dict) + len(a.offs)*4 + len(a.ptr)
}

func (a *DictByteArray) HeapSize() int {
	return dictByteArraySz + len(a.dict) + len(a.offs)*8 + len(a.ptr)
}

func (a *DictByteArray) WriteTo(w io.Writer) (int, error) {
	w.Write([]byte{bytesDictFormat << 4})
	count := 1

	// write len in elements
	var num [binary.MaxVarintLen64]byte
	l := binary.PutUvarint(num[:], uint64(a.n))
	w.Write(num[:l])
	count += l

	// write log2
	l = binary.PutUvarint(num[:], uint64(a.log2))
	w.Write(num[:l])
	count += l

	// write dict len in elements
	l = binary.PutUvarint(num[:], uint64(len(a.offs)))
	w.Write(num[:l])
	count += l

	// prepare and write offsets (sizes can be reconstructed)
	scratch := make([]int64, len(a.offs))
	for i, v := range a.offs {
		scratch[i] = int64(v)
	}
	olen, err := compress.IntegerArrayEncodeAll(scratch, w)
	if err != nil {
		return count, err
	}
	count += olen

	// write dict
	l = binary.PutUvarint(num[:], uint64(len(a.dict)))
	w.Write(num[:l])
	count += l
	w.Write(a.dict)
	count += len(a.dict)

	// write ptr
	l = binary.PutUvarint(num[:], uint64(len(a.ptr)))
	w.Write(num[:l])
	count += l
	w.Write(a.ptr)
	count += len(a.ptr)

	// write compressed offset length last
	binary.BigEndian.PutUint32(num[:], uint32(olen))
	w.Write(num[:4])
	count += 4

	return count, nil
}

func (a *DictByteArray) Decode(buf []byte) error {
	if len(buf) == 0 {
		return nil
	}

	// check the encoding type
	if buf[0] != byte(bytesDictFormat<<4) {
		return fmt.Errorf("dict: reading header: %w", errUnexpectedFormat)
	}

	// skip the encoding type
	buf = buf[1:]

	// read len in elements
	val, n := binary.Uvarint(buf)
	if n <= 0 {
		return fmt.Errorf("dict: reading count: %w", errInvalidLength)
	}
	buf = buf[n:]
	a.n = int(val)

	// read log2 in elements
	val, n = binary.Uvarint(buf)
	if n <= 0 {
		return fmt.Errorf("dict: reading log2: %w", errInvalidLength)
	}
	buf = buf[n:]
	a.log2 = int(val)

	// read dict len in entries
	val, n = binary.Uvarint(buf)
	if n <= 0 {
		return fmt.Errorf("dict: reading dict len: %w", errInvalidLength)
	}
	buf = buf[n:]

	// ensure slices size
	if cap(a.offs) < int(val) {
		a.offs = make([]int32, 0, int(val))
		a.size = make([]int32, 0, int(val))
	}
	a.offs = a.offs[:int(val)]
	a.size = a.size[:int(val)]
	scratch := make([]int64, int(val))

	// read compressed offs and size array lengths (stored at end of buffer)
	if len(buf) < 4 {
		return fmt.Errorf("dict: reading offset len: %w", errShortBuffer)
	}
	olen := int(binary.BigEndian.Uint32(buf[len(buf)-4:]))
	buf = buf[:len(buf)-4]

	// unpack offsets and reconstruct sizes (offsets are guaranteed to be
	// strictly monotonic)
	if len(buf) < olen {
		return fmt.Errorf("dict: reading offsets have=%d want=%d: %w", len(buf), olen, errShortBuffer)
	}

	var err error
	scratch, err = compress.IntegerArrayDecodeAll(buf[:olen], scratch)
	if err != nil {
		return fmt.Errorf("dict: decoding offsets: %w", err)
	}
	for i, v := range scratch {
		a.offs[i] = int32(v)
		if i > 0 {
			a.size[i-1] = a.offs[i] - a.offs[i-1]
		}
	}
	buf = buf[olen:]

	// read dict size and dict slice
	val, n = binary.Uvarint(buf)
	if n <= 0 {
		return fmt.Errorf("dict: reading dict size: %w", errInvalidLength)
	}
	if len(buf) < int(val) {
		return fmt.Errorf("dict: reading dict data: %w", errShortBuffer)
	}
	buf = buf[n:]
	if cap(a.dict) < int(val) {
		a.dict = make([]byte, 0, int(val))
	}
	a.dict = a.dict[:int(val)]
	copy(a.dict, buf)
	buf = buf[int(val):]
	// set last size val
	a.size[len(a.size)-1] = int32(val) - a.offs[len(a.size)-1]

	// read ptr size and ptr slice
	val, n = binary.Uvarint(buf)
	if n <= 0 {
		return fmt.Errorf("dict: reading ptr len: %w", errInvalidLength)
	}
	if len(buf) < int(val) {
		return fmt.Errorf("dict: reading ptr data: %w", errShortBuffer)
	}
	buf = buf[n:]
	if cap(a.ptr) < int(val) {
		a.ptr = make([]byte, 0, int(val))
	}
	a.ptr = a.ptr[:int(val)]
	copy(a.ptr, buf)

	return nil
}

func (a *DictByteArray) Materialize() ByteArray {
	// copy to avoid referencing memory
	ss := a.Slice()
	for i, v := range ss {
		buf := make([]byte, len(v))
		copy(buf, v)
		ss[i] = buf
	}
	return newNativeByteArrayFromBytes(ss)
}

func (a *DictByteArray) IsMaterialized() bool {
	return false
}

func (a *DictByteArray) Optimize() ByteArray {
	return a
}

func (a *DictByteArray) IsOptimized() bool {
	return true
}

func (a *DictByteArray) Less(i, j int) bool {
	return bytes.Compare(a.Elem(i), a.Elem(j)) < 0
}

func (a *DictByteArray) Swap(i, j int) {
	pi := unpack(a.ptr, i, a.log2)
	pj := unpack(a.ptr, j, a.log2)
	pack(a.ptr, i, pj, a.log2)
	pack(a.ptr, j, pi, a.log2)
}

func (a *DictByteArray) MatchEqual(val []byte, bits, mask *vec.Bitset) *vec.Bitset {
	return matchEqual(a, val, bits, mask)
}

func (a *DictByteArray) MatchNotEqual(val []byte, bits, mask *vec.Bitset) *vec.Bitset {
	return matchNotEqual(a, val, bits, mask)
}

func (a *DictByteArray) MatchLessThan(val []byte, bits, mask *vec.Bitset) *vec.Bitset {
	return matchLessThan(a, val, bits, mask)
}

func (a *DictByteArray) MatchLessThanEqual(val []byte, bits, mask *vec.Bitset) *vec.Bitset {
	return matchLessThanEqual(a, val, bits, mask)
}

func (a *DictByteArray) MatchGreaterThan(val []byte, bits, mask *vec.Bitset) *vec.Bitset {
	return matchGreaterThan(a, val, bits, mask)
}

func (a *DictByteArray) MatchGreaterThanEqual(val []byte, bits, mask *vec.Bitset) *vec.Bitset {
	return matchGreaterThanEqual(a, val, bits, mask)
}

func (a *DictByteArray) MatchBetween(from, to []byte, bits, mask *vec.Bitset) *vec.Bitset {
	return matchBetween(a, from, to, bits, mask)
}
