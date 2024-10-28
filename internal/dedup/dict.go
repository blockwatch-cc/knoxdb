// Copyright (c) 2018-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package dedup

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"blockwatch.cc/knoxdb/internal/arena"
	"blockwatch.cc/knoxdb/internal/bitset"
	"blockwatch.cc/knoxdb/internal/zip"
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

func (a *DictByteArray) Grow(int) ByteArray {
	panic("dict: Grow unsupported")
}

func (a *DictByteArray) Set(int, []byte) {
	panic("dict: Set unsupported")
}

func (a *DictByteArray) SetZeroCopy(int, []byte) {
	panic("dict: Set unsupported")
}

func (a *DictByteArray) Append(...[]byte) ByteArray {
	panic("dict: Append unsupported")
}

func (a *DictByteArray) AppendZeroCopy(...[]byte) ByteArray {
	panic("dict: Append unsupported")
}

func (a *DictByteArray) AppendFrom(ByteArray) ByteArray {
	panic("dict: AppendFrom unsupported")
}

func (a *DictByteArray) Insert(int, ...[]byte) ByteArray {
	panic("dict: Insert unsupported")
}

func (a *DictByteArray) InsertFrom(int, ByteArray) ByteArray {
	panic("dict: InsertFrom unsupported")
}

func (a *DictByteArray) Copy(ByteArray, int, int, int) ByteArray {
	panic("dict: Copy unsupported")
}

func (a *DictByteArray) Delete(int, int) ByteArray {
	panic("dict: Delete unsupported")
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

func (a *DictByteArray) WriteTo(w io.Writer) (int64, error) {
	var count int64
	n, err := w.Write([]byte{bytesDictFormat << 4})
	count += int64(n)
	if err != nil {
		return count, fmt.Errorf("dict: writing format %w", err)
	}

	// write len in elements
	err = binary.Write(w, binary.LittleEndian, uint32(a.n))
	if err != nil {
		return count, fmt.Errorf("dict: writing length elements %w", err)
	}
	count += 4

	// write log2
	n, err = w.Write([]byte{byte(a.log2)})
	count += int64(n)
	if err != nil {
		return count, fmt.Errorf("dict: writing log2 %w", err)
	}

	// write dict len in elements
	err = binary.Write(w, binary.LittleEndian, uint32(len(a.offs)))
	if err != nil {
		return count, fmt.Errorf("dict: writing dict length elements %w", err)
	}
	count += 4

	// use scratch buffer
	scratch := arena.Alloc(arena.AllocBytes, zip.Int32EncodedSize(len(a.offs)))
	defer arena.Free(arena.AllocBytes, scratch)
	buf := bytes.NewBuffer(scratch.([]byte)[:0])

	// prepare and write offsets (sizes can be reconstructed)
	olen, err := zip.EncodeInt32(a.offs, buf)
	if err != nil {
		return int64(count), err
	}

	// write compressed offset len
	err = binary.Write(w, binary.LittleEndian, uint32(olen))
	if err != nil {
		return count, fmt.Errorf("dict: writing compressed offset len: %w", err)
	}
	count += 4

	// write compressed offset data
	n, err = w.Write(buf.Bytes())
	count += int64(n)
	if err != nil {
		return count, fmt.Errorf("dict: writing compressed offset data %w", err)
	}

	// write dict
	err = binary.Write(w, binary.LittleEndian, uint32(len(a.dict)))
	if err != nil {
		return count, fmt.Errorf("dict: writing length dict %w", err)
	}
	count += 4

	n, err = w.Write(a.dict)
	count += int64(n)
	if err != nil {
		return count, fmt.Errorf("dict: writing dict %w", err)
	}

	// write ptr
	err = binary.Write(w, binary.LittleEndian, uint32(len(a.ptr)))
	if err != nil {
		return count, fmt.Errorf("dict: writing length ptr %w", err)
	}
	count += 4

	n, err = w.Write(a.ptr)
	count += int64(n)
	if err != nil {
		return count, fmt.Errorf("dict: writing ptr %w", err)
	}

	return int64(count), nil
}

func (a *DictByteArray) ReadFrom(r io.Reader) (int64, error) {
	// read len in elements
	var l uint32
	err := binary.Read(r, binary.LittleEndian, &l)
	if err != nil {
		return 0, fmt.Errorf("dict: reading count: %w", err)
	}
	c := int64(4)
	a.n = int(l)

	// read log2 in elements
	var b byte
	err = binary.Read(r, binary.LittleEndian, &b)
	if err != nil {
		return c, fmt.Errorf("dict: reading log2: %w", err)
	}
	a.log2 = int(b)
	c++

	// read dict len in entries
	err = binary.Read(r, binary.LittleEndian, &l)
	if err != nil {
		return 0, fmt.Errorf("dict: reading dict len: %w", err)
	}
	c += 4
	// ensure slices size
	if cap(a.offs) < int(l) {
		a.offs = make([]int32, 0, int(l))
		a.size = make([]int32, 0, int(l))
	}
	a.offs = a.offs[:int(l)]
	a.size = a.size[:int(l)]

	// read compressed offs and size array lengths (stored at end of buffer)
	err = binary.Read(r, binary.LittleEndian, &l)
	if err != nil {
		return 0, fmt.Errorf("dict: reading offs len: %w", err)
	}
	c += 4

	// alloc scratch space large enough for offs and size encoded data
	scratch := arena.Alloc(arena.AllocBytes, zip.Int32EncodedSize(int(l)))
	defer arena.Free(arena.AllocBytes, scratch)
	buf := scratch.([]byte)[:int(l)]

	// read offs encoded data
	n, err := io.ReadFull(r, buf)
	c += int64(n)
	if err != nil {
		return c, fmt.Errorf("dict: reading offsets: %w", err)
	}

	// decode offs
	n, err = zip.DecodeInt32(a.offs, buf)
	if err != nil {
		return c, fmt.Errorf("dict: decoding offsets: %w", err)
	}
	a.offs = a.offs[:n]

	// reconstruct sizes
	if len(a.offs) > 1 {
		for i, v := range a.offs[1:] {
			a.size[i] = a.offs[i+1] - v
		}
	}

	// read dict size
	err = binary.Read(r, binary.LittleEndian, &l)
	if err != nil {
		return 0, fmt.Errorf("dict: reading dict size: %w", err)
	}
	c += 4

	// ensure dict space
	if cap(a.dict) < int(l) {
		a.dict = make([]byte, 0, int(l))
	}
	a.dict = a.dict[:int(l)]

	// calculate last size val
	if len(a.size) > 0 && len(a.offs) > 0 {
		a.size[len(a.size)-1] = int32(l) - a.offs[len(a.size)-1]
	}

	// read dict data
	n, err = io.ReadFull(r, a.dict)
	c += int64(n)
	if err != nil {
		return c, fmt.Errorf("dict: reading dict data: %w", err)
	}

	// read ptr size and ptr slice
	err = binary.Read(r, binary.LittleEndian, &l)
	if err != nil {
		return 0, fmt.Errorf("dict: reading ptr size: %w", err)
	}
	c += 4
	if cap(a.ptr) < int(l) {
		a.ptr = make([]byte, 0, int(l))
	}
	a.ptr = a.ptr[:int(l)]

	// read ptr data
	n, err = io.ReadFull(r, a.ptr)
	c += int64(n)
	if err != nil {
		return c, fmt.Errorf("dict: reading ptr data: %w", err)
	}

	return c, nil
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
	if len(buf) < 5 {
		return fmt.Errorf("dict: reading count: %w", errInvalidLength)
	}
	l := int(binary.LittleEndian.Uint32(buf))
	buf = buf[4:]
	a.n = l

	// read log2 in elements
	a.log2 = int(buf[0])
	buf = buf[1:]

	// read dict len in entries
	if len(buf) < 5 {
		return fmt.Errorf("dict: reading dict len: %w", errInvalidLength)
	}
	l = int(binary.LittleEndian.Uint32(buf))
	buf = buf[4:]

	// ensure slices size
	if cap(a.offs) < l {
		a.offs = make([]int32, 0, l)
		a.size = make([]int32, 0, l)
	}
	a.offs = a.offs[:l]
	a.size = a.size[:l]

	// read compressed offs and size array lengths (stored at end of buffer)
	if len(buf) < 4 {
		return fmt.Errorf("dict: reading offset len: %w", errShortBuffer)
	}
	olen := int(binary.BigEndian.Uint32(buf))
	buf = buf[4:]

	// unpack offsets and reconstruct sizes (offsets are guaranteed to be
	// strictly monotonic)
	if len(buf) < olen {
		return fmt.Errorf("dict: reading offsets have=%d want=%d: %w", len(buf), olen, errShortBuffer)
	}

	n, err := zip.DecodeInt32(a.offs, buf[:olen])
	if err != nil {
		return fmt.Errorf("dict: decoding offsets: %w", err)
	}
	a.offs = a.offs[:n]
	for i, v := range a.offs[1:] {
		a.size[i] = a.offs[i+1] - v
	}
	buf = buf[olen:]

	// read dict size and dict slice
	if len(buf) < 4 {
		return fmt.Errorf("dict: reading dict size: %w", errShortBuffer)
	}
	l = int(binary.LittleEndian.Uint32(buf))
	buf = buf[4:]
	if len(buf) < l {
		return fmt.Errorf("dict: reading dict data: %w", errShortBuffer)
	}
	if cap(a.dict) < l {
		a.dict = make([]byte, 0, l)
	}
	a.dict = a.dict[:l]
	copy(a.dict, buf)
	buf = buf[l:]
	// set last size val
	a.size[len(a.size)-1] = int32(l) - a.offs[len(a.size)-1]

	// read ptr size and ptr slice
	if len(buf) < 4 {
		return fmt.Errorf("dict: reading ptr len: %w", errShortBuffer)
	}
	l = int(binary.LittleEndian.Uint32(buf))
	buf = buf[4:]
	if len(buf) < l {
		return fmt.Errorf("dict: reading ptr data: %w", errShortBuffer)
	}
	if cap(a.ptr) < l {
		a.ptr = make([]byte, 0, l)
	}
	a.ptr = a.ptr[:l]
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

func (a DictByteArray) ForEach(fn func(int, []byte)) {
	for i := 0; i < a.n; i++ {
		fn(i, a.Elem(i))
	}
}

func (a DictByteArray) ForEachUnique(fn func(int, []byte)) {
	var offs int32
	for i := 0; i < a.n; i++ {
		ptr := unpack(a.ptr, i, a.log2)
		o := a.offs[ptr]
		if o < offs {
			continue
		}
		fn(i, a.dict[o:o+a.size[ptr]])
		offs = o
	}
}

func (a *DictByteArray) MatchEqual(val []byte, bits, mask *bitset.Bitset) *bitset.Bitset {
	return matchEqual(a, val, bits, mask)
}

func (a *DictByteArray) MatchNotEqual(val []byte, bits, mask *bitset.Bitset) *bitset.Bitset {
	return matchNotEqual(a, val, bits, mask)
}

func (a *DictByteArray) MatchLess(val []byte, bits, mask *bitset.Bitset) *bitset.Bitset {
	return matchLess(a, val, bits, mask)
}

func (a *DictByteArray) MatchLessEqual(val []byte, bits, mask *bitset.Bitset) *bitset.Bitset {
	return matchLessEqual(a, val, bits, mask)
}

func (a *DictByteArray) MatchGreater(val []byte, bits, mask *bitset.Bitset) *bitset.Bitset {
	return matchGreater(a, val, bits, mask)
}

func (a *DictByteArray) MatchGreaterEqual(val []byte, bits, mask *bitset.Bitset) *bitset.Bitset {
	return matchGreaterEqual(a, val, bits, mask)
}

func (a *DictByteArray) MatchBetween(from, to []byte, bits, mask *bitset.Bitset) *bitset.Bitset {
	return matchBetween(a, from, to, bits, mask)
}
