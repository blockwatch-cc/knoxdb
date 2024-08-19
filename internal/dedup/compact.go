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

type CompactByteArray struct {
	buf  []byte
	offs []int32
	size []int32
}

func newCompactByteArray(sz, n int) *CompactByteArray {
	return &CompactByteArray{
		buf:  make([]byte, 0, sz),
		offs: make([]int32, 0, n),
		size: make([]int32, 0, n),
	}
}

func makeCompactByteArray(sz, card int, data [][]byte, dupmap []int) *CompactByteArray {
	a := &CompactByteArray{
		buf:  make([]byte, 0, sz),
		offs: make([]int32, len(data)),
		size: make([]int32, len(data)),
	}
	uniq := make([]int, 0, card)
	for i, v := range data {
		k := dupmap[i]
		if k < 0 {
			// append non duplicate
			a.offs[i] = int32(len(a.buf))
			a.size[i] = int32(len(v))
			a.buf = append(a.buf, v...)
			uniq = append(uniq, i)
		} else {
			// reference as duplicate
			a.offs[i] = a.offs[uniq[k]]
			a.size[i] = a.size[uniq[k]]
		}
	}
	return a
}

func (a *CompactByteArray) Len() int {
	return len(a.offs)
}

func (a *CompactByteArray) Cap() int {
	return cap(a.offs)
}

func (a *CompactByteArray) MaxEncodedSize() int {
	return 1 + zip.Int32EncodedSize(len(a.offs)) + len(a.buf)
}

func (a *CompactByteArray) HeapSize() int {
	return compactByteArraySz + len(a.buf) + 8*len(a.offs)
}

func (a *CompactByteArray) Elem(index int) []byte {
	return a.buf[a.offs[index] : a.offs[index]+a.size[index]]
}

func (a *CompactByteArray) Grow(int) ByteArray {
	panic("compact: Grow unsupported")
}

func (a *CompactByteArray) Set(int, []byte) {
	panic("compact: Set unsupported")
}

func (a *CompactByteArray) SetZeroCopy(int, []byte) {
	panic("compact: Set unsupported")
}

func (a *CompactByteArray) Append(...[]byte) ByteArray {
	panic("compact: Append unsupported")
}

func (a *CompactByteArray) AppendZeroCopy(...[]byte) ByteArray {
	panic("compact: Append unsupported")
}

func (a *CompactByteArray) AppendFrom(ByteArray) ByteArray {
	panic("compact: AppendFrom unsupported")
}

func (a *CompactByteArray) Insert(int, ...[]byte) ByteArray {
	panic("compact: Insert unsupported")
}

func (a *CompactByteArray) InsertFrom(int, ByteArray) ByteArray {
	panic("compact: InsertFrom unsupported")
}

func (a *CompactByteArray) Copy(ByteArray, int, int, int) ByteArray {
	panic("compact: Copy unsupported")
}

func (a *CompactByteArray) Delete(int, int) ByteArray {
	panic("compact: Delete unsupported")
}

func (a *CompactByteArray) Clear() {
	a.buf = a.buf[:0]
	a.offs = a.offs[:0]
	a.size = a.size[:0]
}

func (a *CompactByteArray) Release() {
	a.Clear()
	a.buf = nil
	a.offs = nil
	a.size = nil
}

func (a *CompactByteArray) Slice() [][]byte {
	return toSlice(a)
}

func (a *CompactByteArray) Subslice(start, end int) [][]byte {
	return toSubSlice(a, start, end)
}

func (a *CompactByteArray) MinMax() ([]byte, []byte) {
	return minMax(a)
}

func (a *CompactByteArray) WriteTo(w io.Writer) (int64, error) {
	w.Write([]byte{bytesCompactFormat << 4})
	count := 1

	// write len in elements
	binary.Write(w, binary.LittleEndian, uint32(len(a.offs)))
	count += 4

	// use scratch buffer
	scratch := arena.Alloc(arena.AllocBytes, zip.Int32EncodedSize(len(a.offs)))
	defer arena.Free(arena.AllocBytes, scratch)
	buf := bytes.NewBuffer(scratch.([]byte)[:0])

	// prepare and write offsets
	olen, err := zip.EncodeInt32(a.offs, buf)
	if err != nil {
		return int64(count), err
	}

	// write compressed offset len
	binary.Write(w, binary.LittleEndian, uint32(olen))
	count += 4

	// write compressed offset data
	w.Write(buf.Bytes())
	count += olen

	// prepare and write sizes
	buf.Reset()
	slen, err := zip.EncodeInt32(a.size, buf)
	if err != nil {
		return int64(count), err
	}

	// write compressed sizes
	binary.Write(w, binary.LittleEndian, uint32(slen))
	count += 4

	// write compressed sizes data
	w.Write(buf.Bytes())
	count += slen
	buf.Reset()

	// write raw data with leading size
	binary.Write(w, binary.LittleEndian, uint32(len(a.buf)))
	count += 4
	w.Write(a.buf)
	count += len(a.buf)

	return int64(count), nil
}

func (a *CompactByteArray) ReadFrom(r io.Reader) (int64, error) {
	var l uint32
	err := binary.Read(r, binary.LittleEndian, &l)
	if err != nil {
		return 0, fmt.Errorf("compact: reading count: %w", err)
	}
	c := int64(4)

	// ensure slices size
	if cap(a.offs) < int(l) {
		a.offs = make([]int32, 0, int(l))
		a.size = make([]int32, 0, int(l))
	}
	a.offs = a.offs[:int(l)]
	a.size = a.size[:int(l)]

	// read compressed offs array
	err = binary.Read(r, binary.LittleEndian, &l)
	if err != nil {
		return c, fmt.Errorf("compact: reading offset len: %w", err)
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
		return c, fmt.Errorf("compact: reading offsets: %w", err)
	}

	// decode offs data
	n, err = zip.DecodeInt32(a.offs, buf)
	if err != nil {
		return c, fmt.Errorf("compact: decoding offsets: %w", err)
	}
	a.offs = a.offs[:n]

	// read compressed size array
	err = binary.Read(r, binary.LittleEndian, &l)
	if err != nil {
		return c, fmt.Errorf("compact: reading size len: %w", err)
	}
	c += 4

	// read size encoded data
	n, err = io.ReadFull(r, buf)
	c += int64(n)
	if err != nil {
		return c, fmt.Errorf("compact: reading sizes: %w", err)
	}

	// decode size data
	n, err = zip.DecodeInt32(a.size, buf)
	if err != nil {
		return c, fmt.Errorf("compact: decoding sizes: %w", err)
	}
	a.size = a.size[:n]

	// read raw data array
	err = binary.Read(r, binary.LittleEndian, &l)
	if err != nil {
		return c, fmt.Errorf("compact: reading data len: %w", err)
	}
	c += 4

	// copy data to private buffer
	if cap(a.buf) < int(l) {
		a.buf = make([]byte, 0, int(l))
	}
	a.buf = a.buf[:int(l)]

	n, err = io.ReadFull(r, a.buf)
	c += int64(n)
	if err != nil {
		return c, fmt.Errorf("compact: reading data: %w", err)
	}

	return c, nil
}

func (a *CompactByteArray) Decode(buf []byte) error {
	if len(buf) == 0 {
		return nil
	}

	// check the encoding type
	if buf[0] != byte(bytesCompactFormat<<4) {
		return fmt.Errorf("compact: reading header: %w", errUnexpectedFormat)
	}

	// skip the encoding type
	buf = buf[1:]

	// read len in elements
	if len(buf) < 4 {
		return fmt.Errorf("compact: reading count: %w", errInvalidLength)
	}

	l := int(binary.LittleEndian.Uint32(buf))
	buf = buf[4:]

	// ensure slices size
	if cap(a.offs) < l {
		a.offs = make([]int32, 0, l)
		a.size = make([]int32, 0, l)
	}
	a.offs = a.offs[:l]
	a.size = a.size[:l]

	// read compressed offs array lengths
	if len(buf) < 4 {
		return fmt.Errorf("compact: reading offset len: %w", errShortBuffer)
	}
	olen := int(binary.BigEndian.Uint32(buf))
	buf = buf[4:]

	// unpack offsets
	if len(buf) < olen {
		return fmt.Errorf("compact: reading offset data: %w", errShortBuffer)
	}

	n, err := zip.DecodeInt32(a.offs, buf[:olen])
	if err != nil {
		return fmt.Errorf("compact: decoding offsets: %w", err)
	}
	buf = buf[olen:]
	a.offs = a.offs[:n]

	// read compressed offs array lengths
	if len(buf) < 4 {
		return fmt.Errorf("compact: reading size len: %w", errShortBuffer)
	}
	slen := int(binary.BigEndian.Uint32(buf))
	buf = buf[4:]

	// unpack sizes
	if len(buf) < slen {
		return fmt.Errorf("compact: reading size data: %w", errShortBuffer)
	}
	n, err = zip.DecodeInt32(a.size, buf[:slen])
	if err != nil {
		return fmt.Errorf("compact: decoding offsets: %w", err)
	}
	buf = buf[slen:]
	a.size = a.size[:n]

	// read data len in elements
	if len(buf) < 4 {
		return fmt.Errorf("compact: reading data len: %w", errShortBuffer)
	}
	l = int(binary.LittleEndian.Uint32(buf))
	buf = buf[4:]

	if len(buf) < l {
		return fmt.Errorf("compact: reading data: %w", errShortBuffer)
	}

	// copy data to private buffer
	if cap(a.buf) < len(buf) {
		a.buf = make([]byte, 0, len(buf))
	}
	a.buf = a.buf[:len(buf)]
	copy(a.buf, buf)

	return nil
}

func (a *CompactByteArray) Materialize() ByteArray {
	// copy to avoid referencing memory
	ss := a.Slice()
	for i, v := range ss {
		buf := make([]byte, len(v))
		copy(buf, v)
		ss[i] = buf
	}
	return newNativeByteArrayFromBytes(ss)
}

func (a *CompactByteArray) IsMaterialized() bool {
	return false
}

func (a *CompactByteArray) Optimize() ByteArray {
	return a
}

func (a *CompactByteArray) IsOptimized() bool {
	return true
}

func (a *CompactByteArray) Less(i, j int) bool {
	return bytes.Compare(a.Elem(i), a.Elem(j)) < 0
}

func (a *CompactByteArray) Swap(i, j int) {
	a.offs[i], a.offs[j] = a.offs[j], a.offs[i]
	a.size[i], a.size[j] = a.size[j], a.size[i]
}

func (a CompactByteArray) ForEach(fn func(int, []byte)) {
	for i, o := range a.offs {
		fn(i, a.buf[o:o+a.size[i]])
	}
}

func (a CompactByteArray) ForEachUnique(fn func(int, []byte)) {
	var offs int32
	for i, o := range a.offs {
		if o < offs {
			continue
		}
		fn(i, a.buf[o:o+a.size[i]])
		offs = o
	}
}

func (a *CompactByteArray) MatchEqual(val []byte, bits, mask *bitset.Bitset) *bitset.Bitset {
	return matchEqual(a, val, bits, mask)
}

func (a *CompactByteArray) MatchNotEqual(val []byte, bits, mask *bitset.Bitset) *bitset.Bitset {
	return matchNotEqual(a, val, bits, mask)
}

func (a *CompactByteArray) MatchLess(val []byte, bits, mask *bitset.Bitset) *bitset.Bitset {
	return matchLess(a, val, bits, mask)
}

func (a *CompactByteArray) MatchLessEqual(val []byte, bits, mask *bitset.Bitset) *bitset.Bitset {
	return matchLessEqual(a, val, bits, mask)
}

func (a *CompactByteArray) MatchGreater(val []byte, bits, mask *bitset.Bitset) *bitset.Bitset {
	return matchGreater(a, val, bits, mask)
}

func (a *CompactByteArray) MatchGreaterEqual(val []byte, bits, mask *bitset.Bitset) *bitset.Bitset {
	return matchGreaterEqual(a, val, bits, mask)
}

func (a *CompactByteArray) MatchBetween(from, to []byte, bits, mask *bitset.Bitset) *bitset.Bitset {
	return matchBetween(a, from, to, bits, mask)
}
