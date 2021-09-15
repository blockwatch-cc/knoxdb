// Copyright (c) 2018-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package dedup

import (
	"bytes"
	"encoding/binary"
	"io"

	"blockwatch.cc/knoxdb/encoding/compress"
	"blockwatch.cc/knoxdb/vec"
)

type CompactByteArray struct {
	buf  []byte
	offs []int32
	size []int32
}

func newCompactByteArray(n, sz int) *CompactByteArray {
	return &CompactByteArray{
		buf:  make([]byte, 0, sz),
		offs: make([]int32, 0, n),
		size: make([]int32, 0, n),
	}
}

func makeCompactByteArray(sz int, data [][]byte, dupmap []int) *CompactByteArray {
	a := &CompactByteArray{
		buf:  make([]byte, 0, sz),
		offs: make([]int32, len(data)),
		size: make([]int32, len(data)),
	}
	for i, v := range data {
		k := dupmap[i]
		if k < 0 {
			// append non duplicate
			a.offs[i] = int32(len(a.buf))
			a.size[i] = int32(len(v))
			a.buf = append(a.buf, v...)
		} else {
			// reference as duplicate
			a.offs[i] = a.offs[k]
			a.size[i] = a.size[k]
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
	return 1 + compress.Int32ArrayEncodedSize(a.offs) + len(a.buf)
}

func (a *CompactByteArray) HeapSize() int {
	return compactByteArraySz + len(a.buf) + 8*len(a.offs)
}

func (a *CompactByteArray) Elem(index int) []byte {
	return a.buf[a.offs[index] : a.offs[index]+a.size[index]]
}

func (a *CompactByteArray) Set(index int, buf []byte) {
	// unsupported
}

func (a *CompactByteArray) Append(val ...[]byte) ByteArray {
	// unsupported
	return a
}

func (a *CompactByteArray) AppendFrom(src ByteArray) ByteArray {
	// unsupported
	return a
}

func (a *CompactByteArray) Insert(index int, buf ...[]byte) ByteArray {
	// unsupported
	return a
}

func (a *CompactByteArray) InsertFrom(index int, src ByteArray) ByteArray {
	// unsupported
	return a
}

func (a *CompactByteArray) Copy(src ByteArray, dstPos, srcPos, n int) ByteArray {
	// unsupported
	return a
}

func (a *CompactByteArray) Delete(index, n int) ByteArray {
	// unsupported
	return a
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

func (a *CompactByteArray) WriteTo(w io.Writer) (int, error) {
	w.Write([]byte{bytesCompactFormat << 4})
	if len(a.offs) == 0 {
		return 1, nil
	}

	// write len in elements
	count := 1
	var num [binary.MaxVarintLen64]byte
	l := binary.PutUvarint(num[:], uint64(len(a.offs)))
	w.Write(num[:l])
	count += l

	// prepare and write offsets
	scratch := make([]int64, len(a.offs))
	for i, v := range a.offs {
		scratch[i] = int64(v)
	}
	olen, err := compress.IntegerArrayEncodeAll(scratch, w)
	if err != nil {
		return count, err
	}
	count += olen

	// write raw data with leading size
	l = binary.PutUvarint(num[:], uint64(len(a.buf)))
	w.Write(num[:l])
	w.Write(a.buf)
	count += l + len(a.buf)

	// write compressed offset and size lens last
	binary.BigEndian.PutUint64(num[:], uint64(olen))
	w.Write(num[:8])
	count += 8

	return count, nil
}

func (a *CompactByteArray) Decode(buf []byte) error {
	if len(buf) == 0 {
		return nil
	}

	// check the encoding type
	if buf[0] != byte(bytesCompactFormat<<4) {
		return errUnexpectedFormat
	}

	// skip the encoding type
	buf = buf[1:]

	// read len in elements
	length, n := binary.Uvarint(buf)
	if n <= 0 {
		return errInvalidLength
	}
	buf = buf[n:]
	l := int(length)

	// ensure slices size
	if cap(a.offs) < l {
		a.offs = make([]int32, 0, l)
		a.size = make([]int32, 0, l)
	}
	a.offs = a.offs[:l]
	a.size = a.size[:l]
	scratch := make([]int64, l)

	// read compressed offs and size array lens (stored at end of buffer)
	if len(buf) < 16 {
		return errShortBuffer
	}
	olen := int(binary.BigEndian.Uint64(buf[len(buf)-8:]))
	buf = buf[:len(buf)-8]

	// unpack offsets
	if len(buf) < olen {
		return errShortBuffer
	}

	var err error
	scratch, err = compress.IntegerArrayDecodeAll(buf[:olen], scratch)
	if err != nil {
		return err
	}
	for i, v := range scratch {
		a.offs[i] = int32(v)
		if i > 0 {
			a.size[i-1] = a.offs[i] - a.offs[i-1]
		}
	}
	buf = buf[olen:]
	a.size[len(a.size)-1] = int32(len(buf)) - a.offs[len(a.size)-1]

	// copy data to private buffer
	if cap(a.buf) < len(buf) {
		a.buf = make([]byte, 0, len(buf))
	}
	a.buf = a.buf[:len(buf)]
	copy(a.buf, buf)

	return nil
}

func (a *CompactByteArray) Materialize() ByteArray {
	return newNativeByteArrayFromBytes(a.Slice())
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

func (a *CompactByteArray) MatchEqual(val []byte, bits, mask *vec.Bitset) *vec.Bitset {
	return matchEqual(a, val, bits, mask)
}

func (a *CompactByteArray) MatchNotEqual(val []byte, bits, mask *vec.Bitset) *vec.Bitset {
	return matchNotEqual(a, val, bits, mask)
}

func (a *CompactByteArray) MatchLessThan(val []byte, bits, mask *vec.Bitset) *vec.Bitset {
	return matchLessThan(a, val, bits, mask)
}

func (a *CompactByteArray) MatchLessThanEqual(val []byte, bits, mask *vec.Bitset) *vec.Bitset {
	return matchLessThanEqual(a, val, bits, mask)
}

func (a *CompactByteArray) MatchGreaterThan(val []byte, bits, mask *vec.Bitset) *vec.Bitset {
	return matchGreaterThan(a, val, bits, mask)
}

func (a *CompactByteArray) MatchGreaterThanEqual(val []byte, bits, mask *vec.Bitset) *vec.Bitset {
	return matchGreaterThanEqual(a, val, bits, mask)
}

func (a *CompactByteArray) MatchBetween(from, to []byte, bits, mask *vec.Bitset) *vec.Bitset {
	return matchBetween(a, from, to, bits, mask)
}
