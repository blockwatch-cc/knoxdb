// Copyright (c) 2018-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package dedup

import (
	"bytes"
	"encoding/binary"
	"io"
	"sync"

	"blockwatch.cc/knoxdb/vec"
)

var bytesPool = &sync.Pool{
	New: func() interface{} { return make([][]byte, 0, DefaultMaxPointsPerBlock) },
}

type NativeByteArray struct {
	bufs [][]byte
}

func newNativeByteArray(n int) *NativeByteArray {
	a := &NativeByteArray{}
	if n <= DefaultMaxPointsPerBlock {
		a.bufs = bytesPool.Get().([][]byte)[:0]
	} else {
		a.bufs = make([][]byte, 0, n)
	}
	return a
}

func newNativeByteArrayFromBytes(b [][]byte) *NativeByteArray {
	return &NativeByteArray{bufs: b}
}

func (a NativeByteArray) Len() int {
	return len(a.bufs)
}

func (a NativeByteArray) Cap() int {
	return cap(a.bufs)
}

func (a NativeByteArray) Elem(index int) []byte {
	return a.bufs[index]
}

func (a NativeByteArray) Set(index int, buf []byte) {
	a.bufs[index] = make([]byte, len(buf))
	copy(a.bufs[index], buf)

}

func (a *NativeByteArray) Append(vals ...[]byte) ByteArray {
	a.bufs = append(a.bufs, vals...)
	return a
}

func (a *NativeByteArray) AppendFrom(src ByteArray) ByteArray {
	a.bufs = append(a.bufs, src.Slice()...)
	return a
}

func (a *NativeByteArray) Insert(index int, vals ...[]byte) ByteArray {
	a.bufs = vec.Bytes.Insert(a.bufs, index, vals...)
	return a
}

func (a *NativeByteArray) InsertFrom(index int, src ByteArray) ByteArray {
	a.bufs = vec.Bytes.Insert(a.bufs, index, src.Slice()...)
	return a
}

func (a *NativeByteArray) Copy(src ByteArray, dstPos, srcPos, n int) ByteArray {
	for j, v := range src.Slice()[srcPos : srcPos+n] {
		// always allocate new slice to avoid sharing memory
		buf := make([]byte, len(v))
		copy(buf, v)
		a.bufs[dstPos+j] = buf
	}
	return a
}

func (a *NativeByteArray) Delete(index, n int) ByteArray {
	// avoid mem leaks
	for j, l := index, index+n; j < l; j++ {
		a.bufs[j] = nil
	}
	a.bufs = append(a.bufs[:index], a.bufs[index+n:]...)
	return a
}

func (a *NativeByteArray) Clear() {
	for j := range a.bufs {
		a.bufs[j] = nil
	}
	a.bufs = a.bufs[:0]
}

func (a *NativeByteArray) Release() {
	for j := range a.bufs {
		a.bufs[j] = nil
	}
	if cap(a.bufs) == DefaultMaxPointsPerBlock {
		bytesPool.Put(a.bufs[:0])
	}
	a.bufs = nil
}

func (a NativeByteArray) Slice() [][]byte {
	return [][]byte(a.bufs)
}

func (a NativeByteArray) Subslice(start, end int) [][]byte {
	return [][]byte(a.bufs)[start:end]
}

func (a NativeByteArray) MinMax() ([]byte, []byte) {
	return vec.Bytes.MinMax(a.bufs)
}

func (a NativeByteArray) MaxEncodedSize() int {
	var sz int
	for _, v := range a.bufs {
		l := len(v)
		sz += l + uvarIntLen(l)
	}
	return sz + 1
}

func (a NativeByteArray) HeapSize() int {
	sz := nativeByteArraySz
	for _, v := range a.bufs {
		sz += len(v) + 24
	}
	return sz
}

func (a NativeByteArray) WriteTo(w io.Writer) (int, error) {
	w.Write([]byte{bytesNativeFormat << 4})
	if len(a.bufs) == 0 {
		return 1, nil
	}

	count := 1
	var buf [binary.MaxVarintLen64]byte
	for i := range a.bufs {
		l := binary.PutUvarint(buf[:], uint64(len(a.bufs[i])))
		w.Write(buf[:l])
		w.Write(a.bufs[i])
		count += l + len(a.bufs[i])
	}

	return count, nil
}

func (a *NativeByteArray) Decode(buf []byte) error {
	if len(buf) == 0 {
		return nil
	}

	// check the encoding type
	if buf[0] != byte(bytesNativeFormat<<4) {
		return errUnexpectedFormat
	}

	// skip the encoding type
	buf = buf[1:]
	var i, l int

	sz := cap(a.bufs)
	if sz == 0 {
		sz = DefaultMaxPointsPerBlock
		a.bufs = make([][]byte, sz)
	} else {
		a.bufs = a.bufs[:sz]
	}

	// copy the rest of our input buffer to avoid referencing memory
	cp := make([]byte, len(buf))
	copy(cp, buf)
	buf = cp

	j := 0

	for i < len(buf) {
		length, n := binary.Uvarint(buf[i:])
		if n <= 0 {
			return errInvalidLength
		}

		// The length of this string plus the length of the variable byte encoded length
		l = int(length) + n

		lower := i + n
		upper := lower + int(length)
		if upper < lower {
			return errLengthOverflow
		}
		if upper > len(buf) {
			return errShortBuffer
		}

		val := buf[lower:upper]
		if j < len(a.bufs) {
			a.bufs[j] = val
		} else {
			a.bufs = append(a.bufs, val) // force a resize
			a.bufs = a.bufs[:cap(a.bufs)]
		}
		i += l
		j++
	}
	a.bufs = a.bufs[:j]

	return nil
}

func (a *NativeByteArray) Materialize() ByteArray {
	return a
}

func (a NativeByteArray) IsMaterialized() bool {
	return true
}

func (a *NativeByteArray) Optimize() ByteArray {
	return optimize(a.bufs)
}

func (a NativeByteArray) IsOptimized() bool {
	return false
}

func (a NativeByteArray) Less(i, j int) bool {
	return bytes.Compare(a.bufs[i], a.bufs[j]) < 0
}

func (a NativeByteArray) Swap(i, j int) {
	a.bufs[i], a.bufs[j] = a.bufs[j], a.bufs[i]
}

func (a NativeByteArray) MatchEqual(val []byte, bits, mask *vec.Bitset) *vec.Bitset {
	return vec.MatchBytesEqual(a.bufs, val, bits, mask)
}

func (a NativeByteArray) MatchNotEqual(val []byte, bits, mask *vec.Bitset) *vec.Bitset {
	return vec.MatchBytesNotEqual(a.bufs, val, bits, mask)
}

func (a NativeByteArray) MatchLessThan(val []byte, bits, mask *vec.Bitset) *vec.Bitset {
	return vec.MatchBytesLessThan(a.bufs, val, bits, mask)
}

func (a NativeByteArray) MatchLessThanEqual(val []byte, bits, mask *vec.Bitset) *vec.Bitset {
	return vec.MatchBytesLessThanEqual(a.bufs, val, bits, mask)
}

func (a NativeByteArray) MatchGreaterThan(val []byte, bits, mask *vec.Bitset) *vec.Bitset {
	return vec.MatchBytesGreaterThan(a.bufs, val, bits, mask)
}

func (a NativeByteArray) MatchGreaterThanEqual(val []byte, bits, mask *vec.Bitset) *vec.Bitset {
	return vec.MatchBytesGreaterThanEqual(a.bufs, val, bits, mask)
}

func (a NativeByteArray) MatchBetween(from, to []byte, bits, mask *vec.Bitset) *vec.Bitset {
	return vec.MatchBytesBetween(a.bufs, from, to, bits, mask)
}
