// Copyright (c) 2018-2023 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package dedup

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"sync/atomic"

	"blockwatch.cc/knoxdb/internal/arena"
	"blockwatch.cc/knoxdb/internal/bitset"
	"blockwatch.cc/knoxdb/internal/cmp"
	"blockwatch.cc/knoxdb/pkg/slicex"
	"golang.org/x/exp/slices"
)

type NativeByteArray struct {
	size int64
	bufs [][]byte
}

func newNativeByteArray(sz int) *NativeByteArray {
	return &NativeByteArray{
		bufs: arena.Alloc(arena.AllocBytesSlice, sz).([][]byte)[:0],
	}
}

func newNativeByteArrayFromBytes(b [][]byte) *NativeByteArray {
	return &NativeByteArray{bufs: b, size: heapSize(b)}
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
	if len(a.bufs) <= index {
		return
	}
	diff := len(buf) - len(a.bufs[index])
	if cap(a.bufs[index]) < len(buf) {
		a.bufs[index] = make([]byte, len(buf))
	} else {
		a.bufs[index] = a.bufs[index][:len(buf)]
	}
	copy(a.bufs[index], buf)
	atomic.AddInt64(&a.size, int64(diff))
}

func (a NativeByteArray) SetZeroCopy(index int, buf []byte) {
	if len(a.bufs) <= index {
		return
	}
	diff := len(buf) - len(a.bufs[index])
	atomic.AddInt64(&a.size, int64(diff))
	a.bufs[index] = buf
}

func (a *NativeByteArray) Grow(n int) ByteArray {
	a.bufs = slices.Grow(a.bufs, n)
	atomic.AddInt64(&a.size, int64(24*n))
	return a
}

func (a *NativeByteArray) Append(vals ...[]byte) ByteArray {
	for _, v := range vals {
		a.bufs = append(a.bufs, bytes.Clone(v))
	}
	atomic.AddInt64(&a.size, heapSize(vals))
	return a
}

func (a *NativeByteArray) AppendZeroCopy(vals ...[]byte) ByteArray {
	a.bufs = append(a.bufs, vals...)
	atomic.AddInt64(&a.size, heapSize(vals))
	return a
}

func (a *NativeByteArray) AppendFrom(src ByteArray) ByteArray {
	ss := src.Slice()
	for _, v := range ss {
		buf := make([]byte, len(v))
		copy(buf, v)
		a.bufs = append(a.bufs, buf)
	}
	atomic.AddInt64(&a.size, heapSize(ss))
	if src.IsOptimized() {
		recycle(ss)
	}
	return a
}

func (a *NativeByteArray) Insert(index int, vals ...[]byte) ByteArray {
	pre := a.bufs
	a.bufs = slices.Insert(a.bufs, index, vals...)
	if cap(pre) != cap(a.bufs) {
		recycle(pre)
	}
	atomic.AddInt64(&a.size, heapSize(vals))
	return a
}

func (a *NativeByteArray) InsertFrom(index int, src ByteArray) ByteArray {
	ss := src.Slice()
	pre := a.bufs
	a.bufs = slices.Insert(a.bufs, index, ss...)
	if src.IsOptimized() {
		recycle(ss)
	}
	atomic.AddInt64(&a.size, heapSize(ss))
	if cap(pre) != cap(a.bufs) {
		recycle(pre)
	}
	return a
}

func (a *NativeByteArray) Copy(src ByteArray, dstPos, srcPos, n int) ByteArray {
	ss := src.Subslice(srcPos, srcPos+n)
	diff := heapSize(ss)
	for j, v := range ss {
		diff -= int64(len(a.bufs[dstPos+j]))
		// always allocate new slice to avoid sharing memory
		buf := make([]byte, len(v))
		copy(buf, v)
		a.bufs[dstPos+j] = buf
	}
	atomic.AddInt64(&a.size, diff)
	if src.IsOptimized() {
		recycle(ss)
	}
	return a
}

func (a *NativeByteArray) Delete(index, n int) ByteArray {
	// avoid mem leaks
	var diff int
	for j, l := index, index+n; j < l; j++ {
		diff -= len(a.bufs[j]) + 24
		a.bufs[j] = nil
	}
	atomic.AddInt64(&a.size, int64(diff))
	a.bufs = append(a.bufs[:index], a.bufs[index+n:]...)
	return a
}

func (a *NativeByteArray) Clear() {
	for j := range a.bufs {
		a.bufs[j] = nil
	}
	a.bufs = a.bufs[:0]
	atomic.StoreInt64(&a.size, 0)
}

func (a *NativeByteArray) Release() {
	for j := range a.bufs {
		a.bufs[j] = nil
	}
	recycle(a.bufs)
	a.bufs = nil
	atomic.StoreInt64(&a.size, 0)
}

func (a NativeByteArray) Slice() [][]byte {
	return a.bufs
}

func (a NativeByteArray) Subslice(start, end int) [][]byte {
	return a.bufs[start:end]
}

func (a NativeByteArray) MinMax() ([]byte, []byte) {
	cmin, cmax := slicex.BytesMinMax(a.bufs)
	return slices.Clone(cmin), slices.Clone(cmax)
}

func (a NativeByteArray) Min() []byte {
	return slices.Clone(slicex.BytesMin(a.bufs))
}

func (a NativeByteArray) Max() []byte {
	return slices.Clone(slicex.BytesMax(a.bufs))
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
	return nativeByteArraySz + int(atomic.LoadInt64(&a.size))
}

func (a NativeByteArray) WriteTo(w io.Writer) (int64, error) {
	var count int64
	n, err := w.Write([]byte{bytesNativeFormat << 4})
	count += int64(n)
	if err != nil {
		return count, err
	}
	err = binary.Write(w, binary.LittleEndian, uint32(len(a.bufs)))
	if err != nil {
		return count, err
	}
	count += 4
	for i := range a.bufs {
		err = binary.Write(w, binary.LittleEndian, uint32(len(a.bufs[i])))
		if err != nil {
			return count, err
		}
		count += 4
		n, err := w.Write(a.bufs[i])
		count += int64(n)
		if err != nil {
			return count, err
		}
	}
	return count, nil
}

func (a *NativeByteArray) ReadFrom(r io.Reader) (int64, error) {
	// read number of elements
	var l uint32
	err := binary.Read(r, binary.LittleEndian, &l)
	if err != nil {
		return 0, fmt.Errorf("native: reading count: %w", err)
	}
	c := int64(4)

	// prepare or re-alloc slice headers
	if cap(a.bufs) < int(l) {
		arena.Free(arena.AllocBytesSlice, a.bufs)
		a.bufs = arena.Alloc(arena.AllocBytesSlice, int(l)).([][]byte)[:int(l)]
	} else {
		a.bufs = a.bufs[:int(l)]
	}

	// read until EOF
	j := 0
	var heapSize int
	for {
		err := binary.Read(r, binary.LittleEndian, &l)
		if err != nil {
			if err == io.EOF {
				break
			}
			return c, fmt.Errorf("native: reading element len: %w", err)
		}
		c += 4
		// alloc or dimension element slice
		if cap(a.bufs[j]) < int(l) {
			a.bufs[j] = make([]byte, int(l))
		} else {
			a.bufs[j] = a.bufs[j][:int(l)]
		}
		// read val from stream
		n, err := r.Read(a.bufs[j])
		c += int64(n)
		heapSize += n + 24
		if err != nil {
			return c, fmt.Errorf("compact: reading element: %w", err)
		}
		j++
	}
	a.bufs = a.bufs[:j]
	atomic.StoreInt64(&a.size, int64(heapSize))
	return c, nil
}

func (a *NativeByteArray) Decode(buf []byte) error {
	if len(buf) == 0 {
		return nil
	}

	// check the encoding type
	if buf[0] != byte(bytesNativeFormat<<4) {
		return fmt.Errorf("native: reading header: %w", errUnexpectedFormat)
	}

	// skip the encoding type
	buf = buf[1:]

	// read number of slices
	sz := int(binary.LittleEndian.Uint32(buf))
	buf = buf[4:]

	if cap(a.bufs) < sz {
		arena.Free(arena.AllocBytesSlice, a.bufs)
		a.bufs = arena.Alloc(arena.AllocBytesSlice, sz).([][]byte)[:sz]
	} else {
		a.bufs = a.bufs[:sz]
	}

	// copy the rest of our input buffer to avoid referencing memory
	cp := make([]byte, len(buf))
	copy(cp, buf)
	buf = cp

	j := 0
	var heapSize int
	for len(buf) > 0 {
		if len(buf) < 4 {
			return fmt.Errorf("native: reading element len: %w", errInvalidLength)
		}
		l := int(binary.LittleEndian.Uint32(buf))
		buf = buf[4:]
		if l > len(buf) {
			return fmt.Errorf("native: reading element: %w", errShortBuffer)
		}
		a.bufs[j] = buf[:l:l]
		buf = buf[l:]
		j++
		heapSize += l + 24
	}
	a.bufs = a.bufs[:j]
	atomic.StoreInt64(&a.size, int64(heapSize))
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

func (a NativeByteArray) ForEach(fn func(int, []byte)) {
	for i, v := range a.bufs {
		fn(i, v)
	}
}

func (a NativeByteArray) ForEachUnique(fn func(int, []byte)) {
	a.ForEach(fn)
}

func (a NativeByteArray) MatchEqual(val []byte, bits, mask *bitset.Bitset) *bitset.Bitset {
	return cmp.MatchBytesEqual(a.bufs, val, bits, mask)
}

func (a NativeByteArray) MatchNotEqual(val []byte, bits, mask *bitset.Bitset) *bitset.Bitset {
	return cmp.MatchBytesNotEqual(a.bufs, val, bits, mask)
}

func (a NativeByteArray) MatchLess(val []byte, bits, mask *bitset.Bitset) *bitset.Bitset {
	return cmp.MatchBytesLess(a.bufs, val, bits, mask)
}

func (a NativeByteArray) MatchLessEqual(val []byte, bits, mask *bitset.Bitset) *bitset.Bitset {
	return cmp.MatchBytesLessEqual(a.bufs, val, bits, mask)
}

func (a NativeByteArray) MatchGreater(val []byte, bits, mask *bitset.Bitset) *bitset.Bitset {
	return cmp.MatchBytesGreater(a.bufs, val, bits, mask)
}

func (a NativeByteArray) MatchGreaterEqual(val []byte, bits, mask *bitset.Bitset) *bitset.Bitset {
	return cmp.MatchBytesGreaterEqual(a.bufs, val, bits, mask)
}

func (a NativeByteArray) MatchBetween(from, to []byte, bits, mask *bitset.Bitset) *bitset.Bitset {
	return cmp.MatchBytesBetween(a.bufs, from, to, bits, mask)
}
