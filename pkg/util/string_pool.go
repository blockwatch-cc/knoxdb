// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package util

import (
	"bytes"
	"iter"
	"slices"
	"sync"
	"unsafe"

	"blockwatch.cc/knoxdb/internal/arena"
)

var (
	stringPool = sync.Pool{
		New: func() any { return new(StringPool) },
	}
)

type StringSetter interface {
	Append([]byte)
	Set(int, []byte)
	Delete(int)
}

const StringPoolDefaultSize = 64

// TODO
// extensible buf (avoid buf realloc/memcopy on append)
// - add new buffers of max fixed size on Append/Set as needed
// - unify addressing into a single u64: buf_id + ofs + len
// - requires we define an absolute max size per string (=max block size)
//

// StringPool is a memory efficient pool for variable length strings.
// Its main purpose is to manage a slice of strings without the 24-byte
// slice header overheads of [][]byte.
type StringPool struct {
	buf []byte   // buffer pool
	ptr []uint64 // string offsets/lengths in buf
}

func ptr2pair(p uint64) (uint32, uint32) {
	return uint32(p >> 32), uint32(p)
}

func pair2ptr(o, l uint32) uint64 {
	return uint64(o)<<32 | uint64(l)
}

func NewStringPool(n int) *StringPool {
	return NewStringPoolSize(n, StringPoolDefaultSize)
}

func NewStringPoolSize(n, sz int) *StringPool {
	p := stringPool.Get().(*StringPool)
	p.buf = arena.Alloc[byte](n * sz)
	p.ptr = arena.Alloc[uint64](n)
	return p
}

func (p *StringPool) Close() {
	arena.Free(p.buf)
	arena.Free(p.ptr)
	p.buf = nil
	p.ptr = nil
	stringPool.Put(p)
}

func (p *StringPool) Clear() {
	p.buf = p.buf[:0]
	p.ptr = p.ptr[:0]
}

func (p *StringPool) Len() int {
	return len(p.ptr)
}

func (p *StringPool) Size() int {
	return len(p.buf)
}

func (p *StringPool) MinMax() ([]byte, []byte, int, int) {
	if p.Len() == 0 {
		return nil, nil, 0, 0
	}
	vmin := p.Get(0)
	vmax := vmin
	lmin := len(vmin)
	lmax := lmin
	for v := range p.Iterator() {
		if bytes.Compare(v, vmin) < 0 {
			vmin = v
		} else if bytes.Compare(v, vmax) > 0 {
			vmax = v
		}
		lmin = min(lmin, len(v))
		lmax = max(lmax, len(v))
	}
	return vmin, vmax, lmin, lmax
}

// unary iterator `for v := range pool.Iterator() {}`
func (p *StringPool) Iterator() iter.Seq[[]byte] {
	return func(fn func([]byte) bool) {
		// beware of all empty strings
		var base unsafe.Pointer
		if len(p.buf) > 0 {
			base = unsafe.Pointer(&p.buf[0])
		}
		for _, ptr := range p.ptr {
			ofs, len := ptr2pair(ptr)
			if !fn(unsafe.Slice((*byte)(unsafe.Add(base, ofs)), len)) {
				return
			}
		}
	}
}

// 2-ary iterator `for i, v := range pool.Iterator2() {}`
func (p *StringPool) Iterator2() iter.Seq2[int, []byte] {
	return func(fn func(int, []byte) bool) {
		// beware of all empty strings
		var base unsafe.Pointer
		if len(p.buf) > 0 {
			base = unsafe.Pointer(&p.buf[0])
		}
		for i, ptr := range p.ptr {
			ofs, len := ptr2pair(ptr)
			if !fn(i, unsafe.Slice((*byte)(unsafe.Add(base, ofs)), len)) {
				return
			}
		}
	}
}

// Append adds a new element and will reuse existing strings.
func (p *StringPool) Append(val []byte) {
	vlen := uint32(len(val))
	// append empty string
	if vlen == 0 {
		p.ptr = append(p.ptr, 0)
		return
	}
	vofs := uint32(len(p.buf))
	p.ptr = append(p.ptr, pair2ptr(vofs, vlen))
	p.buf = append(p.buf, val...)
}

func (p *StringPool) AppendMany(vals ...[]byte) {
	for _, v := range vals {
		p.Append(v)
	}
}

func (p *StringPool) AppendString(val string) {
	p.Append(UnsafeGetBytes(val))
}

func (p *StringPool) AppendManyStrings(vals ...string) {
	for _, v := range vals {
		p.Append(UnsafeGetBytes(v))
	}
}

// AppendTo adds selected strings to a destination pool or all if sel is nil.
func (p *StringPool) AppendTo(dst StringSetter, sel []uint32) {
	if sel == nil {
		for _, ptr := range p.ptr {
			ofs, len := ptr2pair(ptr)
			dst.Append(p.buf[ofs : ofs+len])
		}
	} else {
		for _, v := range sel {
			ofs, len := ptr2pair(p.ptr[int(v)])
			dst.Append(p.buf[ofs : ofs+len])
		}
	}
}

// Get returns element at position i or nil of i is out of bounds.
func (p *StringPool) Get(i int) []byte {
	if i < 0 || len(p.ptr) <= i {
		return nil
	}
	ofs, sz := ptr2pair(p.ptr[i])
	return p.buf[ofs : ofs+sz]
}

// Set replaces element at position i with a new string. The new string
// is added to the buffer (without duplicate check) and the previous
// string becomes garbage.
func (p *StringPool) Set(i int, val []byte) {
	if i < 0 || i >= len(p.ptr) {
		return
	}

	// insert val at place i and append string to the end of buf
	vlen := uint32(len(val))
	vofs := uint32(len(p.buf))
	p.ptr[i] = pair2ptr(vlen, vofs)
	p.buf = append(p.buf, val...)
}

// Delete removes the element at position i but does not change the
// contents of the string buffer.
func (p *StringPool) Delete(i int) {
	if i < 0 || i > len(p.ptr) {
		return
	}
	p.ptr = slices.Delete(p.ptr, int(i), int(i+1))
}
