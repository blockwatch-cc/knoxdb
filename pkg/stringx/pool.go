// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package stringx

import (
	"bytes"
	"iter"
	"slices"
	"sync"
	"unsafe"

	"blockwatch.cc/knoxdb/internal/arena"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/util"
)

var (
	stringPool = sync.Pool{
		New: func() any { return new(StringPool) },
	}
)

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

func (p *StringPool) Cap() int {
	return cap(p.ptr)
}

func (p *StringPool) Size() int {
	return 48 + cap(p.buf) + cap(p.ptr)*8
}

func (p *StringPool) DataSize() int {
	return len(p.buf)
}

// Compares strings at index i and j and returns
//
// - -1 when s[i] < s[j],
// - 1 when s[i] > s[j],
// - 0 when both strings are equal
//
// Panics when i or j are out of bounds.
func (p *StringPool) Cmp(i, j int) int {
	ofs, sz := ptr2pair(p.ptr[i])
	x := p.buf[ofs : ofs+sz]
	ofs, sz = ptr2pair(p.ptr[j])
	y := p.buf[ofs : ofs+sz]
	return bytes.Compare(x, y)
}

func (p *StringPool) MinMax() ([]byte, []byte) {
	if p.Len() == 0 {
		return nil, nil
	}
	vmin := p.Get(0)
	vmax := vmin
	for v := range p.Values() {
		if bytes.Compare(v, vmin) < 0 {
			vmin = v
		} else if bytes.Compare(v, vmax) > 0 {
			vmax = v
		}
	}
	return vmin, vmax
}

func (p *StringPool) Min() []byte {
	if p.Len() == 0 {
		return nil
	}
	vmin := p.Get(0)
	for v := range p.Values() {
		if bytes.Compare(v, vmin) < 0 {
			vmin = v
		}
	}
	return vmin
}

func (p *StringPool) Max() []byte {
	if p.Len() == 0 {
		return nil
	}
	vmax := p.Get(0)
	for v := range p.Values() {
		if bytes.Compare(v, vmax) > 0 {
			vmax = v
		}
	}
	return vmax
}

func (p *StringPool) MinMaxLen() (int, int) {
	if p.Len() == 0 {
		return 0, 0
	}
	var (
		lmin uint32 = 1<<32 - 1
		lmax uint32 = 0
	)
	for _, v := range p.ptr {
		_, l := ptr2pair(v)
		lmin = min(lmin, l)
		lmax = max(lmax, l)
	}
	return int(lmin), int(lmax)
}

// unary iterator `for v := range pool.Iterator() {}`
func (p *StringPool) Values() iter.Seq[[]byte] {
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
func (p *StringPool) Iterator() iter.Seq2[int, []byte] {
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
	p.Append(util.UnsafeGetBytes(val))
}

func (p *StringPool) AppendManyStrings(vals ...string) {
	for _, v := range vals {
		p.Append(util.UnsafeGetBytes(v))
	}
}

// AppendTo adds selected strings to a destination pool or all if sel is nil.
func (p *StringPool) AppendTo(dst types.StringWriter, sel []uint32) {
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

// Get returns element at position i. Panics if i is out of bounds.
func (p *StringPool) Get(i int) []byte {
	ofs, sz := ptr2pair(p.ptr[i])
	return p.buf[ofs : ofs+sz]
}

// Set replaces element at position i with a new string. The new string
// is added to the buffer (without duplicate check) and the previous
// string becomes garbage. Panics if i is out of bounds.
func (p *StringPool) Set(i int, val []byte) {
	// insert val at place i and append string to the end of buf
	vlen := uint32(len(val))
	vofs := uint32(len(p.buf))
	p.ptr[i] = pair2ptr(vlen, vofs)
	p.buf = append(p.buf, val...)
}

// Delete removes elements [i:j] where indices form an open
// interval [i,j). Panics if i or j are out of bounds.
func (p *StringPool) Delete(i, j int) {
	p.ptr = slices.Delete(p.ptr, i, j)
}

// Range returns a new StringPool referencing a range of the original
// pool. Do not close this pool as memory is shared. Panics if i or j
// are out of bounds.
func (p *StringPool) Range(i, j int) *StringPool {
	return &StringPool{
		buf: p.buf,
		ptr: p.ptr[i:j],
	}
}
