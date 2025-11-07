// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package stringx

import (
	"bytes"
	"fmt"
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

	// ensure we implement required interfaces
	_ types.StringAccessor = (*StringPool)(nil)

	// zero is a zero length zero capacity slice uses as placeholder
	// for returning zero length strings and to avoid allocations
	zero = make([]byte, 0)[:0:0]
)

const StringPoolDefaultSize = 64

// StringPool is a memory efficient pool for variable length strings.
// Its not thread-safe and may realloc/copy internal memory on growth.
// Users can prevent reallocs by defining a correct max number and
// max total length for all strings. Strings can be retrieved from
// a pool by their position assigned during insert.
//
// The main purpose of StringPool is to lower memory overhead of
// [][]byte (24 bytes per slice), especially for small strings.
// Even though space savings are great, they come at higher
// cost for random access. The main reason is the cost of non-inlined
// function calls to Get() and iterator yield callbacks.
type StringPool struct {
	buf  []byte   // buffer pool
	ptr  []uint64 // string content pointers [ off << 32 | len ]
	base unsafe.Pointer
	pp   *uint64
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
	p.base = unsafe.Pointer(unsafe.SliceData(p.buf))
	p.pp = unsafe.SliceData(p.ptr)
	return p
}

func (p *StringPool) Close() {
	arena.Free(p.buf)
	arena.Free(p.ptr)
	p.buf = nil
	p.ptr = nil
	p.base = nil
	p.pp = nil
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
	return len(p.buf)
}

func (p *StringPool) HeapSize() int {
	return 48 + cap(p.buf) + cap(p.ptr)*8
}

// Compares strings at index i and j and returns
//
// - -1 when s[i] < s[j],
// - 1 when s[i] > s[j],
// - 0 when both strings are equal
//
// Panics when i or j are out of bounds.
func (p *StringPool) Cmp(i, j int) int {
	if l := uint(len(p.ptr)); uint(i) > l || uint(j) > l {
		return 0
	}
	return bytes.Compare(p.get(i), p.get(j))
}

func (p *StringPool) MinMax() ([]byte, []byte) {
	if p.Len() == 0 {
		return nil, nil
	}
	vmin := p.get(0)
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
	vmin := p.get(0)
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
	vmax := p.get(0)
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
		for _, ptr := range p.ptr {
			ofs, len := ptr2pair(ptr)
			if !fn(unsafe.Slice((*byte)(unsafe.Add(p.base, ofs)), len)) {
				return
			}
		}
	}
}

// 2-ary iterator `for i, v := range pool.Iterator2() {}`
func (p *StringPool) Iterator() iter.Seq2[int, []byte] {
	return func(fn func(int, []byte) bool) {
		for i, ptr := range p.ptr {
			ofs, len := ptr2pair(ptr)
			if !fn(i, unsafe.Slice((*byte)(unsafe.Add(p.base, ofs)), len)) {
				return
			}
		}
	}
}

func (p *StringPool) Chunks() types.StringIterator {
	return NewIterator(p)
}

// Append adds a new entry and returns its position in the pool.
func (p *StringPool) Append(val []byte) int {
	vlen := len(val)
	if vlen > maxPageSize {
		panic(fmt.Errorf("stringpool: string value too large"))
	}
	willGrow := len(p.ptr) == cap(p.ptr) || len(p.buf)+vlen > cap(p.buf)
	if vlen == 0 {
		// append empty string, may realloc
		p.ptr = append(p.ptr, 0)
	} else {
		// may realloc
		p.ptr = append(p.ptr, pair2ptr(uint32(len(p.buf)), uint32(vlen)))
		p.buf = append(p.buf, val...)
	}
	// check for realloc and update pointers
	if willGrow {
		p.base = unsafe.Pointer(unsafe.SliceData(p.buf))
		p.pp = unsafe.SliceData(p.ptr)
	}
	return len(p.ptr) - 1
}

// AppendMany appends multiple strings and returns the position of
// the first new value in the pool.
func (p *StringPool) AppendMany(vals ...[]byte) int {
	if len(vals) == 0 {
		return -1
	}
	n := len(p.ptr)
	for _, v := range vals {
		p.Append(v)
	}
	return n
}

func (p *StringPool) AppendString(val string) int {
	return p.Append(util.UnsafeGetBytes(val))
}

func (p *StringPool) AppendManyStrings(vals ...string) int {
	if len(vals) == 0 {
		return -1
	}
	n := len(p.ptr)
	for _, v := range vals {
		p.Append(util.UnsafeGetBytes(v))
	}
	return n
}

// AppendTo adds selected strings to a destination pool or all if sel is nil.
func (p *StringPool) AppendTo(dst types.StringWriter, sel []uint32) {
	if sel == nil {
		for _, ptr := range p.ptr {
			ofs, len := ptr2pair(ptr)
			dst.Append(unsafe.Slice((*byte)(unsafe.Add(p.base, ofs)), len))
		}
	} else {
		for _, v := range sel {
			ofs, len := ptr2pair(*(*uint64)(unsafe.Add(unsafe.Pointer(p.pp), v*8)))
			dst.Append(unsafe.Slice((*byte)(unsafe.Add(p.base, ofs)), len))
		}
	}
}

// Get returns an entry at position i. The underlying buffer is shared
// with the pool and only valid until close. Returns empty slice when i
// is out of bounds.
func (p *StringPool) Get(i int) []byte {
	if uint(i) > uint(len(p.ptr)) {
		return zero
	}
	return p.get(i)
}

// get returns entry at position i unchecked. It uses pointer arithmentic
// like in C (Go requires unsafe here) to avoid slice boundary checks.
func (p *StringPool) get(i int) []byte {
	ofs, sz := ptr2pair(*(*uint64)(unsafe.Add(unsafe.Pointer(p.pp), i*8)))
	return unsafe.Slice((*byte)(unsafe.Add(p.base, ofs)), sz)
}

// GetString returns an entry at position i casted to string type.
// Other than regular Go strings memory is shared with the pool,
// so its unsafe to use the string after close.
// Panics if i is out of bounds.
func (p *StringPool) GetString(i int) string {
	return util.UnsafeGetString(p.Get(i))
}

// Set replaces entry at position i with a new string. The new string
// is added to the buffer (without duplicate check) and the previous
// string becomes garbage. Panics if i is out of bounds.
func (p *StringPool) Set(i int, val []byte) {
	// insert val at place i and append string to the end of buf
	vlen := uint32(len(val))
	vofs := uint32(len(p.buf))
	p.ptr[i] = pair2ptr(vofs, vlen)
	p.buf = append(p.buf, val...)
}

// Delete removes entries [i:j] where indices form an open
// interval [i,j). Panics if i or j are out of bounds.
func (p *StringPool) Delete(i, j int) {
	p.ptr = slices.Delete(p.ptr, i, j)
}

// Range returns a new StringPool referencing a range of the original
// pool. Do not close this pool as memory is shared. Panics if i or j
// are out of bounds.
func (p *StringPool) Range(i, j int) *StringPool {
	rg := p.ptr[i:j]
	return &StringPool{
		buf:  p.buf,
		ptr:  rg,
		base: p.base,
		pp:   unsafe.SliceData(rg),
	}
}
