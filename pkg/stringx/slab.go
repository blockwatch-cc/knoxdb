// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package stringx

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"iter"
	"slices"
	"sync"
	"sync/atomic"
	"unsafe"

	"blockwatch.cc/knoxdb/internal/arena"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/util"
)

const (
	// minPageSize defines the minimal and initial default page size in bytes
	minPageSize = 1 << 16 // 16k

	// maxPageSize defines the maximum size of a page in bytes
	maxPageSize = 1 << 32 // 4G

	// pageListGrow defines the quantum of increase for dynamic page lists
	pageListGrow = 8
)

var (
	// pre-allocate slab struct and dynamic page slice and re-use both allocs
	slabPool = sync.Pool{
		New: func() any {
			p := new(SlabPool)
			list := make([]*page, pageListGrow)
			p.pages.Store(&list)
			return p
		},
	}
	pagePool = sync.Pool{
		New: func() any { return new(page) },
	}
)

// SlabPool is fast thread-safe dynamic size string pool which can
// grow without reallocating memory buffers. It uses atomic primitives
// for lock-free synchronization between a single writer and mutiple
// concurrent readers. Multi-writer sync would be more complex and is
// currently unsupported as we don't require it in KnoxDB at the moment.
//
// SlabPool is optimized for storing many (~2k+) small strings of
// similar size (10s - 100s of bytes). For large strings (kB, MB) users
// should initialize a pool with an appropriate larger start page size
// that can fit multiple strings per page.
//
// Internally SlabPool works with a list of variable sized pages. The first
// page starts at a default of 64kB or a user defined value. On growth
// the initial page size is doubled for each additional page up until
// a max of 4GB (uint32 max).
type SlabPool struct {
	pages  atomic.Pointer[[]*page] // list of  pages
	wpos   atomic.Int32            // ptr write position == pool length
	tail   *page                   // one writeable page (single writer access)
	tidx   int                     // tail index in page list
	pagesz int                     // default page size (will double on growth)
	ptr    []uint64                // content pointers [page id << 32 | page ofs]
}

// page represents a single slab buffer page. It uses []byte slice to
// conveniently store page size (as len == cap) and make the Go gc happy.
type page struct {
	buf []byte
	ofs int
}

func newPage(sz int) *page {
	p := pagePool.Get().(*page)
	p.buf = arena.Alloc[byte](sz)[:sz]
	p.ofs = 0
	return p
}

func (p *page) free() {
	arena.Free(p.buf)
	p.buf = nil
	p.ofs = 0
	pagePool.Put(p)
}

// Go compiler does not inline this (too heavy: weight 96 > 80)
// func (p *page) elem(ofs int) []byte {
// 	base := unsafe.Pointer(unsafe.SliceData(p.buf))
// 	l, n := binary.Uvarint(unsafe.Slice((*byte)(unsafe.Add(base, ofs)), 10))
// 	return unsafe.Slice((*byte)(unsafe.Add(base, ofs+n)), l)
// }

// slab addressing uses [page id << 32 | page ofs] pointers
// while string length is inlined as uvarint
func pg2ptr(pg, ofs int) uint64 {
	return uint64(pg)<<32 | uint64(ofs&0xFFFFFFFF)
}

func ptr2pg(p uint64) (int, int) {
	return int(p >> 32), int(p & 0xFFFFFFFF)
}

// NewSlabPool allocates a new string pool for up to n entries. Use
// it to create pools for small strings. With default initial page
// size, one page of 64kB can approximatley fit up to n strings of
// max length 65536/n, but note there is a small overhead for storing
// string length as inline varint (1 byte when len < 128).
func NewSlabPool(n int) *SlabPool {
	return NewSlabPoolSize(n, 0)
}

// NewSlabPoolSize alocates a new string pool for up to n strings
// with a custom initial page size. Use this for better control over
// allocation strategy when storing large strings (> 1kB) or many
// strings (n > 2048).
func NewSlabPoolSize(n, pagesz int) *SlabPool {
	p := slabPool.Get().(*SlabPool)
	p.pagesz = max(pagesz, minPageSize)
	p.tidx = 0
	p.ptr = arena.Alloc[uint64](n)[:n]
	p.tail = newPage(p.pagesz)
	p.tail.buf[0] = 0 // empty string placeholder (i.e. len=0)
	p.tail.ofs = 1
	(*p.pages.Load())[0] = p.tail
	return p
}

func (p *SlabPool) Close() {
	if p.pagesz == 0 {
		// double close or cloned pool (not writable, don't free)
		return
	}
	arena.Free(p.ptr)
	pages := *p.pages.Load()
	for _, pg := range pages[:p.tidx+1] {
		pg.free()
	}
	clear(pages)
	p.tail = nil
	p.wpos.Store(0)
	p.tidx = 0
	p.ptr = nil
	p.pagesz = 0
	slabPool.Put(p)
}

func (p *SlabPool) Clear() {
	if p.pagesz == 0 {
		return
	}
	p.wpos.Store(0)
	pages := *p.pages.Load()
	for _, pg := range pages[1:] {
		pg.free()
	}
	clear(pages[1:])
	pages[0].ofs = 1
	p.tail = pages[0]
	p.tidx = 0
}

func (p *SlabPool) Len() int {
	if p.pagesz == 0 {
		return len(p.ptr)
	}
	return int(p.wpos.Load())
}

func (p *SlabPool) Cap() int {
	return cap(p.ptr)
}

func (p *SlabPool) Size() int {
	var sz int
	for _, pg := range *p.pages.Load() {
		sz += pg.ofs
	}
	return sz
}

func (p *SlabPool) HeapSize() int {
	sz := 60 // struct size
	sz += cap(p.ptr) * 8
	for _, pg := range *p.pages.Load() {
		sz += 32          // page size
		sz += len(pg.buf) // buffer size
	}
	return sz
}

// Get returns an entry at position i. The underlying buffer is shared
// with the pool and only valid until close. Get is concurrency safe
// and the returned slice remains valid when the pool grows.
// Does not perform bounds checks on i.
func (p *SlabPool) Get(i int) []byte {
	// bounds-check free p.ptr[i]
	pp := (*uint64)(unsafe.Add(unsafe.Pointer(unsafe.SliceData(p.ptr)), i*8))

	// identify and load page
	pgid, ofs := ptr2pg(atomic.LoadUint64(pp))
	pg := (*p.pages.Load())[pgid]

	// bounds-check free pg.buf[x]
	base := unsafe.Pointer(unsafe.SliceData(pg.buf))
	l, n := binary.Uvarint(unsafe.Slice((*byte)(unsafe.Add(base, ofs)), 10))
	return unsafe.Slice((*byte)(unsafe.Add(base, ofs+n)), l)
}

// GetString returns an entry at position i casted to string type.
// Other than regular Go strings memory is shared with the pool,
// so its unsafe to use the string after close.
// Panics if i is out of bounds.
func (p *SlabPool) GetString(i int) string {
	return util.UnsafeGetString(p.Get(i))
}

// Set replaces entry at position i with a new string that is
// is appended to a page. Panics if i is out of bounds.
func (p *SlabPool) Set(i int, val []byte) {
	ptr := p.insert(val)
	atomic.StoreUint64(&p.ptr[i], ptr)
}

// Delete removes entries [i:j] where indices form an open
// interval [i,j). Panics if i or j are out of bounds.
func (p *SlabPool) Delete(i, j int) {
	p.ptr = slices.Delete(p.ptr, i, j)
	p.wpos.Add(int32(i - j))
}

// Range returns a read-only view on a range of the original
// pool. Memory is shared, so the view is only valid as long as
// the original pool is not closed. Closing the view is a noop.
func (p *SlabPool) Range(i, j int) *SlabPool {
	// share memory, but don't free
	rg := &SlabPool{
		ptr: p.ptr[i:j],
	}
	rg.pages.Store(p.pages.Load())
	return rg
}

// Append adds a new entry and returns its position in the pool.
func (p *SlabPool) Append(val []byte) int {
	ptr := p.insert(val)
	pos := int(p.wpos.Add(1)) - 1
	atomic.StoreUint64(&p.ptr[pos], ptr)
	return pos
}

func (p *SlabPool) insert(val []byte) uint64 {
	var (
		l    = len(val)
		ofs  int
		pgid int
	)

	// fail when value is too large
	if l > maxPageSize {
		panic(fmt.Errorf("slabpool: string value too large"))
	}

	// when l == 0 reference zero string on first page
	if l > 0 {
		var buf [binary.MaxVarintLen64]byte
		n := binary.PutUvarint(buf[:], uint64(l))

		// try tail page first
		ofs = p.tail.ofs
		if ofs+l+n > cap(p.tail.buf) {
			// no space: double page size, create and store new page

			// ensure value fits into page
			sz := min(maxPageSize, max(p.pagesz<<(p.tidx+1), util.Log2ceil(l)))
			pg := newPage(sz)

			// insert page into page list, grow when full
			pages := p.pages.Load()
			p.tidx++
			if p.tidx == cap(*pages) {
				// extend page list
				ext := make([]*page, cap(*pages)+pageListGrow)
				copy(ext, *pages)
				pages = &ext
				p.pages.Store(pages)
			}

			// link new page
			(*pages)[p.tidx] = pg
			p.tail = pg
			ofs = 0
		}

		// insert val into buffer
		copy(p.tail.buf[ofs:], buf[:n])
		copy(p.tail.buf[ofs+n:], val)
		p.tail.ofs = ofs + l + n
		pgid = p.tidx
	}

	return pg2ptr(pgid, ofs)
}

// AppendMany appends multiple entries and returns the first position.
func (p *SlabPool) AppendMany(vals ...[]byte) int {
	if len(vals) == 0 {
		return -1
	}
	n := p.Append(vals[0])
	for _, v := range vals[1:] {
		p.Append(v)
	}
	return n
}

func (p *SlabPool) AppendString(val string) int {
	return p.Append(util.UnsafeGetBytes(val))
}

func (p *SlabPool) AppendManyStrings(vals ...string) int {
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
func (p *SlabPool) AppendTo(dst types.StringWriter, sel []uint32) {
	var (
		pg    *page
		pgid  = -1
		base  unsafe.Pointer
		pages = *p.pages.Load()
	)
	if sel == nil {
		for _, ptr := range p.ptr {
			id, ofs := ptr2pg(ptr)
			if id != pgid {
				pgid = id
				pg = pages[id]
				base = unsafe.Pointer(unsafe.SliceData(pg.buf))
			}
			l, n := binary.Uvarint(unsafe.Slice((*byte)(unsafe.Add(base, ofs)), 10))
			dst.Append(unsafe.Slice((*byte)(unsafe.Add(base, ofs+n)), l))
		}
	} else {
		for _, v := range sel {
			id, ofs := ptr2pg(p.ptr[int(v)])
			if id != pgid {
				pgid = id
				pg = pages[id]
				base = unsafe.Pointer(unsafe.SliceData(pg.buf))
			}
			l, n := binary.Uvarint(unsafe.Slice((*byte)(unsafe.Add(base, ofs)), 10))
			dst.Append(unsafe.Slice((*byte)(unsafe.Add(base, ofs+n)), l))
		}
	}
}

// 2-ary iterator `for i, v := range pool.Iterator2() {}`
func (p *SlabPool) Iterator() iter.Seq2[int, []byte] {
	return func(fn func(int, []byte) bool) {
		var (
			buf   []byte
			pg    *page
			base  unsafe.Pointer
			pgid  = -1
			pages = *p.pages.Load()
		)
		for i, ptr := range p.ptr[:p.wpos.Load()] {
			id, ofs := ptr2pg(ptr)
			if id != pgid {
				pgid = id
				pg = pages[id]
				base = unsafe.Pointer(unsafe.SliceData(pg.buf))
			}
			l, n := binary.Uvarint(unsafe.Slice((*byte)(unsafe.Add(base, ofs)), 10))
			buf = unsafe.Slice((*byte)(unsafe.Add(base, ofs+n)), l)
			if !fn(i, buf) {
				return
			}
		}
	}
}

// unary iterator `for v := range pool.Iterator() {}`
func (p *SlabPool) Values() iter.Seq[[]byte] {
	return func(fn func([]byte) bool) {
		var (
			pg    *page
			base  unsafe.Pointer
			pgid  = -1
			pages = *p.pages.Load()
		)
		for _, ptr := range p.ptr[:p.wpos.Load()] {
			id, ofs := ptr2pg(ptr)
			if id != pgid {
				pgid = id
				pg = pages[id]
				base = unsafe.Pointer(unsafe.SliceData(pg.buf))
			}
			l, n := binary.Uvarint(unsafe.Slice((*byte)(unsafe.Add(base, ofs)), 10))
			if !fn(unsafe.Slice((*byte)(unsafe.Add(base, ofs+n)), l)) {
				return
			}
		}
	}
}

func (p *SlabPool) Chunks() types.StringIterator {
	return NewSlabIterator(p)
}

// Compares strings at index i and j and returns
//
// - -1 when s[i] < s[j],
// - 1 when s[i] > s[j],
// - 0 when both strings are equal
//
// Panics when i or j are out of bounds.
func (p *SlabPool) Cmp(i, j int) int {
	return bytes.Compare(p.Get(i), p.Get(j))
}

func (p *SlabPool) MinMax() ([]byte, []byte) {
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

func (p *SlabPool) Min() []byte {
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

func (p *SlabPool) Max() []byte {
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

func (p *SlabPool) MinMaxLen() (int, int) {
	if p.Len() == 0 {
		return 0, 0
	}
	var (
		lmin  uint32 = 1<<32 - 1
		lmax  uint32 = 0
		pg    *page
		pgid  = -1
		pages = *p.pages.Load()
		base  unsafe.Pointer
	)

	for _, ptr := range p.ptr[:p.wpos.Load()] {
		id, ofs := ptr2pg(ptr)
		if id != pgid {
			pg = pages[id]
			pgid = id
			base = unsafe.Pointer(unsafe.SliceData(pg.buf))
		}
		l, _ := binary.Uvarint(unsafe.Slice((*byte)(unsafe.Add(base, ofs)), 10))
		lmin = min(lmin, uint32(l))
		lmax = max(lmax, uint32(l))
	}
	return int(lmin), int(lmax)
}

var _ types.StringIterator = (*SlabChunkIterator)(nil)

type SlabChunkIterator struct {
	chunk [CHUNK_SIZE][]byte
	pool  *SlabPool
	base  int
}

func NewSlabIterator(p *SlabPool) *SlabChunkIterator {
	return &SlabChunkIterator{
		pool: p,
		base: 0,
	}
}

func (it *SlabChunkIterator) Len() int {
	return len(it.pool.ptr)
}

func (it *SlabChunkIterator) Get(n int) []byte {
	return it.pool.Get(n)
}

func (it *SlabChunkIterator) Seek(n int) bool {
	l := len(it.pool.ptr)
	if n < 0 || n >= l {
		it.base = l
		return false
	}
	it.base = types.ChunkBase(n)
	return true
}

func (it *SlabChunkIterator) NextChunk() (*[CHUNK_SIZE][]byte, int) {
	l := len(it.pool.ptr)
	if it.base >= l {
		return nil, 0
	}

	// refill
	var (
		pg    *page
		pgid  = -1
		base  unsafe.Pointer
		pages = *it.pool.pages.Load()
		n     = min(CHUNK_SIZE, int(it.pool.wpos.Load())-it.base)
	)

	for i, ptr := range it.pool.ptr[it.base : it.base+n] {
		id, ofs := ptr2pg(ptr)
		if id != pgid {
			pg = pages[id]
			pgid = id
			base = unsafe.Pointer(unsafe.SliceData(pg.buf))
		}
		l, n := binary.Uvarint(unsafe.Slice((*byte)(unsafe.Add(base, ofs)), 10))
		it.chunk[i] = unsafe.Slice((*byte)(unsafe.Add(base, ofs+n)), l)
	}
	clear(it.chunk[n:])

	// advance base for next iteration
	it.base += n

	return &it.chunk, n
}

func (it *SlabChunkIterator) SkipChunk() int {
	n := min(types.CHUNK_SIZE, len(it.pool.ptr)-it.base)
	it.base += n
	return n
}

func (it *SlabChunkIterator) Close() {
	clear(it.chunk[:])
	it.pool = nil
	it.base = 0
}
