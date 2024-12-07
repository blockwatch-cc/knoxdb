// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package block

import (
	"reflect"
	"sync"
	"sync/atomic"
	"unsafe"

	"golang.org/x/exp/slices"

	"blockwatch.cc/knoxdb/internal/arena"
	"blockwatch.cc/knoxdb/internal/bitset"
	"blockwatch.cc/knoxdb/internal/dedup"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/assert"
	"blockwatch.cc/knoxdb/pkg/num"
)

var (
	blockSz = int(reflect.TypeOf(Block{}).Size())

	blockPool = &sync.Pool{
		New: func() any { return &Block{} },
	}

	BlockSz = int(reflect.TypeOf(Block{}).Size())

	blockTypeDataSize = [...]int{
		BlockTime:    8,
		BlockInt64:   8,
		BlockInt32:   4,
		BlockInt16:   2,
		BlockInt8:    1,
		BlockUint64:  8,
		BlockUint32:  4,
		BlockUint16:  2,
		BlockUint8:   1,
		BlockFloat64: 8,
		BlockFloat32: 4,
		BlockBool:    1,
		BlockString:  0, // variable
		BlockBytes:   0, // variable
		BlockInt256:  32,
		BlockInt128:  16,
	}
)

type BlockType = types.BlockType

const (
	BlockTime    = types.BlockTime
	BlockInt64   = types.BlockInt64
	BlockInt32   = types.BlockInt32
	BlockInt16   = types.BlockInt16
	BlockInt8    = types.BlockInt8
	BlockUint64  = types.BlockUint64
	BlockUint32  = types.BlockUint32
	BlockUint16  = types.BlockUint16
	BlockUint8   = types.BlockUint8
	BlockFloat64 = types.BlockFloat64
	BlockFloat32 = types.BlockFloat32
	BlockBool    = types.BlockBool
	BlockString  = types.BlockString
	BlockBytes   = types.BlockBytes
	BlockInt128  = types.BlockInt128
	BlockInt256  = types.BlockInt256
)

type Block struct {
	refCount int64
	ptr      unsafe.Pointer // ptr to first byte of store
	buf      []byte         // backing store
	len      int            // in type units
	cap      int            // in type units
	typ      BlockType
	dirty    bool
	_        [6]byte // pad to 64 bytes
}

func New(typ BlockType, sz int) *Block {
	b := blockPool.Get().(*Block)
	b.typ = typ
	b.dirty = true
	b.refCount = 1
	b.len = 0
	b.cap = sz
	switch typ {
	case BlockInt128:
		var i128 num.Int128Stride
		i128.X0 = arena.Alloc(arena.AllocInt64, sz).([]int64)[:0]
		i128.X1 = arena.Alloc(arena.AllocUint64, sz).([]uint64)[:0]
		b.ptr = unsafe.Pointer(&i128)
	case BlockInt256:
		var i256 num.Int256Stride
		i256.X0 = arena.Alloc(arena.AllocInt64, sz).([]int64)[:0]
		i256.X1 = arena.Alloc(arena.AllocUint64, sz).([]uint64)[:0]
		i256.X2 = arena.Alloc(arena.AllocUint64, sz).([]uint64)[:0]
		i256.X3 = arena.Alloc(arena.AllocUint64, sz).([]uint64)[:0]
		b.ptr = unsafe.Pointer(&i256)
	case BlockBool:
		b.ptr = unsafe.Pointer(bitset.NewBitset(sz).Resize(0))
	case BlockString, BlockBytes:
		arr := dedup.NewByteArray(sz)
		b.ptr = unsafe.Pointer(&arr)
	default:
		byteSize := sz * blockTypeDataSize[typ]
		b.buf = arena.Alloc(arena.AllocBytes, byteSize).([]byte)[:byteSize]
		b.ptr = unsafe.Pointer(unsafe.SliceData(b.buf))
	}
	return b
}

// only applicable to managed memory blocks, not byte, i128, i258, bool
func (b *Block) data() []byte {
	return b.buf[:b.len*blockTypeDataSize[b.typ]]
}

func (b *Block) IncRef() int64 {
	assert.Always(b != nil, "nil block, potential use after free")
	assert.Always(atomic.LoadInt64(&b.refCount) >= 0, "block refcount < 0")
	return atomic.AddInt64(&b.refCount, 1)
}

func (b *Block) DecRef() int64 {
	assert.Always(b != nil, "nil block, potential use after free", nil)
	assert.Always(atomic.LoadInt64(&b.refCount) > 0, "block refcount <= 0")
	val := atomic.AddInt64(&b.refCount, -1)
	if val == 0 {
		b.free()
	}
	return val
}

func (b Block) Type() BlockType {
	return b.typ
}

func (b *Block) IsDirty() bool {
	return b.dirty
}

func (b *Block) SetDirty() {
	b.dirty = true
}

func (b *Block) SetClean() {
	b.dirty = false
}

func (b *Block) CanOptimize() bool {
	assert.Always(b != nil, "nil block, potential use after free")
	assert.Always(b.ptr != nil, "nil block ptr, potential use after free")
	return (b.typ == BlockBytes || b.typ == BlockString) && !(*(*dedup.ByteArray)(b.ptr)).IsOptimized()
}

func (b *Block) Optimize() {
	assert.Always(b != nil, "nil block, potential use after free")
	assert.Always(b.ptr != nil, "nil block ptr, potential use after free")
	switch b.typ {
	case BlockBytes, BlockString:
		// ok
	default:
		// not (yet) supported
		// TODO: support frame-of-reference (min-value) and truncation i64 -> 32/16/8
		return
	}
	da := *(*dedup.ByteArray)(b.ptr)
	if da.IsOptimized() {
		return
	}
	do := da.Optimize()
	da.Release()
	b.ptr = unsafe.Pointer(&do)
}

func (b *Block) Materialize() {
	assert.Always(b != nil, "nil block, potential use after free")
	assert.Always(b.ptr != nil, "nil block ptr, potential use after free")
	switch b.typ {
	case BlockBytes, BlockString:
		// ok
	default:
		// not (yet) supported
		return
	}
	da := *(*dedup.ByteArray)(b.ptr)
	if da.IsMaterialized() {
		return
	}
	dm := da.Materialize()
	da.Release()
	b.ptr = unsafe.Pointer(&dm)
}

func (b *Block) Len() int {
	assert.Always(b != nil, "nil block, potential use after free")
	assert.Always(b.ptr != nil, "nil block ptr, potential use after free")
	switch b.typ {
	case BlockBool:
		return (*bitset.Bitset)(b.ptr).Len()
	case BlockString, BlockBytes:
		return (*(*dedup.ByteArray)(b.ptr)).Len()
	case BlockInt128:
		return (*num.Int128Stride)(b.ptr).Len()
	case BlockInt256:
		return (*num.Int256Stride)(b.ptr).Len()
	default:
		return b.len
	}
}

func (b *Block) Cap() int {
	assert.Always(b != nil, "nil block, potential use after free")
	assert.Always(b.ptr != nil, "nil block ptr, potential use after free")
	switch b.typ {
	case BlockBool:
		return (*bitset.Bitset)(b.ptr).Cap()
	case BlockString, BlockBytes:
		return (*(*dedup.ByteArray)(b.ptr)).Cap()
	case BlockInt128:
		return (*num.Int128Stride)(b.ptr).Cap()
	case BlockInt256:
		return (*num.Int256Stride)(b.ptr).Cap()
	default:
		return b.cap
	}
}

func (b *Block) HeapSize() int {
	assert.Always(b != nil, "nil block, potential use after free")
	assert.Always(b.ptr != nil, "nil block ptr, potential use after free")
	sz := blockSz
	switch b.typ {
	case BlockBool:
		sz += (*bitset.Bitset)(b.ptr).HeapSize()
	case BlockString, BlockBytes:
		sz += (*(*dedup.ByteArray)(b.ptr)).HeapSize()
	case BlockInt128:
		sz += (*num.Int128Stride)(b.ptr).Len() * 16
	case BlockInt256:
		sz += (*num.Int256Stride)(b.ptr).Len() * 32
	default:
		sz += cap(b.buf)
	}
	return sz
}

func (b *Block) Clone(sz int) *Block {
	assert.Always(b != nil, "nil block, potential use after free")
	assert.Always(b.ptr != nil, "nil block ptr, potential use after free")
	assert.Always(b.Len() <= sz, "clone size smaller than block size")
	c := New(b.typ, sz)
	c.dirty = true
	switch b.typ {
	case BlockString, BlockBytes:
		(*(*dedup.ByteArray)(c.ptr)).AppendFrom((*(*dedup.ByteArray)(b.ptr)))
	case BlockBool:
		((*bitset.Bitset)(c.ptr)).AppendFrom(((*bitset.Bitset)(b.ptr)), 0, b.Len())
	case BlockInt128:
		d128 := (*num.Int128Stride)(c.ptr)
		s128 := (*num.Int128Stride)(b.ptr)
		d128.X0 = append(d128.X0, s128.X0...)
		d128.X1 = append(d128.X1, s128.X1...)
		d128.X0 = d128.X0[:b.len]
		d128.X1 = d128.X1[:b.len]
	case BlockInt256:
		d256 := (*num.Int256Stride)(c.ptr)
		s256 := (*num.Int256Stride)(b.ptr)
		d256.X0 = append(d256.X0, s256.X0...)
		d256.X1 = append(d256.X1, s256.X1...)
		d256.X2 = append(d256.X2, s256.X2...)
		d256.X3 = append(d256.X3, s256.X3...)
		d256.X0 = d256.X0[:b.len]
		d256.X1 = d256.X1[:b.len]
		d256.X2 = d256.X2[:b.len]
		d256.X3 = d256.X3[:b.len]
	default:
		c.len = b.len
		copy(c.buf, b.buf)
	}
	return c
}

// Grow increases the block's capacity by n elements and reallocates if necessary.
// This makes space for appending additional n elements but does not increase the
// block's length. If n is negative or too large to allocate memory, Grow panics.
func (b *Block) Grow(n int) {
	assert.Always(b != nil, "nil block, potential use after free")
	assert.Always(b.ptr != nil, "nil block ptr, potential use after free")
	b.cap += n
	switch b.typ {
	case BlockString, BlockBytes:
		(*(*dedup.ByteArray)(b.ptr)).Grow(n)
	case BlockBool:
		(*bitset.Bitset)(b.ptr).Grow(n)
	case BlockInt128:
		i128 := (*num.Int128Stride)(b.ptr)
		i128.X0 = slices.Grow(i128.X0, n)
		i128.X1 = slices.Grow(i128.X1, n)
	case BlockInt256:
		i256 := (*num.Int256Stride)(b.ptr)
		i256.X0 = slices.Grow(i256.X0, n)
		i256.X1 = slices.Grow(i256.X1, n)
		i256.X2 = slices.Grow(i256.X2, n)
		i256.X3 = slices.Grow(i256.X3, n)
	default:
		n *= blockTypeDataSize[b.typ]
		buf := slices.Grow(b.buf, n)
		buf = buf[:len(buf)+n]
		if &buf[0] != &b.buf[0] {
			arena.Free(arena.AllocBytes, b.buf)
			b.buf = buf
			b.ptr = unsafe.Pointer(unsafe.SliceData(b.buf))
		}
	}
	b.dirty = true
}

// Delete removes n elements starting at position i (i.e. [i:from+n])
// and decreases the blocks size, but not its capacity. Delete is O(len(s)-(from+n))
// as it mem-moves trailing items to overwrite the deleted range.
func (b *Block) Delete(from, n int) {
	assert.Always(b != nil, "nil block, potential use after free")
	assert.Always(b.ptr != nil, "nil block ptr, potential use after free")
	assert.Always(b.Len() <= from+n, "out of bounds", "dst.len", b.Len(), "from", from, "n", n)
	switch b.typ {
	case BlockString, BlockBytes:
		(*(*dedup.ByteArray)(b.ptr)).Delete(from, n)
	case BlockBool:
		(*bitset.Bitset)(b.ptr).Delete(from, n)
	case BlockInt128:
		i128 := (*num.Int128Stride)(b.ptr)
		i128.X0 = slices.Delete(i128.X0, from, from+n)
		i128.X1 = slices.Delete(i128.X1, from, from+n)
	case BlockInt256:
		i256 := (*num.Int256Stride)(b.ptr)
		i256.X0 = slices.Delete(i256.X0, from, from+n)
		i256.X1 = slices.Delete(i256.X1, from, from+n)
		i256.X2 = slices.Delete(i256.X2, from, from+n)
		i256.X3 = slices.Delete(i256.X3, from, from+n)
	default:
		b.len -= n
		from *= blockTypeDataSize[b.typ]
		n *= blockTypeDataSize[b.typ]
		slices.Delete(b.buf, from, from+n)
	}
	b.dirty = true
}

// Clear resets the block's length to zero, but does not deallocate memory.
func (b *Block) Clear() {
	assert.Always(b != nil, "nil block, potential use after free")
	assert.Always(b.ptr != nil, "nil block ptr, potential use after free")
	switch b.typ {
	case BlockString, BlockBytes:
		(*(*dedup.ByteArray)(b.ptr)).Clear()
	case BlockBool:
		(*bitset.Bitset)(b.ptr).Reset()
	case BlockInt128:
		i128 := (*num.Int128Stride)(b.ptr)
		i128.X0 = i128.X0[:0]
		i128.X1 = i128.X1[:0]
	case BlockInt256:
		i256 := (*num.Int256Stride)(b.ptr)
		i256.X0 = i256.X0[:0]
		i256.X1 = i256.X1[:0]
		i256.X2 = i256.X2[:0]
		i256.X3 = i256.X3[:0]
	default:
		b.len = 0
	}
	b.dirty = true
}

// Free returns allocated memory to the arena and makes the block struct
// reusable for future allocations. Since blocks are reference counted free
// is only called from DecRef().
func (b *Block) free() {
	assert.Always(b != nil, "nil block release, potential use after free")
	switch b.typ {
	case BlockInt128:
		i128 := (*num.Int128Stride)(b.ptr)
		arena.Free(arena.AllocInt64, i128.X0[:0])
		arena.Free(arena.AllocUint64, i128.X1[:0])
		i128.X0 = nil
		i128.X1 = nil
	case BlockInt256:
		i256 := (*num.Int256Stride)(b.ptr)
		arena.Free(arena.AllocInt64, i256.X0[:0])
		arena.Free(arena.AllocUint64, i256.X1[:0])
		arena.Free(arena.AllocUint64, i256.X2[:0])
		arena.Free(arena.AllocUint64, i256.X3[:0])
		i256.X0 = nil
		i256.X1 = nil
		i256.X2 = nil
		i256.X3 = nil
	case BlockBool:
		(*bitset.Bitset)(b.ptr).Close()
	case BlockString, BlockBytes:
		(*(*dedup.ByteArray)(b.ptr)).Release()
	default:
		arena.Free(arena.AllocBytes, b.buf)
	}
	b.dirty = false
	b.ptr = nil
	b.buf = nil
	b.refCount = 0
	b.typ = 0
	b.len = 0
	b.cap = 0
	blockPool.Put(b)
}

func (b *Block) ReplaceBlock(src *Block, from, to, n int) {
	assert.Always(b != nil, "nil block, potential use after free")
	assert.Always(src != nil, "nil source block, potential use after free")
	assert.Always(b.typ == src.typ, "block type mismatch", b.typ, src.typ)
	assert.Always(to+n <= b.Len(), "dst out of bounds", "to", to, "n", n, "dst.len", b.Len())
	assert.Always(from+n <= src.Len(), "src out of bounds", "from", from, "n", n, "src.len", src.Len())
	switch b.typ {
	case BlockString, BlockBytes:
		(*(*dedup.ByteArray)(b.ptr)).Copy((*(*dedup.ByteArray)(src.ptr)), to, from, n)
	case BlockBool:
		((*bitset.Bitset)(b.ptr)).ReplaceFrom(((*bitset.Bitset)(src.ptr)), from, n, to)
	case BlockInt128:
		d128 := (*num.Int128Stride)(b.ptr)
		s128 := (*num.Int128Stride)(src.ptr)
		slices.Replace(d128.X0, to, to+n, s128.X0[from:from+n]...)
		slices.Replace(d128.X1, to, to+n, s128.X1[from:from+n]...)
	case BlockInt256:
		d256 := (*num.Int256Stride)(b.ptr)
		s256 := (*num.Int256Stride)(src.ptr)
		slices.Replace(d256.X0, to, to+n, s256.X0[from:from+n]...)
		slices.Replace(d256.X1, to, to+n, s256.X1[from:from+n]...)
		slices.Replace(d256.X2, to, to+n, s256.X2[from:from+n]...)
		slices.Replace(d256.X3, to, to+n, s256.X3[from:from+n]...)
	default:
		from *= blockTypeDataSize[b.typ]
		to *= blockTypeDataSize[b.typ]
		n *= blockTypeDataSize[b.typ]
		slices.Replace(b.buf, to, to+n, src.buf[from:from+n]...)
	}
	b.dirty = true
}

func (b *Block) AppendBlock(src *Block, from, n int) {
	assert.Always(b != nil, "nil block, potential use after free")
	assert.Always(src != nil, "nil source block, potential use after free")
	assert.Always(b.typ == src.typ, "block type mismatch", b.typ, src.typ)
	assert.Always(b.Len()+n <= b.Cap(), "dst out of bounds", "dst.len", b.Len(), "dst.cap", b.Cap())
	assert.Always(from+n <= src.Len(), "src out of bounds", "src.len", src.Len(), "from", from)
	switch b.typ {
	case BlockString, BlockBytes:
		if n == 1 {
			(*(*dedup.ByteArray)(b.ptr)).Append(src.Bytes().Elem(from))
		} else {
			(*(*dedup.ByteArray)(b.ptr)).Append(src.Bytes().Subslice(from, from+n)...)
		}
	case BlockBool:
		((*bitset.Bitset)(b.ptr)).AppendFrom(((*bitset.Bitset)(src.ptr)), from, n)
	case BlockInt128:
		d128 := (*num.Int128Stride)(b.ptr)
		s128 := (*num.Int128Stride)(src.ptr)
		d128.X0 = append(d128.X0, s128.X0[from:from+n]...)
		d128.X1 = append(d128.X1, s128.X1[from:from+n]...)
	case BlockInt256:
		d256 := (*num.Int256Stride)(b.ptr)
		s256 := (*num.Int256Stride)(src.ptr)
		d256.X0 = append(d256.X0, s256.X0[from:from+n]...)
		d256.X1 = append(d256.X1, s256.X1[from:from+n]...)
		d256.X2 = append(d256.X2, s256.X2[from:from+n]...)
		d256.X3 = append(d256.X3, s256.X3[from:from+n]...)
	default:
		end := b.len
		b.len += n
		from *= blockTypeDataSize[b.typ]
		end *= blockTypeDataSize[b.typ]
		n *= blockTypeDataSize[b.typ]
		copy(b.buf[end:], src.buf[from:from+n])
	}
	b.dirty = true
}

func (b *Block) InsertBlock(src *Block, from, to, n int) {
	assert.Always(b != nil, "nil block, potential use after free")
	assert.Always(src != nil, "nil source block, potential use after free")
	assert.Always(b.typ == src.typ, "block type mismatch", b.typ, src.typ)
	assert.Always(b.Len()+n <= b.Cap(), "dst out of bounds", "dst.len", b.Len(), "n", n, "dst.cap", b.Cap())
	assert.Always(from+n <= src.Len(), "src out of bounds", "src.len", src.Len(), "from", from)
	switch b.typ {
	case BlockString, BlockBytes:
		(*(*dedup.ByteArray)(b.ptr)).Insert(to, src.Bytes().Subslice(from, from+n)...)
	case BlockBool:
		((*bitset.Bitset)(b.ptr)).InsertFrom(((*bitset.Bitset)(src.ptr)), from, n, to)
	case BlockInt128:
		d128 := (*num.Int128Stride)(b.ptr)
		s128 := (*num.Int128Stride)(src.ptr)
		slices.Insert(d128.X0, to, s128.X0[from:from+n]...)
		slices.Insert(d128.X1, to, s128.X1[from:from+n]...)
	case BlockInt256:
		d256 := (*num.Int256Stride)(b.ptr)
		s256 := (*num.Int256Stride)(src.ptr)
		slices.Insert(d256.X0, to, s256.X0[from:from+n]...)
		slices.Insert(d256.X1, to, s256.X1[from:from+n]...)
		slices.Insert(d256.X2, to, s256.X2[from:from+n]...)
		slices.Insert(d256.X3, to, s256.X3[from:from+n]...)
	default:
		b.len += n
		from *= blockTypeDataSize[b.typ]
		to *= blockTypeDataSize[b.typ]
		n *= blockTypeDataSize[b.typ]
		slices.Insert(b.buf, to, src.buf[from:from+n]...)
	}
	b.dirty = true
}
