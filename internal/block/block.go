// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package block

import (
	"bytes"
	"slices"
	"sync"
	"sync/atomic"
	"unsafe"

	"blockwatch.cc/knoxdb/internal/arena"
	"blockwatch.cc/knoxdb/internal/bitset"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/assert"
	"blockwatch.cc/knoxdb/pkg/num"
	"blockwatch.cc/knoxdb/pkg/stringx"
	"blockwatch.cc/knoxdb/pkg/util"
)

var (
	blockPool = &sync.Pool{
		New: func() any { return &Block{} },
	}
	blockSize = int(unsafe.Sizeof(Block{}))
)

// TODO: replace with BufferManager page
type page *byte

type BlockType = types.BlockType

const (
	BlockInvalid = types.BlockInvalid
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
	BlockBytes   = types.BlockBytes
	BlockInt128  = types.BlockInt128
	BlockInt256  = types.BlockInt256
)

type BlockFlags byte

const (
	BlockFlagDirty = 1 << iota // block has been written to
	BlockFlagRaw               // backing object is writable
)

// Challenge
//
// Define a common abstraction across vector types (int64, int32, float64,
// string, bits) and vector representations (compressed, materialized) that
// provides efficient typed access (as slices `[]int64`, iterator `for range .. {}`
// and chunks `*[128]int64`) without using Go `any` at the API level.
//
// One can build a common vector interface with Go `any`, however in/out
// data types must be wrapped into `any`. This is inefficient for heavy access
// data containers in a database core. Go interfaces have too high hidden
// allocation costs, they cost an indirection and type check at runtime
// and have bad coding ergonomics `x.(int64)`.
//
// Go generics are not an alternative either. Although one can define typed
// interfaces `type X[T any] interface { ... }` this produces a family of
// incompatible actual interface types when instantiated. There is no way
// to make generics implement a common type interface.
//
// Solution
//
// 1. Use untyped `[]byte` backing buffer for fixed size native types (int64),
// an internal `any` for variable size custom types (string pools, bitset) and
// reuse the same `any` for compressed vectors.
//
// 2. Wrap native/materialized types into accessors. Expose access wrappers
// via typed functions (e.g. `func (b *Block) Int64() NumberAccessor[int64] {}`
//
// 3. Put compressed vectors into containers with the same common interface as
// materialized containers (can be generic native typed as well, e.g.
// `IntegerDictionary[int64]`).

type Block struct {
	nref     atomic.Int64 // ref counter, nocopy, 64-bit aligned
	buf      *byte        // backing store for raw numeric types ([0:n:n] n = sz*cap)
	_        *page        // buffer page reference (to release page lock on close)
	any      any          // interface to embedded vector container
	len      uint32       // in type units
	cap      uint32       // in type units
	sz       byte         // type size
	typ      BlockType    // type
	dirty    bool         // flags
	writable bool         // flags
	// _     [21]byte     // pad to 64 bytes
}

func New(typ BlockType, sz int) *Block {
	b := blockPool.Get().(*Block)
	b.nref.Store(1)
	b.len = 0
	b.cap = uint32(sz)
	b.sz = byte(typ.Size())
	b.typ = typ
	b.dirty = false
	b.writable = true
	switch typ {
	case BlockInt128:
		b.any = num.NewInt128Stride(sz)
	case BlockInt256:
		b.any = num.NewInt256Stride(sz)
	case BlockBool:
		b.any = bitset.New(sz).Resize(0)
	case BlockBytes:
		b.any = stringx.NewStringPool(sz)
	default:
		b.buf = unsafe.SliceData(arena.AllocBytes(sz * int(b.sz)))
	}
	return b
}

func (b *Block) buffer() []byte {
	return unsafe.Slice(b.buf, int(b.sz)*int(b.cap))
}

func (b *Block) data() []byte {
	return unsafe.Slice(b.buf, int(b.sz)*int(b.len))
}

// Free returns allocated memory to the arena and makes the block struct
// reusable for future allocations. Since blocks are reference counted free
// is only called from Deref().
func (b *Block) free() {
	assert.Always(b != nil, "free: nil block release, potential use after free")
	switch b.typ {
	case BlockInt128:
		b.Int128().Close()
	case BlockInt256:
		b.Int256().Close()
	case BlockBool:
		b.Bool().Close()
	case BlockBytes:
		b.Bytes().Close()
	default:
		if b.IsMaterialized() {
			arena.Free(b.buffer())
		} else {
			b.any.(Closer).Close()
		}
	}
	b.dirty = false
	b.writable = false
	b.any = nil
	b.buf = nil
	b.nref.Store(0)
	b.typ = 0
	b.len = 0
	b.cap = 0
	b.sz = 0
	blockPool.Put(b)
}

func (b *Block) Ref() int64 {
	assert.Always(b != nil, "ref: nil block, potential use after free")
	assert.Always(b.nref.Load() >= 0, "block refcount < 0")
	for {
		val := b.nref.Load()
		if b.nref.CompareAndSwap(val, val+1) {
			return val + 1
		}
	}
}

func (b *Block) Deref() int64 {
	assert.Always(b != nil, "deref: nil block, potential use after free", nil)
	assert.Always(b.nref.Load() > 0, "block refcount <= 0")
	for {
		val := b.nref.Load()
		if b.nref.CompareAndSwap(val, val-1) {
			val -= 1
			if val == 0 {
				b.free()
			}
			return val
		}
	}
}

func (b *Block) Container() any {
	return b.any
}

func (b *Block) Type() BlockType {
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

func (b *Block) IsMaterialized() bool {
	return b.writable
}

func (b *Block) Len() int {
	assert.Always(b != nil, "len: nil block, potential use after free")
	switch b.typ {
	case BlockBool:
		return b.Bool().Len()
	case BlockBytes:
		return b.Bytes().Len()
	case BlockInt128:
		return b.Int128().Len()
	case BlockInt256:
		return b.Int256().Len()
	default:
		return int(b.len)
	}
}

func (b *Block) Cap() int {
	assert.Always(b != nil, "cap: nil block, potential use after free")
	if !b.IsMaterialized() {
		return 0
	}
	switch b.typ {
	case BlockBool:
		return b.Bool().Cap()
	case BlockBytes:
		return b.Bytes().Cap()
	case BlockInt128:
		return b.Int128().Cap()
	case BlockInt256:
		return b.Int256().Cap()
	default:
		return int(b.cap)
	}
}

func (b *Block) Size() int {
	assert.Always(b != nil, "size: nil block, potential use after free")
	sz := blockSize
	switch b.typ {
	case BlockBool:
		sz += b.Bool().Size()
	case BlockBytes:
		sz += b.Bytes().Size()
	case BlockInt128:
		sz += b.Int128().Size()
	case BlockInt256:
		sz += b.Int256().Size()
	default:
		sz += int(b.cap) * int(b.sz)
	}
	return sz
}

func (b *Block) Clone(sz int) *Block {
	assert.Always(b != nil, "clone: nil block, potential use after free")
	assert.Always(b.Len() <= sz, "clone: size smaller than block size")
	if sz == 0 {
		sz = int(b.cap)
	}
	c := New(b.typ, sz)
	switch b.typ {
	case BlockInt64:
		b.Int64().AppendTo(c.Int64().Slice(), nil)
	case BlockInt32:
		b.Int32().AppendTo(c.Int32().Slice(), nil)
	case BlockInt16:
		b.Int16().AppendTo(c.Int16().Slice(), nil)
	case BlockInt8:
		b.Int8().AppendTo(c.Int8().Slice(), nil)
	case BlockUint64:
		b.Uint64().AppendTo(c.Uint64().Slice(), nil)
	case BlockUint32:
		b.Uint32().AppendTo(c.Uint32().Slice(), nil)
	case BlockUint16:
		b.Uint16().AppendTo(c.Uint16().Slice(), nil)
	case BlockUint8:
		b.Uint8().AppendTo(c.Uint8().Slice(), nil)
	case BlockFloat64:
		b.Float64().AppendTo(c.Float64().Slice(), nil)
	case BlockFloat32:
		b.Float32().AppendTo(c.Float32().Slice(), nil)
	case BlockBytes:
		b.Bytes().AppendTo(c.Bytes(), nil)
	case BlockBool:
		b.Bool().AppendTo(c.Bool().Writer(), nil)
	case BlockInt128:
		b.Int128().AppendTo(c.Int128(), nil)
	case BlockInt256:
		b.Int256().AppendTo(c.Int256(), nil)
	}
	c.len = uint32(b.Len())
	c.SetDirty()
	return c
}

// Delete removes range [i:j] with half open bounds [i,j) and decreases block length.
// Costs are O(j-i) due to memmove of trailing items.
func (b *Block) Delete(i, j int) {
	assert.Always(b != nil, "delete: nil block, potential use after free")
	assert.Always(b.IsMaterialized(), "delete: block not materialized")
	assert.Always(i >= 0 && j >= 0 && b.Len() >= i && b.Len() >= j,
		"delete: out of bounds", "dst.len", b.Len(), "i", i, "j", j)
	switch b.typ {
	case BlockBytes:
		b.Bytes().Delete(i, j)
	case BlockBool:
		b.Bool().Delete(i, j)
	case BlockInt128:
		b.Int128().Delete(i, j)
	case BlockInt256:
		b.Int256().Delete(i, j)
	default:
		_ = slices.Delete(b.buffer(), i*int(b.sz), j*int(b.sz))
		b.len -= uint32(j - i)
	}
	b.SetDirty()
}

// Clear resets the block's length to zero, but does not deallocate memory.
func (b *Block) Clear() {
	assert.Always(b != nil, "clear: nil block, potential use after free")
	assert.Always(b.IsMaterialized(), "clear: block not materialized")
	switch b.typ {
	case BlockBytes:
		b.Bytes().Clear()
	case BlockBool:
		b.Bool().Clear()
	case BlockInt128:
		b.Int128().Clear()
	case BlockInt256:
		b.Int256().Clear()
	default:
		b.len = 0
	}
	b.SetDirty()
}

type Closer interface {
	Close()
}

// Appends range [i:j] from src to the block. Panics if range would overflow.
// Src may be materialized or compressed block.
func (b *Block) AppendRange(src *Block, i, j int) {
	assert.Always(b != nil, "append: nil block, potential use after free")
	assert.Always(b.IsMaterialized(), "append: block not materialized")
	assert.Always(src != nil, "append: nil source block, potential use after free")
	assert.Always(b.typ == src.typ, "append: block type mismatch", b.typ, src.typ)
	n := uint32(j - i)
	assert.Always(b.len+n <= b.cap, "append: out of bounds",
		"dst.len", b.len, "dst.cap", b.cap, "n", n)
	assert.Always(j <= int(src.len), "append: src out of bounds", "src.len", src.len, "j", j)
	if b.len+n > b.cap || i > j || j > int(src.len) {
		panic(ErrBlockOutOfBounds)
	}
	switch b.typ {
	case BlockBytes:
		switch {
		case n == 1:
			// single value
			b.Bytes().Append(src.Bytes().Get(i))
		case src.IsMaterialized():
			// src is uncompressed (can optimize)
			src.any.(*stringx.StringPool).Range(i, j).AppendTo(b.Bytes(), nil)
		default:
			sel := types.NewRange(i, j-1).AsSelection()
			src.Bytes().AppendTo(b.Bytes(), sel)
		}

	case BlockBool:
		switch {
		case n == 1:
			// single value
			b.Bool().Append(src.Bool().Get(i))
		case src.IsMaterialized():
			// src is uncompressed (can optimize)
			b.Bool().Writer().AppendRange(src.Bool().Writer(), i, j)
		default:
			// src is compressed
			sel := types.NewRange(i, j-1).AsSelection()
			src.Bool().AppendTo(b.Bool().Writer(), sel)
		}

	case BlockInt128:
		switch {
		case n == 1:
			// single value
			b.Int128().Append(src.Int128().Get(i))
		case src.IsMaterialized():
			// src is uncompressed (can optimize)
			s128 := src.any.(*num.Int128Stride)
			d128 := b.any.(*num.Int128Stride)
			s128.Range(i, j).AppendTo(d128, nil)
		default:
			// src is compressed
			sel := types.NewRange(i, j-1).AsSelection()
			d128 := b.any.(*num.Int128Stride)
			src.Int128().AppendTo(d128, sel)
		}

	case BlockInt256:
		switch {
		case n == 1:
			// single value
			b.Int256().Append(src.Int256().Get(i))
		case src.IsMaterialized():
			// src is uncompressed (can optimize)
			s256 := src.any.(*num.Int256Stride)
			d256 := b.any.(*num.Int256Stride)
			s256.Range(i, j).AppendTo(d256, nil)
		default:
			// src is compressed
			sel := types.NewRange(i, j-1).AsSelection()
			d256 := b.any.(*num.Int256Stride)
			src.Int256().AppendTo(d256, sel)
		}

	default:
		switch {
		case n == 1:
			// single value
			switch b.typ {
			case BlockUint64, BlockInt64:
				b.Uint64().Append(src.Uint64().Get(i))
			case BlockUint32, BlockInt32:
				b.Uint32().Append(src.Uint32().Get(i))
			case BlockUint16, BlockInt16:
				b.Uint16().Append(src.Uint16().Get(i))
			case BlockUint8, BlockInt8:
				b.Uint8().Append(src.Uint8().Get(i))
			case BlockFloat64:
				b.Float64().Append(src.Float64().Get(i))
			case BlockFloat32:
				b.Float32().Append(src.Float32().Get(i))
			}
		case src.IsMaterialized():
			// src is uncompressed (can optimize)
			i *= int(b.sz)
			j *= int(b.sz)
			ofs := int(b.len) * int(b.sz)
			dbuf := b.buffer()
			sbuf := src.buffer()
			copy(dbuf[ofs:], sbuf[i:j])
			b.len += n
		default:
			// src is compressed
			sel := types.NewRange(i, j-1).AsSelection()
			switch b.typ {
			case BlockUint64, BlockInt64:
				src.Uint64().AppendTo(b.Uint64().Slice(), sel)
			case BlockUint32, BlockInt32:
				src.Uint32().AppendTo(b.Uint32().Slice(), sel)
			case BlockUint16, BlockInt16:
				src.Uint16().AppendTo(b.Uint16().Slice(), sel)
			case BlockUint8, BlockInt8:
				src.Uint8().AppendTo(b.Uint8().Slice(), sel)
			case BlockFloat64:
				src.Float64().AppendTo(b.Float64().Slice(), sel)
			case BlockFloat32:
				src.Float32().AppendTo(b.Float32().Slice(), sel)
			}
			b.len += n
		}
	}
	b.SetDirty()
}

// AppendTo appends all (sel = nil) or selected elements to dst. Dst
// must be materialized and src may be compressed.
func (b *Block) AppendTo(dst *Block, sel []uint32) {
	// prevent dst overflow
	n := min(b.Len(), dst.Cap()-dst.Len())
	if sel != nil {
		n = min(len(sel), n)
		sel = sel[:n]
	}
	assert.Always(b != nil, "appendTo: nil block, potential use after free")
	assert.Always(dst != nil, "appendTo: nil dst block, potential use after free")
	assert.Always(dst.IsMaterialized(), "appendTo: dst block not materialized")
	assert.Always(dst.Cap()-dst.Len() >= n, "appendTo: dst free capacity smaller than selection")
	switch b.typ {
	case BlockInt64:
		b.Int64().AppendTo(dst.Int64().Slice(), sel)
	case BlockInt32:
		b.Int32().AppendTo(dst.Int32().Slice(), sel)
	case BlockInt16:
		b.Int16().AppendTo(dst.Int16().Slice(), sel)
	case BlockInt8:
		b.Int8().AppendTo(dst.Int8().Slice(), sel)
	case BlockUint64:
		b.Uint64().AppendTo(dst.Uint64().Slice(), sel)
	case BlockUint32:
		b.Uint32().AppendTo(dst.Uint32().Slice(), sel)
	case BlockUint16:
		b.Uint16().AppendTo(dst.Uint16().Slice(), sel)
	case BlockUint8:
		b.Uint8().AppendTo(dst.Uint8().Slice(), sel)
	case BlockFloat64:
		b.Float64().AppendTo(dst.Float64().Slice(), sel)
	case BlockFloat32:
		b.Float32().AppendTo(dst.Float32().Slice(), sel)
	case BlockBytes:
		b.Bytes().AppendTo(dst.Bytes(), sel)
	case BlockBool:
		b.Bool().AppendTo(dst.Bool().Writer(), sel)
	case BlockInt128:
		b.Int128().AppendTo(dst.Int128(), sel)
	case BlockInt256:
		b.Int256().AppendTo(dst.Int256(), sel)
	}
	dst.len += uint32(n)
	dst.SetDirty()
}

func (b *Block) Append(val any) {
	switch b.typ {
	case BlockInt64:
		b.Int64().Append(val.(int64))
	case types.BlockInt32:
		b.Int32().Append(val.(int32))
	case types.BlockInt16:
		b.Int16().Append(val.(int16))
	case types.BlockInt8:
		b.Int8().Append(val.(int8))
	case types.BlockUint64:
		b.Uint64().Append(val.(uint64))
	case types.BlockUint32:
		b.Uint32().Append(val.(uint32))
	case types.BlockUint16:
		b.Uint16().Append(val.(uint16))
	case types.BlockUint8:
		b.Uint8().Append(val.(uint8))
	case types.BlockFloat64:
		b.Float64().Append(val.(float64))
	case types.BlockFloat32:
		b.Float32().Append(val.(float32))
	case types.BlockBool:
		b.Bool().Append(val.(bool))
	case types.BlockBytes:
		b.Bytes().Append(val.([]byte))
	case types.BlockInt128:
		b.Int128().Append(val.(num.Int128))
	case types.BlockInt256:
		b.Int256().Append(val.(num.Int256))
	}
	b.SetDirty()
}

func (b *Block) Get(row int) any {
	switch b.typ {
	case BlockInt64:
		return b.Int64().Get(row)
	case types.BlockInt32:
		return b.Int32().Get(row)
	case types.BlockInt16:
		return b.Int16().Get(row)
	case types.BlockInt8:
		return b.Int8().Get(row)
	case types.BlockUint64:
		return b.Uint64().Get(row)
	case types.BlockUint32:
		return b.Uint32().Get(row)
	case types.BlockUint16:
		return b.Uint16().Get(row)
	case types.BlockUint8:
		return b.Uint8().Get(row)
	case types.BlockFloat64:
		return b.Float64().Get(row)
	case types.BlockFloat32:
		return b.Float32().Get(row)
	case types.BlockBool:
		return b.Bool().Get(row)
	case types.BlockBytes:
		return b.Bytes().Get(row)
	case types.BlockInt128:
		return b.Int128().Get(row)
	case types.BlockInt256:
		return b.Int256().Get(row)
	default:
		return nil
	}
}

func (b *Block) Set(row int, val any) {
	switch b.typ {
	case BlockInt64:
		b.Int64().Set(row, val.(int64))
	case types.BlockInt32:
		b.Int32().Set(row, val.(int32))
	case types.BlockInt16:
		b.Int16().Set(row, val.(int16))
	case types.BlockInt8:
		b.Int8().Set(row, val.(int8))
	case types.BlockUint64:
		b.Uint64().Set(row, val.(uint64))
	case types.BlockUint32:
		b.Uint32().Set(row, val.(uint32))
	case types.BlockUint16:
		b.Uint16().Set(row, val.(uint16))
	case types.BlockUint8:
		b.Uint8().Set(row, val.(uint8))
	case types.BlockFloat64:
		b.Float64().Set(row, val.(float64))
	case types.BlockFloat32:
		b.Float32().Set(row, val.(float32))
	case types.BlockBool:
		if val.(bool) {
			b.Bool().Set(row)
		} else {
			b.Bool().Unset(row)
		}
	case types.BlockBytes:
		b.Bytes().Set(row, val.([]byte))
	case types.BlockInt128:
		b.Int128().Set(row, val.(num.Int128))
	case types.BlockInt256:
		b.Int256().Set(row, val.(num.Int256))
	}
	b.SetDirty()
}

func (b *Block) MinMax() (any, any) {
	switch b.typ {
	case BlockInt64:
		return util.MinMax(b.Int64().Slice()...)
	case BlockInt32:
		return util.MinMax(b.Int32().Slice()...)
	case BlockInt16:
		return util.MinMax(b.Int16().Slice()...)
	case BlockInt8:
		return util.MinMax(b.Int8().Slice()...)
	case BlockUint64:
		return util.MinMax(b.Uint64().Slice()...)
	case BlockUint32:
		return util.MinMax(b.Uint32().Slice()...)
	case BlockUint16:
		return util.MinMax(b.Uint16().Slice()...)
	case BlockUint8:
		return util.MinMax(b.Uint8().Slice()...)
	case BlockInt128:
		return b.Int128().MinMax()
	case BlockInt256:
		return b.Int256().MinMax()
	case BlockFloat64:
		return util.MinMax(b.Float64().Slice()...)
	case BlockFloat32:
		return util.MinMax(b.Float32().Slice()...)
	case BlockBytes:
		minv, maxv := b.Bytes().MinMax()
		return bytes.Clone(minv), bytes.Clone(maxv) // clone
	case BlockBool:
		switch {
		case b.Bool().All():
			return true, true
		case b.Bool().Any():
			return false, true
		default:
			return false, false
		}
	default:
		return nil, nil
	}
}

func (b *Block) Min() any {
	switch b.typ {
	case BlockInt64:
		return util.Min(b.Int64().Slice()...)
	case BlockInt32:
		return util.Min(b.Int32().Slice()...)
	case BlockInt16:
		return util.Min(b.Int16().Slice()...)
	case BlockInt8:
		return util.Min(b.Int8().Slice()...)
	case BlockUint64:
		return util.Min(b.Uint64().Slice()...)
	case BlockUint32:
		return util.Min(b.Uint32().Slice()...)
	case BlockUint16:
		return util.Min(b.Uint16().Slice()...)
	case BlockUint8:
		return util.Min(b.Uint8().Slice()...)
	case BlockInt128:
		return b.Int128().Min()
	case BlockInt256:
		return b.Int256().Min()
	case BlockFloat64:
		return util.Min(b.Float64().Slice()...)
	case BlockFloat32:
		return util.Min(b.Float32().Slice()...)
	case BlockBytes:
		return bytes.Clone(b.Bytes().Min())
	case BlockBool:
		return b.Bool().All()
	default:
		return nil
	}
}

func (b *Block) Max() any {
	switch b.typ {
	case BlockInt64:
		return util.Max(b.Int64().Slice()...)
	case BlockInt32:
		return util.Max(b.Int32().Slice()...)
	case BlockInt16:
		return util.Max(b.Int16().Slice()...)
	case BlockInt8:
		return util.Max(b.Int8().Slice()...)
	case BlockUint64:
		return util.Max(b.Uint64().Slice()...)
	case BlockUint32:
		return util.Max(b.Uint32().Slice()...)
	case BlockUint16:
		return util.Max(b.Uint16().Slice()...)
	case BlockUint8:
		return util.Max(b.Uint8().Slice()...)
	case BlockInt128:
		return b.Int128().Max()
	case BlockInt256:
		return b.Int256().Max()
	case BlockFloat64:
		return util.Max(b.Float64().Slice()...)
	case BlockFloat32:
		return util.Max(b.Float32().Slice()...)
	case BlockBytes:
		return bytes.Clone(b.Bytes().Max())
	case BlockBool:
		return b.Bool().Any()
	default:
		return nil
	}
}
