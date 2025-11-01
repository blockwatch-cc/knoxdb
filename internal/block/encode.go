// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package block

import (
	"bytes"
	"fmt"
	"io"

	"blockwatch.cc/knoxdb/internal/arena"
	"blockwatch.cc/knoxdb/internal/bitset"
	"blockwatch.cc/knoxdb/internal/encode"
	"blockwatch.cc/knoxdb/internal/types"
)

func (b *Block) Encode(c types.BlockCompression) ([]byte, encode.ContextExporter, error) {
	if !b.IsMaterialized() {
		return nil, nil, ErrBlockNotMaterialized
	}

	// encode with best scheme selection
	buf, ctx, err := b.encode()
	if err != nil {
		return nil, nil, err
	}

	// optional: compress block buffer
	if c > 0 {
		cbuf := bytes.NewBuffer(arena.AllocBytes(len(buf)))
		cbuf.WriteByte(byte(c))
		enc := NewCompressor(cbuf, c)
		if _, err := enc.Write(buf); err != nil {
			return nil, nil, err
		}
		if err := enc.Close(); err != nil {
			return nil, nil, err
		}
		arena.Free(buf)
		buf = cbuf.Bytes()
	} else {
		buf[0] = byte(types.BlockCompressNone)
	}
	return buf, ctx, nil
}

func (b *Block) encode() ([]byte, encode.ContextExporter, error) {
	switch b.typ {
	case BlockInt64:
		src := b.Int64().Slice()
		ctx := encode.AnalyzeInt(src, true)
		enc := encode.EncodeInt(ctx, src)
		// add zero byte for compression
		buf := arena.AllocBytes(enc.Size() + 1)
		buf = enc.Store(buf[:1])
		enc.Close()
		return buf, ctx, nil

	case BlockInt32:
		src := b.Int32().Slice()
		ctx := encode.AnalyzeInt(src, true)
		enc := encode.EncodeInt(ctx, src)
		// add zero byte for compression
		buf := arena.AllocBytes(enc.Size() + 1)
		buf = enc.Store(buf[:1])
		enc.Close()
		return buf, ctx, nil

	case BlockInt16:
		src := b.Int16().Slice()
		ctx := encode.AnalyzeInt(src, true)
		enc := encode.EncodeInt(ctx, src)
		// add zero byte for compression
		buf := arena.AllocBytes(enc.Size() + 1)
		buf = enc.Store(buf[:1])
		enc.Close()
		return buf, ctx, nil

	case BlockInt8:
		src := b.Int8().Slice()
		ctx := encode.AnalyzeInt(src, true)
		enc := encode.EncodeInt(ctx, src)
		// add zero byte for compression
		buf := arena.AllocBytes(enc.Size() + 1)
		buf = enc.Store(buf[:1])
		enc.Close()
		return buf, ctx, nil

	case BlockUint64:
		src := b.Uint64().Slice()
		ctx := encode.AnalyzeInt(src, true)
		enc := encode.EncodeInt(ctx, src)
		// add zero byte for compression
		buf := arena.AllocBytes(enc.Size() + 1)
		buf = enc.Store(buf[:1])
		enc.Close()
		return buf, ctx, nil

	case BlockUint32:
		src := b.Uint32().Slice()
		ctx := encode.AnalyzeInt(src, true)
		enc := encode.EncodeInt(ctx, src)
		// add zero byte for compression
		buf := arena.AllocBytes(enc.Size() + 1)
		buf = enc.Store(buf[:1])
		enc.Close()
		return buf, ctx, nil

	case BlockUint16:
		src := b.Uint16().Slice()
		ctx := encode.AnalyzeInt(src, true)
		enc := encode.EncodeInt(ctx, src)
		// add zero byte for compression
		buf := arena.AllocBytes(enc.Size() + 1)
		buf = enc.Store(buf[:1])
		enc.Close()
		return buf, ctx, nil

	case BlockUint8:
		src := b.Uint8().Slice()
		ctx := encode.AnalyzeInt(src, true)
		enc := encode.EncodeInt(ctx, src)
		// add zero byte for compression
		buf := arena.AllocBytes(enc.Size() + 1)
		buf = enc.Store(buf[:1])
		enc.Close()
		return buf, ctx, nil

	case BlockFloat64:
		src := b.Float64().Slice()
		ctx := encode.AnalyzeFloat(src, true, true)
		enc := encode.EncodeFloat(ctx, src)
		// add zero byte for compression
		buf := arena.AllocBytes(enc.Size() + 1)
		buf = enc.Store(buf[:1])
		enc.Close()
		return buf, ctx, nil

	case BlockFloat32:
		src := b.Float32().Slice()
		ctx := encode.AnalyzeFloat(src, true, true)
		enc := encode.EncodeFloat(ctx, src)
		// add zero byte for compression
		buf := arena.AllocBytes(enc.Size() + 1)
		buf = enc.Store(buf[:1])
		enc.Close()
		return buf, ctx, nil

	case BlockBool:
		src := b.Bool()
		ctx := encode.AnalyzeBitmap(src.(*bitset.Bitset))
		enc := encode.EncodeBitmap(ctx, src.(*bitset.Bitset))
		// add zero byte for compression
		buf := arena.AllocBytes(enc.Size() + 1)
		buf = enc.Store(buf[:1])
		enc.Close()
		return buf, ctx, nil

	case BlockBytes:
		src := b.Bytes()
		ctx := encode.AnalyzeString(src)
		enc := encode.EncodeString(ctx, src)
		// add zero byte for compression
		buf := arena.AllocBytes(enc.Size() + 1)
		buf = enc.Store(buf[:1])
		enc.Close()
		return buf, ctx, nil

	case BlockInt128:
		i128 := b.Int128().Slice()
		ctx := encode.AnalyzeInt128(i128)
		enc := encode.EncodeInt128(ctx, i128)
		// add zero byte for compression
		buf := arena.AllocBytes(enc.Size() + 1)
		buf = enc.Store(buf[:1])
		enc.Close()
		return buf, ctx, nil

	case BlockInt256:
		i256 := b.Int256().Slice()
		ctx := encode.AnalyzeInt256(i256)
		enc := encode.EncodeInt256(ctx, i256)
		// add zero byte for compression
		buf := arena.AllocBytes(enc.Size() + 1)
		buf = enc.Store(buf[:1])
		enc.Close()
		return buf, ctx, nil

	default:
		return nil, nil, fmt.Errorf("block: unsupported data type %s (%[1]d)", b.typ)
	}
}

func Decode(typ BlockType, buf []byte) (*Block, error) {
	if len(buf) == 0 {
		return nil, io.ErrShortBuffer
	}

	// read optional block compression
	comp := types.BlockCompression(buf[0])

	if comp > 0 {
		// decode block data with optional decompressor
		dec := NewDecompressor(bytes.NewBuffer(buf[1:]), comp)
		dbuf, err := io.ReadAll(dec)
		if err != nil {
			return nil, err
		}
		if err := dec.Close(); err != nil {
			return nil, err
		}
		buf = dbuf[1:]

		// TODO: BufferManager: at this point we hold a copy of the decompressed
		// data which will be referenced by an encode container. we can release
		// any page locks

	} else {
		// TODO: BufferManager: here we reference data from a buffer page and
		// must hold the lock until the block is released (page lock release
		// happens during block.Deref or we replace Deref with page ref)

		// with boltdb the backing buffer may become invalid after the tx closes
		// hence we must make a copy here to allow the block to be cached and shared
		buf = bytes.Clone(buf[1:])
	}

	b := blockPool.Get().(*Block)
	b.nref.Store(1)
	b.typ = typ

	// decode from buffer, set len/cap
	switch b.typ {
	case BlockInt64:
		c, err := encode.LoadInt[int64](buf)
		if err != nil {
			return nil, err
		}
		b.any = c
		b.len = uint32(c.Len())

	case BlockUint64:
		c, err := encode.LoadInt[uint64](buf)
		if err != nil {
			return nil, err
		}
		b.any = c
		b.len = uint32(c.Len())

	case BlockInt32:
		c, err := encode.LoadInt[int32](buf)
		if err != nil {
			return nil, err
		}
		b.any = c
		b.len = uint32(c.Len())

	case BlockUint32:
		c, err := encode.LoadInt[uint32](buf)
		if err != nil {
			return nil, err
		}
		b.any = c
		b.len = uint32(c.Len())

	case BlockInt16:
		c, err := encode.LoadInt[int16](buf)
		if err != nil {
			return nil, err
		}
		b.any = c
		b.len = uint32(c.Len())

	case BlockUint16:
		c, err := encode.LoadInt[uint16](buf)
		if err != nil {
			return nil, err
		}
		b.any = c
		b.len = uint32(c.Len())

	case BlockInt8:
		c, err := encode.LoadInt[int8](buf)
		if err != nil {
			return nil, err
		}
		b.any = c
		b.len = uint32(c.Len())

	case BlockUint8:
		c, err := encode.LoadInt[uint8](buf)
		if err != nil {
			return nil, err
		}
		b.any = c
		b.len = uint32(c.Len())

	case BlockFloat64:
		c, err := encode.LoadFloat[float64](buf)
		if err != nil {
			return nil, err
		}
		b.any = c
		b.len = uint32(c.Len())

	case BlockFloat32:
		c, err := encode.LoadFloat[float32](buf)
		if err != nil {
			return nil, err
		}
		b.any = c
		b.len = uint32(c.Len())

	case BlockBytes:
		c, err := encode.LoadString(buf)
		if err != nil {
			return nil, err
		}
		b.any = c
		b.len = uint32(c.Len())

	case BlockBool:
		c, err := encode.LoadBitmap(buf)
		if err != nil {
			return nil, err
		}
		b.any = c
		b.len = uint32(c.Len())

	case BlockInt128:
		c, err := encode.LoadInt128(buf)
		if err != nil {
			return nil, err
		}
		b.any = c
		b.len = uint32(c.Len())

	case BlockInt256:
		c, err := encode.LoadInt256(buf)
		if err != nil {
			return nil, err
		}
		b.any = c
		b.len = uint32(c.Len())

	default:
		return nil, fmt.Errorf("block: unsupported data type %s (%[1]d)", b.typ)
	}
	b.cap = b.len

	return b, nil
}
