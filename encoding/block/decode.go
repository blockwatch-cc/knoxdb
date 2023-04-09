// Copyright (c) 2018-2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package block

import (
	"bytes"
	"fmt"
	"io"
	"sync"

	"blockwatch.cc/knoxdb/encoding/bignum"
	"blockwatch.cc/knoxdb/encoding/bitset"
	"blockwatch.cc/knoxdb/encoding/compress"
	"blockwatch.cc/knoxdb/encoding/dedup"
	"github.com/klauspost/compress/snappy"
	"github.com/pierrec/lz4"
)

// 512k - size of a single block that fits 32k 8byte values + 1 page extra headers
const bufSizeHint = 1<<18 + 4096

var bufferPool = &sync.Pool{
	New: func() interface{} { return make([]byte, 0, bufSizeHint) },
}

func decodeTimeBlock(block []byte, dst []int64) ([]int64, error) {
	buf, canRecycle, err := unpackBlock(block, BlockTypeTime)
	if err != nil {
		return nil, err
	}

	b, err := compress.TimeArrayDecodeAll(buf, dst)
	if canRecycle && cap(buf) == bufSizeHint {
		bufferPool.Put(buf[:0])
	}
	return b, err
}

func decodeFloat64Block(block []byte, dst []float64) ([]float64, error) {
	buf, canRecycle, err := unpackBlock(block, BlockTypeFloat64)
	if err != nil {
		return nil, err
	}
	b, err := compress.FloatArrayDecodeAll(buf, dst)
	if canRecycle && cap(buf) == bufSizeHint {
		bufferPool.Put(buf[:0])
	}
	return b, err
}

func decodeFloat32Block(block []byte, dst []float32) ([]float32, error) {
	buf, canRecycle, err := unpackBlock(block, BlockTypeFloat32)
	if err != nil {
		return nil, err
	}

	v := arena.Alloc(BlockTypeFloat64, len(dst))
	cp := v.([]float64)[:len(dst)]
	b, err := compress.FloatArrayDecodeAll(buf, cp)
	if cap(dst) >= len(b) {
		dst = dst[:len(b)]
	} else {
		dst = make([]float32, len(b))
	}

	for i, _ := range b {
		dst[i] = float32(b[i])
	}
	arena.Free(BlockTypeFloat64, v)

	if canRecycle && cap(buf) == bufSizeHint {
		bufferPool.Put(buf[:0])
	}
	return dst, err
}

func decodeInt256Block(block []byte, dst bignum.Int256LLSlice) (bignum.Int256LLSlice, error) {
	buf, canRecycle, err := unpackBlock(block, BlockTypeInt256)
	if err != nil {
		return dst, err
	}

	// empty blocks are empty
	if len(buf) == 0 {
		return dst, nil
	}

	v := arena.Alloc(BlockTypeInt64, dst.Len())
	tmp := v.([]int64)[:0]

	defer func() {
		if canRecycle && cap(buf) == bufSizeHint {
			bufferPool.Put(buf[:0])
		}
		arena.Free(BlockTypeInt64, v)
	}()

	// unpack 4 int64 strides
	strideBuf := bytes.NewBuffer(buf)
	for i := 0; i < 4; i++ {
		strideLen := int(bigEndian.Uint32(strideBuf.Next(4)[:]))
		tmp, err := compress.ArrayDecodeAllInt64(strideBuf.Next(strideLen), tmp)
		if err != nil {
			return dst, err
		}

		// copy stride
		switch i {
		case 0:
			dst.X0 = dst.X0[:len(tmp)]
			copy(dst.X0, tmp)
		case 1:
			dst.X1 = dst.X1[:len(tmp)]
			srcint := compress.ReintepretInt64ToUint64Slice(tmp)
			copy(dst.X1, srcint)
		case 2:
			dst.X2 = dst.X2[:len(tmp)]
			srcint := compress.ReintepretInt64ToUint64Slice(tmp)
			copy(dst.X2, srcint)
		case 3:
			dst.X3 = dst.X3[:len(tmp)]
			srcint := compress.ReintepretInt64ToUint64Slice(tmp)
			copy(dst.X3, srcint)
		}
	}
	return dst, nil
}

func decodeInt128Block(block []byte, dst bignum.Int128LLSlice) (bignum.Int128LLSlice, error) {
	buf, canRecycle, err := unpackBlock(block, BlockTypeInt128)
	if err != nil {
		return dst, err
	}

	// empty blocks are empty
	if len(buf) == 0 {
		return dst, nil
	}

	// use a temp int64 slice for decoding
	v := arena.Alloc(BlockTypeInt64, dst.Len())
	tmp := v.([]int64)[:0]

	defer func() {
		if canRecycle && cap(buf) == bufSizeHint {
			bufferPool.Put(buf[:0])
		}
		arena.Free(BlockTypeInt64, v)
	}()

	// unpack 2 int64 strides
	strideBuf := bytes.NewBuffer(buf)
	for i := 0; i < 2; i++ {
		strideLen := int(bigEndian.Uint32(strideBuf.Next(4)[:]))
		tmp, err := compress.ArrayDecodeAllInt64(strideBuf.Next(strideLen), tmp)
		if err != nil {
			return dst, err
		}

		// copy stride
		if i == 0 {
			dst.X0 = dst.X0[:len(tmp)]
			copy(dst.X0, tmp)
		} else {
			dst.X1 = dst.X1[:len(tmp)]
			srcint := compress.ReintepretInt64ToUint64Slice(tmp)
			copy(dst.X1, srcint)
		}
	}
	return dst, nil
}

func decodeInt64Block(block []byte, dst []int64) ([]int64, error) {
	buf, canRecycle, err := unpackBlock(block, BlockTypeInt64)
	if err != nil {
		return nil, err
	}
	b, err := compress.ArrayDecodeAllInt64(buf, dst)
	if canRecycle && cap(buf) == bufSizeHint {
		bufferPool.Put(buf[:0])
	}
	return b, err
}

func decodeInt32Block(block []byte, dst []int32) ([]int32, error) {
	buf, canRecycle, err := unpackBlock(block, BlockTypeInt32)
	if err != nil {
		return nil, err
	}
	b, err := compress.ArrayDecodeAllInt32(buf, dst)
	if canRecycle && cap(buf) == bufSizeHint {
		bufferPool.Put(buf[:0])
	}
	return b, err
}

func decodeInt16Block(block []byte, dst []int16) ([]int16, error) {
	buf, canRecycle, err := unpackBlock(block, BlockTypeInt16)
	if err != nil {
		return nil, err
	}
	b, err := compress.ArrayDecodeAllInt16(buf, dst)
	if canRecycle && cap(buf) == bufSizeHint {
		bufferPool.Put(buf[:0])
	}
	return b, err
}

func decodeInt8Block(block []byte, dst []int8) ([]int8, error) {
	buf, canRecycle, err := unpackBlock(block, BlockTypeInt8)
	if err != nil {
		return nil, err
	}
	b, err := compress.ArrayDecodeAllInt8(buf, dst)
	if canRecycle && cap(buf) == bufSizeHint {
		bufferPool.Put(buf[:0])
	}
	return b, err
}

func decodeUint64Block(block []byte, dst []uint64) ([]uint64, error) {
	buf, canRecycle, err := unpackBlock(block, BlockTypeUint64)
	if err != nil {
		return nil, err
	}
	b, err := compress.ArrayDecodeAllUint64(buf, dst)
	if canRecycle && cap(buf) == bufSizeHint {
		bufferPool.Put(buf[:0])
	}
	return b, err
}

func decodeUint32Block(block []byte, dst []uint32) ([]uint32, error) {
	buf, canRecycle, err := unpackBlock(block, BlockTypeUint32)
	if err != nil {
		return nil, err
	}
	b, err := compress.ArrayDecodeAllUint32(buf, dst)
	if canRecycle && cap(buf) == bufSizeHint {
		bufferPool.Put(buf[:0])
	}
	return b, err
}

func decodeUint16Block(block []byte, dst []uint16) ([]uint16, error) {
	buf, canRecycle, err := unpackBlock(block, BlockTypeUint16)
	if err != nil {
		return nil, err
	}
	b, err := compress.ArrayDecodeAllUint16(buf, dst)
	if canRecycle && cap(buf) == bufSizeHint {
		bufferPool.Put(buf[:0])
	}
	return b, err
}

func decodeUint8Block(block []byte, dst []uint8) ([]uint8, error) {
	buf, canRecycle, err := unpackBlock(block, BlockTypeUint8)
	if err != nil {
		return nil, err
	}
	b, err := compress.ArrayDecodeAllUint8(buf, dst)
	if canRecycle && cap(buf) == bufSizeHint {
		bufferPool.Put(buf[:0])
	}
	return b, err
}

func decodeBoolBlock(block []byte, dst *bitset.Bitset) (*bitset.Bitset, error) {
	buf, canRecycle, err := unpackBlock(block, BlockTypeBool)
	if err != nil {
		return nil, err
	}
	b, err := compress.BitsetDecodeAll(buf, dst)
	if canRecycle && cap(buf) == bufSizeHint {
		bufferPool.Put(buf[:0])
	}
	return b, err
}

func decodeStringBlock(block []byte, dst dedup.ByteArray, sz int) (dedup.ByteArray, error) {
	buf, canRecycle, err := unpackBlock(block, BlockTypeString)
	if err != nil {
		return nil, err
	}
	b, err := dedup.Decode(buf, dst, sz)
	if canRecycle && cap(buf) == bufSizeHint {
		bufferPool.Put(buf[:0])
	}
	return b, err
}

func decodeBytesBlock(block []byte, dst dedup.ByteArray, sz int) (dedup.ByteArray, error) {
	buf, canRecycle, err := unpackBlock(block, BlockTypeBytes)
	if err != nil {
		return nil, err
	}
	b, err := dedup.Decode(buf, dst, sz)
	if canRecycle && cap(buf) == bufSizeHint {
		bufferPool.Put(buf[:0])
	}
	return b, err
}

func unpackBlock(block []byte, typ BlockType) ([]byte, bool, error) {
	err := ensureBlockType(block, typ)
	if err != nil {
		return nil, false, err
	}
	comp, err := readBlockCompression(block)
	switch comp {
	case SnappyCompression:
		// snappy will allocate a new block when more space is needed
		sz, err := snappy.DecodedLen(block[1:])
		if err != nil {
			return nil, false, err
		}
		var dst []byte
		canRecycle := typ != BlockTypeBytes && typ != BlockTypeString
		if sz <= bufSizeHint {
			dst = bufferPool.Get().([]byte)[:0]
		} else {
			dst = make([]byte, 0, int(sz))
			canRecycle = false
		}
		buf, err := snappy.Decode(dst[:sz], block[1:])
		if err != nil {
			if canRecycle {
				bufferPool.Put(dst[:0])
			}
			return nil, false, fmt.Errorf("pack: snappy decode error: %v", err)
		}
		return buf, canRecycle, nil

	case LZ4Compression:
		// FIXME: fails with data blocks > 4M
		buf := bytes.NewBuffer(block[1:])
		dec := lz4ReaderPool.Get().(*lz4.Reader)
		dec.Reset(buf)
		// parse lz4 frame header
		_, err := dec.Read(nil)
		if err != nil {
			lz4ReaderPool.Put(dec)
			return nil, false, fmt.Errorf("pack: lz4 header decode error: %v", err)
		}
		// alloc output buffer (note: this will be referenced if type is byte or string)
		sz := dec.Header.Size
		var dst []byte
		canRecycle := typ != BlockTypeBytes && typ != BlockTypeString
		if sz <= bufSizeHint {
			dst = bufferPool.Get().([]byte)[:0]
		} else {
			dst = make([]byte, 0, int(sz))
			canRecycle = false
		}
		n, err := io.Copy(bytes.NewBuffer(dst), dec)
		lz4ReaderPool.Put(dec)
		if err != nil {
			return nil, false, fmt.Errorf("pack: lz4 body decode error: %v", err)
		}
		return dst[:n], canRecycle, nil

	case NoCompression:
		// Just strip the header byte, dedup.ByteArray will copy data and
		// never reference to prevent keeping refs into mmapped buffers
		// (boltdb only guarantees buffer mappings are stable inside a tx).
		return block[1:], false, nil

	default:
		return nil, false, err
	}
}

// readBlockType returns the type of value encoded in a block or an error
// if the block type is unknown.
func readBlockType(block []byte) (BlockType, error) {
	blockType := BlockType(block[0] & blockTypeMask)
	switch blockType {
	case BlockTypeTime,
		BlockTypeInt64,
		BlockTypeUint64,
		BlockTypeFloat64,
		BlockTypeBool,
		BlockTypeString,
		BlockTypeBytes,
		BlockTypeInt32,
		BlockTypeInt16,
		BlockTypeInt8,
		BlockTypeUint32,
		BlockTypeUint16,
		BlockTypeUint8,
		BlockTypeFloat32,
		BlockTypeInt128,
		BlockTypeInt256:
		return blockType, nil
	default:
		return 0, fmt.Errorf("pack: unknown block type: %d", blockType)
	}
}

// ensureBlockType reads and checks the type of a block and returns an error
// if the block type is unknown or unexpected.
func ensureBlockType(block []byte, typ BlockType) error {
	t, err := readBlockType(block)
	if err != nil {
		return err
	}
	if t != typ {
		return fmt.Errorf("pack: unexpected block type %s for %s", t, typ)
	}
	return nil
}

// readBlockCompression returns the compression type of encoded block or an error
// if the block compression is unknown.
func readBlockCompression(block []byte) (Compression, error) {
	blockCompression := Compression((block[0] >> 5) & blockCompressionMask)
	switch blockCompression {
	case NoCompression, LZ4Compression, SnappyCompression:
		return blockCompression, nil
	default:
		return 0, fmt.Errorf("pack: unknown block compression: %d", blockCompression)
	}
}
