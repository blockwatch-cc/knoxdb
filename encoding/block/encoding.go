// Copyright (c) 2018-2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package block

import (
	"bytes"
	"errors"
	"fmt"
	"io"

	"blockwatch.cc/knoxdb/encoding/compress"
	"github.com/golang/snappy"
	"github.com/pierrec/lz4"
)

const (
	// default encoder/decoder buffer size in elements (32k)
	DefaultMaxPointsPerBlock = 1 << 15

	// 256k - size of a single block that fits 32k 8byte values
	BlockSizeHint = 1 << 18

	// encodedBlockHeaderSize is the size of the header for an encoded block.
	// There is one byte encoding the type of the block.
	encodedBlockHeaderSize = 1
)

func encodeTimeBlock(buf *bytes.Buffer, val []int64, comp Compression) (int64, int64, error) {
	if len(val) == 0 {
		return 0, 0, writeEmptyBlock(buf, BlockTime)
	}

	buf.WriteByte(byte(comp<<5) | byte(BlockTime))
	w := getWriter(buf, comp)

	// copy source values to avoid overwriting them
	var (
		cp []int64
		v  interface{}
	)
	if len(val) <= DefaultMaxPointsPerBlock {
		v = int64Pool.Get()
		cp = v.([]int64)[:len(val)]
	} else {
		cp = make([]int64, len(val))
	}
	copy(cp, val)

	min, max, err := compress.TimeArrayEncodeAll(cp, w)
	if v != nil {
		int64Pool.Put(v)
	}
	if err != nil {
		_ = w.Close()
		putWriter(w, comp)
		return 0, 0, err
	}

	err = w.Close()
	putWriter(w, comp)
	return min, max, err
}

func decodeTimeBlock(block []byte, dst []int64) ([]int64, error) {
	buf, canRecycle, err := unpackBlock(block, BlockTime)
	if err != nil {
		return nil, err
	}

	b, err := compress.TimeArrayDecodeAll(buf, dst)
	if canRecycle && cap(buf) == BlockSizeHint {
		BlockEncoderPool.Put(buf[:0])
	}
	return b, err
}

func encodeFloat64Block(buf *bytes.Buffer, val []float64, comp Compression) (float64, float64, error) {
	if len(val) == 0 {
		return 0, 0, writeEmptyBlock(buf, BlockFloat64)
	}

	buf.WriteByte(byte(comp<<5) | byte(BlockFloat64))
	w := getWriter(buf, comp)
	min, max, err := compress.FloatArrayEncodeAll(val, w)
	if err != nil {
		_ = w.Close()
		putWriter(w, comp)
		return 0, 0, err
	}

	err = w.Close()
	putWriter(w, comp)
	return min, max, err
}

func encodeFloat32Block(buf *bytes.Buffer, val []float32, comp Compression) (float32, float32, error) {
	return 0, 0, errors.New("encodeFloat32Block not yet implemented")
	/*if len(val) == 0 {
		return 0, 0, writeEmptyBlock(buf, BlockFloat64)
	}

	buf.WriteByte(byte(comp<<5) | byte(BlockFloat64))
	w := getWriter(buf, comp)
	min, max, err := compress.FloatArrayEncodeAll(val, w)
	if err != nil {
		_ = w.Close()
		putWriter(w, comp)
		return 0, 0, err
	}

	err = w.Close()
	putWriter(w, comp)
	return min, max, err*/
}

func decodeFloat64Block(block []byte, dst []float64) ([]float64, error) {
	buf, canRecycle, err := unpackBlock(block, BlockFloat64)
	if err != nil {
		return nil, err
	}
	b, err := compress.FloatArrayDecodeAll(buf, dst)
	if canRecycle && cap(buf) == BlockSizeHint {
		BlockEncoderPool.Put(buf[:0])
	}
	return b, err
}

func decodeFloat32Block(block []byte, dst []float32) ([]float32, error) {
	return nil, errors.New("decodeFloat32Block not yet implemented")
	/* buf, canRecycle, err := unpackBlock(block, BlockFloat32)
	if err != nil {
		return nil, err
	}
	b, err := compress.FloatArrayDecodeAll(buf, dst)
	if canRecycle && cap(buf) == BlockSizeHint {
		BlockEncoderPool.Put(buf[:0])
	}
	return b, err */
}

func encodeInt64Block(buf *bytes.Buffer, val []int64, comp Compression) (int64, int64, error) {
	if len(val) == 0 {
		return 0, 0, writeEmptyBlock(buf, BlockInt64)
	}

	buf.WriteByte(byte(comp<<5) | byte(BlockInt64))
	w := getWriter(buf, comp)
	var (
		cp []int64
		v  interface{}
	)
	if len(val) <= DefaultMaxPointsPerBlock {
		v = int64Pool.Get()
		cp = v.([]int64)[:len(val)]
	} else {
		cp = make([]int64, len(val))
	}
	copy(cp, val)

	min, max, err := compress.IntegerArrayEncodeAll(cp, w)
	if v != nil {
		int64Pool.Put(v)
	}
	if err != nil {
		_ = w.Close()
		putWriter(w, comp)
		return 0, 0, err
	}

	err = w.Close()
	putWriter(w, comp)
	return min, max, err
}

func encodeInt32Block(buf *bytes.Buffer, val []int32, comp Compression) (int32, int32, error) {
	if len(val) == 0 {
		return 0, 0, writeEmptyBlock(buf, BlockInt32)
	}

	buf.WriteByte(byte(comp<<5) | byte(BlockInt32))
	w := getWriter(buf, comp)
	var (
		cp []int64
		v  interface{}
	)
	if len(val) <= DefaultMaxPointsPerBlock {
		v = int64Pool.Get()
		cp = v.([]int64)[:len(val)]
	} else {
		cp = make([]int64, len(val))
	}
	//copy(cp, val)
	for i, _ := range val {
		cp[i] = int64(val[i])
	}

	min, max, err := compress.IntegerArrayEncodeAll(cp, w)
	if v != nil {
		int64Pool.Put(v)
	}
	if err != nil {
		_ = w.Close()
		putWriter(w, comp)
		return 0, 0, err
	}

	err = w.Close()
	putWriter(w, comp)
	return int32(min), int32(max), err
}

func encodeInt16Block(buf *bytes.Buffer, val []int16, comp Compression) (int16, int16, error) {
	if len(val) == 0 {
		return 0, 0, writeEmptyBlock(buf, BlockInt16)
	}

	buf.WriteByte(byte(comp<<5) | byte(BlockInt16))
	w := getWriter(buf, comp)
	var (
		cp []int64
		v  interface{}
	)
	if len(val) <= DefaultMaxPointsPerBlock {
		v = int64Pool.Get()
		cp = v.([]int64)[:len(val)]
	} else {
		cp = make([]int64, len(val))
	}
	//copy(cp, val)
	for i, _ := range val {
		cp[i] = int64(val[i])
	}

	min, max, err := compress.IntegerArrayEncodeAll(cp, w)
	if v != nil {
		int64Pool.Put(v)
	}
	if err != nil {
		_ = w.Close()
		putWriter(w, comp)
		return 0, 0, err
	}

	err = w.Close()
	putWriter(w, comp)
	return int16(min), int16(max), err
}

func encodeInt8Block(buf *bytes.Buffer, val []int8, comp Compression) (int8, int8, error) {
	if len(val) == 0 {
		return 0, 0, writeEmptyBlock(buf, BlockInt8)
	}

	buf.WriteByte(byte(comp<<5) | byte(BlockInt8))
	w := getWriter(buf, comp)
	var (
		cp []int64
		v  interface{}
	)
	if len(val) <= DefaultMaxPointsPerBlock {
		v = int64Pool.Get()
		cp = v.([]int64)[:len(val)]
	} else {
		cp = make([]int64, len(val))
	}
	//copy(cp, val)
	for i, _ := range val {
		cp[i] = int64(val[i])
	}

	min, max, err := compress.IntegerArrayEncodeAll(cp, w)
	if v != nil {
		int64Pool.Put(v)
	}
	if err != nil {
		_ = w.Close()
		putWriter(w, comp)
		return 0, 0, err
	}

	err = w.Close()
	putWriter(w, comp)
	return int8(min), int8(max), err
}

func decodeInt64Block(block []byte, dst []int64) ([]int64, error) {
	buf, canRecycle, err := unpackBlock(block, BlockInt64)
	if err != nil {
		return nil, err
	}
	b, err := compress.IntegerArrayDecodeAll(buf, dst)
	if canRecycle && cap(buf) == BlockSizeHint {
		BlockEncoderPool.Put(buf[:0])
	}
	return b, err
}

func decodeInt32Block(block []byte, dst []int32) ([]int32, error) {
	buf, canRecycle, err := unpackBlock(block, BlockInt32)
	if err != nil {
		return nil, err
	}
	var (
		cp []int64
		v  interface{}
	)
	if len(dst) <= DefaultMaxPointsPerBlock {
		v = int64Pool.Get()
		cp = v.([]int64)[:len(dst)]
	} else {
		cp = make([]int64, len(dst))
	}

	b, err := compress.IntegerArrayDecodeAll(buf, cp)

	if cap(dst) >= len(b) {
		dst = dst[:len(b)]
	} else {
		dst = make([]int32, len(b))
	}

	for i, _ := range b {
		dst[i] = int32(b[i])
	}

	if v != nil {
		int64Pool.Put(v)
	}

	if canRecycle && cap(buf) == BlockSizeHint {
		BlockEncoderPool.Put(buf[:0])
	}
	return dst, err
}

func decodeInt16Block(block []byte, dst []int16) ([]int16, error) {
	buf, canRecycle, err := unpackBlock(block, BlockInt16)
	if err != nil {
		return nil, err
	}
	var (
		cp []int64
		v  interface{}
	)
	if len(dst) <= DefaultMaxPointsPerBlock {
		v = int64Pool.Get()
		cp = v.([]int64)[:len(dst)]
	} else {
		cp = make([]int64, len(dst))
	}

	b, err := compress.IntegerArrayDecodeAll(buf, cp)

	if cap(dst) >= len(b) {
		dst = dst[:len(b)]
	} else {
		dst = make([]int16, len(b))
	}

	for i, _ := range b {
		dst[i] = int16(b[i])
	}

	if v != nil {
		int64Pool.Put(v)
	}

	if canRecycle && cap(buf) == BlockSizeHint {
		BlockEncoderPool.Put(buf[:0])
	}
	return dst, err
}

func decodeInt8Block(block []byte, dst []int8) ([]int8, error) {
	buf, canRecycle, err := unpackBlock(block, BlockInt8)
	if err != nil {
		return nil, err
	}
	var (
		cp []int64
		v  interface{}
	)
	if len(dst) <= DefaultMaxPointsPerBlock {
		v = int64Pool.Get()
		cp = v.([]int64)[:len(dst)]
	} else {
		cp = make([]int64, len(dst))
	}

	b, err := compress.IntegerArrayDecodeAll(buf, cp)

	if cap(dst) >= len(b) {
		dst = dst[:len(b)]
	} else {
		dst = make([]int8, len(b))
	}

	for i, _ := range b {
		dst[i] = int8(b[i])
	}

	if v != nil {
		int64Pool.Put(v)
	}

	if canRecycle && cap(buf) == BlockSizeHint {
		BlockEncoderPool.Put(buf[:0])
	}
	return dst, err
}

func encodeUint64Block(buf *bytes.Buffer, val []uint64, comp Compression) (uint64, uint64, error) {
	if len(val) == 0 {
		return 0, 0, writeEmptyBlock(buf, BlockUint64)
	}

	buf.WriteByte(byte(comp<<5) | byte(BlockUint64))
	w := getWriter(buf, comp)
	var (
		cp []uint64
		v  interface{}
	)
	if len(val) <= DefaultMaxPointsPerBlock {
		v = int64Pool.Get()
		cp = compress.ReintepretInt64ToUint64Slice(v.([]int64)[:len(val)])
	} else {
		cp = make([]uint64, len(val))
	}
	copy(cp, val)

	min, max, err := compress.UnsignedArrayEncodeAll(cp, w)
	if v != nil {
		int64Pool.Put(v)
	}
	if err != nil {
		_ = w.Close()
		putWriter(w, comp)
		return 0, 0, err
	}

	err = w.Close()
	putWriter(w, comp)
	return min, max, err
}

func encodeUint32Block(buf *bytes.Buffer, val []uint32, comp Compression) (uint32, uint32, error) {
	return 0, 0, errors.New("encodeUint32Block not yet implemented")
	/*if len(val) == 0 {
		return 0, 0, writeEmptyBlock(buf, BlockUint64)
	}

	buf.WriteByte(byte(comp<<5) | byte(BlockUint64))
	w := getWriter(buf, comp)
	var (
		cp []uint64
		v  interface{}
	)
	if len(val) <= DefaultMaxPointsPerBlock {
		v = int64Pool.Get()
		cp = compress.ReintepretInt64ToUint64Slice(v.([]int64)[:len(val)])
	} else {
		cp = make([]uint64, len(val))
	}
	copy(cp, val)

	min, max, err := compress.UnsignedArrayEncodeAll(cp, w)
	if v != nil {
		int64Pool.Put(v)
	}
	if err != nil {
		_ = w.Close()
		putWriter(w, comp)
		return 0, 0, err
	}

	err = w.Close()
	putWriter(w, comp)
	return min, max, err*/
}

func encodeUint16Block(buf *bytes.Buffer, val []uint16, comp Compression) (uint16, uint16, error) {
	return 0, 0, errors.New("encodeUint16Block not yet implemented")
	/*if len(val) == 0 {
		return 0, 0, writeEmptyBlock(buf, BlockUint64)
	}

	buf.WriteByte(byte(comp<<5) | byte(BlockUint64))
	w := getWriter(buf, comp)
	var (
		cp []uint64
		v  interface{}
	)
	if len(val) <= DefaultMaxPointsPerBlock {
		v = int64Pool.Get()
		cp = compress.ReintepretInt64ToUint64Slice(v.([]int64)[:len(val)])
	} else {
		cp = make([]uint64, len(val))
	}
	copy(cp, val)

	min, max, err := compress.UnsignedArrayEncodeAll(cp, w)
	if v != nil {
		int64Pool.Put(v)
	}
	if err != nil {
		_ = w.Close()
		putWriter(w, comp)
		return 0, 0, err
	}

	err = w.Close()
	putWriter(w, comp)
	return min, max, err*/
}

func encodeUint8Block(buf *bytes.Buffer, val []uint8, comp Compression) (uint8, uint8, error) {
	return 0, 0, errors.New("encodeUint8Block not yet implemented")
	/*if len(val) == 0 {
		return 0, 0, writeEmptyBlock(buf, BlockUint64)
	}

	buf.WriteByte(byte(comp<<5) | byte(BlockUint64))
	w := getWriter(buf, comp)
	var (
		cp []uint64
		v  interface{}
	)
	if len(val) <= DefaultMaxPointsPerBlock {
		v = int64Pool.Get()
		cp = compress.ReintepretInt64ToUint64Slice(v.([]int64)[:len(val)])
	} else {
		cp = make([]uint64, len(val))
	}
	copy(cp, val)

	min, max, err := compress.UnsignedArrayEncodeAll(cp, w)
	if v != nil {
		int64Pool.Put(v)
	}
	if err != nil {
		_ = w.Close()
		putWriter(w, comp)
		return 0, 0, err
	}

	err = w.Close()
	putWriter(w, comp)
	return min, max, err*/
}

func decodeUint64Block(block []byte, dst []uint64) ([]uint64, error) {
	buf, canRecycle, err := unpackBlock(block, BlockUint64)
	if err != nil {
		return nil, err
	}
	b, err := compress.UnsignedArrayDecodeAll(buf, dst)
	if canRecycle && cap(buf) == BlockSizeHint {
		BlockEncoderPool.Put(buf[:0])
	}
	return b, err
}

func decodeUint32Block(block []byte, dst []uint32) ([]uint32, error) {
	return nil, errors.New("decodeUint32Block not yet implemented")
	/* buf, canRecycle, err := unpackBlock(block, BlockUint64)
	if err != nil {
		return nil, err
	}
	b, err := compress.UnsignedArrayDecodeAll(buf, dst)
	if canRecycle && cap(buf) == BlockSizeHint {
		BlockEncoderPool.Put(buf[:0])
	}
	return b, err */
}

func decodeUint16Block(block []byte, dst []uint16) ([]uint16, error) {
	return nil, errors.New("decodeUint16Block not yet implemented")
	/* buf, canRecycle, err := unpackBlock(block, BlockUint64)
	if err != nil {
		return nil, err
	}
	b, err := compress.UnsignedArrayDecodeAll(buf, dst)
	if canRecycle && cap(buf) == BlockSizeHint {
		BlockEncoderPool.Put(buf[:0])
	}
	return b, err */
}

func decodeUint8Block(block []byte, dst []uint8) ([]uint8, error) {
	return nil, errors.New("decodeUint8Block not yet implemented")
	/* buf, canRecycle, err := unpackBlock(block, BlockUint64)
	if err != nil {
		return nil, err
	}
	b, err := compress.UnsignedArrayDecodeAll(buf, dst)
	if canRecycle && cap(buf) == BlockSizeHint {
		BlockEncoderPool.Put(buf[:0])
	}
	return b, err */
}

func encodeBoolBlock(buf *bytes.Buffer, val []bool, comp Compression) (bool, bool, error) {
	if len(val) == 0 {
		return false, false, writeEmptyBlock(buf, BlockBool)
	}

	buf.WriteByte(byte(comp<<5) | byte(BlockBool))
	w := getWriter(buf, comp)
	min, max, err := compress.BooleanArrayEncodeAll(val, w)
	if err != nil {
		_ = w.Close()
		putWriter(w, comp)
		return false, false, err
	}

	err = w.Close()
	putWriter(w, comp)
	return min, max, err
}

func decodeBoolBlock(block []byte, dst []bool) ([]bool, error) {
	buf, canRecycle, err := unpackBlock(block, BlockBool)
	if err != nil {
		return nil, err
	}
	b, err := compress.BooleanArrayDecodeAll(buf, dst)
	if canRecycle && cap(buf) == BlockSizeHint {
		BlockEncoderPool.Put(buf[:0])
	}
	return b, err
}

func encodeStringBlock(buf *bytes.Buffer, val []string, comp Compression) (string, string, error) {
	if len(val) == 0 {
		return "", "", writeEmptyBlock(buf, BlockString)
	}

	buf.WriteByte(byte(comp<<5) | byte(BlockString))
	w := getWriter(buf, comp)
	min, max, err := compress.StringArrayEncodeAll(val, w)
	if err != nil {
		_ = w.Close()
		putWriter(w, comp)
		return "", "", err
	}

	err = w.Close()
	putWriter(w, comp)
	return min, max, err
}

func decodeStringBlock(block []byte, dst []string) ([]string, error) {
	buf, canRecycle, err := unpackBlock(block, BlockString)
	if err != nil {
		return nil, err
	}
	b, err := compress.StringArrayDecodeAll(buf, dst)
	if canRecycle && cap(buf) == BlockSizeHint {
		BlockEncoderPool.Put(buf[:0])
	}
	return b, err
}

func encodeBytesBlock(buf *bytes.Buffer, val [][]byte, comp Compression) ([]byte, []byte, error) {
	if len(val) == 0 {
		return nil, nil, writeEmptyBlock(buf, BlockBytes)
	}

	buf.WriteByte(byte(comp<<5) | byte(BlockBytes))
	w := getWriter(buf, comp)
	min, max, err := compress.BytesArrayEncodeAll(val, w)
	if err != nil {
		_ = w.Close()
		putWriter(w, comp)
		return nil, nil, err
	}

	err = w.Close()
	putWriter(w, comp)
	return min, max, err
}

func decodeBytesBlock(block []byte, dst [][]byte) ([][]byte, error) {
	buf, canRecycle, err := unpackBlock(block, BlockBytes)
	if err != nil {
		return nil, err
	}
	b, err := compress.BytesArrayDecodeAll(buf, dst)
	if canRecycle && cap(buf) == BlockSizeHint {
		BlockEncoderPool.Put(buf[:0])
	}
	return b, err
}

func writeEmptyBlock(buf *bytes.Buffer, typ BlockType) error {
	return buf.WriteByte(byte(typ))
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
		canRecycle := typ != BlockBytes && typ != BlockString
		if sz <= BlockSizeHint {
			dst = BlockEncoderPool.Get().([]byte)[:0]
		} else {
			dst = make([]byte, 0, int(sz))
			canRecycle = false
		}
		buf, err := snappy.Decode(dst[:sz], block[1:])
		if err != nil {
			if canRecycle {
				BlockEncoderPool.Put(dst[:0])
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
		canRecycle := typ != BlockBytes && typ != BlockString
		if sz <= BlockSizeHint {
			dst = BlockEncoderPool.Get().([]byte)[:0]
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
		switch typ {
		case BlockBytes, BlockString:
			// legacy format uses snappy for bytes and strings, we peek the
			// subsequent header to detect it
			if len(block) > 1 && block[1] == 0 {
				// copy the block to a new slice because memory will be
				// referenced, but the input data may come from an mmaped file
				buf := make([]byte, len(block)-1)
				copy(buf, block[1:])
				return buf, false, nil
			}
			fallthrough
		default:
			// just strip the header byte
			return block[1:], false, nil
		}

	default:
		return nil, false, err
	}
}

// readBlockType returns the type of value encoded in a block or an error
// if the block type is unknown.
func readBlockType(block []byte) (BlockType, error) {
	blockType := BlockType(block[0] & 0x1f)
	switch blockType {
	case BlockTime, BlockFloat64, BlockFloat32, BlockBool, BlockString, BlockBytes:
		return blockType, nil
	case BlockInt64, BlockInt32, BlockInt16, BlockInt8, BlockUint64, BlockUint32, BlockUint16, BlockUint8:
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
	blockCompression := Compression((block[0] >> 5) & 0x3)
	switch blockCompression {
	case NoCompression, LZ4Compression, SnappyCompression:
		return blockCompression, nil
	default:
		return 0, fmt.Errorf("pack: unknown block compression: %d", blockCompression)
	}
}

// readBlockPrecision returns the float precision used for unpacking uint64 to float64.
func readBlockPrecision(block []byte) int {
	return int(block[0]) & 0xf
}

// readBlockFlags returns the flags used for signalling type conversions, etc
func readBlockFlags(block []byte) BlockFlags {
	return BlockFlags((block[0] >> 4) & 0xf)
}
