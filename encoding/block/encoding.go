// Copyright (c) 2018-2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package block

import (
	"bytes"
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
		v = integerPool.Get()
		cp = v.([]int64)[:len(val)]
	} else {
		cp = make([]int64, len(val))
	}
	copy(cp, val)

	min, max, err := compress.TimeArrayEncodeAll(cp, w)
	if v != nil {
		integerPool.Put(v)
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

func encodeFloatBlock(buf *bytes.Buffer, val []float64, comp Compression) (float64, float64, error) {
	if len(val) == 0 {
		return 0, 0, writeEmptyBlock(buf, BlockFloat)
	}

	buf.WriteByte(byte(comp<<5) | byte(BlockFloat))
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

func decodeFloatBlock(block []byte, dst []float64) ([]float64, error) {
	buf, canRecycle, err := unpackBlock(block, BlockFloat)
	if err != nil {
		return nil, err
	}
	b, err := compress.FloatArrayDecodeAll(buf, dst)
	if canRecycle && cap(buf) == BlockSizeHint {
		BlockEncoderPool.Put(buf[:0])
	}
	return b, err
}

func encodeIntegerBlock(buf *bytes.Buffer, val []int64, comp Compression) (int64, int64, error) {
	if len(val) == 0 {
		return 0, 0, writeEmptyBlock(buf, BlockInteger)
	}

	buf.WriteByte(byte(comp<<5) | byte(BlockInteger))
	w := getWriter(buf, comp)
	var (
		cp []int64
		v  interface{}
	)
	if len(val) <= DefaultMaxPointsPerBlock {
		v = integerPool.Get()
		cp = v.([]int64)[:len(val)]
	} else {
		cp = make([]int64, len(val))
	}
	copy(cp, val)

	min, max, err := compress.IntegerArrayEncodeAll(cp, w)
	if v != nil {
		integerPool.Put(v)
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

func decodeIntegerBlock(block []byte, dst []int64) ([]int64, error) {
	buf, canRecycle, err := unpackBlock(block, BlockInteger)
	if err != nil {
		return nil, err
	}
	b, err := compress.IntegerArrayDecodeAll(buf, dst)
	if canRecycle && cap(buf) == BlockSizeHint {
		BlockEncoderPool.Put(buf[:0])
	}
	return b, err
}

func encodeUnsignedBlock(buf *bytes.Buffer, val []uint64, comp Compression) (uint64, uint64, error) {
	if len(val) == 0 {
		return 0, 0, writeEmptyBlock(buf, BlockUnsigned)
	}

	buf.WriteByte(byte(comp<<5) | byte(BlockUnsigned))
	w := getWriter(buf, comp)
	var (
		cp []uint64
		v  interface{}
	)
	if len(val) <= DefaultMaxPointsPerBlock {
		v = integerPool.Get()
		cp = compress.ReintepretInt64ToUint64Slice(v.([]int64)[:len(val)])
	} else {
		cp = make([]uint64, len(val))
	}
	copy(cp, val)

	min, max, err := compress.UnsignedArrayEncodeAll(cp, w)
	if v != nil {
		integerPool.Put(v)
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

func decodeUnsignedBlock(block []byte, dst []uint64) ([]uint64, error) {
	buf, canRecycle, err := unpackBlock(block, BlockUnsigned)
	if err != nil {
		return nil, err
	}
	b, err := compress.UnsignedArrayDecodeAll(buf, dst)
	if canRecycle && cap(buf) == BlockSizeHint {
		BlockEncoderPool.Put(buf[:0])
	}
	return b, err
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
	case BlockTime, BlockFloat, BlockInteger, BlockUnsigned, BlockBool, BlockString, BlockBytes:
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
