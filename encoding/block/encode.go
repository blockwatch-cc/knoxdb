// Copyright (c) 2018-2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package block

import (
	"bytes"

	"blockwatch.cc/knoxdb/encoding/compress"
	"blockwatch.cc/knoxdb/encoding/dedup"
	"blockwatch.cc/knoxdb/vec"
)

const (
	// storedBlockHeaderSize is the size of the header for an encoded block.
	// There is one byte encoding the type of the block.
	storedBlockHeaderSize = 1

	// header info masks
	blockTypeMask        byte = 0x1f
	blockCompressionMask byte = 0x03
)

func encodeTimeBlock(buf *bytes.Buffer, val []int64, comp Compression) (int, error) {
	if len(val) == 0 {
		return writeEmptyBlock(buf, BlockTypeTime)
	}
	l := buf.Len()

	buf.WriteByte(byte(comp<<5) | byte(BlockTypeTime))
	w := getWriter(buf, comp)

	// copy source values to avoid overwriting them
	v := arena.Alloc(BlockTypeInt64, len(val))
	cp := v.([]int64)[:len(val)]
	copy(cp, val)

	_, err := compress.TimeArrayEncodeAll(cp, w)
	arena.Free(BlockTypeInt64, v)
	if err != nil {
		_ = w.Close()
		putWriter(w, comp)
		return 0, err
	}

	err = w.Close()
	putWriter(w, comp)
	return buf.Len() - l, err
}

func encodeFloat64Block(buf *bytes.Buffer, val []float64, comp Compression) (int, error) {
	if len(val) == 0 {
		return writeEmptyBlock(buf, BlockTypeFloat64)
	}
	l := buf.Len()

	buf.WriteByte(byte(comp<<5) | byte(BlockTypeFloat64))
	w := getWriter(buf, comp)
	_, err := compress.FloatArrayEncodeAll(val, w)
	if err != nil {
		_ = w.Close()
		putWriter(w, comp)
		return 0, err
	}

	err = w.Close()
	putWriter(w, comp)
	return buf.Len() - l, err
}

func encodeFloat32Block(buf *bytes.Buffer, val []float32, comp Compression) (int, error) {
	if len(val) == 0 {
		return writeEmptyBlock(buf, BlockTypeFloat32)
	}
	l := buf.Len()

	buf.WriteByte(byte(comp<<5) | byte(BlockTypeFloat32))
	w := getWriter(buf, comp)

	v := arena.Alloc(BlockTypeFloat64, len(val))
	cp := v.([]float64)[:len(val)]
	for i, _ := range val {
		cp[i] = float64(val[i])
	}

	_, err := compress.FloatArrayEncodeAll(cp, w)
	arena.Free(BlockTypeFloat64, v)
	if err != nil {
		_ = w.Close()
		putWriter(w, comp)
		return 0, err
	}
	err = w.Close()
	putWriter(w, comp)
	return buf.Len() - l, err
}

func encodeInt256Block(buf *bytes.Buffer, val vec.Int256LLSlice, comp Compression) (int, error) {
	if val.Len() == 0 {
		return writeEmptyBlock(buf, BlockTypeInt256)
	}
	l := buf.Len()

	buf.WriteByte(byte(comp<<5) | byte(BlockTypeInt256))
	w := getWriter(buf, comp)

	// prepare scratch space
	v := arena.Alloc(BlockTypeInt64, val.Len())
	cp := v.([]int64)[:val.Len()]

	// repack int256 into 4 int64 strides
	var err error
	for i := 0; i < 4; i++ {
		if i == 0 {
			copy(cp, val.X0)
		} else {
			srcint := compress.ReintepretUint64ToInt64Slice(val.X1)
			copy(cp, srcint)
		}

		ebuf := bufferPool.Get().([]byte)[:0]
		stride := bytes.NewBuffer(ebuf)
		_, err = compress.ArrayEncodeAllInt64(cp, stride)
		if err != nil {
			break
		}
		var strideLen [4]byte
		bigEndian.PutUint32(strideLen[:], uint32(stride.Len()))
		if _, err = w.Write(strideLen[:]); err != nil {
			break
		}
		if _, err = w.Write(stride.Bytes()); err != nil {
			break
		}
		bufferPool.Put(ebuf)
	}

	// cleanup
	arena.Free(BlockTypeInt64, v)
	if err != nil {
		_ = w.Close()
		putWriter(w, comp)
		return 0, err
	}

	err = w.Close()
	putWriter(w, comp)
	return buf.Len() - l, err
}

func encodeInt128Block(buf *bytes.Buffer, val vec.Int128LLSlice, comp Compression) (int, error) {
	if val.Len() == 0 {
		return writeEmptyBlock(buf, BlockTypeInt128)
	}
	l := buf.Len()

	buf.WriteByte(byte(comp<<5) | byte(BlockTypeInt128))
	w := getWriter(buf, comp)

	// prepare scratch space
	v := arena.Alloc(BlockTypeInt64, val.Len())
	cp := v.([]int64)[:val.Len()]

	// repack int128 into 2 int64 strides
	var err error
	for i := 0; i < 2; i++ {
		if i == 0 {
			copy(cp, val.X0)
		} else {
			srcint := compress.ReintepretUint64ToInt64Slice(val.X1)
			copy(cp, srcint)
		}

		ebuf := bufferPool.Get().([]byte)[:0]
		stride := bytes.NewBuffer(ebuf)
		_, err = compress.ArrayEncodeAllInt64(cp, stride)
		if err != nil {
			break
		}
		var strideLen [4]byte
		bigEndian.PutUint32(strideLen[:], uint32(stride.Len()))
		if _, err = w.Write(strideLen[:]); err != nil {
			break
		}
		if _, err = w.Write(stride.Bytes()); err != nil {
			break
		}
		bufferPool.Put(ebuf)
	}

	// cleanup
	arena.Free(BlockTypeInt64, v)
	if err != nil {
		_ = w.Close()
		putWriter(w, comp)
		return 0, err
	}

	err = w.Close()
	putWriter(w, comp)
	return buf.Len() - l, err
}

func encodeInt64Block(buf *bytes.Buffer, val []int64, comp Compression) (int, error) {
	if len(val) == 0 {
		return writeEmptyBlock(buf, BlockTypeInt64)
	}
	l := buf.Len()

	buf.WriteByte(byte(comp<<5) | byte(BlockTypeInt64))
	w := getWriter(buf, comp)

	v := arena.Alloc(BlockTypeInt64, len(val))
	cp := v.([]int64)[:len(val)]
	copy(cp, val)

	_, err := compress.ArrayEncodeAllInt64(cp, w)

	arena.Free(BlockTypeInt64, v)
	if err != nil {
		_ = w.Close()
		putWriter(w, comp)
		return 0, err
	}

	err = w.Close()
	putWriter(w, comp)
	return buf.Len() - l, err
}

func encodeInt32Block(buf *bytes.Buffer, val []int32, comp Compression) (int, error) {
	if len(val) == 0 {
		return writeEmptyBlock(buf, BlockTypeInt32)
	}
	l := buf.Len()

	buf.WriteByte(byte(comp<<5) | byte(BlockTypeInt32))
	w := getWriter(buf, comp)
	v := arena.Alloc(BlockTypeInt32, len(val))
	cp := v.([]int32)[:len(val)]
	copy(cp, val)

	_, err := compress.ArrayEncodeAllInt32(cp, w)

	arena.Free(BlockTypeInt32, v)
	if err != nil {
		_ = w.Close()
		putWriter(w, comp)
		return 0, err
	}

	err = w.Close()
	putWriter(w, comp)
	return buf.Len() - l, err
}

func encodeInt16Block(buf *bytes.Buffer, val []int16, comp Compression) (int, error) {
	if len(val) == 0 {
		return writeEmptyBlock(buf, BlockTypeInt16)
	}
	l := buf.Len()

	buf.WriteByte(byte(comp<<5) | byte(BlockTypeInt16))
	w := getWriter(buf, comp)
	v := arena.Alloc(BlockTypeInt16, len(val))
	cp := v.([]int16)[:len(val)]
	copy(cp, val)

	_, err := compress.ArrayEncodeAllInt16(cp, w)

	arena.Free(BlockTypeInt16, v)
	if err != nil {
		_ = w.Close()
		putWriter(w, comp)
		return 0, err
	}

	err = w.Close()
	putWriter(w, comp)
	return buf.Len() - l, err
}

func encodeInt8Block(buf *bytes.Buffer, val []int8, comp Compression) (int, error) {
	if len(val) == 0 {
		return writeEmptyBlock(buf, BlockTypeInt8)
	}
	l := buf.Len()

	buf.WriteByte(byte(comp<<5) | byte(BlockTypeInt8))
	w := getWriter(buf, comp)
	v := arena.Alloc(BlockTypeInt8, len(val))
	cp := v.([]int8)[:len(val)]
	copy(cp, val)

	_, err := compress.ArrayEncodeAllInt8(cp, w)

	arena.Free(BlockTypeInt8, v)
	if err != nil {
		_ = w.Close()
		putWriter(w, comp)
		return 0, err
	}

	err = w.Close()
	putWriter(w, comp)
	return buf.Len() - l, err
}

func encodeUint64Block(buf *bytes.Buffer, val []uint64, comp Compression) (int, error) {
	if len(val) == 0 {
		return writeEmptyBlock(buf, BlockTypeUint64)
	}
	l := buf.Len()

	buf.WriteByte(byte(comp<<5) | byte(BlockTypeUint64))
	w := getWriter(buf, comp)
	v := arena.Alloc(BlockTypeUint64, len(val))
	cp := v.([]uint64)[:len(val)]
	copy(cp, val)

	_, err := compress.ArrayEncodeAllUint64(cp, w)
	arena.Free(BlockTypeUint64, v)
	if err != nil {
		_ = w.Close()
		putWriter(w, comp)
		return 0, err
	}

	err = w.Close()
	putWriter(w, comp)
	return buf.Len() - l, err
}

func encodeUint32Block(buf *bytes.Buffer, val []uint32, comp Compression) (int, error) {
	if len(val) == 0 {
		return writeEmptyBlock(buf, BlockTypeUint32)
	}
	l := buf.Len()

	buf.WriteByte(byte(comp<<5) | byte(BlockTypeUint32))
	w := getWriter(buf, comp)
	v := arena.Alloc(BlockTypeUint32, len(val))
	cp := v.([]uint32)[:len(val)]
	copy(cp, val)

	_, err := compress.ArrayEncodeAllUint32(cp, w)

	arena.Free(BlockTypeUint32, v)
	if err != nil {
		_ = w.Close()
		putWriter(w, comp)
		return 0, err
	}

	err = w.Close()
	putWriter(w, comp)
	return buf.Len() - l, err
}

func encodeUint16Block(buf *bytes.Buffer, val []uint16, comp Compression) (int, error) {
	if len(val) == 0 {
		return writeEmptyBlock(buf, BlockTypeUint16)
	}
	l := buf.Len()

	buf.WriteByte(byte(comp<<5) | byte(BlockTypeUint16))
	w := getWriter(buf, comp)
	v := arena.Alloc(BlockTypeUint16, len(val))
	cp := v.([]uint16)[:len(val)]
	copy(cp, val)

	_, err := compress.ArrayEncodeAllUint16(cp, w)

	arena.Free(BlockTypeUint16, v)
	if err != nil {
		_ = w.Close()
		putWriter(w, comp)
		return 0, err
	}

	err = w.Close()
	putWriter(w, comp)
	return buf.Len() - l, err
}

func encodeUint8Block(buf *bytes.Buffer, val []uint8, comp Compression) (int, error) {
	if len(val) == 0 {
		return writeEmptyBlock(buf, BlockTypeUint8)
	}
	l := buf.Len()

	buf.WriteByte(byte(comp<<5) | byte(BlockTypeUint8))
	w := getWriter(buf, comp)
	v := arena.Alloc(BlockTypeUint8, len(val))
	cp := v.([]uint8)[:len(val)]
	copy(cp, val)

	_, err := compress.ArrayEncodeAllUint8(cp, w)

	arena.Free(BlockTypeUint8, v)
	if err != nil {
		_ = w.Close()
		putWriter(w, comp)
		return 0, err
	}

	err = w.Close()
	putWriter(w, comp)
	return buf.Len() - l, err
}

func encodeBoolBlock(buf *bytes.Buffer, val *vec.Bitset, comp Compression) (int, error) {
	if val.Len() == 0 {
		return writeEmptyBlock(buf, BlockTypeBool)
	}
	l := buf.Len()

	buf.WriteByte(byte(comp<<5) | byte(BlockTypeBool))
	w := getWriter(buf, comp)
	_, err := compress.BitsetEncodeAll(val, w)
	if err != nil {
		_ = w.Close()
		putWriter(w, comp)
		return 0, err
	}

	err = w.Close()
	putWriter(w, comp)
	return buf.Len() - l, err
}

func encodeStringBlock(buf *bytes.Buffer, val dedup.ByteArray, comp Compression) (int, error) {
	if val.Len() == 0 {
		return writeEmptyBlock(buf, BlockTypeString)
	}
	l := buf.Len()

	buf.WriteByte(byte(comp<<5) | byte(BlockTypeString))
	w := getWriter(buf, comp)
	_, err := val.WriteTo(w)
	if err != nil {
		_ = w.Close()
		putWriter(w, comp)
		return 0, err
	}

	err = w.Close()
	putWriter(w, comp)
	return buf.Len() - l, err
}

func encodeBytesBlock(buf *bytes.Buffer, val dedup.ByteArray, comp Compression) (int, error) {
	if val.Len() == 0 {
		return writeEmptyBlock(buf, BlockTypeBytes)
	}
	l := buf.Len()

	buf.WriteByte(byte(comp<<5) | byte(BlockTypeBytes))
	w := getWriter(buf, comp)
	_, err := val.WriteTo(w)
	if err != nil {
		_ = w.Close()
		putWriter(w, comp)
		return 0, err
	}

	err = w.Close()
	putWriter(w, comp)
	return buf.Len() - l, err
}

func writeEmptyBlock(buf *bytes.Buffer, typ BlockType) (int, error) {
	return 1, buf.WriteByte(byte(typ))
}
