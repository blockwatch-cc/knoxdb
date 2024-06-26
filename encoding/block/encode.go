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
		return writeEmptyBlock(buf, BlockTime)
	}
	l := buf.Len()

	buf.WriteByte(byte(comp<<5) | byte(BlockTime))
	w := getWriter(buf, comp)

	// copy source values to avoid overwriting them
	v := arena.Alloc(BlockInt64, len(val))
	cp := v.([]int64)[:len(val)]
	copy(cp, val)

	_, err := compress.TimeArrayEncodeAll(cp, w)
	arena.Free(BlockInt64, v)
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
		return writeEmptyBlock(buf, BlockFloat64)
	}
	l := buf.Len()

	buf.WriteByte(byte(comp<<5) | byte(BlockFloat64))
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
		return writeEmptyBlock(buf, BlockFloat32)
	}
	l := buf.Len()

	buf.WriteByte(byte(comp<<5) | byte(BlockFloat32))
	w := getWriter(buf, comp)

	v := arena.Alloc(BlockFloat64, len(val))
	cp := v.([]float64)[:len(val)]
	for i, _ := range val {
		cp[i] = float64(val[i])
	}

	_, err := compress.FloatArrayEncodeAll(cp, w)
	arena.Free(BlockFloat64, v)
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
		return writeEmptyBlock(buf, BlockInt256)
	}
	l := buf.Len()

	buf.WriteByte(byte(comp<<5) | byte(BlockInt256))
	w := getWriter(buf, comp)

	// prepare scratch space
	v := arena.Alloc(BlockInt64, val.Len())
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
	arena.Free(BlockInt64, v)
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
		return writeEmptyBlock(buf, BlockInt128)
	}
	l := buf.Len()

	buf.WriteByte(byte(comp<<5) | byte(BlockInt128))
	w := getWriter(buf, comp)

	// prepare scratch space
	v := arena.Alloc(BlockInt64, val.Len())
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
	arena.Free(BlockInt64, v)
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
		return writeEmptyBlock(buf, BlockInt64)
	}
	l := buf.Len()

	buf.WriteByte(byte(comp<<5) | byte(BlockInt64))
	w := getWriter(buf, comp)

	v := arena.Alloc(BlockInt64, len(val))
	cp := v.([]int64)[:len(val)]
	copy(cp, val)

	_, err := compress.ArrayEncodeAllInt64(cp, w)

	arena.Free(BlockInt64, v)
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
		return writeEmptyBlock(buf, BlockInt32)
	}
	l := buf.Len()

	buf.WriteByte(byte(comp<<5) | byte(BlockInt32))
	w := getWriter(buf, comp)
	v := arena.Alloc(BlockInt32, len(val))
	cp := v.([]int32)[:len(val)]
	copy(cp, val)

	_, err := compress.ArrayEncodeAllInt32(cp, w)

	arena.Free(BlockInt32, v)
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
		return writeEmptyBlock(buf, BlockInt16)
	}
	l := buf.Len()

	buf.WriteByte(byte(comp<<5) | byte(BlockInt16))
	w := getWriter(buf, comp)
	v := arena.Alloc(BlockInt16, len(val))
	cp := v.([]int16)[:len(val)]
	copy(cp, val)

	_, err := compress.ArrayEncodeAllInt16(cp, w)

	arena.Free(BlockInt16, v)
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
		return writeEmptyBlock(buf, BlockInt8)
	}
	l := buf.Len()

	buf.WriteByte(byte(comp<<5) | byte(BlockInt8))
	w := getWriter(buf, comp)
	v := arena.Alloc(BlockInt8, len(val))
	cp := v.([]int8)[:len(val)]
	copy(cp, val)

	_, err := compress.ArrayEncodeAllInt8(cp, w)

	arena.Free(BlockInt8, v)
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
		return writeEmptyBlock(buf, BlockUint64)
	}
	l := buf.Len()

	buf.WriteByte(byte(comp<<5) | byte(BlockUint64))
	w := getWriter(buf, comp)
	v := arena.Alloc(BlockUint64, len(val))
	cp := v.([]uint64)[:len(val)]
	copy(cp, val)

	_, err := compress.ArrayEncodeAllUint64(cp, w)
	arena.Free(BlockUint64, v)
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
		return writeEmptyBlock(buf, BlockUint32)
	}
	l := buf.Len()

	buf.WriteByte(byte(comp<<5) | byte(BlockUint32))
	w := getWriter(buf, comp)
	v := arena.Alloc(BlockUint32, len(val))
	cp := v.([]uint32)[:len(val)]
	copy(cp, val)

	_, err := compress.ArrayEncodeAllUint32(cp, w)

	arena.Free(BlockUint32, v)
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
		return writeEmptyBlock(buf, BlockUint16)
	}
	l := buf.Len()

	buf.WriteByte(byte(comp<<5) | byte(BlockUint16))
	w := getWriter(buf, comp)
	v := arena.Alloc(BlockUint16, len(val))
	cp := v.([]uint16)[:len(val)]
	copy(cp, val)

	_, err := compress.ArrayEncodeAllUint16(cp, w)

	arena.Free(BlockUint16, v)
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
		return writeEmptyBlock(buf, BlockUint8)
	}
	l := buf.Len()

	buf.WriteByte(byte(comp<<5) | byte(BlockUint8))
	w := getWriter(buf, comp)
	v := arena.Alloc(BlockUint8, len(val))
	cp := v.([]uint8)[:len(val)]
	copy(cp, val)

	_, err := compress.ArrayEncodeAllUint8(cp, w)

	arena.Free(BlockUint8, v)
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
		return writeEmptyBlock(buf, BlockBool)
	}
	l := buf.Len()

	buf.WriteByte(byte(comp<<5) | byte(BlockBool))
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
		return writeEmptyBlock(buf, BlockString)
	}
	l := buf.Len()

	buf.WriteByte(byte(comp<<5) | byte(BlockString))
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
		return writeEmptyBlock(buf, BlockBytes)
	}
	l := buf.Len()

	buf.WriteByte(byte(comp<<5) | byte(BlockBytes))
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
