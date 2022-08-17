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
	// default encoder/decoder buffer size in elements (32k)
	DefaultMaxPointsPerBlock = 1 << 15

	// 512k - size of a single block that fits 32k 8byte values + 1 page extra headers
	BlockSizeHint = 1<<18 + 4096

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

	_, err := compress.TimeArrayEncodeAll(cp, w)
	if v != nil {
		int64Pool.Put(v)
	}
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
	var (
		cp []float64
		v  interface{}
	)
	if len(val) <= DefaultMaxPointsPerBlock {
		v = float64Pool.Get()
		cp = v.([]float64)[:len(val)]
	} else {
		cp = make([]float64, len(val))
	}
	//copy(cp, val)
	for i, _ := range val {
		cp[i] = float64(val[i])
	}

	_, err := compress.FloatArrayEncodeAll(cp, w)
	if v != nil {
		float64Pool.Put(v)
	}
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
	var (
		cp []int64
		v  interface{}
	)
	if val.Len() <= DefaultMaxPointsPerBlock {
		v = int64Pool.Get()
		cp = v.([]int64)[:val.Len()]
	} else {
		cp = make([]int64, val.Len())
	}

	// repack int256 into 4 int64 strides
	var err error
	for i := 0; i < 4; i++ {
		if i == 0 {
			copy(cp, val.X0)
		} else {
			srcint := compress.ReintepretUint64ToInt64Slice(val.X1)
			copy(cp, srcint)
		}

		ebuf := BlockEncoderPool.Get().([]byte)[:0]
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
		BlockEncoderPool.Put(ebuf)
	}

	// cleanup
	if v != nil {
		int64Pool.Put(v)
	}
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
	var (
		cp []int64
		v  interface{}
	)
	if val.Len() <= DefaultMaxPointsPerBlock {
		v = int64Pool.Get()
		cp = v.([]int64)[:val.Len()]
	} else {
		cp = make([]int64, val.Len())
	}

	// repack int128 into 2 int64 strides
	var err error
	for i := 0; i < 2; i++ {
		if i == 0 {
			copy(cp, val.X0)
		} else {
			srcint := compress.ReintepretUint64ToInt64Slice(val.X1)
			copy(cp, srcint)
		}

		ebuf := BlockEncoderPool.Get().([]byte)[:0]
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
		BlockEncoderPool.Put(ebuf)
	}

	// cleanup
	if v != nil {
		int64Pool.Put(v)
	}
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

	_, err := compress.ArrayEncodeAllInt64(cp, w)
	if v != nil {
		int64Pool.Put(v)
	}
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
	var (
		cp []int32
		v  interface{}
	)
	if len(val) <= DefaultMaxPointsPerBlock {
		v = int32Pool.Get()
		cp = v.([]int32)[:len(val)]
	} else {
		cp = make([]int32, len(val))
	}
	copy(cp, val)

	_, err := compress.ArrayEncodeAllInt32(cp, w)
	if v != nil {
		int32Pool.Put(v)
	}
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
	var (
		cp []int16
		v  interface{}
	)
	if len(val) <= DefaultMaxPointsPerBlock {
		v = int16Pool.Get()
		cp = v.([]int16)[:len(val)]
	} else {
		cp = make([]int16, len(val))
	}
	copy(cp, val)

	_, err := compress.ArrayEncodeAllInt16(cp, w)
	if v != nil {
		int16Pool.Put(v)
	}
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
	var (
		cp []int8
		v  interface{}
	)
	if len(val) <= DefaultMaxPointsPerBlock {
		v = int8Pool.Get()
		cp = v.([]int8)[:len(val)]
	} else {
		cp = make([]int8, len(val))
	}
	copy(cp, val)

	_, err := compress.ArrayEncodeAllInt8(cp, w)
	if v != nil {
		int8Pool.Put(v)
	}
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
	var (
		cp []uint64
		v  interface{}
	)
	if len(val) <= DefaultMaxPointsPerBlock {
		v = uint64Pool.Get()
		cp = v.([]uint64)[:len(val)]
	} else {
		cp = make([]uint64, len(val))
	}
	copy(cp, val)

	_, err := compress.ArrayEncodeAllUint64(cp, w)
	if v != nil {
		uint64Pool.Put(v)
	}
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
	var (
		cp []uint32
		v  interface{}
	)
	if len(val) <= DefaultMaxPointsPerBlock {
		v = uint32Pool.Get()
		cp = v.([]uint32)[:len(val)]
	} else {
		cp = make([]uint32, len(val))
	}
	copy(cp, val)

	_, err := compress.ArrayEncodeAllUint32(cp, w)
	if v != nil {
		uint32Pool.Put(v)
	}
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
	var (
		cp []uint16
		v  interface{}
	)
	if len(val) <= DefaultMaxPointsPerBlock {
		v = uint16Pool.Get()
		cp = v.([]uint16)[:len(val)]
	} else {
		cp = make([]uint16, len(val))
	}
	copy(cp, val)

	_, err := compress.ArrayEncodeAllUint16(cp, w)
	if v != nil {
		uint16Pool.Put(v)
	}
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
	var (
		cp []uint8
		v  interface{}
	)
	if len(val) <= DefaultMaxPointsPerBlock {
		v = uint8Pool.Get()
		cp = v.([]uint8)[:len(val)]
	} else {
		cp = make([]uint8, len(val))
	}
	copy(cp, val)

	_, err := compress.ArrayEncodeAllUint8(cp, w)
	if v != nil {
		uint8Pool.Put(v)
	}
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
