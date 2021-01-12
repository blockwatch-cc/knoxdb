// Copyright (c) 2018-2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package block

import (
	"bytes"

	"blockwatch.cc/knoxdb/encoding/compress"
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

	err := compress.TimeArrayEncodeAll(cp, w)
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
	err := compress.FloatArrayEncodeAll(val, w)
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

	err := compress.FloatArrayEncodeAll(cp, w)
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

func encodeInt256Block(buf *bytes.Buffer, val []vec.Int256, comp Compression) (int, error) {
	if len(val) == 0 {
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
	if len(val) <= DefaultMaxPointsPerBlock {
		v = int64Pool.Get()
		cp = v.([]int64)[:len(val)]
	} else {
		cp = make([]int64, len(val))
	}

	// repack int256 into 4 int64 strides
	var err error
	for i := 0; i < 4; i++ {
		for j, v := range val {
			cp[j] = int64(v[i])
		}
		ebuf := BlockEncoderPool.Get().([]byte)[:0]
		stride := bytes.NewBuffer(ebuf)
		err = compress.IntegerArrayEncodeAll(cp, stride)
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

func encodeInt128Block(buf *bytes.Buffer, val []vec.Int128, comp Compression) (int, error) {
	if len(val) == 0 {
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
	if len(val) <= DefaultMaxPointsPerBlock {
		v = int64Pool.Get()
		cp = v.([]int64)[:len(val)]
	} else {
		cp = make([]int64, len(val))
	}

	// repack int128 into 2 int64 strides
	var err error
	for i := 0; i < 2; i++ {
		for j, v := range val {
			cp[j] = int64(v[i])
		}
		ebuf := BlockEncoderPool.Get().([]byte)[:0]
		stride := bytes.NewBuffer(ebuf)
		err = compress.IntegerArrayEncodeAll(cp, stride)
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

	err := compress.IntegerArrayEncodeAll(cp, w)
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
		cp []int64
		v  interface{}
	)
	if len(val) <= DefaultMaxPointsPerBlock {
		v = int64Pool.Get()
		cp = v.([]int64)[:len(val)]
	} else {
		cp = make([]int64, len(val))
	}
	for i, _ := range val {
		cp[i] = int64(val[i])
	}

	err := compress.IntegerArrayEncodeAll(cp, w)
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

func encodeInt16Block(buf *bytes.Buffer, val []int16, comp Compression) (int, error) {
	if len(val) == 0 {
		return writeEmptyBlock(buf, BlockInt16)
	}
	l := buf.Len()

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
	for i, _ := range val {
		cp[i] = int64(val[i])
	}

	err := compress.IntegerArrayEncodeAll(cp, w)
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

func encodeInt8Block(buf *bytes.Buffer, val []int8, comp Compression) (int, error) {
	if len(val) == 0 {
		return writeEmptyBlock(buf, BlockInt8)
	}
	l := buf.Len()

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
	for i, _ := range val {
		cp[i] = int64(val[i])
	}

	err := compress.IntegerArrayEncodeAll(cp, w)
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

	err := compress.UnsignedArrayEncodeAll(cp, w)
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
		cp []uint64
		v  interface{}
	)
	if len(val) <= DefaultMaxPointsPerBlock {
		v = uint64Pool.Get()
		cp = v.([]uint64)[:len(val)]
	} else {
		cp = make([]uint64, len(val))
	}
	for i, _ := range val {
		cp[i] = uint64(val[i])
	}

	err := compress.UnsignedArrayEncodeAll(cp, w)
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

func encodeUint16Block(buf *bytes.Buffer, val []uint16, comp Compression) (int, error) {
	if len(val) == 0 {
		return writeEmptyBlock(buf, BlockUint16)
	}
	l := buf.Len()

	buf.WriteByte(byte(comp<<5) | byte(BlockUint16))
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
	//copy(cp, val)
	for i, _ := range val {
		cp[i] = uint64(val[i])
	}

	err := compress.UnsignedArrayEncodeAll(cp, w)
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

func encodeUint8Block(buf *bytes.Buffer, val []uint8, comp Compression) (int, error) {
	if len(val) == 0 {
		return writeEmptyBlock(buf, BlockUint8)
	}
	l := buf.Len()

	buf.WriteByte(byte(comp<<5) | byte(BlockUint8))
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
	for i, _ := range val {
		cp[i] = uint64(val[i])
	}

	err := compress.UnsignedArrayEncodeAll(cp, w)
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

func encodeBoolBlock(buf *bytes.Buffer, val *vec.BitSet, comp Compression) (int, error) {
	if val.Len() == 0 {
		return writeEmptyBlock(buf, BlockBool)
	}
	l := buf.Len()

	buf.WriteByte(byte(comp<<5) | byte(BlockBool))
	w := getWriter(buf, comp)
	err := compress.BitsetEncodeAll(val, w)
	if err != nil {
		_ = w.Close()
		putWriter(w, comp)
		return 0, err
	}

	err = w.Close()
	putWriter(w, comp)
	return buf.Len() - l, err
}

func encodeStringBlock(buf *bytes.Buffer, val []string, comp Compression) (int, error) {
	if len(val) == 0 {
		return writeEmptyBlock(buf, BlockString)
	}
	l := buf.Len()

	buf.WriteByte(byte(comp<<5) | byte(BlockString))
	w := getWriter(buf, comp)
	err := compress.StringArrayEncodeAll(val, w)
	if err != nil {
		_ = w.Close()
		putWriter(w, comp)
		return 0, err
	}

	err = w.Close()
	putWriter(w, comp)
	return buf.Len() - l, err
}

func encodeBytesBlock(buf *bytes.Buffer, val [][]byte, comp Compression) (int, error) {
	if len(val) == 0 {
		return writeEmptyBlock(buf, BlockBytes)
	}
	l := buf.Len()

	buf.WriteByte(byte(comp<<5) | byte(BlockBytes))
	w := getWriter(buf, comp)
	err := compress.BytesArrayEncodeAll(val, w)
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
