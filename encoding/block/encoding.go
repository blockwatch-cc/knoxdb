// Copyright (c) 2018-2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package block

import (
	"bytes"
	"fmt"
	"io"

	"blockwatch.cc/knoxdb/encoding/compress"
	"blockwatch.cc/knoxdb/vec"
	"github.com/golang/snappy"
	"github.com/pierrec/lz4"
)

const (
	// default encoder/decoder buffer size in elements (64k)
	DefaultMaxPointsPerBlock = 1 << 16

	// 512k - size of a single block that fits 64k 8byte values + 1 page extra headers
	BlockSizeHint = 1<<19 + 4096

	// storedBlockHeaderSize is the size of the header for an encoded block.
	// There is one byte encoding the type of the block.
	storedBlockHeaderSize = 1
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
	if len(val) == 0 {
		return 0, 0, writeEmptyBlock(buf, BlockFloat32)
	}

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

	min, max, err := compress.FloatArrayEncodeAll(cp, w)

	if v != nil {
		float64Pool.Put(v)
	}
	if err != nil {
		_ = w.Close()
		putWriter(w, comp)
		return 0, 0, err
	}
	err = w.Close()
	putWriter(w, comp)
	return float32(min), float32(max), err
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
	buf, canRecycle, err := unpackBlock(block, BlockFloat32)
	if err != nil {
		return nil, err
	}
	var (
		cp []float64
		v  interface{}
	)
	if len(dst) <= DefaultMaxPointsPerBlock {
		v = float64Pool.Get()
		cp = v.([]float64)[:len(dst)]
	} else {
		cp = make([]float64, len(dst))
	}
	b, err := compress.FloatArrayDecodeAll(buf, cp)
	if cap(dst) >= len(b) {
		dst = dst[:len(b)]
	} else {
		dst = make([]float32, len(b))
	}

	for i, _ := range b {
		dst[i] = float32(b[i])
	}

	if v != nil {
		float64Pool.Put(v)
	}

	if canRecycle && cap(buf) == BlockSizeHint {
		BlockEncoderPool.Put(buf[:0])
	}
	return dst, err
}

func encodeInt256Block(buf *bytes.Buffer, val []vec.Int256, comp Compression) (vec.Int256, vec.Int256, error) {
	if len(val) == 0 {
		return vec.ZeroInt256, vec.ZeroInt256, writeEmptyBlock(buf, BlockInt256)
	}

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

	// find min/max values (and load data into cache)
	min, max := val[0], val[0]
	for i := range val {
		min = vec.Min256(min, val[i])
		max = vec.Max256(max, val[i])
	}

	// repack int256 into 4 int64 strides
	var err error
	for i := 0; i < 4; i++ {
		for j, v := range val {
			cp[j] = int64(v[i])
		}
		ebuf := BlockEncoderPool.Get().([]byte)[:0]
		stride := bytes.NewBuffer(ebuf)
		_, _, err = compress.IntegerArrayEncodeAll(cp, stride)
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
		return vec.ZeroInt256, vec.ZeroInt256, err
	}

	err = w.Close()
	putWriter(w, comp)
	return min, max, err
}

func encodeInt128Block(buf *bytes.Buffer, val []vec.Int128, comp Compression) (vec.Int128, vec.Int128, error) {
	if len(val) == 0 {
		return vec.ZeroInt128, vec.ZeroInt128, writeEmptyBlock(buf, BlockInt128)
	}

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

	// find min/max values (and load data into cache)
	min, max := val[0], val[0]
	for i := range val {
		min = vec.Min128(min, val[i])
		max = vec.Max128(max, val[i])
	}

	// repack int128 into 2 int64 strides
	var err error
	for i := 0; i < 2; i++ {
		for j, v := range val {
			cp[j] = int64(v[i])
		}
		ebuf := BlockEncoderPool.Get().([]byte)[:0]
		stride := bytes.NewBuffer(ebuf)
		_, _, err = compress.IntegerArrayEncodeAll(cp, stride)
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
		return vec.ZeroInt128, vec.ZeroInt128, err
	}

	err = w.Close()
	putWriter(w, comp)
	return min, max, err
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

func decodeInt256Block(block []byte, dst []vec.Int256) ([]vec.Int256, error) {
	buf, canRecycle, err := unpackBlock(block, BlockInt256)
	if err != nil {
		return nil, err
	}

	// empty blocks are empty
	if len(buf) == 0 {
		return dst, nil
	}

	// use a temp int64 slice for decoding
	v := int64Pool.Get()
	tmp := v.([]int64)[:0]

	defer func() {
		if canRecycle && cap(buf) == BlockSizeHint {
			BlockEncoderPool.Put(buf[:0])
		}
		int64Pool.Put(v)
	}()

	// unpack 4 int64 strides
	strideBuf := bytes.NewBuffer(buf)
	for i := 0; i < 4; i++ {
		strideLen := int(bigEndian.Uint32(strideBuf.Next(4)[:]))
		tmp, err := compress.IntegerArrayDecodeAll(strideBuf.Next(strideLen), tmp)
		if err != nil {
			return dst, err
		}
		// only happens in first loop iteration
		if cap(dst) < len(tmp) {
			if len(tmp) <= DefaultMaxPointsPerBlock {
				dst = int256Pool.Get().([]vec.Int256)[:len(tmp)]
			} else {
				dst = make([]vec.Int256, len(tmp))
			}
		} else {
			dst = dst[:len(tmp)]
		}

		// copy stride
		for j := range tmp {
			dst[j][i] = uint64(tmp[j])
		}
	}

	return dst, nil
}

func decodeInt128Block(block []byte, dst []vec.Int128) ([]vec.Int128, error) {
	buf, canRecycle, err := unpackBlock(block, BlockInt128)
	if err != nil {
		return nil, err
	}

	// empty blocks are empty
	if len(buf) == 0 {
		return dst, nil
	}

	// use a temp int64 slice for decoding
	v := int64Pool.Get()
	tmp := v.([]int64)[:0]

	defer func() {
		if canRecycle && cap(buf) == BlockSizeHint {
			BlockEncoderPool.Put(buf[:0])
		}
		int64Pool.Put(v)
	}()

	// unpack 2 int64 strides
	strideBuf := bytes.NewBuffer(buf)
	for i := 0; i < 2; i++ {
		strideLen := int(bigEndian.Uint32(strideBuf.Next(4)[:]))
		tmp, err := compress.IntegerArrayDecodeAll(strideBuf.Next(strideLen), tmp)
		if err != nil {
			return dst, err
		}
		// only happens in first loop iteration
		if cap(dst) < len(tmp) {
			if len(tmp) <= DefaultMaxPointsPerBlock {
				dst = int128Pool.Get().([]vec.Int128)[:len(tmp)]
			} else {
				dst = make([]vec.Int128, len(tmp))
			}
		} else {
			dst = dst[:len(tmp)]
		}

		// copy stride
		for j := range tmp {
			dst[j][i] = uint64(tmp[j])
		}
	}

	return dst, nil
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
		v = uint64Pool.Get()
		cp = v.([]uint64)[:len(val)]
	} else {
		cp = make([]uint64, len(val))
	}
	copy(cp, val)

	min, max, err := compress.UnsignedArrayEncodeAll(cp, w)
	if v != nil {
		uint64Pool.Put(v)
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
	if len(val) == 0 {
		return 0, 0, writeEmptyBlock(buf, BlockUint32)
	}

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
	//copy(cp, val)
	for i, _ := range val {
		cp[i] = uint64(val[i])
	}

	min, max, err := compress.UnsignedArrayEncodeAll(cp, w)
	if v != nil {
		uint64Pool.Put(v)
	}
	if err != nil {
		_ = w.Close()
		putWriter(w, comp)
		return 0, 0, err
	}

	err = w.Close()
	putWriter(w, comp)
	return uint32(min), uint32(max), err
}

func encodeUint16Block(buf *bytes.Buffer, val []uint16, comp Compression) (uint16, uint16, error) {
	if len(val) == 0 {
		return 0, 0, writeEmptyBlock(buf, BlockUint16)
	}

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

	min, max, err := compress.UnsignedArrayEncodeAll(cp, w)
	if v != nil {
		uint64Pool.Put(v)
	}
	if err != nil {
		_ = w.Close()
		putWriter(w, comp)
		return 0, 0, err
	}

	err = w.Close()
	putWriter(w, comp)
	return uint16(min), uint16(max), err
}

func encodeUint8Block(buf *bytes.Buffer, val []uint8, comp Compression) (uint8, uint8, error) {
	if len(val) == 0 {
		return 0, 0, writeEmptyBlock(buf, BlockUint8)
	}

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
	//copy(cp, val)
	for i, _ := range val {
		cp[i] = uint64(val[i])
	}

	min, max, err := compress.UnsignedArrayEncodeAll(cp, w)
	if v != nil {
		uint64Pool.Put(v)
	}
	if err != nil {
		_ = w.Close()
		putWriter(w, comp)
		return 0, 0, err
	}

	err = w.Close()
	putWriter(w, comp)
	return uint8(min), uint8(max), err
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
	buf, canRecycle, err := unpackBlock(block, BlockUint32)
	if err != nil {
		return nil, err
	}

	var (
		cp []uint64
		v  interface{}
	)
	if len(dst) <= DefaultMaxPointsPerBlock {
		v = uint64Pool.Get()
		cp = v.([]uint64)[:len(dst)]
	} else {
		cp = make([]uint64, len(dst))
	}

	b, err := compress.UnsignedArrayDecodeAll(buf, cp)

	if cap(dst) >= len(b) {
		dst = dst[:len(b)]
	} else {
		dst = make([]uint32, len(b))
	}

	for i, _ := range b {
		dst[i] = uint32(b[i])
	}

	if v != nil {
		uint64Pool.Put(v)
	}

	if canRecycle && cap(buf) == BlockSizeHint {
		BlockEncoderPool.Put(buf[:0])
	}
	return dst, err
}

func decodeUint16Block(block []byte, dst []uint16) ([]uint16, error) {
	buf, canRecycle, err := unpackBlock(block, BlockUint16)
	if err != nil {
		return nil, err
	}

	var (
		cp []uint64
		v  interface{}
	)
	if len(dst) <= DefaultMaxPointsPerBlock {
		v = uint64Pool.Get()
		cp = v.([]uint64)[:len(dst)]
	} else {
		cp = make([]uint64, len(dst))
	}

	b, err := compress.UnsignedArrayDecodeAll(buf, cp)

	if cap(dst) >= len(b) {
		dst = dst[:len(b)]
	} else {
		dst = make([]uint16, len(b))
	}

	for i, _ := range b {
		dst[i] = uint16(b[i])
	}

	if v != nil {
		uint64Pool.Put(v)
	}

	if canRecycle && cap(buf) == BlockSizeHint {
		BlockEncoderPool.Put(buf[:0])
	}
	return dst, err
}

func decodeUint8Block(block []byte, dst []uint8) ([]uint8, error) {
	buf, canRecycle, err := unpackBlock(block, BlockUint8)
	if err != nil {
		return nil, err
	}

	var (
		cp []uint64
		v  interface{}
	)
	if len(dst) <= DefaultMaxPointsPerBlock {
		v = uint64Pool.Get()
		cp = v.([]uint64)[:len(dst)]
	} else {
		cp = make([]uint64, len(dst))
	}

	b, err := compress.UnsignedArrayDecodeAll(buf, cp)

	if cap(dst) >= len(b) {
		dst = dst[:len(b)]
	} else {
		dst = make([]uint8, len(b))
	}

	for i, _ := range b {
		dst[i] = uint8(b[i])
	}

	if v != nil {
		uint64Pool.Put(v)
	}

	if canRecycle && cap(buf) == BlockSizeHint {
		BlockEncoderPool.Put(buf[:0])
	}
	return dst, err
}

func encodeBoolBlock(buf *bytes.Buffer, val *vec.BitSet, comp Compression) (bool, bool, error) {
	if val.Len() == 0 {
		return false, false, writeEmptyBlock(buf, BlockBool)
	}

	buf.WriteByte(byte(comp<<5) | byte(BlockBool))
	w := getWriter(buf, comp)
	min, max, err := compress.BitsetEncodeAll(val, w)
	if err != nil {
		_ = w.Close()
		putWriter(w, comp)
		return false, false, err
	}

	err = w.Close()
	putWriter(w, comp)
	return min, max, err
}

func decodeBoolBlock(block []byte, dst *vec.BitSet) (*vec.BitSet, error) {
	buf, canRecycle, err := unpackBlock(block, BlockBool)
	if err != nil {
		return nil, err
	}
	b, err := compress.BitsetDecodeAll(buf, dst)
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
				// referenced, but the input data may come from an mmapped file
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
	blockType := BlockType(block[0] & blockTypeMask)
	switch blockType {
	case BlockTime,
		BlockInt64,
		BlockUint64,
		BlockFloat64,
		BlockBool,
		BlockString,
		BlockBytes,
		BlockInt32,
		BlockInt16,
		BlockInt8,
		BlockUint32,
		BlockUint16,
		BlockUint8,
		BlockFloat32,
		BlockInt128,
		BlockInt256:
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

// readBlockScale returns the scale for fixed decimal types
func readBlockScale(block []byte) int {
	return int(block[0]) & int(blockScaleMask)
}
