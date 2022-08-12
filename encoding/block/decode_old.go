// Copyright (c) 2018-2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package block

import (
	"bytes"

	"blockwatch.cc/knoxdb/encoding/compress"
	"blockwatch.cc/knoxdb/vec"
)

func decodeTimeBlockOld(block []byte, dst []int64) ([]int64, error) {
	buf, canRecycle, err := unpackBlock(block, BlockTime)
	if err != nil {
		return nil, err
	}

	b, err := compress.TimeArrayDecodeAllOld(buf, dst)
	if canRecycle && cap(buf) == BlockSizeHint {
		BlockEncoderPool.Put(buf[:0])
	}
	return b, err
}

func decodeInt256BlockOld(block []byte, dst vec.Int256LLSlice) (vec.Int256LLSlice, error) {
	buf, canRecycle, err := unpackBlock(block, BlockInt256)
	if err != nil {
		return dst, err
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
		tmp, err := compress.IntegerArrayDecodeAllOld(strideBuf.Next(strideLen), tmp)
		if err != nil {
			return dst, err
		}

		switch i {
		case 0:
			if cap(dst.X0) < len(tmp) {
				if len(tmp) <= DefaultMaxPointsPerBlock {
					dst.X0 = int64Pool.Get().([]int64)[:len(tmp)]
				} else {
					dst.X0 = make([]int64, len(tmp))
				}
			} else {
				dst.X0 = dst.X0[:len(tmp)]
			}

			// copy stride
			copy(dst.X0, tmp)
		case 1:
			if cap(dst.X1) < len(tmp) {
				if len(tmp) <= DefaultMaxPointsPerBlock {
					dst.X1 = uint64Pool.Get().([]uint64)[:len(tmp)]
				} else {
					dst.X1 = make([]uint64, len(tmp))
				}
			} else {
				dst.X1 = dst.X1[:len(tmp)]
			}

			// copy stride
			srcint := compress.ReintepretInt64ToUint64Slice(tmp)
			copy(dst.X1, srcint)
		case 2:
			if cap(dst.X2) < len(tmp) {
				if len(tmp) <= DefaultMaxPointsPerBlock {
					dst.X2 = uint64Pool.Get().([]uint64)[:len(tmp)]
				} else {
					dst.X2 = make([]uint64, len(tmp))
				}
			} else {
				dst.X2 = dst.X2[:len(tmp)]
			}

			// copy stride
			srcint := compress.ReintepretInt64ToUint64Slice(tmp)
			copy(dst.X2, srcint)
		case 3:
			if cap(dst.X3) < len(tmp) {
				if len(tmp) <= DefaultMaxPointsPerBlock {
					dst.X3 = uint64Pool.Get().([]uint64)[:len(tmp)]
				} else {
					dst.X3 = make([]uint64, len(tmp))
				}
			} else {
				dst.X3 = dst.X3[:len(tmp)]
			}

			// copy stride
			srcint := compress.ReintepretInt64ToUint64Slice(tmp)
			copy(dst.X3, srcint)
		}
	}
	return dst, nil
}

func decodeInt128BlockOld(block []byte, dst vec.Int128LLSlice) (vec.Int128LLSlice, error) {
	buf, canRecycle, err := unpackBlock(block, BlockInt128)
	if err != nil {
		return dst, err
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
		tmp, err := compress.IntegerArrayDecodeAllOld(strideBuf.Next(strideLen), tmp)
		if err != nil {
			return dst, err
		}

		if i == 0 {
			if cap(dst.X0) < len(tmp) {
				if len(tmp) <= DefaultMaxPointsPerBlock {
					dst.X0 = int64Pool.Get().([]int64)[:len(tmp)]
				} else {
					dst.X0 = make([]int64, len(tmp))
				}
			} else {
				dst.X0 = dst.X0[:len(tmp)]
			}

			// copy stride
			copy(dst.X0, tmp)
		} else {
			if cap(dst.X1) < len(tmp) {
				if len(tmp) <= DefaultMaxPointsPerBlock {
					dst.X1 = uint64Pool.Get().([]uint64)[:len(tmp)]
				} else {
					dst.X1 = make([]uint64, len(tmp))
				}
			} else {
				dst.X1 = dst.X1[:len(tmp)]
			}

			// copy stride
			srcint := compress.ReintepretInt64ToUint64Slice(tmp)
			copy(dst.X1, srcint)
		}
	}
	return dst, nil
}

func decodeInt64BlockOld(block []byte, dst []int64) ([]int64, error) {
	buf, canRecycle, err := unpackBlock(block, BlockInt64)
	if err != nil {
		return nil, err
	}
	b, err := compress.IntegerArrayDecodeAllOld(buf, dst)
	if canRecycle && cap(buf) == BlockSizeHint {
		BlockEncoderPool.Put(buf[:0])
	}
	return b, err
}

func decodeInt32BlockOld(block []byte, dst []int32) ([]int32, error) {
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

	b, err := compress.IntegerArrayDecodeAllOld(buf, cp)

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

func decodeInt16BlockOld(block []byte, dst []int16) ([]int16, error) {
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

	b, err := compress.IntegerArrayDecodeAllOld(buf, cp)

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

func decodeInt8BlockOld(block []byte, dst []int8) ([]int8, error) {
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

	b, err := compress.IntegerArrayDecodeAllOld(buf, cp)

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

func decodeUint64BlockOld(block []byte, dst []uint64) ([]uint64, error) {
	buf, canRecycle, err := unpackBlock(block, BlockUint64)
	if err != nil {
		return nil, err
	}
	b, err := compress.UnsignedArrayDecodeAllOld(buf, dst)
	if canRecycle && cap(buf) == BlockSizeHint {
		BlockEncoderPool.Put(buf[:0])
	}
	return b, err
}

func decodeUint32BlockOld(block []byte, dst []uint32) ([]uint32, error) {
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

	b, err := compress.UnsignedArrayDecodeAllOld(buf, cp)

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

func decodeUint16BlockOld(block []byte, dst []uint16) ([]uint16, error) {
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

	b, err := compress.UnsignedArrayDecodeAllOld(buf, cp)

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

func decodeUint8BlockOld(block []byte, dst []uint8) ([]uint8, error) {
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

	b, err := compress.UnsignedArrayDecodeAllOld(buf, cp)

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
