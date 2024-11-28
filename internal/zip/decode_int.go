// Copyright (c) 2018-2023 Blockwatch Data Inc.
// Author: alex@blockwatch.cc
//
// Inspired by InfluxData, MIT, https://github.com/influxdata/influxdb
package zip

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"blockwatch.cc/knoxdb/internal/arena"
	"blockwatch.cc/knoxdb/pkg/num"
)

// DecodeInt128 is an efficient int128 integer decoder.
func DecodeInt128(dst *num.Int128Stride, buf []byte) error {
	if len(buf) == 0 {
		return nil
	}
	if len(buf) < 8 {
		return fmt.Errorf("zip: DecodeInt128 short buffer")
	}

	sbuf := bytes.NewBuffer(buf)
	slen := int(binary.LittleEndian.Uint32(sbuf.Next(4)))
	if sbuf.Len() < slen {
		return fmt.Errorf("zip: DecodeInt128 stride x0: short buffer")
	}
	n, err := decodeUint64(asU64(dst.X0), sbuf.Next(slen))
	if err != nil {
		return fmt.Errorf("zip: DecodeInt128 stride x0: %v", err)
	}
	dst.X0 = dst.X0[:n]

	if sbuf.Len() < 4 {
		return fmt.Errorf("zip: DecodeInt128 stride x1: short buffer")
	}
	slen = int(binary.LittleEndian.Uint32(sbuf.Next(4)))
	if sbuf.Len() < slen {
		return fmt.Errorf("zip: DecodeInt128 stride x1: short buffer")
	}
	n, err = decodeUint64(dst.X1, sbuf.Next(slen))
	if err != nil {
		return fmt.Errorf("zip: DecodeInt128 stride x1: %v", err)
	}
	dst.X1 = dst.X1[:n]
	return nil
}

// ReadInt128 is the io.Reader variant of the i128 decoder. It is
// less efficient than the buffer variant because data is first read into
// a staging buffer costing one more buffer allocation and one more copy.
func ReadInt128(dst *num.Int128Stride, r io.Reader) (int64, error) {
	// Note: no encoding header, format is
	// - stride 0 size
	// - stride 0 data
	// - stride 1 size
	// - stride 1 data

	// read stride 0
	var sz uint32
	err := binary.Read(r, binary.LittleEndian, &sz)
	if err != nil {
		return 0, fmt.Errorf("zip: ReadInt128 stride x0: %v", err)
	}
	c := int64(4)

	// prepare scratch space for this stride
	scratch := arena.Alloc(arena.AllocBytes, Int64EncodedSize(dst.Cap()))
	defer arena.Free(arena.AllocBytes, scratch)
	buf := scratch.([]byte)[:sz]
	_, err = io.ReadFull(r, buf)
	if err != nil {
		return c, fmt.Errorf("zip: ReadInt128 stride x0: %v", err)
	}
	c += int64(sz)
	n, err := decodeUint64(asU64(dst.X0), buf)
	if err != nil {
		return c, fmt.Errorf("zip: ReadInt128 stride x0: %v", err)
	}

	// read stride 1
	err = binary.Read(r, binary.LittleEndian, &sz)
	if err != nil {
		return c, fmt.Errorf("zip: ReadInt128 strice x1: %v", err)
	}
	c += 4
	buf = buf[:sz]
	_, err = io.ReadFull(r, buf)
	if err != nil {
		return c, fmt.Errorf("zip: ReadInt128 stride x1: %v", err)
	}
	c += int64(sz)
	m, err := decodeUint64(dst.X1, buf)
	if err != nil {
		return c, fmt.Errorf("zip: ReadInt128 stride x1: %v", err)
	}
	if m != n {
		return c, fmt.Errorf("zip: ReadInt128 stride x1 length mismatch: got=%d exp=%d", m, n)
	}

	dst.X0 = dst.X0[:n]
	dst.X1 = dst.X1[:n]
	return c, nil
}

// DecodeInt256 is an efficient int128 integer decoder.
func DecodeInt256(dst *num.Int256Stride, buf []byte) error {
	if len(buf) == 0 {
		return nil
	}
	if len(buf) < 8 {
		return fmt.Errorf("zip: DecodeInt256: short buffer")
	}

	sbuf := bytes.NewBuffer(buf)

	// stride 0
	slen := int(binary.LittleEndian.Uint32(sbuf.Next(4)))
	if sbuf.Len() < slen {
		return fmt.Errorf("zip: DecodeInt256 stride x0: short buffer")
	}
	n, err := decodeUint64(asU64(dst.X0), sbuf.Next(slen))
	if err != nil {
		return fmt.Errorf("zip: DecodeInt256 stride x0: %v", err)
	}

	// strides 1..3
	for i, stride := range [][]uint64{dst.X1, dst.X2, dst.X3} {
		if sbuf.Len() < 4 {
			return fmt.Errorf("zip: DecodeInt256 stride x%d: short buffer", i)
		}
		slen = int(binary.LittleEndian.Uint32(sbuf.Next(4)))
		if sbuf.Len() < slen {
			return fmt.Errorf("zip: DecodeInt256 stride x%d: short buffer", i)
		}
		m, err := decodeUint64(stride, sbuf.Next(slen))
		if err != nil {
			return fmt.Errorf("zip: DecodeInt256 stride x%d: %v", i, err)
		}
		if m != n {
			return fmt.Errorf("zip: DecodeInt256 stride x%d length mismatch: got=%d exp=%d", i, m, n)
		}
	}
	dst.X0 = dst.X0[:n]
	dst.X1 = dst.X1[:n]
	dst.X2 = dst.X2[:n]
	dst.X3 = dst.X3[:n]
	return nil

}

// ReadInt128 is the io.Reader variant of the i128 decoder. It is
// less efficient than the buffer variant because data is first read into
// a staging buffer costing one more buffer allocation and one more copy.
func ReadInt256(dst *num.Int256Stride, r io.Reader) (int64, error) {
	// Note: no encoding header, format is
	// - stride 0 size
	// - stride 0 data
	//   ...
	// - stride 3 size
	// - stride 3 data

	// read stride size
	var sz uint32
	err := binary.Read(r, binary.LittleEndian, &sz)
	if err != nil {
		return 0, fmt.Errorf("zip: ReadInt256 read x0 size: %v", err)
	}
	c := int64(4)

	// prepare scratch space for each stride of int64 data
	scratch := arena.Alloc(arena.AllocBytes, Int64EncodedSize(dst.Cap()))
	defer arena.Free(arena.AllocBytes, scratch)
	buf := scratch.([]byte)[:sz]
	_, err = io.ReadFull(r, buf)
	if err != nil {
		return c, fmt.Errorf("zip: ReadInt256 read x0 data: %v", err)
	}
	c += int64(sz)
	n, err := decodeUint64(asU64(dst.X0), buf)
	if err != nil {
		return c, fmt.Errorf("zip: ReadInt256 decode x0 data: %v", err)
	}
	dst.X0 = dst.X0[:n]

	// read strides 1 .. 3
	for i, stride := range [][]uint64{dst.X1, dst.X2, dst.X3} {
		// read size
		sz = 0
		err = binary.Read(r, binary.LittleEndian, &sz)
		if err != nil {
			return c, fmt.Errorf("zip: ReadInt256 read x%d size: %v", i+1, err)
		}
		c += 4
		buf = buf[:sz]
		_, err = io.ReadFull(r, buf)
		if err != nil {
			return c, fmt.Errorf("zip: ReadInt256 read x%d data: %v", i+1, err)
		}
		c += int64(sz)

		m, err := decodeUint64(stride, buf)
		if err != nil {
			return c, fmt.Errorf("zip: ReadInt256 stride x%d decode: %v", i+1, err)
		}
		if m != n {
			return c, fmt.Errorf("zip: ReadInt256 stride x%d length mismatch: got=%d exp=%d", i, m, n)
		}
	}

	dst.X0 = dst.X0[:n]
	dst.X1 = dst.X1[:n]
	dst.X2 = dst.X2[:n]
	dst.X3 = dst.X3[:n]
	return c, nil
}
