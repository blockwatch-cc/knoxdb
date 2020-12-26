// Copyright (c) 2018-2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package block

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"time"

	"blockwatch.cc/knoxdb/vec"
)

const (
	headerBaseSize            = 2
	headerListVersion    byte = 1
	blockTypeMask        byte = 0x1f
	blockScaleMask       byte = 0x7f
	blockCompressionMask byte = 0x03
)

type Header struct {
	Type        BlockType
	Compression Compression
	Scale       int
	MinValue    interface{}
	MaxValue    interface{}
}

func (h Header) IsValid() bool {
	return h.Type != BlockIgnore && h.MinValue != nil && h.MaxValue != nil
}

type HeaderList []Header

func (h HeaderList) Encode(buf *bytes.Buffer) error {
	buf.WriteByte(headerListVersion)
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], uint32(len(h)))
	buf.Write(b[:])
	for _, v := range h {
		if err := v.Encode(buf); err != nil {
			return err
		}
	}
	return nil
}

func (h *HeaderList) Decode(buf *bytes.Buffer) error {
	if buf.Len() < 5 {
		return fmt.Errorf("block: short block header list, length %d", buf.Len())
	}

	// read and check version byte
	b, _ := buf.ReadByte()
	if b != headerListVersion {
		return fmt.Errorf("block: invalid block header list version %d", b)
	}

	// read slice length
	l := int(binary.BigEndian.Uint32(buf.Next(4)))

	// alloc slice
	*h = make(HeaderList, l)

	// decode header parts
	for i := range *h {
		if err := (*h)[i].Decode(buf); err != nil {
			return err
		}
	}
	return nil
}

func NewHeader(typ BlockType, comp Compression, scale int) Header {
	h := Header{
		Type:        typ,
		Compression: comp,
		Scale:       scale,
	}
	switch typ {
	case BlockTime:
		h.MinValue = time.Time{}
		h.MaxValue = time.Time{}
	case BlockFloat64:
		h.MinValue = float64(0.0)
		h.MaxValue = float64(0.0)
	case BlockFloat32:
		h.MinValue = float32(0.0)
		h.MaxValue = float32(0.0)
	case BlockInt64:
		h.MinValue = int64(0)
		h.MaxValue = int64(0)
	case BlockInt32:
		h.MinValue = int32(0)
		h.MaxValue = int32(0)
	case BlockInt16:
		h.MinValue = int16(0)
		h.MaxValue = int16(0)
	case BlockInt8:
		h.MinValue = int8(0)
		h.MaxValue = int8(0)
	case BlockUint64:
		h.MinValue = uint64(0)
		h.MaxValue = uint64(0)
	case BlockUint32:
		h.MinValue = uint32(0)
		h.MaxValue = uint32(0)
	case BlockUint16:
		h.MinValue = uint16(0)
		h.MaxValue = uint16(0)
	case BlockUint8:
		h.MinValue = uint8(0)
		h.MaxValue = uint8(0)
	case BlockBool:
		h.MinValue = false
		h.MaxValue = false
	case BlockString:
		h.MinValue = ""
		h.MaxValue = ""
	case BlockBytes:
		h.MinValue = []byte{}
		h.MaxValue = []byte{}
	case BlockInt128:
		h.MinValue = vec.Int128Zero
		h.MaxValue = vec.Int128Zero
	case BlockInt256:
		h.MinValue = vec.Int256Zero
		h.MaxValue = vec.Int256Zero
	}
	return h
}
func (h Header) Clone() Header {
	cp := Header{
		Type:        h.Type,
		Compression: h.Compression,
		Scale:       h.Scale,
	}
	if h.MinValue == nil || h.MaxValue == nil {
		return cp
	}
	switch h.Type {
	case BlockTime:
		min, max := h.MinValue.(time.Time), h.MaxValue.(time.Time)
		cp.MinValue = min
		cp.MaxValue = max
	case BlockFloat64:
		min, max := h.MinValue.(float64), h.MaxValue.(float64)
		cp.MinValue = min
		cp.MaxValue = max
	case BlockFloat32:
		min, max := h.MinValue.(float32), h.MaxValue.(float32)
		cp.MinValue = min
		cp.MaxValue = max
	case BlockInt64:
		min, max := h.MinValue.(int64), h.MaxValue.(int64)
		cp.MinValue = min
		cp.MaxValue = max
	case BlockInt32:
		min, max := h.MinValue.(int32), h.MaxValue.(int32)
		cp.MinValue = min
		cp.MaxValue = max
	case BlockInt16:
		min, max := h.MinValue.(int16), h.MaxValue.(int16)
		cp.MinValue = min
		cp.MaxValue = max
	case BlockInt8:
		min, max := h.MinValue.(int8), h.MaxValue.(int8)
		cp.MinValue = min
		cp.MaxValue = max
	case BlockUint64:
		min, max := h.MinValue.(uint64), h.MaxValue.(uint64)
		cp.MinValue = min
		cp.MaxValue = max
	case BlockUint32:
		min, max := h.MinValue.(uint32), h.MaxValue.(uint32)
		cp.MinValue = min
		cp.MaxValue = max
	case BlockUint16:
		min, max := h.MinValue.(uint16), h.MaxValue.(uint16)
		cp.MinValue = min
		cp.MaxValue = max
	case BlockUint8:
		min, max := h.MinValue.(uint8), h.MaxValue.(uint8)
		cp.MinValue = min
		cp.MaxValue = max
	case BlockBool:
		min, max := h.MinValue.(bool), h.MaxValue.(bool)
		cp.MinValue = min
		cp.MaxValue = max
	case BlockString:
		// copy contents to avoid memleak
		min, max := h.MinValue.(string), h.MaxValue.(string)
		cp.MinValue = min
		cp.MaxValue = max
	case BlockBytes:
		// copy contents to avoid memleak
		min, max := h.MinValue.([]byte), h.MaxValue.([]byte)
		mincopy := make([]byte, len(min))
		copy(mincopy, min)
		maxcopy := make([]byte, len(max))
		copy(maxcopy, max)
		cp.MinValue = mincopy
		cp.MaxValue = maxcopy
	case BlockInt128:
		cp.MinValue = h.MinValue.(vec.Int128)
		cp.MaxValue = h.MaxValue.(vec.Int128)
	case BlockInt256:
		cp.MinValue = h.MinValue.(vec.Int256)
		cp.MaxValue = h.MaxValue.(vec.Int256)
	}
	return cp
}

func (h Header) EncodedSize() int {
	switch h.Type {
	case BlockInt64,
		BlockTime,
		BlockUint64,
		BlockFloat64:
		return headerBaseSize + 16
	case BlockBool:
		return headerBaseSize + 1
	case BlockString:
		return headerBaseSize + len(h.MinValue.(string)) + len(h.MaxValue.(string)) + 2
	case BlockBytes:
		min, max := h.MinValue.([]byte), h.MaxValue.([]byte)
		l1, l2 := len(min), len(max)
		var v [8]byte
		i1 := binary.PutUvarint(v[:], uint64(l1))
		i2 := binary.PutUvarint(v[:], uint64(l2))
		return headerBaseSize + l1 + l2 + i1 + i2
	case BlockInt32:
		return headerBaseSize + 8
	case BlockInt16:
		return headerBaseSize + 4
	case BlockInt8:
		return headerBaseSize + 2
	case BlockUint32:
		return headerBaseSize + 8
	case BlockUint16:
		return headerBaseSize + 4
	case BlockUint8:
		return headerBaseSize + 2
	case BlockFloat32:
		return headerBaseSize + 8
	case BlockInt128:
		return headerBaseSize + 32
	case BlockInt256:
		return headerBaseSize + 128
	default:
		return 0
	}
}

// same encoding as low level block header, except takes care of conversion flags
func (h Header) Encode(buf *bytes.Buffer) error {
	// 8                 7 6          5 4 3 2 1
	// ext header flag   compression  block type
	buf.WriteByte(byte(h.Type)&blockTypeMask | (byte(h.Compression)&blockCompressionMask)<<5 | 0x80)

	// extension header
	// - 7 lower bits are used for storing scale (0..127)
	// - 1 upper bits is extension flag (currently unused)
	buf.WriteByte(byte(h.Scale) & blockScaleMask)

	switch h.Type {
	case BlockTime:
		var v [16]byte
		min, max := h.MinValue.(time.Time), h.MaxValue.(time.Time)
		vmin, vmax := min.UnixNano(), max.UnixNano()
		bigEndian.PutUint64(v[0:], uint64(vmin))
		bigEndian.PutUint64(v[8:], uint64(vmax))
		_, _ = buf.Write(v[:])

	case BlockFloat64:
		var v [16]byte
		min, max := h.MinValue.(float64), h.MaxValue.(float64)
		bigEndian.PutUint64(v[0:], math.Float64bits(min))
		bigEndian.PutUint64(v[8:], math.Float64bits(max))
		_, _ = buf.Write(v[:])

	case BlockFloat32:
		var v [8]byte
		min, max := h.MinValue.(float32), h.MaxValue.(float32)
		bigEndian.PutUint32(v[0:], math.Float32bits(min))
		bigEndian.PutUint32(v[4:], math.Float32bits(max))
		_, _ = buf.Write(v[:])

	case BlockInt64:
		var v [16]byte
		min, max := h.MinValue.(int64), h.MaxValue.(int64)
		bigEndian.PutUint64(v[0:], uint64(min))
		bigEndian.PutUint64(v[8:], uint64(max))
		_, _ = buf.Write(v[:])

	case BlockInt32:
		var v [8]byte
		min, max := h.MinValue.(int32), h.MaxValue.(int32)
		bigEndian.PutUint32(v[0:], uint32(min))
		bigEndian.PutUint32(v[4:], uint32(max))
		_, _ = buf.Write(v[:])

	case BlockInt16:
		var v [4]byte
		min, max := h.MinValue.(int16), h.MaxValue.(int16)
		bigEndian.PutUint16(v[0:], uint16(min))
		bigEndian.PutUint16(v[2:], uint16(max))
		_, _ = buf.Write(v[:])

	case BlockInt8:
		var v [2]byte
		min, max := h.MinValue.(int8), h.MaxValue.(int8)
		v[0] = uint8(min)
		v[1] = uint8(max)
		_, _ = buf.Write(v[:])

	case BlockUint64:
		var v [16]byte
		min, max := h.MinValue.(uint64), h.MaxValue.(uint64)
		bigEndian.PutUint64(v[0:], min)
		bigEndian.PutUint64(v[8:], max)
		_, _ = buf.Write(v[:])

	case BlockUint32:
		var v [8]byte
		min, max := h.MinValue.(uint32), h.MaxValue.(uint32)
		bigEndian.PutUint32(v[0:], min)
		bigEndian.PutUint32(v[4:], max)
		_, _ = buf.Write(v[:])

	case BlockUint16:
		var v [4]byte
		min, max := h.MinValue.(uint16), h.MaxValue.(uint16)
		bigEndian.PutUint16(v[0:], min)
		bigEndian.PutUint16(v[2:], max)
		_, _ = buf.Write(v[:])

	case BlockUint8:
		var v [2]byte
		min, max := h.MinValue.(uint8), h.MaxValue.(uint8)
		v[0] = min
		v[1] = max
		_, _ = buf.Write(v[:])

	case BlockBool:
		var v byte
		min, max := h.MinValue.(bool), h.MaxValue.(bool)
		if min {
			v = 1
		}
		if max {
			v += 2
		}
		buf.WriteByte(v)

	case BlockString:
		// null terminated string
		min, max := h.MinValue.(string), h.MaxValue.(string)
		_, _ = buf.WriteString(min)
		buf.WriteByte(0)
		_, _ = buf.WriteString(max)
		buf.WriteByte(0)

	case BlockBytes:
		// len prefixed byte slice
		min, max := h.MinValue.([]byte), h.MaxValue.([]byte)
		var v [8]byte
		i := binary.PutUvarint(v[:], uint64(len(min)))
		_, _ = buf.Write(v[:i])
		_, _ = buf.Write(min)

		i = binary.PutUvarint(v[:], uint64(len(max)))
		_, _ = buf.Write(v[:i])
		_, _ = buf.Write(max)

	case BlockInt128:
		min, max := h.MinValue.(vec.Int128).Bytes16(), h.MaxValue.(vec.Int128).Bytes16()
		_, _ = buf.Write(min[:])
		_, _ = buf.Write(max[:])

	case BlockInt256:
		min, max := h.MinValue.(vec.Int256).Bytes32(), h.MaxValue.(vec.Int256).Bytes32()
		_, _ = buf.Write(min[:])
		_, _ = buf.Write(max[:])

	case BlockIgnore:
		return nil

	default:
		return fmt.Errorf("block: invalid data type %d", h.Type)
	}
	return nil
}

func (h *Header) Decode(buf *bytes.Buffer) error {
	val := buf.Next(1)
	var err error
	h.Type, err = readBlockType(val)
	if err != nil {
		return err
	}
	h.Compression, err = readBlockCompression(val)
	if err != nil {
		return err
	}

	if val[0]&0x80 > 0 {
		val = buf.Next(1)
		h.Scale = readBlockScale(val)
	}

	switch h.Type {
	case BlockTime:
		v := buf.Next(16)
		vmin := bigEndian.Uint64(v[0:])
		vmax := bigEndian.Uint64(v[8:])
		h.MinValue = time.Unix(0, int64(vmin)).UTC()
		h.MaxValue = time.Unix(0, int64(vmax)).UTC()

	case BlockFloat64:
		v := buf.Next(16)
		h.MinValue = math.Float64frombits(bigEndian.Uint64(v[0:]))
		h.MaxValue = math.Float64frombits(bigEndian.Uint64(v[8:]))

	case BlockFloat32:
		v := buf.Next(8)
		h.MinValue = math.Float32frombits(bigEndian.Uint32(v[0:]))
		h.MaxValue = math.Float32frombits(bigEndian.Uint32(v[4:]))

	case BlockInt64:
		v := buf.Next(16)
		h.MinValue = int64(bigEndian.Uint64(v[0:]))
		h.MaxValue = int64(bigEndian.Uint64(v[8:]))

	case BlockInt32:
		v := buf.Next(8)
		h.MinValue = int32(bigEndian.Uint32(v[0:]))
		h.MaxValue = int32(bigEndian.Uint32(v[4:]))

	case BlockInt16:
		v := buf.Next(4)
		h.MinValue = int16(bigEndian.Uint16(v[0:]))
		h.MaxValue = int16(bigEndian.Uint16(v[2:]))

	case BlockInt8:
		v := buf.Next(2)
		h.MinValue = int8(v[0])
		h.MaxValue = int8(v[1])

	case BlockUint64:
		v := buf.Next(16)
		h.MinValue = bigEndian.Uint64(v[0:])
		h.MaxValue = bigEndian.Uint64(v[8:])

	case BlockUint32:
		v := buf.Next(8)
		h.MinValue = bigEndian.Uint32(v[0:])
		h.MaxValue = bigEndian.Uint32(v[4:])

	case BlockUint16:
		v := buf.Next(4)
		h.MinValue = uint16(bigEndian.Uint16(v[0:]))
		h.MaxValue = uint16(bigEndian.Uint16(v[2:]))

	case BlockUint8:
		v := buf.Next(2)
		h.MinValue = uint8(v[0])
		h.MaxValue = uint8(v[1])

	case BlockBool:
		v := buf.Next(1)
		h.MinValue = v[0]&1 > 0
		h.MaxValue = v[0]&2 > 0

	case BlockString:
		min, err := buf.ReadString(0)
		if err != nil {
			return fmt.Errorf("block: reading min string block header: %v", err)
		}
		max, err := buf.ReadString(0)
		if err != nil {
			return fmt.Errorf("block: reading max string block header: %v", err)
		}
		// don't reference buffer data!
		mincopy := min[:len(min)-1]
		maxcopy := max[:len(max)-1]
		h.MinValue = mincopy
		h.MaxValue = maxcopy

	case BlockBytes:
		length, err := binary.ReadUvarint(buf)
		if err != nil {
			return fmt.Errorf("block: reading min []byte block header: %v", err)
		}
		min := buf.Next(int(length))
		length, err = binary.ReadUvarint(buf)
		if err != nil {
			return fmt.Errorf("block: reading max []byte block header: %v", err)
		}
		max := buf.Next(int(length))

		// don't reference buffer data!
		mincopy := make([]byte, len(min))
		maxcopy := make([]byte, len(max))
		copy(mincopy, min)
		copy(maxcopy, max)
		h.MinValue = mincopy
		h.MaxValue = maxcopy

	case BlockInt128:
		v := buf.Next(32)
		h.MinValue = vec.Int128FromBytes(v[0:16])
		h.MaxValue = vec.Int128FromBytes(v[16:32])

	case BlockInt256:
		v := buf.Next(64)
		h.MinValue = vec.Int256FromBytes(v[0:32])
		h.MaxValue = vec.Int256FromBytes(v[32:64])

	default:
		return fmt.Errorf("block: invalid data type %d", h.Type)
	}

	return nil
}

func (h *Header) Clear() {
	switch h.Type {
	case BlockTime:
		h.MinValue = time.Time{}
		h.MaxValue = time.Time{}
	case BlockInt64:
		h.MinValue = int64(0)
		h.MaxValue = int64(0)
	case BlockInt32:
		h.MinValue = int32(0)
		h.MaxValue = int32(0)
	case BlockInt16:
		h.MinValue = int16(0)
		h.MaxValue = int16(0)
	case BlockInt8:
		h.MinValue = int8(0)
		h.MaxValue = int8(0)
	case BlockUint64:
		h.MinValue = uint64(0)
		h.MaxValue = uint64(0)
	case BlockUint32:
		h.MinValue = uint32(0)
		h.MaxValue = uint32(0)
	case BlockUint16:
		h.MinValue = uint16(0)
		h.MaxValue = uint16(0)
	case BlockUint8:
		h.MinValue = uint8(0)
		h.MaxValue = uint8(0)
	case BlockFloat64:
		h.MinValue = float64(0.0)
		h.MaxValue = float64(0.0)
	case BlockFloat32:
		h.MinValue = float32(0.0)
		h.MaxValue = float32(0.0)
	case BlockString:
		h.MinValue = ""
		h.MaxValue = ""
	case BlockBytes:
		h.MinValue = []byte{}
		h.MaxValue = []byte{}
	case BlockBool:
		h.MinValue = false
		h.MaxValue = false
	case BlockInt128:
		h.MinValue = vec.Int128Zero
		h.MaxValue = vec.Int128Zero
	case BlockInt256:
		h.MinValue = vec.Int256Zero
		h.MaxValue = vec.Int256Zero
	}
}
