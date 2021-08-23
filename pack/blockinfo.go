// Copyright (c) 2018-2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package pack

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"time"

	. "blockwatch.cc/knoxdb/encoding/block"
	"blockwatch.cc/knoxdb/filter/bloom"
	"blockwatch.cc/knoxdb/vec"
)

const (
	headerBaseSize            = 2
	headerListVersion    byte = 2 // 2: +cardinality
	blockTypeMask        byte = 0x1f
	blockCompressionMask byte = 0x03
	blockScaleMask       byte = 0x7f
)

type BlockInfo struct {
	Type        BlockType
	Compression Compression
	Scale       int

	// statistics
	dirty       bool          // update required
	MinValue    interface{}   // vector min
	MaxValue    interface{}   // vector max
	Cardinality uint64        // unique items in vector
	Bloom       *bloom.Filter // not stored (yet)
}

func (h BlockInfo) IsValid() bool {
	return h.Type != BlockIgnore && h.MinValue != nil && h.MaxValue != nil
}

func (h BlockInfo) IsDirty() bool {
	return h.dirty
}

func (h BlockInfo) SetDirty() {
	h.dirty = true
}

type BlockInfoList []BlockInfo

func (h BlockInfoList) Encode(buf *bytes.Buffer) error {
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

func (h *BlockInfoList) Decode(buf *bytes.Buffer) error {
	if buf.Len() < 5 {
		return fmt.Errorf("pack: short block info list, length %d", buf.Len())
	}

	// read and check version byte
	ver, _ := buf.ReadByte()
	if ver > headerListVersion {
		return fmt.Errorf("pack: invalid block info list version %d", ver)
	}

	// read slice length
	l := int(binary.BigEndian.Uint32(buf.Next(4)))

	// alloc slice
	*h = make(BlockInfoList, l)

	// decode header parts
	for i := range *h {
		if err := (*h)[i].Decode(buf, ver); err != nil {
			return err
		}
	}
	return nil
}

func NewBlockInfo(b *Block, field Field) BlockInfo {
	h := BlockInfo{
		Type:        b.Type(),
		Compression: b.Compression(),
		Scale:       field.Scale,
		dirty:       b.Len() > 0,
	}
	switch b.Type() {
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
		h.MinValue = vec.ZeroInt128
		h.MaxValue = vec.ZeroInt128
	case BlockInt256:
		h.MinValue = vec.ZeroInt256
		h.MaxValue = vec.ZeroInt256
	}
	return h
}

func (h BlockInfo) EncodedSize() int {
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

func (h BlockInfo) Encode(buf *bytes.Buffer) error {
	// same encoding as lower level block header,
	// 8                 7 6          5 4 3 2 1
	// ext header flag   compression  block type
	buf.WriteByte(byte(h.Type)&blockTypeMask | (byte(h.Compression)&blockCompressionMask)<<5 | 0x80)

	// extension header
	// - 7 lower bits are used for storing scale (0..127)
	// - 1 upper bits is extension flag (currently unused)
	buf.WriteByte(byte(h.Scale) & blockScaleMask)

	// write cardinality, 32bit
	var b [4]byte
	bigEndian.PutUint32(b[0:], uint32(h.Cardinality))
	_, _ = buf.Write(b[:])

	// write type-specific min/max values
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

	default:
		return fmt.Errorf("pack: invalid block type %d", h.Type)
	}
	h.dirty = false
	return nil
}

func (h *BlockInfo) Decode(buf *bytes.Buffer, version byte) error {
	val := buf.Next(1)
	h.Type = BlockType(val[0] & blockTypeMask)
	h.Compression = Compression((val[0] >> 5) & blockCompressionMask)
	if val[0]&0x80 > 0 {
		val = buf.Next(1)
		h.Scale = int(val[0] & blockScaleMask)
	}
	h.dirty = false

	// be backwards compatible
	if version > 1 {
		h.Cardinality = uint64(bigEndian.Uint32(buf.Next(4)))
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
			return fmt.Errorf("pack: reading min string block info: %v", err)
		}
		max, err := buf.ReadString(0)
		if err != nil {
			return fmt.Errorf("pack: reading max string block info: %v", err)
		}
		// don't reference buffer data!
		mincopy := min[:len(min)-1]
		maxcopy := max[:len(max)-1]
		h.MinValue = mincopy
		h.MaxValue = maxcopy

	case BlockBytes:
		length, err := binary.ReadUvarint(buf)
		if err != nil {
			return fmt.Errorf("pack: reading min []byte block info: %v", err)
		}
		min := buf.Next(int(length))
		length, err = binary.ReadUvarint(buf)
		if err != nil {
			return fmt.Errorf("pack: reading max []byte block info: %v", err)
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
		return fmt.Errorf("pack: invalid block type %d", h.Type)
	}

	return nil
}
