// Copyright (c) 2018-2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package block

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"time"
)

const BlockHeaderVersion = byte(1)

type BlockHeader struct {
	Type        BlockType
	Compression Compression
	Precision   int
	Flags       BlockFlags
	MinValue    interface{}
	MaxValue    interface{}
}

func (h BlockHeader) IsValid() bool {
	return h.Type != BlockIgnore && h.MinValue != nil && h.MaxValue != nil
}

type BlockHeaderList []BlockHeader

func (h BlockHeaderList) Encode(buf *bytes.Buffer) error {
	buf.WriteByte(BlockHeaderVersion)
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

func (h *BlockHeaderList) Decode(buf *bytes.Buffer) error {
	if buf.Len() < 5 {
		return fmt.Errorf("pack: short block header list, length %d", buf.Len())
	}

	// read and check version byte
	b, _ := buf.ReadByte()
	if b != BlockHeaderVersion {
		return fmt.Errorf("pack: invalid block header list version %d", b)
	}

	// read slice length
	l := int(binary.BigEndian.Uint32(buf.Next(4)))

	// alloc slice
	*h = make(BlockHeaderList, l)

	// decode header parts
	for i := range *h {
		if err := (*h)[i].Decode(buf); err != nil {
			return err
		}
	}
	return nil
}

// copies header data and converts min/max values when flagged
func (b *Block) CloneHeader() BlockHeader {
	// block header data must be valid (only applies to v2 blocks!)
	if b.MinValue == nil || b.MaxValue == nil {
		return BlockHeader{}
	}
	bh := BlockHeader{
		Type:        b.Type,
		Compression: b.Compression,
		Precision:   b.Precision,
		Flags:       b.Flags,
	}
	switch b.Type {
	case BlockTime:
		min, max := b.MinValue.(time.Time), b.MaxValue.(time.Time)
		bh.MinValue = min
		bh.MaxValue = max
	case BlockFloat:
		min, max := b.MinValue.(float64), b.MaxValue.(float64)
		bh.MinValue = min
		bh.MaxValue = max
	case BlockInteger:
		min, max := b.MinValue.(int64), b.MaxValue.(int64)
		bh.MinValue = min
		bh.MaxValue = max
	case BlockInt32:
		min, max := b.MinValue.(int32), b.MaxValue.(int32)
		bh.MinValue = min
		bh.MaxValue = max
	case BlockUnsigned:
		min, max := b.MinValue.(uint64), b.MaxValue.(uint64)
		if b.Flags&BlockFlagConvert > 0 {
			// uint64 -> float64 incl compression
			bh.MinValue = ConvertValue(DecompressAmount(min), b.Precision)
			bh.MaxValue = ConvertValue(DecompressAmount(max), b.Precision)
		} else if b.Flags&BlockFlagCompress > 0 {
			// uint64 compression only
			bh.MinValue = DecompressAmount(min)
			bh.MaxValue = DecompressAmount(max)
		} else {
			bh.MinValue = min
			bh.MaxValue = max
		}
	case BlockBool:
		min, max := b.MinValue.(bool), b.MaxValue.(bool)
		bh.MinValue = min
		bh.MaxValue = max
	case BlockString:
		// copy contents to avoid memleak
		min, max := b.MinValue.(string), b.MaxValue.(string)
		bh.MinValue = min
		bh.MaxValue = max
	case BlockBytes:
		// copy contents to avoid memleak
		min, max := b.MinValue.([]byte), b.MaxValue.([]byte)
		mincopy := make([]byte, len(min))
		copy(mincopy, min)
		maxcopy := make([]byte, len(max))
		copy(maxcopy, max)
		bh.MinValue = mincopy
		bh.MaxValue = maxcopy
	}
	return bh
}

// same encoding as low level block header, except takes care of conversion flags
func (h BlockHeader) Encode(buf *bytes.Buffer) error {
	// 8                 7 6          5 4 3 2 1
	// ext header flag   compression  block type
	buf.WriteByte(byte(h.Type&0x1f) | byte(h.Compression&0x3)<<5 | 0x80)

	// extension header
	// - 4 lower bits are used for storing precision
	// - 4 upper bits are flags
	buf.WriteByte((byte(h.Flags)&0xf)<<4 | byte(h.Precision)&0xf)

	switch h.Type {
	case BlockTime:
		var v [16]byte
		min, max := h.MinValue.(time.Time), h.MaxValue.(time.Time)
		vmin, vmax := min.UnixNano(), max.UnixNano()
		bigEndian.PutUint64(v[0:], uint64(vmin))
		bigEndian.PutUint64(v[8:], uint64(vmax))
		_, _ = buf.Write(v[:])

	case BlockFloat:
		var v [16]byte
		min, max := h.MinValue.(float64), h.MaxValue.(float64)
		bigEndian.PutUint64(v[0:], math.Float64bits(min))
		bigEndian.PutUint64(v[8:], math.Float64bits(max))
		_, _ = buf.Write(v[:])

	case BlockInteger:
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

	case BlockUnsigned:
		var v [16]byte
		if h.Flags&BlockFlagConvert > 0 {
			// Note: data is float64
			min, max := h.MinValue.(float64), h.MaxValue.(float64)
			bigEndian.PutUint64(v[0:], math.Float64bits(min))
			bigEndian.PutUint64(v[8:], math.Float64bits(max))
		} else {
			// Note: data is uint64
			min, max := h.MinValue.(uint64), h.MaxValue.(uint64)
			bigEndian.PutUint64(v[0:], min)
			bigEndian.PutUint64(v[8:], max)
		}
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

	case BlockIgnore:
		return nil

	default:
		return fmt.Errorf("pack: invalid data type %d", h.Type)
	}
	return nil
}

// same encoding as low level block header, except takes care of conversion flags
func (h *BlockHeader) Decode(buf *bytes.Buffer) error {
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
		h.Precision = readBlockPrecision(val)
		h.Flags = readBlockFlags(val)
	}

	switch h.Type {
	case BlockTime:
		v := buf.Next(16)
		vmin := bigEndian.Uint64(v[0:])
		vmax := bigEndian.Uint64(v[8:])
		h.MinValue = time.Unix(0, int64(vmin)).UTC()
		h.MaxValue = time.Unix(0, int64(vmax)).UTC()

	case BlockFloat:
		v := buf.Next(16)
		h.MinValue = math.Float64frombits(bigEndian.Uint64(v[0:]))
		h.MaxValue = math.Float64frombits(bigEndian.Uint64(v[8:]))

	case BlockInteger:
		v := buf.Next(16)
		h.MinValue = int64(bigEndian.Uint64(v[0:]))
		h.MaxValue = int64(bigEndian.Uint64(v[8:]))

	case BlockInt32:
		v := buf.Next(8)
		h.MinValue = int32(bigEndian.Uint32(v[0:]))
		h.MaxValue = int32(bigEndian.Uint32(v[4:]))

	case BlockUnsigned:
		v := buf.Next(16)
		if h.Flags&BlockFlagConvert > 0 {
			// data is float64
			h.MinValue = math.Float64frombits(bigEndian.Uint64(v[0:]))
			h.MaxValue = math.Float64frombits(bigEndian.Uint64(v[8:]))
		} else {
			// data is uint64
			h.MinValue = bigEndian.Uint64(v[0:])
			h.MaxValue = bigEndian.Uint64(v[8:])
		}

	case BlockBool:
		v := buf.Next(1)
		h.MinValue = v[0]&1 > 0
		h.MaxValue = v[0]&2 > 0

	case BlockString:
		min, err := buf.ReadString(0)
		if err != nil {
			return fmt.Errorf("pack: reading min string block header: %v", err)
		}
		max, err := buf.ReadString(0)
		if err != nil {
			return fmt.Errorf("pack: reading max string block header: %v", err)
		}
		// don't reference buffer data!
		mincopy := min[:len(min)-1]
		maxcopy := max[:len(max)-1]
		h.MinValue = mincopy
		h.MaxValue = maxcopy

	case BlockBytes:
		length, err := binary.ReadUvarint(buf)
		if err != nil {
			return fmt.Errorf("pack: reading min []byte block header: %v", err)
		}
		min := buf.Next(int(length))
		length, err = binary.ReadUvarint(buf)
		if err != nil {
			return fmt.Errorf("pack: reading max []byte block header: %v", err)
		}
		max := buf.Next(int(length))

		// don't reference buffer data!
		mincopy := make([]byte, len(min))
		maxcopy := make([]byte, len(max))
		copy(mincopy, min)
		copy(maxcopy, max)
		h.MinValue = mincopy
		h.MaxValue = maxcopy

	default:
		return fmt.Errorf("pack: invalid data type %d", h.Type)
	}

	return nil
}
