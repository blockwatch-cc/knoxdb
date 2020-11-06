// Copyright (c) 2018-2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package pack

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"blockwatch.cc/packdb-pro/encoding/block"
)

func (p *Package) MarshalBinary() ([]byte, error) {
	var (
		bodySize int
		err      error
	)

	buf := bytes.NewBuffer(make([]byte, 0, p.nFields*block.BlockSizeHint))
	buf.WriteByte(byte(p.version))
	buf.WriteByte(0) // empty compression byte to stay compatible with v1/v2

	var b [8]byte
	binary.BigEndian.PutUint32(b[0:], uint32(p.nFields))
	binary.BigEndian.PutUint32(b[4:], uint32(p.nValues))
	buf.Write(b[:])

	// write field names
	for _, v := range p.names {
		_, _ = buf.WriteString(v)
		buf.WriteByte(0)
	}

	// encode all blocks to know their sizes
	headers := make([][]byte, p.nFields)
	encoded := make([][]byte, p.nFields)
	if p.offsets == nil {
		p.offsets = make([]int, p.nFields)
	}
	for i, b := range p.blocks {
		headers[i], encoded[i], err = b.Encode()
		p.offsets[i] = bodySize
		bodySize += len(encoded[i])
		if err != nil {
			return nil, err
		}
	}

	// write block offset table
	var offs [4]byte
	for _, v := range p.offsets {
		binary.BigEndian.PutUint32(offs[:], uint32(v))
		buf.Write(offs[:])
	}

	// write block headers table (min/max values)
	for _, v := range headers {
		buf.Write(v)
		block.BlockEncoderPool.Put(v[:0])
	}

	// write body
	for _, v := range encoded {
		buf.Write(v)
		// recycle block buffers
		if cap(v) == block.BlockSizeHint {
			block.BlockEncoderPool.Put(v[:0])
		}
	}

	p.rawsize = bodySize
	p.packedsize = buf.Len()
	return buf.Bytes(), nil
}

func (p *Package) UnmarshalHeader(data []byte) (PackageHeader, error) {
	buf := bytes.NewBuffer(data)
	if err := p.unmarshalHeader(buf); err != nil {
		return PackageHeader{}, err
	}
	return p.Header(), nil
}

func (p *Package) unmarshalHeader(buf *bytes.Buffer) error {
	blen := buf.Len()
	if blen < 9 {
		return io.ErrShortBuffer
	}
	p.version, _ = buf.ReadByte()
	if p.version > packageStorageFormatVersionV4 {
		return fmt.Errorf("pack: invalid storage format version %d", p.version)
	}
	p.packedsize = blen

	// v1 (OSS) does not write compression byte
	// v2 (PRO) did write compression byte
	// v3 (PRO) compression and precision are obsolete, moved to block
	var precision int
	if p.version >= packageStorageFormatVersionV2 {
		b, _ := buf.ReadByte()
		precision = int(b >> 4)
		if precision == 0 {
			precision = maxPrecision
		}
	}

	// grid size (nFields is stored as uint32)
	p.nFields = int(binary.BigEndian.Uint32(buf.Next(4)))
	p.nValues = int(binary.BigEndian.Uint32(buf.Next(4)))

	// read names, check for existence of names (optional in v2)
	b, _ := buf.ReadByte()
	if b != 0 {
		buf.UnreadByte()
		p.names = make([]string, p.nFields)
		for i := 0; i < p.nFields; i++ {
			// ReadString returns string including the delimiter
			str, err := buf.ReadString(0)
			if err != nil {
				return err
			}
			strcopy := str[:len(str)-1]
			p.names[i] = strcopy
			p.namemap[strcopy] = i
		}
	}

	// read offsets
	p.offsets = make([]int, p.nFields)
	offs := buf.Next(4 * p.nFields)
	for i := 0; i < p.nFields; i++ {
		p.offsets[i] = int(binary.BigEndian.Uint32(offs[i*4:]))
	}

	// read block headers
	// Note: we don't store headers in V1 format, so we only create empty blocks here
	// Note: re-use existing blocks
	if len(p.blocks) != p.nFields {
		p.blocks = make([]*block.Block, p.nFields)
	}
	for i := 0; i < p.nFields; i++ {
		// when packs are reused, their blocks are already allocated
		if p.blocks[i] == nil {
			p.blocks[i] = &block.Block{}
		}
		// read and decode block headers

		// V1 and V3+: read and decode block headers
		if p.version != packageStorageFormatVersionV2 {
			if err := p.blocks[i].DecodeHeader(buf); err != nil {
				return err
			}
			// v2 only: set uint64/float converted block precision from pack header
			if p.version == packageStorageFormatVersionV2 {
				if p.blocks[i].Type == block.BlockUint64 || p.blocks[i].Type == block.BlockFloat64 {
					p.blocks[i].Precision = precision
				}
			}
		}
	}

	// upgrade to v4 so subsequent writes will flush the correct version
	p.version = packageStorageFormatVersionV4

	// treat remaining bytes as pack body
	p.rawsize = buf.Len()
	return nil
}

func (p *Package) UnmarshalBinary(data []byte) error {
	buf := bytes.NewBuffer(data)
	err := p.unmarshalHeader(buf)
	if err != nil {
		return err
	}
	// decode block contents (Note: blocks were already created when reading header)
	for i := 0; i < p.nFields; i++ {
		sz := buf.Len()
		if i+1 < p.nFields {
			sz = p.offsets[i+1] - p.offsets[i]
		}
		if err := p.blocks[i].DecodeBody(buf.Next(sz), p.nValues); err != nil {
			return err
		}
	}
	return nil
}
