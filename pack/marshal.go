// Copyright (c) 2018-2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package pack

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"blockwatch.cc/knoxdb/encoding/block"
)

const (
	// Note: we use a higher format number starting at 0xa0 to prevent collisions
	// with packdb files (i.e. the alpha version of knox)
	packageStorageFormatVersionV1 byte = 0xa1 // KnoxDB v1 (BE, simple compression)
	packageStorageFormatVersionV2 byte = 0xa2 // KnoxDB v2 (LE, vectorized compression)
	currentStorageFormat               = packageStorageFormatVersionV2
)

func (p *Package) MarshalBinary() ([]byte, error) {
	var maxSize int
	for _, b := range p.blocks {
		maxSize += b.MaxStoredSize()
	}

	buf := bytes.NewBuffer(make([]byte, 0, maxSize))
	buf.WriteByte(packageStorageFormatVersionV2)

	var b [8]byte
	binary.BigEndian.PutUint32(b[0:], uint32(p.nFields))
	binary.BigEndian.PutUint32(b[4:], uint32(p.nValues))
	buf.Write(b[:])

	// reserve offset table space
	offsetTablePos := buf.Len()
	buf.Write(bytes.Repeat([]byte{0}, p.nFields*4))
	offsets := make([]int, p.nFields)
	p.size = buf.Len()

	// write blocks
	for i, b := range p.blocks {
		offsets[i] = buf.Len()
		_, err := b.Encode(buf)
		if err != nil {
			return nil, err
		}
	}

	// keep pack statistics
	p.size = buf.Len()
	packed := buf.Bytes()

	// write offset table
	for _, v := range offsets {
		binary.BigEndian.PutUint32(packed[offsetTablePos:offsetTablePos+4], uint32(v))
		offsetTablePos += 4
	}

	return packed, nil
}

func (p *Package) UnmarshalBinary(data []byte) error {
	blen := len(data)
	if blen < 9 {
		return io.ErrShortBuffer
	}

	buf := bytes.NewBuffer(data)
	version, _ := buf.ReadByte()

	// Note: encodings are not upwards or downwards compatible between v1 and v2 !
	if version != currentStorageFormat {
		return fmt.Errorf("pack: invalid v%d storage format", version-0xa0)
	}
	p.size = blen
	p.nFields = int(binary.BigEndian.Uint32(buf.Next(4)))
	p.nValues = int(binary.BigEndian.Uint32(buf.Next(4)))

	// read offsets
	offsets := make([]int, p.nFields)
	for i := 0; i < p.nFields; i++ {
		offsets[i] = int(binary.BigEndian.Uint32(buf.Next(4)))
	}

	// prepare blocks, re-use when pack already contains sufficient blocks
	if len(p.blocks) < p.nFields {
		for i, b := range p.blocks {
			b.Release()
			p.blocks[i] = nil
		}
		p.blocks = make([]block.Block, p.nFields)
		for i := range p.blocks {
			p.blocks[i] = block.AllocBlock()
		}
	} else {
		for i, b := range p.blocks[p.nFields:] {
			b.Release()
			p.blocks[i] = nil
		}
		p.blocks = p.blocks[:p.nFields]
	}

	// decode blocks
	for i := 0; i < p.nFields; i++ {
		// calculate block size from offset table
		var sz int
		if i < p.nFields-1 {
			sz = offsets[i+1] - offsets[i]
		} else {
			sz = blen - offsets[i]
		}
		// skip blocks that are set to type ignore before decoding
		// this is the core magic of skipping blocks on load
		//		if p.blocks[i].IsIgnore() {
		if p.blocks[i] == nil {
			_ = buf.Next(sz)
			continue
		}
		err := p.blocks[i].Decode(buf.Next(sz), p.nValues, sz)
		if err != nil {
			return err
		}
	}
	return nil
}
