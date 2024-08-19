// Copyright (c) 2018-2023 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package pack

import (
	"bytes"
	"encoding/binary"

	"blockwatch.cc/knoxdb/internal/block"
	"blockwatch.cc/knoxdb/internal/metadata"
	"blockwatch.cc/knoxdb/pkg/schema"
)

// TODO
// In the current storage model we store full packs with all blocks serialized
// under the pack's key. On load this requires us to read all data from disk
// even though we might want to skip some blocks.
//
// A better alternative would be to only read requested blocks. This requires
// - (with boltdb) new composite data keys (pack-id + block-id) and multiple reads
// - (post-boltdb) a new file format where blocks are directly addressable

const (
	packFormatV0      byte = 0xa0 // PackDB v1 (alpha version)
	packFormatV1      byte = 0xa1 // KnoxDB v1 (simple compression)
	packFormatV2      byte = 0xa2 // KnoxDB v2 (vectorized compression)
	packFormatV3      byte = 0xa3 // KnoxDB v3 (schema, little endian, block addressable)
	currentPackFormat      = packFormatV3
)

var (
	LE = binary.LittleEndian

	// translate field type to block type
	blockTypes = [...]block.BlockType{
		FieldTypeInvalid:    block.BlockUint8,
		FieldTypeDatetime:   block.BlockTime,
		FieldTypeBoolean:    block.BlockBool,
		FieldTypeString:     block.BlockString,
		FieldTypeBytes:      block.BlockBytes,
		FieldTypeInt8:       block.BlockInt8,
		FieldTypeInt16:      block.BlockInt16,
		FieldTypeInt32:      block.BlockInt32,
		FieldTypeInt64:      block.BlockInt64,
		FieldTypeInt128:     block.BlockInt128,
		FieldTypeInt256:     block.BlockInt256,
		FieldTypeUint8:      block.BlockUint8,
		FieldTypeUint16:     block.BlockUint16,
		FieldTypeUint32:     block.BlockUint32,
		FieldTypeUint64:     block.BlockUint64,
		FieldTypeDecimal32:  block.BlockInt32,
		FieldTypeDecimal64:  block.BlockInt64,
		FieldTypeDecimal128: block.BlockInt128,
		FieldTypeDecimal256: block.BlockInt256,
		FieldTypeFloat32:    block.BlockFloat32,
		FieldTypeFloat64:    block.BlockFloat64,
	}
)

// Encodes a pack including all blocks in storage format.
//
// Deleted blocks are only marked as deleted in schema and omitted from new
// stored packs, existing packs may still contain deleted blocks, but we
// won't load them anymore
func (p *Package) Encode(buf *bytes.Buffer, meta *metadata.PackMetadata) error {
	buf.WriteByte(currentPackFormat)

	var b [8]byte
	LE.PutUint64(b[:], p.schema.Hash())
	buf.Write(b[:])
	LE.PutUint32(b[0:], uint32(p.schema.Version()))
	buf.Write(b[:4])
	LE.PutUint32(b[0:], uint32(p.schema.NumFields()))
	LE.PutUint32(b[4:], uint32(p.nRows))
	buf.Write(b[:])

	// reserve offset table space
	offsetTablePos := buf.Len()
	buf.Write(bytes.Repeat([]byte{0}, p.schema.NumFields()*4))
	offsets := make([]int, p.schema.NumFields())

	// write blocks
	for i, f := range p.schema.Fields() {
		// keep buffer offset for this field
		offsets[i] = buf.Len()

		// skip deleted fields, we still write an empty offset table entry
		if f.Is(schema.FieldFlagDeleted) {
			continue
		}

		// encode block data using optional compressor
		buf.WriteByte(byte(f.Compress()))
		enc := NewCompressor(buf, f.Compress())
		n, err := p.blocks[i].WriteTo(enc)
		enc.Close()
		if err != nil {
			return err
		}

		// export block size to statistics
		meta.Blocks[i].StoredSize = int(n)
	}

	// export pack size to statistics
	meta.StoredSize = buf.Len()

	// write offset table
	packed := buf.Bytes()
	for _, v := range offsets {
		LE.PutUint32(packed[offsetTablePos:offsetTablePos+4], uint32(v))
		offsetTablePos += 4
	}

	return nil
}

func (p *Package) Decode(buf []byte) (int64, error) {
	// read pack dimensions
	nBlocks := uint16(LE.Uint32(buf))
	buf = buf[4:]
	p.nRows = int(LE.Uint32(buf))
	buf = buf[4:]

	// keep offset table pointer
	offsets := buf
	buf = buf[4*nBlocks:]

	// only read blocks requested by schema
	// use schema field pos to map on-disk blocks to pack blocks
	// skip load when a block already exists (its been loaded from cache)
	var n int64
	for i, f := range p.schema.Fields() {
		// skip deleted fields, pack blocks will be nil
		if f.Is(schema.FieldFlagDeleted) {
			continue
		}

		// skip already loaded blocks
		if p.blocks[i] != nil {
			continue
		}

		// calculate block offset and stored block size from offset table
		// note that block id to load is indirectly defined in field.Id
		start := int(LE.Uint32(offsets[f.Id()*4:]))
		end := len(buf)
		if f.Id() < nBlocks-1 {
			end = int(LE.Uint32(offsets[(i+1)*4:]))
		}

		// alloc block (use actual storage size, arena will round up to power of 2)
		b := block.New(blockTypes[f.Type()], p.nRows)

		// read compression
		comp := schema.FieldCompression(buf[start])
		start++

		var err error
		if comp > 0 {
			// decode block data with optional decompressor
			dec := NewDecompressor(bytes.NewBuffer(buf[start:end]), comp)
			_, err = b.ReadFrom(dec)
			defer dec.Close()
		} else {
			// fast-path, decode from buffer
			err = b.Decode(buf[start:end])
		}
		if err != nil {
			return n, err
		}

		// add block to package
		p.blocks[i] = b
		n += int64(end - start)
	}

	return n, nil
}

// TODO: decide where packs are stored

// // Stores package into bucket creating a new or overwriting an existing version
// // and updates pack metadata with storage size info.
// func (p *Package) Store(tx *db.Tx, bucket []byte, meta *metadata.PackMetadata) (int64, error) {
// 	// allocate target buffer
// 	var maxSize int
// 	for _, b := range p.blocks {
// 		if b == nil {
// 			continue
// 		}
// 		maxSize += b.MaxStoredSize()
// 	}

// 	// alloc from arena (configured to support up to 32MB blocks of data)
// 	ibuf := arena.Alloc(arena.AllocBytes, maxSize)
// 	defer arena.Free(arena.AllocBytes, ibuf)
// 	buf := bytes.NewBuffer(ibuf.([]byte)[:0])

// 	if err := p.Encode(buf, meta); err != nil {
// 		return 0, err
// 	}
// 	n := int64(buf.Len())

// 	// store inside db transaction
// 	if err := tx.Put(bucket, p.Key(), buf.Bytes()); err != nil {
// 		return 0, err
// 	}
// 	p.dirty = false

// 	return n, nil
// }

// // Loads missing blocks from disk
// func (p *Package) Load(tx *db.Tx, bucket []byte) (int64, error) {
// 	// TODO: move bolt usage into transaction (hide we're using boltdb
// 	val := tx.Get(bucket, p.Key())
// 	if len(val) < 17 {
// 		return 0, io.ErrShortBuffer
// 	}

// 	// read storage version
// 	version := val[0]
// 	val = val[1:]

// 	// Note: previous encodings are incompatible
// 	if version != currentPackFormat {
// 		return 0, fmt.Errorf("knox: invalid v%d storage format", version-0xa0)
// 	}

// 	// Schema may get extended (delete fields, add new fields, change field flags,
// 	// change field compression) over time while stored packs don't change.

// 	// TODO: allow schema evolution
// 	// storedSchemaKey := LE.Uint64(val)
// 	// val = val[8:]
// 	// storedSchemaVersion := int(LE.Uint32(val))
// 	// val = val[4:]

// 	return p.Decode(val[12:])
// }

// func (p *Package) Drop(tx *db.Tx, bucket []byte) error {
// 	return tx.Del(bucket, p.Key())
// }
