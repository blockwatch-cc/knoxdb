// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package schema

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

type Converter struct {
	parent *Schema
	child  *Schema
	maps   []int // parent field to child field mapping (-1 when field not in child)
	offs   []int // child data buffer write offset (-1 when unknown due to variable length)
	layout binary.ByteOrder
}

func NewConverter(parent, child *Schema, layout binary.ByteOrder) *Converter {
	c := &Converter{
		parent: parent,
		child:  child,
		layout: layout,
		offs:   make([]int, child.NumFields()),
	}
	var err error
	c.maps, err = child.MapTo(parent)
	if err != nil {
		panic(fmt.Errorf("mapping schema %s => %s: %v", child.name, parent.name, err))
	}

	// calculate child schema field offsets (required for fixed schemas only)
	if child.isFixedSize {
		var n int
		ok := true
		for i, f := range child.fields {
			c.offs[i] = n
			if !ok || !f.IsFixedSize() {
				ok = false
				n = -1
			} else {
				if f.fixed > 0 {
					n += int(f.fixed)
				} else {
					n += f.typ.Size()
				}
			}
		}
	}

	return c
}

// Extracts child fields from a buffer that contains wire-encoded parent data.
// Optionally transforms bit layout which is useful to generate sortable big-endian
// keys for LSM KV indexes from little-endian wire data.
func (c *Converter) Extract(buf []byte) []byte {
	if len(buf) == 0 || c.parent == nil || c.child == nil {
		return nil
	}

	if c.child.isFixedSize {
		// faster when child schema is fixed length
		return c.extractFixed(buf)
	}

	// slow path with string/bytes data
	return c.extractVariable(buf)
}

func (c *Converter) extractFixed(buf []byte) []byte {
	res := make([]byte, c.child.minWireSize)
	for i := range c.parent.fields {
		typ, fixed := c.parent.fields[i].typ, c.parent.fields[i].fixed
		sz := typ.Size()
		if fixed > 0 {
			sz = int(fixed)
		}

		// determine target field position in output
		pos := c.maps[i]
		if pos < 0 {
			// skip non-selected fields
			buf = buf[sz:]
			continue
		}

		// calculate output buffer offset
		ofs := c.offs[i]

		// copy data to output
		switch typ {
		case FieldTypeDatetime, FieldTypeInt64, FieldTypeUint64,
			FieldTypeFloat64, FieldTypeDecimal64:
			v, n := ReadUint64(buf)
			c.layout.PutUint64(res[ofs:], v)
			buf = buf[n:]

		case FieldTypeInt32, FieldTypeUint32, FieldTypeFloat32,
			FieldTypeDecimal32:
			v, n := ReadUint32(buf)
			c.layout.PutUint32(res[ofs:], v)
			buf = buf[n:]

		case FieldTypeInt16, FieldTypeUint16:
			v, n := ReadUint16(buf)
			c.layout.PutUint16(res[ofs:], v)
			buf = buf[n:]

		case FieldTypeBoolean, FieldTypeInt8, FieldTypeUint8:
			res[ofs] = buf[0]
			buf = buf[1:]

		case FieldTypeInt256, FieldTypeDecimal256:
			// static big-endian encoding
			copy(res[ofs:], buf[:32])
			buf = buf[32:]

		case FieldTypeInt128, FieldTypeDecimal128:
			// static big-endian encoding
			copy(res[ofs:], buf[:16])
			buf = buf[16:]

		case FieldTypeString, FieldTypeBytes:
			// only fixed length string/byte data here
			copy(res[ofs:], buf[:fixed])
			buf = buf[fixed:]
		}
	}
	return res
}

func (c *Converter) extractVariable(buf []byte) []byte {
	res := make([][]byte, len(c.child.fields))
	var cnt int
	for i, field := range c.parent.fields {
		// init from static size
		sz := field.typ.Size()

		// read dynamic size
		switch field.typ {
		case FieldTypeString, FieldTypeBytes:
			u, n := ReadUint32(buf)
			buf = buf[n:] // advance buffer
			sz = int(u)
		}

		pos := c.maps[i]
		if pos < 0 {
			// skip data when not required
			buf = buf[sz:]
			continue
		}

		// reference or convert when field is in child schema
		switch field.typ {
		case FieldTypeDatetime, FieldTypeInt64, FieldTypeUint64,
			FieldTypeFloat64, FieldTypeDecimal64:
			v, _ := ReadUint64(buf)
			var u64 [8]byte
			c.layout.PutUint64(u64[:], v)
			res[pos] = u64[:]

		case FieldTypeInt32, FieldTypeUint32, FieldTypeFloat32,
			FieldTypeDecimal32:
			v, _ := ReadUint32(buf)
			var u32 [4]byte
			c.layout.PutUint32(u32[:], v)
			res[pos] = u32[:]

		case FieldTypeInt16, FieldTypeUint16:
			v, _ := ReadUint16(buf)
			var u16 [2]byte
			c.layout.PutUint16(u16[:], v)
			res[pos] = u16[:]

		case FieldTypeBoolean, FieldTypeInt8, FieldTypeUint8,
			FieldTypeInt256, FieldTypeDecimal256,
			FieldTypeInt128, FieldTypeDecimal128,
			FieldTypeString, FieldTypeBytes:

			// reference buffer using pre-determined size
			res[pos] = buf[:sz]
		}

		cnt++
		if len(res) == cnt {
			break
		}
		buf = buf[sz:]
	}
	return bytes.Join(res, nil)
}
