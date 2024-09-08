// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package schema

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"blockwatch.cc/knoxdb/internal/types"
)

// Extracts wire encoded messages into raw byte strings. Useful for search
// and hash indexes on a defined list/order of columns. Does not produce legal
// wire format as strings/bytes are encoded without length.
type Converter struct {
	parent  *Schema
	child   *Schema
	maps    []int // parent field to child field mapping (-1 when field not in child)
	offs    []int // child data buffer write offset (-1 when unknown due to variable length)
	layout  binary.ByteOrder
	extract func(*Converter, []byte) []byte
}

func NewConverter(parent, child *Schema, layout binary.ByteOrder) *Converter {
	c := &Converter{
		parent:  parent,
		child:   child,
		layout:  layout,
		offs:    make([]int, child.NumFields()),
		extract: extractNoop,
	}
	if parent != nil && child != nil {
		if child.isFixedSize {
			c.extract = extractFixed
		} else {
			c.extract = extractVariable
		}
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

func (c *Converter) Schema() *Schema {
	return c.child
}

// Extracts child fields from a buffer that contains wire-encoded parent data.
// Optionally transforms bit layout which is useful to generate sortable big-endian
// keys for LSM KV indexes from little-endian wire data.
func (c *Converter) Extract(buf []byte) []byte {
	return c.extract(c, buf)
}

func extractNoop(_ *Converter, _ []byte) []byte {
	return nil
}

func extractFixed(c *Converter, buf []byte) []byte {
	if buf == nil {
		return nil
	}
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
		case types.FieldTypeDatetime, types.FieldTypeInt64, types.FieldTypeUint64,
			types.FieldTypeFloat64, types.FieldTypeDecimal64:
			v, n := ReadUint64(buf)
			c.layout.PutUint64(res[ofs:], v)
			buf = buf[n:]

		case types.FieldTypeInt32, types.FieldTypeUint32, types.FieldTypeFloat32,
			types.FieldTypeDecimal32:
			v, n := ReadUint32(buf)
			c.layout.PutUint32(res[ofs:], v)
			buf = buf[n:]

		case types.FieldTypeInt16, types.FieldTypeUint16:
			v, n := ReadUint16(buf)
			c.layout.PutUint16(res[ofs:], v)
			buf = buf[n:]

		case types.FieldTypeBoolean, types.FieldTypeInt8, types.FieldTypeUint8:
			res[ofs] = buf[0]
			buf = buf[1:]

		case types.FieldTypeInt256, types.FieldTypeDecimal256:
			// static big-endian encoding
			copy(res[ofs:], buf[:32])
			buf = buf[32:]

		case types.FieldTypeInt128, types.FieldTypeDecimal128:
			// static big-endian encoding
			copy(res[ofs:], buf[:16])
			buf = buf[16:]

		case types.FieldTypeString, types.FieldTypeBytes:
			// only fixed length string/byte data here
			copy(res[ofs:], buf[:fixed])
			buf = buf[fixed:]
		}
	}
	return res
}

func extractVariable(c *Converter, buf []byte) []byte {
	if buf == nil {
		return nil
	}
	res := make([][]byte, len(c.child.fields))
	var cnt int
	for i, field := range c.parent.fields {
		// init from static size
		sz := field.typ.Size()

		// read dynamic size
		switch field.typ {
		case types.FieldTypeString, types.FieldTypeBytes:
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
		case types.FieldTypeDatetime, types.FieldTypeInt64, types.FieldTypeUint64,
			types.FieldTypeFloat64, types.FieldTypeDecimal64:
			v, _ := ReadUint64(buf)
			var u64 [8]byte
			c.layout.PutUint64(u64[:], v)
			res[pos] = u64[:]

		case types.FieldTypeInt32, types.FieldTypeUint32, types.FieldTypeFloat32,
			types.FieldTypeDecimal32:
			v, _ := ReadUint32(buf)
			var u32 [4]byte
			c.layout.PutUint32(u32[:], v)
			res[pos] = u32[:]

		case types.FieldTypeInt16, types.FieldTypeUint16:
			v, _ := ReadUint16(buf)
			var u16 [2]byte
			c.layout.PutUint16(u16[:], v)
			res[pos] = u16[:]

		case types.FieldTypeBoolean, types.FieldTypeInt8, types.FieldTypeUint8,
			types.FieldTypeInt256, types.FieldTypeDecimal256,
			types.FieldTypeInt128, types.FieldTypeDecimal128,
			types.FieldTypeString, types.FieldTypeBytes:

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
