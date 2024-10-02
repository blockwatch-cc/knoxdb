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
// and hash indexes on a defined list/order of columns. Produce legal
// wire format where variable length strings/bytes are length prefixed.
type Converter struct {
	parent  *Schema
	child   *Schema
	maps    []int // parent field to child field mapping (-1 when field not in child)
	offs    []int // child data buffer write offset (-1 when unknown due to variable length)
	nExtra  int   // guess on extra bytes required to encode a variable target records
	layout  binary.ByteOrder
	extract func(*Converter, []byte) []byte
	parts   [][]byte // pre-allocated byte slices for fixed parts in child order
	dyn     []int    // position of dynamic parts
	skipLen bool     // skip encoding variable length for strings/bytes (use in indexes)
}

func NewConverter(parent, child *Schema, layout binary.ByteOrder) *Converter {
	c := &Converter{
		parent:  parent,
		child:   child,
		layout:  layout,
		offs:    make([]int, child.NumFields()),
		extract: extractNoop,
	}
	m, err := child.MapTo(parent)
	if err != nil {
		panic(fmt.Errorf("mapping schema %s => %s: %v", child.name, parent.name, err))
	}
	c.maps = m
	var (
		inOrder = true
		last    int
	)
	for _, v := range c.maps {
		if v < 0 {
			continue
		}
		if last > v {
			inOrder = false
			break
		}
		last = v
	}

	// determine converter algorithm
	switch {
	case child.isFixedSize:
		c.extract = extractFixed
	case inOrder:
		c.extract = extractVariableInorder
	default:
		c.extract = extractVariableReorder
		c.parts = make([][]byte, len(c.child.fields))
		// pre-allocate fixed parts
		for i, field := range c.child.fields {
			switch field.typ {
			case types.FieldTypeDatetime, types.FieldTypeInt64, types.FieldTypeUint64,
				types.FieldTypeFloat64, types.FieldTypeDecimal64:
				c.parts[i] = make([]byte, 8)

			case types.FieldTypeInt32, types.FieldTypeUint32, types.FieldTypeFloat32,
				types.FieldTypeDecimal32:
				c.parts[i] = make([]byte, 4)

			case types.FieldTypeInt16, types.FieldTypeUint16:
				c.parts[i] = make([]byte, 2)

			case types.FieldTypeBoolean, types.FieldTypeInt8, types.FieldTypeUint8:
				c.parts[i] = make([]byte, 1)

			case types.FieldTypeInt256, types.FieldTypeDecimal256:
				c.parts[i] = make([]byte, 32)

			case types.FieldTypeInt128, types.FieldTypeDecimal128:
				c.parts[i] = make([]byte, 16)

			case types.FieldTypeString, types.FieldTypeBytes:
				if field.fixed > 0 {
					c.parts[i] = make([]byte, field.fixed)
				} else {
					c.dyn = append(c.dyn, i)
				}
			}
		}
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
	} else {
		// determine extra variable bytes required based on number of
		// variable length fields
		for _, f := range child.fields {
			if f.IsFixedSize() {
				continue
			}
			c.nExtra += defaultVarFieldSize
		}
	}

	return c
}

func (c *Converter) WithSkipLen() *Converter {
	c.skipLen = true
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

func extractVariableInorder(c *Converter, buf []byte) []byte {
	if buf == nil {
		return nil
	}
	maxSz := c.child.minWireSize + c.nExtra
	res := bytes.NewBuffer(make([]byte, 0, maxSz))
	var (
		cnt int
		b   [8]byte
	)
	for i, field := range c.parent.fields {
		// init from static size
		sz := field.typ.Size()

		// read dynamic size
		switch field.typ {
		case types.FieldTypeString, types.FieldTypeBytes:
			if field.fixed > 0 {
				sz = int(field.fixed)
			} else {
				u, n := ReadUint32(buf)
				if c.skipLen {
					sz = int(u)
					buf = buf[n:]
				} else {
					sz = int(u) + n
				}
			}
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
			c.layout.PutUint64(b[:], v)
			res.Write(b[:])

		case types.FieldTypeInt32, types.FieldTypeUint32, types.FieldTypeFloat32,
			types.FieldTypeDecimal32:
			v, _ := ReadUint32(buf)
			c.layout.PutUint32(b[:], v)
			res.Write(b[:4])

		case types.FieldTypeInt16, types.FieldTypeUint16:
			v, _ := ReadUint16(buf)
			c.layout.PutUint16(b[:], v)
			res.Write(b[:2])

		case types.FieldTypeBoolean, types.FieldTypeInt8, types.FieldTypeUint8,
			types.FieldTypeInt256, types.FieldTypeDecimal256,
			types.FieldTypeInt128, types.FieldTypeDecimal128,
			types.FieldTypeString, types.FieldTypeBytes:

			// reference buffer using pre-determined size
			res.Write(buf[:sz])
		}

		cnt++
		if len(c.offs) == cnt {
			break
		}
		buf = buf[sz:]
	}

	// update our estimate on extra bytes required and keep the max
	if res.Len() > maxSz {
		c.nExtra = res.Len() - c.child.minWireSize
	}

	return res.Bytes()
}

func extractVariableReorder(c *Converter, buf []byte) []byte {
	if buf == nil {
		return nil
	}
	var cnt int
	for i, field := range c.parent.fields {
		// init from static size
		sz := field.typ.Size()

		// read dynamic size
		switch field.typ {
		case types.FieldTypeString, types.FieldTypeBytes:
			if field.fixed > 0 {
				sz = int(field.fixed)
			} else {
				u, n := ReadUint32(buf)
				if c.skipLen {
					sz = int(u)
					buf = buf[n:]
				} else {
					sz = int(u) + n
				}
			}
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
			c.layout.PutUint64(c.parts[pos], v)

		case types.FieldTypeInt32, types.FieldTypeUint32, types.FieldTypeFloat32,
			types.FieldTypeDecimal32:
			v, _ := ReadUint32(buf)
			c.layout.PutUint32(c.parts[pos], v)

		case types.FieldTypeInt16, types.FieldTypeUint16:
			v, _ := ReadUint16(buf)
			c.layout.PutUint16(c.parts[pos], v)

		case types.FieldTypeBoolean, types.FieldTypeInt8, types.FieldTypeUint8,
			types.FieldTypeInt256, types.FieldTypeDecimal256,
			types.FieldTypeInt128, types.FieldTypeDecimal128:
			copy(c.parts[pos], buf[:sz])

		case types.FieldTypeString, types.FieldTypeBytes:
			if field.fixed > 0 {
				copy(c.parts[pos], buf[:sz])
			} else {
				// reference buffer using pre-determined size
				c.parts[pos] = buf[:sz]
			}
		}

		cnt++
		if len(c.parts) == cnt {
			break
		}
		buf = buf[sz:]
	}

	res := bytes.Join(c.parts, nil)

	// dereference dynamic fields (so GC does not keep buf around)
	for _, i := range c.dyn {
		c.parts[i] = nil
	}

	return res
}
