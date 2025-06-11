// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package csv

import (
	"io"
	"reflect"
	"unsafe"

	"blockwatch.cc/knoxdb/pkg/schema"
	"github.com/echa/log"
)

type GenericDecoder[L, P any] struct {
	dec *Decoder
}

func NewGenericDecoder[L, P any](r io.Reader) *GenericDecoder[L, P] {
	var t L
	return &GenericDecoder[L, P]{
		dec: NewDecoder(schema.MustSchemaOf(t), r),
	}
}

func (d *GenericDecoder[L, P]) Decode() (*P, error) {
	val, err := d.dec.Decode()
	if err != nil {
		return nil, err
	}
	return (*P)(reflect.ValueOf(val).UnsafePointer()), nil
}

func (d *GenericDecoder[L, P]) DecodeSlice(v []*P) (int, error) {
	// check result slice
	if cap(v) == 0 {
		return 0, ErrEmptySlice
	}
	v = v[:cap(v)]

	// reset string pool
	d.dec.pool.Clear()

	// decode
	var n int
	for n < len(v) {
		// clear value
		var p P
		*v[n] = p

		// read line
		line, err := d.dec.r.Read()
		if err != nil && err != io.EOF {
			return n, err
		}

		// stop at EOF
		if line == nil {
			break
		}

		// read and validate header if requested
		if d.dec.flags&DecoderFlagReadHeader > 0 {
			// validate schema fields
			if err := d.dec.validateHeader(line); err != nil {
				return 0, err
			}

			// reset
			d.dec.flags &^= DecoderFlagReadHeader

			// read another line
			continue
		}

		// decode struct fields
		err = d.dec.decode(unsafe.Pointer(v[n]), line)
		if err != nil {
			if d.dec.flags&DecoderFlagStrictSchema > 0 {
				return n, err
			} else if d.dec.r.flags&ReadFlagQuiet == 0 {
				log.Warnf("csv: decode line %d: %v", d.dec.r.lineNo, err)
			}
		} else {
			n++
		}
	}
	return n, nil
}

func (d *GenericDecoder[L, P]) WithTrim(t bool) *GenericDecoder[L, P] {
	d.dec.WithTrim(t)
	return d
}

func (d *GenericDecoder[L, P]) WithStrictQuotes(t bool) *GenericDecoder[L, P] {
	d.dec.WithStrictQuotes(t)
	return d
}

func (d *GenericDecoder[L, P]) WithSeparator(s rune) *GenericDecoder[L, P] {
	d.dec.WithSeparator(s)
	return d
}

func (d *GenericDecoder[L, P]) WithComment(c rune) *GenericDecoder[L, P] {
	d.dec.WithComment(c)
	return d
}

func (d *GenericDecoder[L, P]) WithBuffer(buf []byte) *GenericDecoder[L, P] {
	d.dec.WithBuffer(buf)
	return d
}

func (d *GenericDecoder[L, P]) WithStrictSchema(t bool) *GenericDecoder[L, P] {
	d.dec.WithStrictSchema(t)
	return d
}

func (d *GenericDecoder[L, P]) WithHeader(t bool) *GenericDecoder[L, P] {
	d.dec.WithHeader(t)
	return d
}

func (d *GenericDecoder[L, P]) WithQuiet(t bool) *GenericDecoder[L, P] {
	d.dec.WithQuiet(t)
	return d
}

func (d *GenericDecoder[L, P]) WithTimeFormat(f string) *GenericDecoder[L, P] {
	d.dec.WithTimeFormat(f)
	return d
}

func (d *GenericDecoder[L, P]) WithDateFormat(f string) *GenericDecoder[L, P] {
	d.dec.WithDateFormat(f)
	return d
}

func (d *GenericDecoder[L, P]) Reset(r io.Reader) *GenericDecoder[L, P] {
	d.dec.Reset(r)
	return d
}
