// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package csv

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"io"
	"reflect"
	"strconv"
	"strings"
	"time"
	"unsafe"

	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/num"
	"blockwatch.cc/knoxdb/pkg/schema"
	"blockwatch.cc/knoxdb/pkg/stringx"
	"blockwatch.cc/knoxdb/pkg/util"
	"github.com/echa/log"
)

// A Decoder reads and decodes records from a CSV stream using an interal Reader.
// A schema with column names and types must be known when creating a decoder. The
// schema may be auto-detected using a Sniffer or any other outside means. If a
// Go struct implementing the schema exists users can call
//
//	s, err := SchemaOf(StructType{})
type Decoder struct {
	r      *Reader
	s      *schema.Schema
	flags  DecoderFlags
	typ    reflect.Type
	pool   *stringx.StringPool
	ofs    []uintptr // offsets in dynamic native struct
	dateAs string    // date format override (optional)
	timeAs string    // timestamp format override (optional)
	buf    []byte    // user provided scan buffer
}

type DecoderFlags byte

const (
	DecoderFlagStrictSchema DecoderFlags = 1 << iota
	DecoderFlagReadHeader                // read and check header fields against schema
)

func NewDecoder(s *schema.Schema, r io.Reader) *Decoder {
	typ := s.NativeStructType()
	var nStringFields int
	for _, f := range s.Exported() {
		switch f.Type {
		case types.FieldTypeString, types.FieldTypeBytes:
			nStringFields++
		}
	}
	return &Decoder{
		r:      NewReader(r, s.NumVisibleFields()),
		s:      s,
		flags:  DecoderFlagStrictSchema | DecoderFlagReadHeader,
		typ:    typ,
		pool:   stringx.NewStringPool(nStringFields * 1024),
		ofs:    structFieldOffsets(typ),
		dateAs: time.DateOnly,    // 2006-01-02
		timeAs: time.RFC3339Nano, // 2006-01-02T15:04:05.999999999Z07:00
	}
}

func structFieldOffsets(typ reflect.Type) []uintptr {
	ofs := make([]uintptr, 0)
	for _, f := range reflect.VisibleFields(typ) {
		if !f.IsExported() || f.Anonymous {
			continue
		}
		ofs = append(ofs, f.Offset)
	}
	return ofs
}

func (d *Decoder) WithTrim(t bool) *Decoder {
	d.r.WithTrim(t)
	return d
}

// Return an error when encountering unclosed quotes or mixed quoted and
// unquoted text. When disabled, text fields will be eagerly parsed.
func (d *Decoder) WithStrictQuotes(t bool) *Decoder {
	d.r.WithStrictQuotes(t)
	return d
}

func (d *Decoder) WithSeparator(s rune) *Decoder {
	d.r.WithSeparator(s)
	return d
}

func (d *Decoder) WithComment(c rune) *Decoder {
	d.r.WithComment(c)
	return d
}

func (d *Decoder) WithBuffer(buf []byte) *Decoder {
	d.buf = buf
	d.r.WithBuffer(buf)
	return d
}

// Return error when encountering a record that cannot be mapped to schema, e.g.
// because it contains more or less fields than defined in schema or because
// type based decoding failed. When disabled, such records will be be ignored
// with a warning only.
func (d *Decoder) WithStrictSchema(t bool) *Decoder {
	if t {
		d.flags |= DecoderFlagStrictSchema
	} else {
		d.flags &^= DecoderFlagStrictSchema
	}
	return d
}

func (d *Decoder) WithHeader(t bool) *Decoder {
	if t {
		d.flags |= DecoderFlagReadHeader
	} else {
		d.flags &^= DecoderFlagReadHeader
	}
	return d
}

func (d *Decoder) WithQuiet(t bool) *Decoder {
	d.r.WithQuiet(t)
	return d
}

func (d *Decoder) WithTimeFormat(f string) *Decoder {
	d.timeAs = f
	return d
}

func (d *Decoder) WithDateFormat(f string) *Decoder {
	d.dateAs = f
	return d
}

// Reset sets a new input reader and leaves the decoder
// configuration untouched. It is useful for reading
// many files or reading the same file after seek.
func (d *Decoder) Reset(r io.Reader) *Decoder {
	d.r.Reset(r)
	if d.buf != nil {
		d.r.WithBuffer(d.buf)
	}
	return d
}

// Allocates a slice of interfaces to structs which can be used to
// decode into. Use this in combination with DecodeSlice to pre-allocate
// and reuse memory when decoding large quantities of data.
func (d *Decoder) MakeSlice(sz int) []any {
	// create elements
	res := make([]any, sz)
	for i := range sz {
		res[i] = reflect.New(d.typ).Interface()
	}
	return res
}

// Decodes the next line into a struct record defined my schema v. v must be struct or
// pointer to struct and match schema. If schema is not defined,
func (d *Decoder) Decode() (any, error) {
	// read line
	line, err := d.r.Read()
	if err != nil {
		return nil, err
	}

	// read and validate header if requested
	if d.flags&DecoderFlagReadHeader > 0 {
		// validate schema fields
		if err := d.validateHeader(line); err != nil {
			return nil, err
		}

		// reset
		d.flags &^= DecoderFlagReadHeader

		// read another line
		line, err = d.r.Read()
		if err != nil {
			return nil, err
		}
	}

	// create new struct
	rval := reflect.New(d.typ)

	// reset string pool
	d.pool.Clear()

	// decode struct fields
	err = d.decode(rval.UnsafePointer(), line)
	if err != nil {
		return nil, err
	}

	return rval.Interface(), nil
}

// Decodes multiple records up until slice capacity and returns
// number of records decoded. Reuses slice elements and zeros them
// before decode so that null values are correct.
func (d *Decoder) DecodeSlice(v []any) (int, error) {
	// check result slice
	if cap(v) == 0 {
		return 0, ErrEmptySlice
	}
	v = v[:cap(v)]

	// reset string pool
	d.pool.Clear()

	// decode
	var n int
	for n < len(v) {
		// clear value
		rval := reflect.ValueOf(v[n]).Elem()
		rval.Set(reflect.Zero(d.typ))

		// read line
		line, err := d.r.Read()
		if err != nil && err != io.EOF {
			return n, err
		}

		// stop at EOF
		if line == nil {
			break
		}

		// read and validate header if requested
		if d.flags&DecoderFlagReadHeader > 0 {
			// validate schema fields
			if err := d.validateHeader(line); err != nil {
				return 0, err
			}

			// reset
			d.flags &^= DecoderFlagReadHeader

			// read another line
			continue
		}

		// decode struct fields
		err = d.decode(rval.Addr().UnsafePointer(), line)
		if err != nil {
			if d.flags&DecoderFlagStrictSchema > 0 {
				return n, err
			} else if d.r.flags&ReadFlagQuiet == 0 {
				log.Warnf("csv: decode line %d: %v", d.r.lineNo, err)
			}
		} else {
			n++
		}
	}
	return n, nil
}

func (d *Decoder) validateHeader(line []string) error {
	if d.flags&DecoderFlagStrictSchema == 0 {
		return nil
	}
	if len(line) != d.s.NumVisibleFields() {
		return schema.ErrSchemaMismatch
	}
	var i int
	for _, f := range d.s.Exported() {
		if !f.IsVisible {
			continue
		}
		if SanitizeFieldName(line[i], i) != f.Name {
			return fmt.Errorf("csv: mismatched field[%d] header name %q, expected %q",
				i+1, line[i], f.Name)
		}
		i++
	}
	return nil
}

func (d *Decoder) decode(base unsafe.Pointer, line []string) error {
	var i int
	for _, f := range d.s.Exported() {
		if !f.IsVisible {
			continue
		}
		if len(line[i]) == 0 || line[i] == NULL {
			i++
			continue
		}
		ptr := unsafe.Add(base, d.ofs[i])
		switch f.Type {
		case types.FieldTypeTimestamp:
			if d.timeAs == "" {
				tm, err := schema.TimeScale(f.Scale).Parse(line[i], false)
				if err != nil {
					return &DecodeError{d.r.lineNo, i, f.Name, err}
				}
				*(*int64)(ptr) = tm
			} else {
				tm, err := time.Parse(d.timeAs, line[i])
				if err != nil {
					return &DecodeError{d.r.lineNo, i, f.Name, err}
				}
				*(*int64)(ptr) = schema.TimeScale(f.Scale).ToUnix(tm)
			}
		case types.FieldTypeDate:
			if d.dateAs == "" {
				tm, err := schema.TimeScale(f.Scale).Parse(line[i], false)
				if err != nil {
					return &DecodeError{d.r.lineNo, i, f.Name, err}
				}
				*(*int64)(ptr) = tm
			} else {
				tm, err := time.Parse(d.dateAs, line[i])
				if err != nil {
					return &DecodeError{d.r.lineNo, i, f.Name, err}
				}
				*(*int64)(ptr) = schema.TimeScale(f.Scale).ToUnix(tm)
			}
		case types.FieldTypeTime:
			if d.timeAs == "" {
				tm, err := schema.TimeScale(f.Scale).Parse(line[i], true)
				if err != nil {
					return &DecodeError{d.r.lineNo, i, f.Name, err}
				}
				*(*int64)(ptr) = tm
			} else {
				tm, err := time.Parse(d.timeAs, line[i])
				if err != nil {
					return &DecodeError{d.r.lineNo, i, f.Name, err}
				}
				*(*int64)(ptr) = schema.TimeScale(f.Scale).ToUnix(tm)
			}
		case types.FieldTypeInt64:
			val, err := strconv.ParseInt(line[i], 10, 64)
			if err != nil {
				return &DecodeError{d.r.lineNo, i, f.Name, err}
			}
			*(*int64)(ptr) = val
		case types.FieldTypeInt32:
			val, err := strconv.ParseInt(line[i], 10, 32)
			if err != nil {
				return &DecodeError{d.r.lineNo, i, f.Name, err}
			}
			*(*int32)(ptr) = int32(val)

		case types.FieldTypeInt16:
			val, err := strconv.ParseInt(line[i], 10, 16)
			if err != nil {
				return &DecodeError{d.r.lineNo, i, f.Name, err}
			}
			*(*int16)(ptr) = int16(val)

		case types.FieldTypeInt8:
			val, err := strconv.ParseInt(line[i], 10, 8)
			if err != nil {
				return &DecodeError{d.r.lineNo, i, f.Name, err}
			}
			*(*int8)(ptr) = int8(val)

		case types.FieldTypeUint64:
			val, err := strconv.ParseUint(line[i], 10, 64)
			if err != nil {
				return &DecodeError{d.r.lineNo, i, f.Name, err}
			}
			*(*uint64)(ptr) = val

		case types.FieldTypeUint32:
			val, err := strconv.ParseUint(line[i], 10, 32)
			if err != nil {
				return &DecodeError{d.r.lineNo, i, f.Name, err}
			}
			*(*uint32)(ptr) = uint32(val)

		case types.FieldTypeUint16:
			val, err := strconv.ParseUint(line[i], 10, 16)
			if err != nil {
				return &DecodeError{d.r.lineNo, i, f.Name, err}
			}
			*(*uint16)(ptr) = uint16(val)

		case types.FieldTypeUint8:
			val, err := strconv.ParseUint(line[i], 10, 8)
			if err != nil {
				return &DecodeError{d.r.lineNo, i, f.Name, err}
			}
			*(*uint8)(ptr) = uint8(val)

		case types.FieldTypeFloat64:
			val, err := strconv.ParseFloat(line[i], 64)
			if err != nil {
				return &DecodeError{d.r.lineNo, i, f.Name, err}
			}
			*(*float64)(ptr) = val

		case types.FieldTypeFloat32:
			val, err := strconv.ParseFloat(line[i], 32)
			if err != nil {
				return &DecodeError{d.r.lineNo, i, f.Name, err}
			}
			*(*float32)(ptr) = float32(val)

		case types.FieldTypeBoolean:
			val, err := strconv.ParseBool(line[i])
			if err != nil {
				return &DecodeError{d.r.lineNo, i, f.Name, err}
			}
			*(*bool)(ptr) = val

		case types.FieldTypeString:
			// use string pool to avoid string allocs
			n := d.pool.Len()
			d.pool.AppendString(line[i])
			*(*string)(ptr) = d.pool.GetString(n)

		case types.FieldTypeBytes:
			// decode hex to binary
			s := strings.TrimPrefix(line[i], "0x")
			if f.Fixed > 0 {
				if len(s) != int(f.Fixed)*2 {
					return &DecodeError{d.r.lineNo, i, f.Name,
						fmt.Errorf("binary array [%d]byte mismatched hex len %d", f.Fixed, len(s))}
				}
				_, err := hex.Decode(unsafe.Slice((*byte)(ptr), f.Fixed), util.UnsafeGetBytes(s))
				if err != nil {
					return &DecodeError{d.r.lineNo, i, f.Name, err}
				}
			} else {
				res, err := hex.DecodeString(s)
				if err != nil {
					return &DecodeError{d.r.lineNo, i, f.Name, err}
				}
				*(*[]byte)(ptr) = res
			}

		case types.FieldTypeInt256:
			i256, err := num.ParseInt256(line[i])
			if err != nil {
				return &DecodeError{d.r.lineNo, i, f.Name, err}
			}
			*(*[32]byte)(ptr) = i256.Bytes32()

		case types.FieldTypeInt128:
			i128, err := num.ParseInt128(line[i])
			if err != nil {
				return &DecodeError{d.r.lineNo, i, f.Name, err}
			}
			*(*[16]byte)(ptr) = i128.Bytes16()

		case types.FieldTypeDecimal256:
			d256, err := num.ParseDecimal256(line[i])
			if err != nil {
				return &DecodeError{d.r.lineNo, i, f.Name, err}
			}
			*(*[32]byte)(ptr) = d256.Quantize(f.Scale).Int256().Bytes32()

		case types.FieldTypeDecimal128:
			d128, err := num.ParseDecimal128(line[i])
			if err != nil {
				return &DecodeError{d.r.lineNo, i, f.Name, err}
			}
			*(*[16]byte)(ptr) = d128.Quantize(f.Scale).Int128().Bytes16()

		case types.FieldTypeDecimal64:
			d64, err := num.ParseDecimal64(line[i])
			if err != nil {
				return &DecodeError{d.r.lineNo, i, f.Name, err}
			}
			*(*int64)(ptr) = d64.Quantize(f.Scale).Int64()

		case types.FieldTypeDecimal32:
			d32, err := num.ParseDecimal32(line[i])
			if err != nil {
				return &DecodeError{d.r.lineNo, i, f.Name, err}
			}
			*(*int32)(ptr) = d32.Quantize(f.Scale).Int32()

		case types.FieldTypeBigint:
			big, err := num.ParseBig(line[i])
			if err != nil {
				return &DecodeError{d.r.lineNo, i, f.Name, err}
			}
			*(*[]byte)(ptr) = bytes.Clone(big.Bytes()) // copy

		default:
			return &DecodeError{d.r.lineNo, i, f.Name, schema.ErrInvalidValueType}
		}
		i++
	}
	return nil
}
