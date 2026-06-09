// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package csv

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strconv"
	"strings"
	"time"
	"unsafe"

	"blockwatch.cc/knoxdb/internal/pack"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/num"
	"blockwatch.cc/knoxdb/pkg/schema"
	sreflect "blockwatch.cc/knoxdb/pkg/schema/reflect"
	"blockwatch.cc/knoxdb/pkg/stringx"
	"blockwatch.cc/knoxdb/pkg/util"
	"github.com/echa/log"
)

var ErrInvalidValueType = errors.New("invalid value type")

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
	decode func(unsafe.Pointer, []string) error
}

type DecoderFlags byte

const (
	DecoderFlagStrictSchema DecoderFlags = 1 << iota
	DecoderFlagReadHeader                // read and check header fields against schema
	DecoderFlagLogicalType               // output logical data types (num.Decimal, time.Time)
)

func NewDecoder(s *schema.Schema, r io.Reader) *Decoder {
	d := &Decoder{
		r:      NewReader(r, s.NumVisible()),
		s:      s,
		flags:  DecoderFlagStrictSchema | DecoderFlagReadHeader,
		dateAs: time.DateOnly,    // 2006-01-02
		timeAs: time.RFC3339Nano, // 2006-01-02T15:04:05.999999999Z07:00
	}
	d.initType()
	return d
}

func (d *Decoder) initType() {
	if d.typ != nil {
		return
	}
	if d.flags&DecoderFlagLogicalType > 0 {
		d.typ = sreflect.StructTypeOf(d.s)
		d.decode = d.decodeLogical
	} else {
		d.typ = sreflect.NativeStructType(d.s)
		d.decode = d.decodePhysical
	}
	var nStringFields int
	for _, f := range d.s.Fields {
		switch f.Type {
		case schema.String, schema.Bytes:
			nStringFields++
		}
	}
	d.pool = stringx.NewStringPool(nStringFields * 1024)
	d.ofs = structFieldOffsets(d.typ)
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

func (d *Decoder) WithLogicalType(t bool) *Decoder {
	if t {
		d.flags |= DecoderFlagLogicalType
	} else {
		d.flags &^= DecoderFlagLogicalType
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

// Decodes multiple records up until pack capacity into pack format.
// Pack and decoder schema must match.
func (d *Decoder) DecodePack(pkg *pack.Package) (int, error) {
	// check pack schema
	if pkg.Schema().Hash != d.s.Hash {
		return 0, schema.ErrSchemaMismatch
	}
	pkg.Clear()
	defer pkg.UpdateLen()

	// decode
	var (
		n int
		c = pkg.FreeSpace()
	)
	for n < c {
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

		// decode fields and append to pack blocks
		err = d.decodePack(pkg, line)
		if err != nil {
			return n, err
		}
		n++
	}
	return n, nil
}

func (d *Decoder) validateHeader(line []string) error {
	if d.flags&DecoderFlagStrictSchema == 0 {
		return nil
	}
	if len(line) != d.s.NumVisible() {
		return schema.ErrSchemaMismatch
	}
	var i int
	for _, f := range d.s.Fields {
		if !f.IsVisible() {
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

func (d *Decoder) decodePhysical(base unsafe.Pointer, line []string) error {
	var i int
	for _, f := range d.s.Fields {
		if !f.IsVisible() {
			continue
		}
		if len(line[i]) == 0 || line[i] == NULL {
			i++
			continue
		}
		ptr := unsafe.Add(base, d.ofs[i])
		switch f.Type {
		case schema.Timestamp:
			if d.timeAs == "" {
				tm, err := schema.TimeScale(f.Scale).Parse(line[i], false)
				if err != nil {
					return &DecodeError{d.r.lineNo, i, f.Name, line[i], err}
				}
				*(*int64)(ptr) = tm
			} else {
				tm, err := time.Parse(d.timeAs, line[i])
				if err != nil {
					return &DecodeError{d.r.lineNo, i, f.Name, line[i], err}
				}
				*(*int64)(ptr) = schema.TimeScale(f.Scale).ToUnix(tm)
			}

		case schema.Duration:
			dur, err := schema.TimeScale(f.Scale).ParseDuration(line[i])
			if err != nil {
				return &DecodeError{d.r.lineNo, i, f.Name, line[i], err}
			}
			*(*int64)(ptr) = schema.TimeScale(f.Scale).Int64(dur)

		case schema.Date:
			if d.dateAs == "" {
				tm, err := schema.TimeScale(f.Scale).Parse(line[i], false)
				if err != nil {
					return &DecodeError{d.r.lineNo, i, f.Name, line[i], err}
				}
				*(*int64)(ptr) = tm
			} else {
				tm, err := time.Parse(d.dateAs, line[i])
				if err != nil {
					return &DecodeError{d.r.lineNo, i, f.Name, line[i], err}
				}
				*(*int64)(ptr) = schema.TimeScale(f.Scale).ToUnix(tm)
			}

		case schema.Time:
			if d.timeAs == "" {
				tm, err := schema.TimeScale(f.Scale).Parse(line[i], true)
				if err != nil {
					return &DecodeError{d.r.lineNo, i, f.Name, line[i], err}
				}
				*(*int64)(ptr) = tm
			} else {
				tm, err := time.Parse(d.timeAs, line[i])
				if err != nil {
					return &DecodeError{d.r.lineNo, i, f.Name, line[i], err}
				}
				*(*int64)(ptr) = schema.TimeScale(f.Scale).ToUnix(tm)
			}

		case schema.Int64:
			val, err := strconv.ParseInt(line[i], 10, 64)
			if err != nil {
				return &DecodeError{d.r.lineNo, i, f.Name, line[i], err}
			}
			*(*int64)(ptr) = val

		case schema.Int32:
			val, err := strconv.ParseInt(line[i], 10, 32)
			if err != nil {
				return &DecodeError{d.r.lineNo, i, f.Name, line[i], err}
			}
			*(*int32)(ptr) = int32(val)

		case schema.Int16:
			val, err := strconv.ParseInt(line[i], 10, 16)
			if err != nil {
				return &DecodeError{d.r.lineNo, i, f.Name, line[i], err}
			}
			*(*int16)(ptr) = int16(val)

		case schema.Int8:
			val, err := strconv.ParseInt(line[i], 10, 8)
			if err != nil {
				return &DecodeError{d.r.lineNo, i, f.Name, line[i], err}
			}
			*(*int8)(ptr) = int8(val)

		case schema.Uint64:
			val, err := strconv.ParseUint(line[i], 10, 64)
			if err != nil {
				return &DecodeError{d.r.lineNo, i, f.Name, line[i], err}
			}
			*(*uint64)(ptr) = val

		case schema.Uint32:
			val, err := strconv.ParseUint(line[i], 10, 32)
			if err != nil {
				return &DecodeError{d.r.lineNo, i, f.Name, line[i], err}
			}
			*(*uint32)(ptr) = uint32(val)

		case schema.Uint16:
			val, err := strconv.ParseUint(line[i], 10, 16)
			if err != nil {
				return &DecodeError{d.r.lineNo, i, f.Name, line[i], err}
			}
			*(*uint16)(ptr) = uint16(val)

		case schema.Uint8:
			val, err := strconv.ParseUint(line[i], 10, 8)
			if err != nil {
				return &DecodeError{d.r.lineNo, i, f.Name, line[i], err}
			}
			*(*uint8)(ptr) = uint8(val)

		case schema.Float64:
			val, err := strconv.ParseFloat(line[i], 64)
			if err != nil {
				return &DecodeError{d.r.lineNo, i, f.Name, line[i], err}
			}
			*(*float64)(ptr) = val

		case schema.Float32:
			val, err := strconv.ParseFloat(line[i], 32)
			if err != nil {
				return &DecodeError{d.r.lineNo, i, f.Name, line[i], err}
			}
			*(*float32)(ptr) = float32(val)

		case schema.Boolean:
			val, err := strconv.ParseBool(line[i])
			if err != nil {
				return &DecodeError{d.r.lineNo, i, f.Name, line[i], err}
			}
			*(*bool)(ptr) = val

		case schema.String, schema.Text:
			// use string pool to avoid string allocs
			n := d.pool.Len()
			d.pool.AppendString(line[i])
			*(*string)(ptr) = d.pool.GetString(n)

		case schema.Bytes, schema.Binary:
			// decode hex to binary
			s := strings.TrimPrefix(line[i], "0x")
			if f.IsArray() {
				if len(s) != int(f.Scale)*2 {
					return &DecodeError{d.r.lineNo, i, f.Name, line[i],
						fmt.Errorf("binary array [%d]byte mismatched hex len %d", f.Scale, len(s))}
				}
				_, err := hex.Decode(unsafe.Slice((*byte)(ptr), f.Scale), util.UnsafeGetBytes(s))
				if err != nil {
					return &DecodeError{d.r.lineNo, i, f.Name, line[i], err}
				}
			} else {
				res, err := hex.DecodeString(s)
				if err != nil {
					return &DecodeError{d.r.lineNo, i, f.Name, line[i], err}
				}
				*(*[]byte)(ptr) = res
			}

		case schema.Int256:
			i256, err := num.ParseInt256(line[i])
			if err != nil {
				return &DecodeError{d.r.lineNo, i, f.Name, line[i], err}
			}
			*(*[32]byte)(ptr) = i256.Bytes32()

		case schema.Int128:
			i128, err := num.ParseInt128(line[i])
			if err != nil {
				return &DecodeError{d.r.lineNo, i, f.Name, line[i], err}
			}
			*(*[16]byte)(ptr) = i128.Bytes16()

		case schema.Decimal256:
			d256, err := num.ParseDecimal256(line[i])
			if err != nil {
				return &DecodeError{d.r.lineNo, i, f.Name, line[i], err}
			}
			*(*[32]byte)(ptr) = d256.Quantize(f.Scale).Int256().Bytes32()

		case schema.Decimal128:
			d128, err := num.ParseDecimal128(line[i])
			if err != nil {
				return &DecodeError{d.r.lineNo, i, f.Name, line[i], err}
			}
			*(*[16]byte)(ptr) = d128.Quantize(f.Scale).Int128().Bytes16()

		case schema.Decimal64:
			d64, err := num.ParseDecimal64(line[i])
			if err != nil {
				return &DecodeError{d.r.lineNo, i, f.Name, line[i], err}
			}
			*(*int64)(ptr) = d64.Quantize(f.Scale).Int64()

		case schema.Decimal32:
			d32, err := num.ParseDecimal32(line[i])
			if err != nil {
				return &DecodeError{d.r.lineNo, i, f.Name, line[i], err}
			}
			*(*int32)(ptr) = d32.Quantize(f.Scale).Int32()

		case schema.Bigint:
			big, err := num.ParseBig(line[i])
			if err != nil {
				return &DecodeError{d.r.lineNo, i, f.Name, line[i], err}
			}
			*(*[]byte)(ptr) = bytes.Clone(big.Bytes()) // copy

		default:
			return &DecodeError{d.r.lineNo, i, f.Name, line[i], ErrInvalidValueType}
		}
		i++
	}
	return nil
}

func (d *Decoder) decodeLogical(base unsafe.Pointer, line []string) error {
	var i int
	for _, f := range d.s.Fields {
		if !f.IsVisible() {
			continue
		}
		if len(line[i]) == 0 || line[i] == NULL {
			i++
			continue
		}
		ptr := unsafe.Add(base, d.ofs[i])
		switch f.Type {
		case schema.Timestamp:
			if d.timeAs == "" {
				tm, err := schema.TimeScale(f.Scale).ParseTime(line[i], false)
				if err != nil {
					return &DecodeError{d.r.lineNo, i, f.Name, line[i], err}
				}
				*(*time.Time)(ptr) = tm
			} else {
				tm, err := time.Parse(d.timeAs, line[i])
				if err != nil {
					return &DecodeError{d.r.lineNo, i, f.Name, line[i], err}
				}
				*(*time.Time)(ptr) = tm
			}

		case schema.Duration:
			dur, err := schema.TimeScale(f.Scale).ParseDuration(line[i])
			if err != nil {
				return &DecodeError{d.r.lineNo, i, f.Name, line[i], err}
			}
			*(*time.Duration)(ptr) = dur

		case schema.Date:
			if d.dateAs == "" {
				tm, err := schema.TimeScale(f.Scale).ParseTime(line[i], false)
				if err != nil {
					return &DecodeError{d.r.lineNo, i, f.Name, line[i], err}
				}
				*(*time.Time)(ptr) = tm
			} else {
				tm, err := time.Parse(d.dateAs, line[i])
				if err != nil {
					return &DecodeError{d.r.lineNo, i, f.Name, line[i], err}
				}
				*(*time.Time)(ptr) = tm
			}

		case schema.Time:
			if d.timeAs == "" {
				tm, err := schema.TimeScale(f.Scale).ParseTime(line[i], true)
				if err != nil {
					return &DecodeError{d.r.lineNo, i, f.Name, line[i], err}
				}
				*(*time.Time)(ptr) = tm
			} else {
				tm, err := time.Parse(d.timeAs, line[i])
				if err != nil {
					return &DecodeError{d.r.lineNo, i, f.Name, line[i], err}
				}
				*(*time.Time)(ptr) = tm
			}

		case schema.Int64:
			val, err := strconv.ParseInt(line[i], 10, 64)
			if err != nil {
				return &DecodeError{d.r.lineNo, i, f.Name, line[i], err}
			}
			*(*int64)(ptr) = val

		case schema.Int32:
			val, err := strconv.ParseInt(line[i], 10, 32)
			if err != nil {
				return &DecodeError{d.r.lineNo, i, f.Name, line[i], err}
			}
			*(*int32)(ptr) = int32(val)

		case schema.Int16:
			val, err := strconv.ParseInt(line[i], 10, 16)
			if err != nil {
				return &DecodeError{d.r.lineNo, i, f.Name, line[i], err}
			}
			*(*int16)(ptr) = int16(val)

		case schema.Int8:
			val, err := strconv.ParseInt(line[i], 10, 8)
			if err != nil {
				return &DecodeError{d.r.lineNo, i, f.Name, line[i], err}
			}
			*(*int8)(ptr) = int8(val)

		case schema.Uint64:
			val, err := strconv.ParseUint(line[i], 10, 64)
			if err != nil {
				return &DecodeError{d.r.lineNo, i, f.Name, line[i], err}
			}
			*(*uint64)(ptr) = val

		case schema.Uint32:
			val, err := strconv.ParseUint(line[i], 10, 32)
			if err != nil {
				return &DecodeError{d.r.lineNo, i, f.Name, line[i], err}
			}
			*(*uint32)(ptr) = uint32(val)

		case schema.Uint16:
			val, err := strconv.ParseUint(line[i], 10, 16)
			if err != nil {
				return &DecodeError{d.r.lineNo, i, f.Name, line[i], err}
			}
			*(*uint16)(ptr) = uint16(val)

		case schema.Uint8:
			val, err := strconv.ParseUint(line[i], 10, 8)
			if err != nil {
				return &DecodeError{d.r.lineNo, i, f.Name, line[i], err}
			}
			*(*uint8)(ptr) = uint8(val)

		case schema.Float64:
			val, err := strconv.ParseFloat(line[i], 64)
			if err != nil {
				return &DecodeError{d.r.lineNo, i, f.Name, line[i], err}
			}
			*(*float64)(ptr) = val

		case schema.Float32:
			val, err := strconv.ParseFloat(line[i], 32)
			if err != nil {
				return &DecodeError{d.r.lineNo, i, f.Name, line[i], err}
			}
			*(*float32)(ptr) = float32(val)

		case schema.Boolean:
			val, err := strconv.ParseBool(line[i])
			if err != nil {
				return &DecodeError{d.r.lineNo, i, f.Name, line[i], err}
			}
			*(*bool)(ptr) = val

		case schema.String, schema.Text:
			// use string pool to avoid string allocs
			n := d.pool.Len()
			d.pool.AppendString(line[i])
			*(*string)(ptr) = d.pool.GetString(n)

		case schema.Bytes, schema.Binary:
			// decode hex to binary
			s := strings.TrimPrefix(line[i], "0x")
			if f.IsArray() {
				if len(s) != int(f.Scale)*2 {
					return &DecodeError{d.r.lineNo, i, f.Name, line[i],
						fmt.Errorf("binary array [%d]byte mismatched hex len %d", f.Scale, len(s))}
				}
				_, err := hex.Decode(unsafe.Slice((*byte)(ptr), f.Scale), util.UnsafeGetBytes(s))
				if err != nil {
					return &DecodeError{d.r.lineNo, i, f.Name, line[i], err}
				}
			} else {
				res, err := hex.DecodeString(s)
				if err != nil {
					return &DecodeError{d.r.lineNo, i, f.Name, line[i], err}
				}
				*(*[]byte)(ptr) = res
			}

		case schema.Int256:
			i256, err := num.ParseInt256(line[i])
			if err != nil {
				return &DecodeError{d.r.lineNo, i, f.Name, line[i], err}
			}
			*(*num.Int256)(ptr) = i256

		case schema.Int128:
			i128, err := num.ParseInt128(line[i])
			if err != nil {
				return &DecodeError{d.r.lineNo, i, f.Name, line[i], err}
			}
			*(*num.Int128)(ptr) = i128

		case schema.Decimal256:
			d256, err := num.ParseDecimal256(line[i])
			if err != nil {
				return &DecodeError{d.r.lineNo, i, f.Name, line[i], err}
			}
			*(*num.Decimal256)(ptr) = d256.Quantize(f.Scale)

		case schema.Decimal128:
			d128, err := num.ParseDecimal128(line[i])
			if err != nil {
				return &DecodeError{d.r.lineNo, i, f.Name, line[i], err}
			}
			*(*num.Decimal128)(ptr) = d128.Quantize(f.Scale)

		case schema.Decimal64:
			d64, err := num.ParseDecimal64(line[i])
			if err != nil {
				return &DecodeError{d.r.lineNo, i, f.Name, line[i], err}
			}
			*(*num.Decimal64)(ptr) = d64.Quantize(f.Scale)

		case schema.Decimal32:
			d32, err := num.ParseDecimal32(line[i])
			if err != nil {
				return &DecodeError{d.r.lineNo, i, f.Name, line[i], err}
			}
			*(*num.Decimal32)(ptr) = d32.Quantize(f.Scale)

		case schema.Bigint:
			big, err := num.ParseBig(line[i])
			if err != nil {
				return &DecodeError{d.r.lineNo, i, f.Name, line[i], err}
			}
			*(*num.Big)(ptr) = big

		default:
			return &DecodeError{d.r.lineNo, i, f.Name, line[i], ErrInvalidValueType}
		}
		i++
	}
	return nil
}

var zeros [32]byte

func (d *Decoder) decodePack(pkg *pack.Package, line []string) error {
	var i int
	for k, f := range pkg.Schema().Fields {
		// skip missing blocks (e.g. after schema change)
		b := pkg.Block(k)
		if b == nil {
			continue
		}

		// fill internal metadata fields
		if f.IsMeta() {
			switch b.Type() {
			case types.BlockUint64:
				b.Uint64().Append(0)
			case types.BlockBool:
				b.Bool().Append(false)
			}
			b.SetDirty()
			continue
		}

		// fill zero for empty lines and null
		if len(line[i]) == 0 || line[i] == NULL {
			switch b.Type() {
			case types.BlockUint64, types.BlockInt64, types.BlockFloat64:
				b.Uint64().Append(0)
			case types.BlockUint32, types.BlockInt32, types.BlockFloat32:
				b.Uint32().Append(0)
			case types.BlockUint16, types.BlockInt16:
				b.Uint16().Append(0)
			case types.BlockUint8, types.BlockInt8:
				b.Uint8().Append(0)
			case types.BlockBool:
				b.Bool().Append(false)
			case types.BlockBytes:
				if f.IsArray() {
					if f.Scale <= 32 {
						b.Bytes().Append(zeros[:f.Scale])
					} else {
						b.Bytes().Append(bytes.Repeat([]byte{0}, int(f.Scale)))
					}
				} else {
					b.Bytes().Append(nil)
				}
			case types.BlockInt256:
				b.Int256().Append(num.ZeroInt256)
			case types.BlockInt128:
				b.Int128().Append(num.ZeroInt128)
			}
			i++
			continue
		}

		// decode strings
		switch f.Type {
		case schema.Timestamp:
			if d.timeAs == "" {
				tm, err := schema.TimeScale(f.Scale).Parse(line[i], false)
				if err != nil {
					return &DecodeError{d.r.lineNo, i, f.Name, line[i], err}
				}
				b.Int64().Append(tm)
			} else {
				tm, err := time.Parse(d.timeAs, line[i])
				if err != nil {
					return &DecodeError{d.r.lineNo, i, f.Name, line[i], err}
				}
				b.Int64().Append(schema.TimeScale(f.Scale).ToUnix(tm))
			}

		case schema.Duration:
			dur, err := schema.TimeScale(f.Scale).ParseDuration(line[i])
			if err != nil {
				return &DecodeError{d.r.lineNo, i, f.Name, line[i], err}
			}
			b.Int64().Append(schema.TimeScale(f.Scale).Int64(dur))

		case schema.Date:
			if d.dateAs == "" {
				tm, err := schema.TimeScale(f.Scale).Parse(line[i], false)
				if err != nil {
					return &DecodeError{d.r.lineNo, i, f.Name, line[i], err}
				}
				b.Int64().Append(tm)
			} else {
				tm, err := time.Parse(d.dateAs, line[i])
				if err != nil {
					return &DecodeError{d.r.lineNo, i, f.Name, line[i], err}
				}
				b.Int64().Append(schema.TimeScale(f.Scale).ToUnix(tm))
			}

		case schema.Time:
			if d.timeAs == "" {
				tm, err := schema.TimeScale(f.Scale).Parse(line[i], true)
				if err != nil {
					return &DecodeError{d.r.lineNo, i, f.Name, line[i], err}
				}
				b.Int64().Append(tm)
			} else {
				tm, err := time.Parse(d.timeAs, line[i])
				if err != nil {
					return &DecodeError{d.r.lineNo, i, f.Name, line[i], err}
				}
				b.Int64().Append(schema.TimeScale(f.Scale).ToUnix(tm))
			}

		case schema.Int64:
			val, err := strconv.ParseInt(line[i], 10, 64)
			if err != nil {
				return &DecodeError{d.r.lineNo, i, f.Name, line[i], err}
			}
			b.Int64().Append(val)

		case schema.Int32:
			val, err := strconv.ParseInt(line[i], 10, 32)
			if err != nil {
				return &DecodeError{d.r.lineNo, i, f.Name, line[i], err}
			}
			b.Int32().Append(int32(val))

		case schema.Int16:
			val, err := strconv.ParseInt(line[i], 10, 16)
			if err != nil {
				return &DecodeError{d.r.lineNo, i, f.Name, line[i], err}
			}
			b.Int16().Append(int16(val))

		case schema.Int8:
			val, err := strconv.ParseInt(line[i], 10, 8)
			if err != nil {
				return &DecodeError{d.r.lineNo, i, f.Name, line[i], err}
			}
			b.Int8().Append(int8(val))

		case schema.Uint64:
			val, err := strconv.ParseUint(line[i], 10, 64)
			if err != nil {
				return &DecodeError{d.r.lineNo, i, f.Name, line[i], err}
			}
			b.Uint64().Append(val)

		case schema.Uint32:
			val, err := strconv.ParseUint(line[i], 10, 32)
			if err != nil {
				return &DecodeError{d.r.lineNo, i, f.Name, line[i], err}
			}
			b.Uint32().Append(uint32(val))

		case schema.Uint16:
			val, err := strconv.ParseUint(line[i], 10, 16)
			if err != nil {
				return &DecodeError{d.r.lineNo, i, f.Name, line[i], err}
			}
			b.Uint16().Append(uint16(val))

		case schema.Uint8:
			val, err := strconv.ParseUint(line[i], 10, 8)
			if err != nil {
				return &DecodeError{d.r.lineNo, i, f.Name, line[i], err}
			}
			b.Uint8().Append(uint8(val))

		case schema.Float64:
			val, err := strconv.ParseFloat(line[i], 64)
			if err != nil {
				return &DecodeError{d.r.lineNo, i, f.Name, line[i], err}
			}
			b.Float64().Append(val)

		case schema.Float32:
			val, err := strconv.ParseFloat(line[i], 32)
			if err != nil {
				return &DecodeError{d.r.lineNo, i, f.Name, line[i], err}
			}
			b.Float32().Append(float32(val))

		case schema.Boolean:
			val, err := strconv.ParseBool(line[i])
			if err != nil {
				return &DecodeError{d.r.lineNo, i, f.Name, line[i], err}
			}
			b.Bool().Append(val)

		case schema.String, schema.Text:
			b.Bytes().Append(util.UnsafeGetBytes(line[i]))

		case schema.Bytes, schema.Binary:
			// decode hex to binary
			s := strings.TrimPrefix(line[i], "0x")
			var (
				buf []byte
				err error
			)
			if f.IsArray() {
				if len(s) != int(f.Scale)*2 {
					return &DecodeError{d.r.lineNo, i, f.Name, line[i],
						fmt.Errorf("binary array [%d]byte mismatched hex len %d", f.Scale, len(s))}
				}
				buf, err = hex.DecodeString(s)
			} else {
				buf, err = hex.DecodeString(s)
			}
			if err != nil {
				return &DecodeError{d.r.lineNo, i, f.Name, line[i], err}
			}
			b.Bytes().Append(buf)

		case schema.Int256:
			i256, err := num.ParseInt256(line[i])
			if err != nil {
				return &DecodeError{d.r.lineNo, i, f.Name, line[i], err}
			}
			b.Int256().Append(i256)

		case schema.Int128:
			i128, err := num.ParseInt128(line[i])
			if err != nil {
				return &DecodeError{d.r.lineNo, i, f.Name, line[i], err}
			}
			b.Int128().Append(i128)

		case schema.Decimal256:
			d256, err := num.ParseDecimal256(line[i])
			if err != nil {
				return &DecodeError{d.r.lineNo, i, f.Name, line[i], err}
			}
			b.Int256().Append(d256.Quantize(f.Scale).Int256())

		case schema.Decimal128:
			d128, err := num.ParseDecimal128(line[i])
			if err != nil {
				return &DecodeError{d.r.lineNo, i, f.Name, line[i], err}
			}
			b.Int128().Append(d128.Quantize(f.Scale).Int128())

		case schema.Decimal64:
			d64, err := num.ParseDecimal64(line[i])
			if err != nil {
				return &DecodeError{d.r.lineNo, i, f.Name, line[i], err}
			}
			b.Int64().Append(d64.Quantize(f.Scale).Int64())

		case schema.Decimal32:
			d32, err := num.ParseDecimal32(line[i])
			if err != nil {
				return &DecodeError{d.r.lineNo, i, f.Name, line[i], err}
			}
			b.Int32().Append(d32.Quantize(f.Scale).Int32())

		case schema.Bigint:
			big, err := num.ParseBig(line[i])
			if err != nil {
				return &DecodeError{d.r.lineNo, i, f.Name, line[i], err}
			}
			b.Bytes().Append(big.Bytes())

		default:
			return &DecodeError{d.r.lineNo, i, f.Name, line[i], ErrInvalidValueType}
		}
		i++
	}
	return nil
}
