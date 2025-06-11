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
	"blockwatch.cc/knoxdb/pkg/util"
)

type Encoder struct {
	w      io.Writer
	s      *schema.Schema
	sep    rune
	eol    []byte
	flags  EncoderFlags
	typ    reflect.Type
	ofs    []uintptr // field offsets in dynamic native struct
	timeAs string    // timestamp format
	buf    []byte    // record write buffer
}

type EncoderFlags byte

const (
	EncoderFlagWriteHeader EncoderFlags = 1 << iota
	EncoderFlagWriteNull                // write null values (unused)
	EncoderFlagTrim                     // trim string fields
)

func NewEncoder(s *schema.Schema, w io.Writer) *Encoder {
	typ := s.NativeStructType()
	return &Encoder{
		w:      w,
		s:      s,
		sep:    Separator,
		eol:    []byte{byte(Eol)},
		flags:  EncoderFlagWriteHeader,
		typ:    typ,
		ofs:    structFieldOffsets(typ),
		timeAs: time.RFC3339Nano,              // 2006-01-02T15:04:05.999999999Z07:00
		buf:    make([]byte, 0, s.WireSize()), // good approximation
	}
}

func (e *Encoder) WithTrim(t bool) *Encoder {
	if t {
		e.flags |= EncoderFlagTrim
	} else {
		e.flags &^= EncoderFlagTrim
	}
	return e
}

func (e *Encoder) WithHeader(t bool) *Encoder {
	if t {
		e.flags |= EncoderFlagWriteHeader
	} else {
		e.flags &^= EncoderFlagWriteHeader
	}
	return e
}

func (e *Encoder) WithSeparator(s rune) *Encoder {
	e.sep = s
	return e
}

func (e *Encoder) WithEol(eol []byte) *Encoder {
	e.eol = eol
	return e
}

func (e *Encoder) WithTimeFormat(f string) *Encoder {
	e.timeAs = f
	return e
}

func (e *Encoder) Encode(v any) error {
	if err := e.writeHeader(); err != nil {
		return err
	}

	val := reflect.ValueOf(v)
	if !val.IsValid() {
		return nil
	}

	switch val.Kind() {
	case reflect.Slice:
		for i := range val.Len() {
			var err error
			val := val.Index(i)
			switch val.Kind() {
			case reflect.Pointer:
				err = e.encode(val.UnsafePointer())
			case reflect.Struct:
				err = e.encode(val.Addr().UnsafePointer())
			default:
				err = fmt.Errorf("csv: encode called with invalid type %s (%s)", val.Type(), val.Kind())
			}
			if err != nil {
				return err
			}
		}
	case reflect.Pointer:
		return e.encode(val.UnsafePointer())
	default:
		return fmt.Errorf("csv: encode called with invalid type %s (%s)", val.Type(), val.Kind())
	}
	return nil
}

func (e *Encoder) EncodeSlice(v []any) error {
	if err := e.writeHeader(); err != nil {
		return err
	}

	for _, val := range v {
		rval := reflect.ValueOf(val)
		if !rval.IsValid() {
			continue
		}
		var err error
		switch rval.Kind() {
		case reflect.Pointer:
			err = e.encode(rval.UnsafePointer())
		case reflect.Struct:
			err = e.encode(rval.Addr().UnsafePointer())
		default:
			err = fmt.Errorf("csv: encode called with invalid type %s (%s)", rval.Type(), rval.Kind())
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (e *Encoder) writeHeader() error {
	if e.flags&EncoderFlagWriteHeader == 0 {
		return nil
	}
	var i int
	for _, f := range e.s.Exported() {
		if !f.IsVisible {
			continue
		}
		if i > 0 {
			_, err := e.w.Write([]byte{byte(e.sep)})
			if err != nil {
				return err
			}
		}
		_, err := e.w.Write(util.UnsafeGetBytes(f.Name))
		if err != nil {
			return err
		}
		i++
	}
	e.w.Write(e.eol)
	e.flags &^= EncoderFlagWriteHeader
	return nil
}

func (e *Encoder) encode(base unsafe.Pointer) error {
	var (
		i int
	)
	e.buf = e.buf[:0]
	for _, f := range e.s.Exported() {
		if !f.IsVisible {
			continue
		}
		if i > 0 {
			e.buf = append(e.buf, byte(e.sep))
		}
		ptr := unsafe.Add(base, e.ofs[i])
		switch f.Type {
		case types.FieldTypeDatetime:
			tm := schema.TimeScale(f.Scale).FromUnix(*(*int64)(ptr))
			e.buf = tm.AppendFormat(e.buf, e.timeAs)

		case types.FieldTypeInt64:
			e.buf = strconv.AppendInt(e.buf, *(*int64)(ptr), 10)

		case types.FieldTypeInt32:
			e.buf = strconv.AppendInt(e.buf, int64(*(*int32)(ptr)), 10)

		case types.FieldTypeInt16:
			e.buf = strconv.AppendInt(e.buf, int64(*(*int16)(ptr)), 10)

		case types.FieldTypeInt8:
			e.buf = strconv.AppendInt(e.buf, int64(*(*int8)(ptr)), 10)

		case types.FieldTypeUint64:
			e.buf = strconv.AppendUint(e.buf, *(*uint64)(ptr), 10)

		case types.FieldTypeUint32:
			e.buf = strconv.AppendUint(e.buf, uint64(*(*uint32)(ptr)), 10)

		case types.FieldTypeUint16:
			e.buf = strconv.AppendUint(e.buf, uint64(*(*uint16)(ptr)), 10)

		case types.FieldTypeUint8:
			e.buf = strconv.AppendUint(e.buf, uint64(*(*uint8)(ptr)), 10)

		case types.FieldTypeFloat64:
			e.buf = strconv.AppendFloat(e.buf, *(*float64)(ptr), 'f', -1, 64)

		case types.FieldTypeFloat32:
			e.buf = strconv.AppendFloat(e.buf, float64(*(*float32)(ptr)), 'f', -1, 32)

		case types.FieldTypeBoolean:
			e.buf = strconv.AppendBool(e.buf, *(*bool)(ptr))

		case types.FieldTypeString:
			// quote strings that contain (a) a separator character or (b)
			// start with a quote character. Escape quotes inside quoted strings.
			s := *(*string)(ptr)
			if e.flags&EncoderFlagTrim > 0 {
				s = strings.TrimSpace(s)
			}
			if strings.ContainsRune(s, e.sep) || strings.HasPrefix(s, string(Quote)) {
				e.buf = appendQuoted(e.buf, s)
			} else {
				e.buf = append(e.buf, util.UnsafeGetBytes(s)...)
			}

		case types.FieldTypeBytes:
			// encode hex
			if f.Fixed > 0 {
				e.buf = hex.AppendEncode(e.buf, unsafe.Slice((*byte)(ptr), f.Fixed))
			} else {
				e.buf = hex.AppendEncode(e.buf, *(*[]byte)(ptr))
			}

		case types.FieldTypeInt256:
			e.buf = (*(*num.Int256)(ptr)).Append(e.buf)

		case types.FieldTypeInt128:
			e.buf = (*(*num.Int128)(ptr)).Append(e.buf)

		case types.FieldTypeDecimal256:
			s := num.NewDecimal256(*(*num.Int256)(ptr), f.Scale).String()
			e.buf = append(e.buf, util.UnsafeGetBytes(s)...)

		case types.FieldTypeDecimal128:
			s := num.NewDecimal128(*(*num.Int128)(ptr), f.Scale).String()
			e.buf = append(e.buf, util.UnsafeGetBytes(s)...)

		case types.FieldTypeDecimal64:
			s := num.NewDecimal64(*(*int64)(ptr), f.Scale).String()
			e.buf = append(e.buf, util.UnsafeGetBytes(s)...)

		case types.FieldTypeDecimal32:
			s := num.NewDecimal32(*(*int32)(ptr), f.Scale).String()
			e.buf = append(e.buf, util.UnsafeGetBytes(s)...)

		case types.FieldTypeBigint:
			e.buf = num.NewBigFromBytes(*(*[]byte)(ptr)).Big().Append(e.buf, 10)

		default:
			return fmt.Errorf("csv: encode field %d (%s): %v", i, f.Name, schema.ErrInvalidValueType)
		}
		i++
	}
	e.buf = append(e.buf, e.eol...)
	_, err := e.w.Write(e.buf)
	return err
}

func appendQuoted(buf []byte, s string) []byte {
	if len(s) == 0 {
		return buf
	}
	b := util.UnsafeGetBytes(s)
	buf = append(buf, byte(Quote))
	for {
		tok, rem, ok := bytes.Cut(b, []byte{byte(Quote)})
		buf = append(buf, tok...)
		if ok {
			buf = append(buf, byte(Quote), byte(Quote))
		} else {
			break
		}
		b = rem
	}
	return append(buf, byte(Quote))
}
