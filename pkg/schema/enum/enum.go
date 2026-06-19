// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package enum

import (
	"bytes"
	"fmt"
	"iter"
	"slices"
	"strings"

	"blockwatch.cc/knoxdb/internal/hash"
	"blockwatch.cc/knoxdb/pkg/util"
)

const (
	EnumMaxSize   = 1<<8 - 1 // 255
	EnumMaxValues = 1 << 16  // 65536 (0 .. 0xFFFF)
)

// Registry implements a basic enum registry that keeps all
// enums in memory only. It is safe to call concurrently and
// may be used as global enum registry, however, due to
// lack of persistence all enums must be registered at least
// once on start.
type Registry struct {
	*util.LockFreeMap[uint64, *Dictionary]
}

func NewRegistry() *Registry {
	return &Registry{util.NewLockFreeMap[uint64, *Dictionary]()}
}

func (r Registry) Register(tag uint64, e *Dictionary) {
	r.Put(tag, e)
}

func (r Registry) Lookup(tag uint64) (*Dictionary, bool) {
	return r.Get(tag)
}

func (r Registry) Find(name string) (*Dictionary, bool) {
	for _, v := range r.Iter() {
		if v.name == name {
			return v, true
		}
	}
	return nil, false
}

// Dictionary implements an enum namespace that keeps all
// values in memory. It is not safe to read and append values
// concurrently. A user must provide its own synchronization
// (e.g. database transaction semantics or write once on load).
// Dictionary offers serialzation methods, but does not handle
// persistence itself. Hence all enum values must be loaded
// before use.
type Dictionary struct {
	name    string
	values  []byte
	offsets []uint32
	codes   map[uint64]uint16
}

func NewDictionary(name string) *Dictionary {
	if name == "" {
		name = "enum"
	}
	return &Dictionary{
		name:    name,
		values:  make([]byte, 0),
		offsets: make([]uint32, 0),
		codes:   make(map[uint64]uint16),
	}
}

func (e *Dictionary) Name() string {
	return e.name
}

func (e *Dictionary) Len() int {
	return len(e.offsets)
}

func (e *Dictionary) Clone() *Dictionary {
	clone := &Dictionary{
		name:    e.name,
		values:  bytes.Clone(e.values),
		offsets: slices.Clone(e.offsets),
		codes:   make(map[uint64]uint16, len(e.codes)),
	}
	for c := range e.codes {
		clone.codes[c] = e.codes[c]
	}
	return clone
}

func (e *Dictionary) Values() iter.Seq[string] {
	return func(yield func(string) bool) {
		for i := range len(e.offsets) {
			if !yield(e.value(i)) {
				return
			}
		}
	}
}

func (e *Dictionary) Value(code uint16) (string, bool) {
	if int(code) >= len(e.offsets) {
		return "", false
	}
	return e.value(int(code)), true
}

func (e *Dictionary) MustValue(code uint16) string {
	if int(code) >= len(e.offsets) {
		panic(ErrEnumNoCode)
	}
	return e.value(int(code))
}

func (e *Dictionary) Code(val string) (uint16, bool) {
	code, ok := e.codes[hash.HashString(val)]
	return code, ok
}

func (e *Dictionary) Append(vals ...string) error {
	if e.Len()+len(vals) > EnumMaxValues {
		return ErrEnumFull
	}
	unique := make(map[string]struct{})
	for _, v := range vals {
		if len(v) > EnumMaxSize {
			return fmt.Errorf("enum: %s %q: %w", e.name, v, ErrEnumTooLong)
		}
		if _, ok := e.Code(v); ok {
			return fmt.Errorf("enum: %s %q: %w", e.name, v, ErrEnumDuplicate)
		}
		if _, ok := unique[v]; ok {
			return fmt.Errorf("enum: %s %q: %w", e.name, v, ErrEnumDuplicate)
		}
		unique[v] = struct{}{}
	}

	clear(e.codes)
	for _, v := range vals {
		e.codes[hash.HashString(v)] = uint16(e.Len())
		e.offsets = append(e.offsets, uint32(len(e.values)))
		e.values = append(e.values, []byte(v)...)
	}
	return nil
}

func (e Dictionary) MarshalBinary() ([]byte, error) {
	buf := make([]byte, 0, len(e.values)+len(e.offsets))
	if len(e.offsets) > 0 {
		var pos uint32
		for _, offs := range e.offsets[1:] {
			buf = append(buf, byte(offs-pos))
			buf = append(buf, e.values[pos:offs]...)
			pos = offs
		}
		buf = append(buf, byte(len(e.values)-int(pos)))
		buf = append(buf, e.values[pos:]...)
	}
	return buf, nil
}

func (e *Dictionary) UnmarshalBinary(buf []byte) error {
	e.values = e.values[:0]
	e.offsets = e.offsets[:0]
	for len(buf) > 0 {
		sz := buf[0]
		buf = buf[1:]
		e.codes[hash.Hash(buf[:sz])] = uint16(len(e.offsets))
		e.offsets = append(e.offsets, uint32(len(e.values)))
		e.values = append(e.values, buf[:sz]...)
		buf = buf[sz:]
	}
	return nil
}

func (e *Dictionary) value(i int) string {
	start, end := int(e.offsets[i]), len(e.values)
	if i < len(e.offsets)-1 {
		end = int(e.offsets[i+1])
	}
	return util.UnsafeGetString(e.values[start:end])
}

// var (
// 	_ parse.ValueParser = (*Dictionary)(nil)
// 	_ cast.ValueCaster  = (*Dictionary)(nil)
// )

// ValueParser interface
func (e *Dictionary) ParseValue(s string) (any, error) {
	code, ok := e.Code(s)
	if !ok {
		return nil, fmt.Errorf("invalid enum value %q", s)
	}
	return code, nil
}

func (e *Dictionary) ParseSlice(s string) (any, error) {
	vals := strings.Split(s, ",")
	codes := make([]uint16, len(vals))
	var ok bool
	for i, v := range vals {
		codes[i], ok = e.Code(v)
		if !ok {
			return nil, fmt.Errorf("invalid enum value %q", v)
		}
	}
	return codes, nil
}

// ValueCaster interface
func (e *Dictionary) CastValue(val any) (any, error) {
	switch v := val.(type) {
	case string:
		code, ok := e.Code(v)
		if !ok {
			return nil, fmt.Errorf("invalid enum value %q", v)
		}
		return code, nil
	case []byte:
		code, ok := e.Code(string(v))
		if !ok {
			return nil, fmt.Errorf("invalid enum value %q", string(v))
		}
		return code, nil
	case uint16:
		if int(v) >= len(e.offsets) {
			return nil, fmt.Errorf("invalid enum code %d", v)
		}
		return v, nil
	default:
		return nil, fmt.Errorf("cast: unexpected value type %T for enum condition", val)
	}
}

func (e *Dictionary) CastSlice(val any) (any, error) {
	switch v := val.(type) {
	case []string:
		codes := make([]uint16, len(v))
		for i, vv := range v {
			code, ok := e.Code(vv)
			if !ok {
				return nil, fmt.Errorf("invalid enum value %q", vv)
			}
			codes[i] = code
		}
		return codes, nil
	case [][]byte:
		codes := make([]uint16, len(v))
		for i, vv := range v {
			code, ok := e.Code(string(vv))
			if !ok {
				return nil, fmt.Errorf("invalid enum value %q", string(vv))
			}
			codes[i] = code
		}
		return codes, nil
	case []uint16:
		for _, vv := range v {
			if int(vv) >= len(e.offsets) {
				return nil, fmt.Errorf("invalid enum code %d", vv)
			}
		}
		return v, nil
	default:
		return nil, fmt.Errorf("cast: unexpected value type %T for enum condition", val)
	}
}
