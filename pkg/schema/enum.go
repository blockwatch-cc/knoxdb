// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package schema

import (
	"bytes"
	"fmt"
	"slices"
	"strings"

	"blockwatch.cc/knoxdb/internal/hash/xxhash64"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/util"
)

const (
	EnumMaxSize   = 1<<8 - 1 // 255
	EnumMaxValues = 1 << 16  // 65536 (0 .. 0xFFFF)
)

type EnumRegistry struct {
	*util.LockFreeMap[uint64, *EnumDictionary]
}

var (
	GlobalRegistry = NewEnumRegistry()
)

func RegisterEnum(tag uint64, e *EnumDictionary) {
	GlobalRegistry.Put(e.Tag()+tag, e)
}

func UnregisterEnum(tag uint64, e *EnumDictionary) {
	GlobalRegistry.Del(e.Tag() + tag)
}

func LookupEnum(tag uint64, name string) (*EnumDictionary, bool) {
	return GlobalRegistry.Get(types.TaggedHash(types.ObjectTagEnum, name) + tag)
}

func NewEnumRegistry() EnumRegistry {
	return EnumRegistry{util.NewLockFreeMap[uint64, *EnumDictionary]()}
}

func (r EnumRegistry) Register(e *EnumDictionary) {
	r.Put(e.Tag(), e)
}

func (r EnumRegistry) Lookup(name string) (*EnumDictionary, bool) {
	return r.Get(types.TaggedHash(types.ObjectTagEnum, name))
}

type EnumDictionary struct {
	name    string
	values  []byte
	offsets []uint32
	codes   map[uint64]uint16
}

func NewEnumDictionary(name string) *EnumDictionary {
	if name == "" {
		name = "enum"
	}
	return &EnumDictionary{
		name:    name,
		values:  make([]byte, 0),
		offsets: make([]uint32, 0),
		codes:   make(map[uint64]uint16),
	}
}

func (e *EnumDictionary) Name() string {
	return e.name
}

func (e *EnumDictionary) Tag() uint64 {
	return types.TaggedHash(types.ObjectTagEnum, e.name)
}

func (e *EnumDictionary) Len() int {
	return len(e.offsets)
}

func (e *EnumDictionary) Clone() *EnumDictionary {
	clone := &EnumDictionary{
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

func (e *EnumDictionary) Values() []string {
	vals := make([]string, len(e.offsets))
	for i := range vals {
		vals[i] = e.value(i)
	}
	return vals
}

func (e *EnumDictionary) Value(code uint16) (string, bool) {
	if int(code) >= len(e.offsets) {
		return "", false
	}
	return e.value(int(code)), true
}

func (e *EnumDictionary) MustValue(code uint16) string {
	if int(code) >= len(e.offsets) {
		panic(ErrInvalidValue)
	}
	return e.value(int(code))
}

func (e *EnumDictionary) Code(val string) (uint16, bool) {
	code, ok := e.codes[xxhash64.Sum64([]byte(val))]
	return code, ok
}

func (e *EnumDictionary) Append(vals ...string) error {
	if e.Len()+len(vals) > EnumMaxValues {
		return ErrEnumFull
	}
	unique := make(map[string]struct{})
	for _, v := range vals {
		if len(v) > EnumMaxSize {
			return fmt.Errorf("enum: %s %q: %w", e.name, v, ErrNameTooLong)
		}
		if _, ok := e.Code(v); ok {
			return fmt.Errorf("enum: %s %q: %w", e.name, v, ErrDuplicateName)
		}
		if _, ok := unique[v]; ok {
			return fmt.Errorf("enum: %s %q: %w", e.name, v, ErrDuplicateName)
		}
		unique[v] = struct{}{}
	}

	clear(e.codes)
	for _, v := range vals {
		e.codes[xxhash64.Sum64([]byte(v))] = uint16(e.Len())
		e.offsets = append(e.offsets, uint32(len(e.values)))
		e.values = append(e.values, []byte(v)...)
	}
	return nil
}

func (e EnumDictionary) MarshalBinary() ([]byte, error) {
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

func (e *EnumDictionary) UnmarshalBinary(buf []byte) error {
	e.values = e.values[:0]
	e.offsets = e.offsets[:0]
	for len(buf) > 0 {
		sz := buf[0]
		buf = buf[1:]
		e.codes[xxhash64.Sum64(buf[:sz])] = uint16(len(e.offsets))
		e.offsets = append(e.offsets, uint32(len(e.values)))
		e.values = append(e.values, buf[:sz]...)
		buf = buf[sz:]
	}
	return nil
}

func (e *EnumDictionary) value(i int) string {
	start, end := int(e.offsets[i]), len(e.values)
	if i < len(e.offsets)-1 {
		end = int(e.offsets[i+1])
	}
	return util.UnsafeGetString(e.values[start:end])
}

var (
	_ ValueCaster = (*EnumDictionary)(nil)
	_ ValueParser = (*EnumDictionary)(nil)
)

// ValueParser interface
func (e *EnumDictionary) ParseValue(s string) (any, error) {
	code, ok := e.Code(s)
	if !ok {
		return nil, fmt.Errorf("invalid enum value %q", s)
	}
	return code, nil
}

func (e *EnumDictionary) ParseSlice(s string) (any, error) {
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
func (e *EnumDictionary) CastValue(val any) (any, error) {
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
		return nil, castError(val, "enum")
	}
}

func (e *EnumDictionary) CastSlice(val any) (any, error) {
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
		return nil, castError(val, "enum")
	}
}
