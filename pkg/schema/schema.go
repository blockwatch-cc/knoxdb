// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package schema

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"strings"

	"blockwatch.cc/knoxdb/internal/hash/fnv"
)

const (
	MAX_FIXED = uint16(1<<16 - 1)

	defaultVarFieldSize = 64 // variable number of bytes for strings and byte slices
)

type Schema struct {
	name        string
	nameHash    uint64
	schemaHash  uint64
	fields      []Field
	exports     []*ExportedField
	minWireSize int
	maxWireSize int
	isFixedSize bool
	isInterface bool
	version     uint32
	encode      []OpCode
	decode      []OpCode
}

func NewSchema() *Schema {
	return &Schema{
		fields:      make([]Field, 0),
		isFixedSize: true,
	}
}

func (s *Schema) NewBuffer(sz int) *bytes.Buffer {
	return bytes.NewBuffer(make([]byte, 0, sz*s.maxWireSize))
}

func (s *Schema) IsValid() bool {
	return len(s.name) != 0 && len(s.fields) != 0 && len(s.encode) != 0
}

func (s *Schema) IsInterface() bool {
	return s.isInterface
}

func (s *Schema) Name() string {
	return s.name
}

func (s *Schema) Version() uint32 {
	return s.version
}

func (s *Schema) WithName(n string) *Schema {
	// can only set name once
	if len(s.name) == 0 && len(n) > 0 {
		s.name = n
	}
	return s
}

func (s *Schema) NameHash() uint64 {
	return s.nameHash
}

func (s *Schema) Hash() uint64 {
	return s.schemaHash
}

func (s *Schema) WithField(f Field) *Schema {
	if f.IsValid() {
		f.id = uint16(len(s.fields) + 1)
		s.fields = append(s.fields, f)
		s.encode, s.decode = nil, nil
	}
	return s
}

// TODO: prevent changing typ, id, fixed, scale, primary flag
func (s *Schema) UpdateField(id uint16, f Field) *Schema {
	if f.IsValid() {
		for i := range s.fields {
			if s.fields[i].id != id {
				continue
			}
			f.id = id
			s.fields[i] = f
			s.encode, s.decode = nil, nil
			break
		}
	}
	return s
}

func (s *Schema) Complete() *Schema {
	if len(s.encode) > 0 {
		return s
	}
	s.minWireSize = 0
	s.maxWireSize = 0
	s.isFixedSize = true
	s.isInterface = false
	s.nameHash = 0
	s.schemaHash = 0

	// generate name hash
	s.nameHash = fnv.Sum64a([]byte(s.name))
	// generate schema hash
	h := fnv.New64a()
	h.Write(Uint32Bytes(uint32(s.version)))

	// collect sizes
	for i := range s.fields {
		sz := s.fields[i].WireSize()
		s.minWireSize += sz
		s.maxWireSize += sz
		s.isInterface = s.isInterface || s.fields[i].IsInterface()
		if !s.fields[i].IsFixedSize() {
			s.isFixedSize = false
			s.maxWireSize += defaultVarFieldSize
		}
		h.Write(Uint16Bytes(s.fields[i].id))
		h.Write([]byte{byte(s.fields[i].typ)})
	}
	s.schemaHash = h.Sum64()
	s.encode, s.decode = compileCodecs(s)
	s.version++

	s.exports = make([]*ExportedField, len(s.fields))
	for i := range s.fields {
		s.exports[i] = &ExportedField{
			Name:      s.fields[i].name,
			Id:        s.fields[i].id,
			Type:      s.fields[i].typ,
			Flags:     s.fields[i].flags,
			Compress:  s.fields[i].compress,
			Index:     s.fields[i].index,
			Fixed:     s.fields[i].fixed,
			Scale:     s.fields[i].scale,
			Offset:    s.fields[i].offset,
			Iface:     s.fields[i].iface,
			IsVisible: s.fields[i].IsVisible(),
			IsArray:   s.fields[i].isArray,
			path:      s.fields[i].path,
		}
	}

	return s
}

func (s *Schema) WireSize() int {
	return s.minWireSize
}

func (s *Schema) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "Schema: %s minSz=%d maxSz=%d fixed=%t iface=%t enc/dec=%d/%d\n",
		s.name,
		s.minWireSize,
		s.maxWireSize,
		s.isFixedSize,
		s.isInterface,
		len(s.encode),
		len(s.decode),
	)
	for i := range s.fields {
		f := &s.fields[i]
		fmt.Fprintf(&b, "  Field #%d: %s %s flags=%08b index=%d fixed=%d scale=%d arr=%t sz=%d/%d iface=%08b enc=%s dec=%s\n",
			i,
			f.name,
			f.typ,
			f.flags,
			f.index,
			f.fixed,
			f.scale,
			f.isArray,
			f.dataSize,
			f.wireSize,
			f.iface,
			s.encode[i],
			s.decode[i],
		)
	}
	return b.String()
}

func (s *Schema) NumFields() int {
	return len(s.fields)
}

func (s *Schema) NumVisibleFields() int {
	var n int
	for i := range s.fields {
		if s.fields[i].IsVisible() {
			n++
		}
	}
	return n
}

func (s *Schema) FieldNames() []string {
	list := make([]string, len(s.fields))
	for i := range s.fields {
		list[i] = s.fields[i].name
	}
	return list
}

func (s *Schema) FieldIDs() []uint16 {
	list := make([]uint16, len(s.fields))
	for i := range s.fields {
		list[i] = s.fields[i].id
	}
	return list
}

func (s *Schema) Fields() []Field {
	return s.fields
}

func (s *Schema) Exported() []*ExportedField {
	return s.exports
}

func (s *Schema) FieldByName(name string) (f Field, ok bool) {
	for _, v := range s.fields {
		if v.name == name {
			ok = true
			f = v
			break
		}
	}
	return
}

func (s *Schema) FieldById(id uint16) (f Field, ok bool) {
	for _, v := range s.fields {
		if v.id == id {
			ok = true
			f = v
			break
		}
	}
	return
}

func (s *Schema) Pk() Field {
	for _, v := range s.fields {
		if v.flags&FieldFlagPrimary > 0 {
			return v
		}
	}
	return Field{}
}

func (s *Schema) PkIndex() int {
	for _, v := range s.fields {
		if v.flags&FieldFlagPrimary > 0 {
			return int(v.Id())
		}
	}
	return -1
}

func (s *Schema) Indexes() (list []Field) {
	for _, v := range s.fields {
		if v.flags&FieldFlagIndexed > 0 {
			list = append(list, v)
		}
	}
	return
}

func (s *Schema) CanSelect(x *Schema) error {
	for _, xf := range x.fields {
		sf, ok := s.FieldByName(xf.name)
		if !ok {
			return fmt.Errorf("missing field %s", xf.name)
		}
		if xf.typ != sf.typ {
			return fmt.Errorf("field %s: type mismatch have=%s want=%s", xf.name, sf.typ, xf.typ)
		}
	}
	return nil
}

func (s *Schema) Select(names ...string) *Schema {
	ns := &Schema{
		fields:      make([]Field, 0, len(names)),
		isFixedSize: true,
		version:     s.version,
	}

	// choose fields
	for _, name := range names {
		f, ok := s.FieldByName(name)
		if !ok {
			continue
		}
		ns.fields = append(ns.fields, f)
	}

	// derive name from original schema name and hash
	base, _, _ := strings.Cut(s.name, "-")
	ns.name = base + "-" + hex.EncodeToString(Uint64Bytes(ns.Hash()))

	// produce mapping to parent fields
	return ns.Complete()
}

// Returns a field position mapping for a child schema that maps child schema
// fields to source schema field positions. Iterating over child fields and
// using this mapping yields the order in which source schema data is encoded or
// layed out in storage containers (i.e. packages of blocks/vectors),
func (s *Schema) MapTo(dst *Schema) ([]int, error) {
	maps := make([]int, 0, dst.NumFields())
	for _, dstField := range dst.Fields() {
		if !dstField.IsVisible() {
			continue
		}
		var (
			srcField Field
			pos      int = -1
		)
		for i, f := range s.fields {
			if dstField.name == f.name {
				pos = i
				srcField = f
				break
			}
		}

		// allow child -> parent and parent -> child mappings
		// if pos < 0 {
		// 	return nil, fmt.Errorf("schema %s: field %s not found in parent schema %s",
		// 		dst.name, dstField.name, s.name)
		// }
		if srcField.typ != dstField.typ {
			return nil, fmt.Errorf("schema %s: field %s type %s mismatch with source type %s",
				dst.name, dstField.name, dstField.typ, srcField.typ)
		}
		if srcField.fixed != dstField.fixed {
			return nil, fmt.Errorf("schema %s: field %s fixed type mismatch",
				dst.name, dstField.name)
		}
		maps = append(maps, pos)
	}
	return maps, nil
}

func (s *Schema) Validate() error {
	// require name
	if s.name == "" {
		return fmt.Errorf("missing schema name")
	}

	if len(s.fields) == 0 {
		return fmt.Errorf("empty schema, no supported fields found")
	}

	// count special fields, require no duplicate names
	unique := make(map[string]struct{})
	var npk int

	for i := range s.fields {
		// fields must validate
		if err := s.fields[i].Validate(); err != nil {
			return fmt.Errorf("schema %s: field %s: %v", s.name, s.fields[i].name, err)
		}

		// count pk fields
		if s.fields[i].flags&FieldFlagPrimary > 0 {
			npk++
		}

		// check name uniqueness
		n := s.fields[i].name
		if _, ok := unique[n]; ok {
			return fmt.Errorf("schema %s: duplicate field name %s", s.name, n)
		} else {
			unique[n] = struct{}{}
		}
	}

	// require pk field exists
	if npk == 0 {
		return fmt.Errorf("schema %s: missing primary key field", s.name)
	}

	// require single pk field (TODO: allow composite keys)
	if npk > 1 {
		return fmt.Errorf("schema %s: multiple primary key fields", s.name)
	}

	// encode opcodes are defined for all fields
	if a, b := len(s.fields), len(s.encode); a > b {
		return fmt.Errorf("schema %s: %d fields without encoder opcodes", s.name, a-b)
	}

	// decode opcodes are defined for all fields
	if a, b := len(s.fields), len(s.decode); a > b {
		return fmt.Errorf("schema %s: %d fields without decoder opcodes", s.name, a-b)
	}

	return nil
}
