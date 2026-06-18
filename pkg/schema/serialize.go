// Copyright (c) 2024-2026 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package schema

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"slices"
	"strconv"

	"blockwatch.cc/knoxdb/pkg/schema/enum"
)

// schema is serialized in LE
var LE = binary.LittleEndian

func (s *Schema) MarshalBinary() ([]byte, error) {
	buf := bytes.NewBuffer(make([]byte, 0, 32*len(s.Fields)+12+len(s.Name)))
	err := s.WriteTo(buf)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (s *Schema) WriteTo(buf *bytes.Buffer) error {
	// version: u32
	binary.Write(buf, LE, s.Version)

	// name: string
	binary.Write(buf, LE, uint32(len(s.Name)))
	buf.WriteString(s.Name)

	// fields
	binary.Write(buf, LE, uint32(len(s.Fields)))
	for _, f := range s.Fields {
		f.WriteTo(buf)
	}

	return nil
}

func (s *Schema) UnmarshalBinary(b []byte) error {
	if len(b) < 12 {
		return io.ErrShortBuffer
	}
	return s.ReadFrom(bytes.NewBuffer(b))
}

func (s *Schema) ReadFrom(buf *bytes.Buffer) (err error) {
	// version: u32
	err = binary.Read(buf, LE, &s.Version)
	if err != nil {
		return
	}

	// name: string
	var l uint32
	err = binary.Read(buf, LE, &l)
	if err != nil {
		return
	}
	s.Name = string(buf.Next(int(l)))
	if len(s.Name) != int(l) {
		return io.ErrShortBuffer
	}

	// fields
	err = binary.Read(buf, LE, &l)
	if err != nil {
		return
	}
	s.Fields = make([]*Field, l)
	for i := range s.Fields {
		f := &Field{}
		if err = f.ReadFrom(buf); err != nil {
			return
		}
		s.Fields[i] = f
	}

	// Build type links to nested fields; the rule is that every
	// nesting level (LIST, MAP types) is represented by a child
	// schema which contains all recursive nested fields (not only
	// direct nested fields!). A parent id encodes the back-link
	// for this relation ship. Here we use these backlinks to
	// iteratively re-construct child schemas. Note a backlink
	// can just encode a unilateral relation to a direct parent.
	// To rebuild the multi-lateral view we use a recursive helper
	// method that traverses the tree upwards and inserts a child
	// field into all parents' lists.
	for _, f := range s.Fields {
		if f.ParentId == 0 {
			continue
		}
		if err := s.linkParent(f.ParentId, f); err != nil {
			return err
		}
	}

	// finalize in reverse order to rollup leaf child schema hashes first
	for _, f := range slices.Backward(s.Fields) {
		if f.Child != nil {
			f.Child.Finalize()
		}
		if f.Cases != nil {
			for _, cf := range *f.Cases {
				cf.Finalize()
			}
		}
	}

	// fill in computed fields
	s.Finalize()

	return s.Validate()
}

func (s *Schema) linkParent(pid uint16, f *Field) error {
	// find the parent field
	p, ok := s.FindId(pid)
	if !ok {
		return ErrInvalidParent
	}

	// check parent type (not all can be containers)
	switch p.Type {
	case List, Map, Union, Variant:
		// ok
	default:
		return ErrInvalidParent
	}

	// create a child schema if none exists yet
	if p.Child == nil {
		p.Child = &Schema{
			Version: s.Version,
			Fields:  make([]*Field, 0),
		}
	}
	p.Child.Fields = append(p.Child.Fields, f)

	// assign to variant case in direct parent
	if f.CaseId > 0 && pid == f.ParentId {
		cs := p.EnsureCase(f.CaseId)
		cs.Fields = append(cs.Fields, f)
	}

	// propagate to parent if set
	if p.ParentId > 0 {
		return s.linkParent(p.ParentId, f)
	}
	return nil
}

// Export creates a JSON/YAML/etc marshaler compatible version of a schema.
func (s *Schema) Export() map[string]any {
	root := map[string]any{
		"name":    s.Name,
		"version": s.Version,
	}
	fields := make([]any, 0, len(s.Fields))
	for _, f := range s.Fields {
		fields = append(fields, f.Export())
	}
	root["fields"] = fields
	return root
}

func (f *Field) Export() map[string]any {
	node := make(map[string]any, 10)
	node["id"] = f.Id
	node["name"] = f.Name
	node["type"] = uint8(f.Type)

	// optionals
	if f.ParentId > 0 {
		node["parent"] = f.ParentId
	}
	if f.Flags > 0 {
		node["flags"] = uint8(f.Flags)
	}
	if f.Filter > 0 {
		node["filter"] = uint8(f.Filter)
	}
	if f.Compress > 0 {
		node["compress"] = uint8(f.Compress)
	}
	if f.Scale > 0 {
		node["scale"] = f.Scale
	}
	if f.Level > 0 {
		node["level"] = f.Level
	}
	if f.CaseId > 0 {
		node["case"] = f.CaseId
	}

	return node
}

func (s *Schema) Import(m map[string]any) error {
	s.Name = m["name"].(string)
	s.Version = uint32(readInt(m["version"]))

	for _, v := range m["fields"].([]any) {
		fm := v.(map[string]any)
		f := NewField(
			FieldType(readInt(fm["type"])),
			WithName(fm["name"].(string)),
		)
		f.Id = uint16(readInt(fm["id"]))
		if v, ok := fm["parent"]; ok {
			f.ParentId = uint16(readInt(v))
		}
		if v, ok := fm["flags"]; ok {
			f.Flags = FieldFlags(readInt(v))
		}
		if v, ok := fm["filter"]; ok {
			f.Filter = FilterType(readInt(v))
		}
		if v, ok := fm["compress"]; ok {
			f.Compress = Compression(readInt(v))
		}
		if v, ok := fm["scale"]; ok {
			f.Scale = uint8(readInt(v))
		}
		if v, ok := fm["level"]; ok {
			f.Level = uint8(readInt(v))
		}
		if v, ok := fm["case"]; ok {
			f.CaseId = uint8(readInt(v))
		}

		// alloc empty enum dict to satisfy field validity
		if f.Type == Enum {
			f.Enum = enum.NewDictionary(f.Name)
		}

		s.Fields = append(s.Fields, f)
	}

	// link nested fields to parents
	for _, f := range s.Fields {
		if f.ParentId == 0 {
			continue
		}
		if err := s.linkParent(f.ParentId, f); err != nil {
			return err
		}
	}

	// finalize in reverse order to rollup leaf child schema hashes first
	for _, f := range slices.Backward(s.Fields) {
		if f.Child != nil {
			f.Child.Finalize()
		}
		if f.Cases != nil {
			for _, cf := range *f.Cases {
				cf.Finalize()
			}
		}
	}

	// fill in computed fields
	s.Finalize()

	return s.Validate()
}

func readInt(v any) int {
	if v == nil {
		return 0
	}
	switch val := v.(type) {
	case float64:
		return int(val)
	case int64:
		return int(val)
	case uint32:
		return int(val)
	case uint16:
		return int(val)
	case uint8:
		return int(val)
	case string:
		n, err := strconv.Atoi(val)
		if err != nil {
			panic(err)
		}
		return n
	default:
		panic(fmt.Errorf("import: unsupported type %T", v))
	}
}
