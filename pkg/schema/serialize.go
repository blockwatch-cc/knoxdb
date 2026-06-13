// Copyright (c) 2024-2026 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package schema

import (
	"bytes"
	"encoding/binary"
	"io"
	"slices"
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
