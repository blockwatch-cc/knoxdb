// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package schema

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"reflect"
	"slices"
	"sort"
	"strconv"
	"strings"

	"blockwatch.cc/knoxdb/internal/hash/xxhash64"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/util"
)

const (
	MAX_FIXED = uint16(1<<16 - 1)

	defaultVarFieldSize = 32 // variable number of bytes for strings and byte slices
)

type Schema struct {
	Name        string
	Hash        uint64
	Fields      []*Field
	Enums       *EnumRegistry
	MinWireSize int
	MaxWireSize int
	IsFixedSize bool
	Version     uint32
	Encode      []OpCode
	Decode      []OpCode
}

func NewSchema() *Schema {
	return &Schema{
		Fields:      make([]*Field, 0),
		IsFixedSize: true,
	}
}

func (s *Schema) WithName(n string) *Schema {
	if len(n) > 0 {
		s.Name = n
	}
	return s
}

func (s *Schema) As(alias string) *Schema {
	s.Name = alias
	return s
}

func (s *Schema) WithVersion(v uint32) *Schema {
	if s.Version < v {
		s.Version = v
	}
	return s
}

func (s *Schema) WithField(f *Field) *Schema {
	if f.IsValid() {
		f.Id = s.nextFieldId()
		s.Fields = append(s.Fields, f)
		s.Encode, s.Decode = nil, nil
	}
	return s
}

func (s *Schema) WithEnums(r *EnumRegistry) *Schema {
	s.Enums = r
	return s
}

func (s *Schema) nextFieldId() uint16 {
	id := uint16(len(s.Fields) + 1)
	if id == 1<<16-1 {
		return 0
	}
	for {
		_, ok := s.FieldById(id)
		if !ok {
			return id
		}
		id++
	}
}

func (s *Schema) HasEnums() bool {
	return s.Enums != nil
}

func (s *Schema) NewBuffer(sz int) *bytes.Buffer {
	return bytes.NewBuffer(make([]byte, 0, sz*s.MaxWireSize))
}

func (s *Schema) IsValid() bool {
	return len(s.Name) != 0 && len(s.Fields) != 0 && len(s.Encode) != 0
}

func (s *Schema) TypeLabel(vendor string) string {
	var b strings.Builder
	if vendor != "" {
		b.WriteString(vendor)
		b.WriteByte('.')
	}
	b.WriteString(s.Name)
	b.WriteString(".v")
	b.WriteString(strconv.Itoa(int(s.Version)))
	return b.String()
}

func (s *Schema) TaggedHash(tag types.ObjectTag) uint64 {
	return types.TaggedHash(tag, s.Name)
}

func (s *Schema) Equal(x *Schema) bool {
	return s != nil && x != nil && s.Hash == x.Hash
}

func (s *Schema) WireSize() int {
	return s.MinWireSize
}

func (s *Schema) AverageSize() int {
	return s.MaxWireSize
}

func (s *Schema) NumFields() int {
	return len(s.Fields)
}

func (s *Schema) NumActiveFields() int {
	var n int
	for _, f := range s.Fields {
		if f.IsActive() {
			n++
		}
	}
	return n
}

func (s *Schema) NumVisibleFields() int {
	var n int
	for _, f := range s.Fields {
		if f.IsVisible() {
			n++
		}
	}
	return n
}

func (s *Schema) NumMetaFields() int {
	var n int
	for _, f := range s.Fields {
		if f.IsMeta() && f.IsActive() {
			n++
		}
	}
	return n
}

func (s *Schema) AllFieldNames() []string {
	list := make([]string, len(s.Fields))
	for i, f := range s.Fields {
		list[i] = f.Name
	}
	return list
}

func (s *Schema) ActiveFieldNames() []string {
	list := make([]string, 0, len(s.Fields))
	for _, f := range s.Fields {
		if f.IsActive() {
			list = append(list, f.Name)
		}
	}
	return list
}

func (s *Schema) VisibleFieldNames() []string {
	list := make([]string, 0, len(s.Fields))
	for _, f := range s.Fields {
		if f.IsVisible() {
			list = append(list, f.Name)
		}
	}
	return list
}

func (s *Schema) MetaFieldNames() []string {
	list := make([]string, 0, len(s.Fields))
	for _, f := range s.Fields {
		if f.IsMeta() && f.IsActive() {
			list = append(list, f.Name)
		}
	}
	return list
}

func (s *Schema) EnumFieldNames() []string {
	list := make([]string, 0)
	for _, f := range s.Fields {
		if f.IsEnum() {
			list = append(list, f.Name)
		}
	}
	return list
}

func (s *Schema) AllFieldIds() []uint16 {
	list := make([]uint16, len(s.Fields))
	for i, f := range s.Fields {
		list[i] = f.Id
	}
	return list
}

func (s *Schema) ActiveFieldIds() []uint16 {
	list := make([]uint16, 0, len(s.Fields))
	for _, f := range s.Fields {
		if f.IsActive() {
			list = append(list, f.Id)
		}
	}
	return list
}

func (s *Schema) VisibleFieldIds() []uint16 {
	list := make([]uint16, 0, len(s.Fields))
	for _, f := range s.Fields {
		if f.IsVisible() {
			list = append(list, f.Id)
		}
	}
	return list
}

func (s *Schema) MetaFieldIds() []uint16 {
	list := make([]uint16, 0, len(s.Fields))
	for _, f := range s.Fields {
		if f.IsMeta() && f.IsActive() {
			list = append(list, f.Id)
		}
	}
	return list
}

func (s *Schema) FieldByName(name string) (f *Field, ok bool) {
	for _, v := range s.Fields {
		if v.Name == name && v.IsActive() {
			ok = true
			f = v
			break
		}
	}
	return
}

func (s *Schema) FieldById(id uint16) (f *Field, ok bool) {
	for _, v := range s.Fields {
		if v.Id == id {
			ok = true
			f = v
			break
		}
	}
	return
}

// func (s *Schema) FieldByIndex(i int) (f *Field, ok bool) {
// 	if i < len(s.Fields) {
// 		return s.Fields[i], true
// 	}
// 	return
// }

func (s *Schema) FieldIndexByName(name string) (idx int, ok bool) {
	for i, f := range s.Fields {
		if f.Name == name && f.IsActive() {
			ok = true
			idx = i
			break
		}
	}
	return
}

func (s *Schema) FieldIndexById(id uint16) (idx int, ok bool) {
	for i, f := range s.Fields {
		if f.Id == id {
			ok = true
			idx = i
			break
		}
	}
	return
}

func (s *Schema) Pk() *Field {
	for _, f := range s.Fields {
		if f.IsPrimary() && f.IsActive() {
			return f
		}
	}
	return &Field{} // nil?
}

// func (s *Schema) CompositePk() []*Field {
// 	res := make([]*Field, 0)
// 	for _, v := range s.Fields {
// 		if v.index.Is(types.IndexTypeComposite) && v.IsActive() {
// 			res = append(res, v)
// 		}
// 	}
// 	return res
// }

func (s *Schema) PkId() uint16 {
	for _, f := range s.Fields {
		if f.IsPrimary() && f.IsActive() {
			return f.Id
		}
	}
	return 0
}

func (s *Schema) PkIndex() int {
	for i, f := range s.Fields {
		if f.IsPrimary() && f.IsActive() {
			return i
		}
	}
	return -1
}

func (s *Schema) RowIdIndex() int {
	for i, f := range s.Fields {
		if f.Id == MetaRid && f.IsMeta() && f.IsActive() {
			return i
		}
	}
	return -1
}

// TODO: return []*IndexInfo
func (s *Schema) Indexes() (list []*Field) {
	for _, f := range s.Fields {
		if f.IsIndexed() && f.IsActive() {
			list = append(list, f)
		}
	}
	return
}

func (s *Schema) Clone() *Schema {
	return &Schema{
		Name:    s.Name,
		Fields:  slices.Clone(s.Fields),
		Enums:   s.Enums,
		Version: s.Version,
	}
}

func (s *Schema) AddField(f *Field) (*Schema, error) {
	if err := f.Validate(); err != nil {
		return nil, err
	}
	// ensure field is unique
	if _, ok := s.FieldByName(f.Name); ok {
		return nil, ErrDuplicateName
	}
	clone := s.Clone()
	f.Id = clone.nextFieldId()
	clone.Fields = append(clone.Fields, f)
	clone.Version++
	return clone.Finalize(), nil
}

func (s *Schema) DeleteField(id uint16) (*Schema, error) {
	for i, v := range s.Fields {
		if v.Id != id {
			continue
		}
		if !v.IsActive() {
			return nil, ErrInvalidField
		}
		if v.IsPrimary() {
			return nil, ErrDeletePrimary
		}
		if v.IsIndexed() {
			return nil, ErrDeleteIndexed
		}
		// delete changes schema version
		clone := s.Clone()
		clone.Fields[i] = clone.Fields[i].Clone()
		clone.Fields[i].Flags |= types.FieldFlagDeleted
		clone.Version++
		return clone.Finalize(), nil
	}
	return nil, ErrInvalidField
}

func (s *Schema) RenameField(id uint16, name string) (*Schema, error) {
	// check pre-conditions
	var pos = -1
	for i, v := range s.Fields {
		// ensure name is unique
		if v.Name == name {
			return nil, ErrDuplicateName
		}
		if v.Id == id {
			// cannot rename deleted fields
			if !v.IsActive() {
				return nil, ErrInvalidField
			}
			// enums are connected to named dictionaries and cannot be changed
			if v.IsEnum() {
				return nil, ErrRenameEnum
			}
			pos = i
		}
	}
	if pos < 0 {
		return nil, ErrInvalidField
	}

	// clone but don't update version & hash
	clone := s.Clone()
	clone.Fields[pos] = clone.Fields[pos].Clone()
	clone.Fields[pos].Name = name
	return clone.Finalize(), nil
}

// switch primary key field to id if exists
func (s *Schema) ResetPk(id uint16) (*Schema, bool) {
	oldPkIdx := s.PkIndex()
	newPkIdx, ok := s.FieldIndexById(id)
	if !ok || s.Fields[newPkIdx].Type != FT_U64 {
		return s, false
	}
	// flip primary key flag
	if oldPkIdx >= 0 {
		s.Fields[oldPkIdx].Flags &^= types.FieldFlagPrimary
	}
	s.Fields[newPkIdx].Flags |= types.FieldFlagPrimary
	return s, true
}

func (s *Schema) CanMatchFields(names ...string) bool {
	if len(names) == 0 || len(names) > len(s.Fields) {
		return false
	}
	for _, name := range names {
		var ok bool
		for _, f := range s.Fields {
			if f.Name == name && f.IsActive() {
				ok = true
				break
			}
		}
		if !ok {
			return false
		}
	}
	return true
}

func (s *Schema) CanSelect(x *Schema) error {
	if x == nil {
		return ErrNilValue
	}
	for _, xf := range x.Fields {
		sf, ok := s.FieldByName(xf.Name)
		if !ok {
			return fmt.Errorf("%w: missing field %s", ErrSchemaMismatch, xf.Name)
		}
		if xf.Type != sf.Type {
			return fmt.Errorf("%w on field %s: type mismatch have=%s want=%s",
				ErrSchemaMismatch, xf.Name, sf.Type, xf.Type)
		}
	}
	return nil
}

func (s *Schema) SelectSchema(x *Schema) (*Schema, error) {
	return s.SelectFields(x.ActiveFieldNames()...)
}

func (s *Schema) SelectFieldIds(fieldIds ...uint16) (*Schema, error) {
	ns := &Schema{
		Fields:      make([]*Field, 0, len(fieldIds)),
		IsFixedSize: true,
		Version:     s.Version,
		Name:        s.Name + "-select",
	}

	for _, fid := range fieldIds {
		f, ok := s.FieldById(fid)
		if !ok || !f.IsActive() {
			return nil, fmt.Errorf("%w: missing field id %d in schema %s", ErrInvalidField, fid, s.Name)
		}
		ns.Fields = append(ns.Fields, f)
	}

	return ns.Finalize(), nil
}

func (s *Schema) SelectFields(fields ...string) (*Schema, error) {
	ns := &Schema{
		Fields:      make([]*Field, 0, len(fields)),
		IsFixedSize: true,
		Version:     s.Version,
		Name:        s.Name + "-select",
	}

	for _, fname := range fields {
		f, ok := s.FieldByName(fname)
		if !ok {
			return nil, fmt.Errorf("%w: missing field name %s in schema %s", ErrInvalidField, fname, s.Name)
		}
		ns.Fields = append(ns.Fields, f)
	}

	return ns.Finalize(), nil
}

func (s *Schema) PkIndexSchema() (*Schema, error) {
	return s.SelectFieldIds(s.PkId(), MetaRid)
}

func (s *Schema) Sort() *Schema {
	sort.Slice(s.Fields, func(i, j int) bool { return s.Fields[i].Id < s.Fields[j].Id })
	s.Encode = nil
	s.Decode = nil
	s.Finalize()
	return s
}

// Returns a field position mapping for child schema dst that maps child
// fields to source schema field positions. Iterating over child fields and
// using this mapping yields the order in which source schema data is encoded or
// layed out in storage containers (i.e. packages of blocks/vectors),
func (s *Schema) MapTo(dst *Schema) ([]int, error) {
	maps := make([]int, 0, len(dst.Fields))
	for _, dstField := range dst.Fields {
		var (
			srcField *Field
			pos      = -1
		)
		for i, f := range s.Fields {
			if dstField.Name == f.Name {
				srcField = f
				// hide inactive source fields
				if f.IsActive() {
					pos = i
				}
				break
			}
		}

		if pos > -1 {
			if srcField.Type != dstField.Type {
				return nil, fmt.Errorf("%w on %s: field %q type %s mismatch with source type %s",
					ErrSchemaMismatch, dst.Name, dstField.Name, dstField.Type, srcField.Type)
			}
			if srcField.Fixed != dstField.Fixed {
				return nil, fmt.Errorf("%w on %s: field %q fixed type mismatch",
					ErrSchemaMismatch, dst.Name, dstField.Name)
			}
			if srcField.Scale != dstField.Scale {
				return nil, fmt.Errorf("%w on %s: field %q scale mismatch",
					ErrSchemaMismatch, dst.Name, dstField.Name)
			}
		}
		maps = append(maps, pos)
	}
	return maps, nil
}

func (s *Schema) Validate() error {
	// require name
	if s.Name == "" {
		return fmt.Errorf("missing schema name")
	}

	if len(s.Fields) == 0 {
		return fmt.Errorf("empty schema, no supported fields found")
	}

	// count special fields, require no duplicate names and ids
	uniqueNames := make(map[string]struct{})
	uniqueIds := make(map[uint16]struct{})

	for i := range s.Fields {
		// fields must validate
		if err := s.Fields[i].Validate(); err != nil {
			return fmt.Errorf("schema %s: field %s: %v", s.Name, s.Fields[i].Name, err)
		}

		// check name uniqueness
		n := s.Fields[i].Name
		if _, ok := uniqueNames[n]; ok {
			return fmt.Errorf("schema %s: duplicate field name %s", s.Name, n)
		} else {
			uniqueNames[n] = struct{}{}
		}

		// check id uniqueness
		id := s.Fields[i].Id
		if _, ok := uniqueIds[id]; ok {
			return fmt.Errorf("schema %s: duplicate field id %d", s.Name, id)
		} else {
			uniqueIds[id] = struct{}{}
		}
	}

	// encode opcodes are defined for all fields
	if a, b := len(s.Fields), len(s.Encode); a > b {
		return fmt.Errorf("schema %s: %d fields without encoder opcodes", s.Name, a-b)
	}

	// decode opcodes are defined for all fields
	if a, b := len(s.Fields), len(s.Decode); a > b {
		return fmt.Errorf("schema %s: %d fields without decoder opcodes", s.Name, a-b)
	}

	return nil
}

func (s Schema) MarshalBinary() ([]byte, error) {
	buf := bytes.NewBuffer(make([]byte, 0, 32*len(s.Fields)+12+len(s.Name)))

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

	return buf.Bytes(), nil
}

func (s *Schema) UnmarshalBinary(b []byte) (err error) {
	if len(b) < 12 {
		return io.ErrShortBuffer
	}
	buf := bytes.NewBuffer(b)

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

	// fill in computed fields
	s.Finalize()
	return nil
}

func (s *Schema) Finalize() *Schema {
	if len(s.Encode) > 0 {
		return s
	}
	s.MinWireSize = 0
	s.MaxWireSize = 0
	s.IsFixedSize = true
	s.Hash = 0

	var b [4]byte

	// generate schema hash from visible fields
	h := xxhash64.New()
	LE.PutUint32(b[:], s.Version)
	h.Write(b[:])

	// check if we need to generate struct layout info
	var styp reflect.Type
	needLayout := len(s.Fields) > 0 && s.Fields[0].Path == nil
	if needLayout {
		styp = s.StructType() // use logical types here
	}

	for i, f := range s.Fields {
		// collect sizes from visible fields
		if f.IsVisible() {
			sz := f.WireSize()
			s.MinWireSize += sz
			s.MaxWireSize += sz
			if !f.IsFixedSize() {
				s.IsFixedSize = false
				s.MaxWireSize += defaultVarFieldSize
			}

			// hash id, type
			LE.PutUint16(b[:], f.Id)
			h.Write(b[:2])
			h.Write([]byte{byte(f.Type)})

			// fill struct type info
			if needLayout {
				sf := styp.Field(i)
				f.Path = sf.Index
				f.Offset = sf.Offset
			}
		}

		// try lookup enum from global registry using tag '0' or generate new enum
		if f.Is(types.FieldFlagEnum) {
			if s.Enums == nil {
				r := NewEnumRegistry()
				s.Enums = &r
			}
			if _, ok := s.Enums.Lookup(f.Name); !ok {
				if e, ok := LookupEnum(0, f.Name); ok {
					s.Enums.Register(e)
				} else {
					s.Enums.Register(NewEnumDictionary(f.Name))
				}
			}
		}
	}
	s.Hash = h.Sum64()
	s.Encode, s.Decode = compileCodecs(s)
	if s.Name == "" {
		s.Name = util.U64String(s.Hash).String()
	}

	return s
}

func (s *Schema) String() string {
	var b strings.Builder
	if s.IsFixedSize {
		fmt.Fprintf(&b, "%q fields=%d sz=%d (fixed)", s.Name, len(s.Fields), s.MinWireSize)
	} else {
		fmt.Fprintf(&b, "%q fields=%d sz_min=%d sz_max=%d",
			s.Name,
			len(s.Fields),
			s.MinWireSize,
			s.MaxWireSize,
		)
	}
	var maxNameLen int
	for _, f := range s.Fields {
		maxNameLen = max(maxNameLen, len(f.Name))
	}
	fmt.Fprintf(&b, "\n#  ID   %[1]*[2]s %-15s Flags", -maxNameLen-1, "Name", "Type")
	for i, f := range s.Fields {
		var typ string
		switch f.Type {
		case FT_TIME, FT_TIMESTAMP:
			typ = f.Type.String() + "(" + TimeScale(f.Scale).ShortName() + ")"
		case FT_D32, FT_D64, FT_D128, FT_D256:
			typ = f.Type.String() + "(" + strconv.Itoa(int(f.Scale)) + ")"
		case FT_STRING, FT_BYTES:
			if f.Fixed > 0 {
				typ = "[" + strconv.Itoa(int(f.Fixed)) + "]" + f.Type.String()
			}
		}
		if typ == "" {
			typ = f.Type.String()
		}
		flags := f.Flags.String()
		if f.Index != nil {
			flags += "," + f.Index.Type.String() + ":" + strconv.Itoa(int(f.Scale))
		}
		fmt.Fprintf(&b, "\n%02d F#%02d %[3]*[4]s %-15s %s",
			i,
			f.Id,
			-maxNameLen-1,
			f.Name,
			typ,
			flags,
		)
	}
	return b.String()
}
