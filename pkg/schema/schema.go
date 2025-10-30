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
	Indexes     []*IndexSchema
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
		_, ok := s.FindId(id)
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

func (s *Schema) Label() string {
	var b strings.Builder
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

func (s *Schema) NumActive() int {
	var n int
	for _, f := range s.Fields {
		if f.IsActive() {
			n++
		}
	}
	return n
}

func (s *Schema) NumVisible() int {
	var n int
	for _, f := range s.Fields {
		if f.IsVisible() {
			n++
		}
	}
	return n
}

func (s *Schema) NumMeta() int {
	var n int
	for _, f := range s.Fields {
		if f.IsMeta() && f.IsActive() {
			n++
		}
	}
	return n
}

func (s *Schema) Names() []string {
	list := make([]string, len(s.Fields))
	for i, f := range s.Fields {
		list[i] = f.Name
	}
	return list
}

func (s *Schema) ActiveNames() []string {
	list := make([]string, 0, len(s.Fields))
	for _, f := range s.Fields {
		if f.IsActive() {
			list = append(list, f.Name)
		}
	}
	return list
}

func (s *Schema) VisibleNames() []string {
	list := make([]string, 0, len(s.Fields))
	for _, f := range s.Fields {
		if f.IsVisible() {
			list = append(list, f.Name)
		}
	}
	return list
}

func (s *Schema) MetaNames() []string {
	list := make([]string, 0, len(s.Fields))
	for _, f := range s.Fields {
		if f.IsMeta() && f.IsActive() {
			list = append(list, f.Name)
		}
	}
	return list
}

func (s *Schema) EnumNames() []string {
	list := make([]string, 0)
	for _, f := range s.Fields {
		if f.IsEnum() {
			list = append(list, f.Name)
		}
	}
	return list
}

func (s *Schema) Ids() []uint16 {
	list := make([]uint16, len(s.Fields))
	for i, f := range s.Fields {
		list[i] = f.Id
	}
	return list
}

func (s *Schema) ActiveIds() []uint16 {
	list := make([]uint16, 0, len(s.Fields))
	for _, f := range s.Fields {
		if f.IsActive() {
			list = append(list, f.Id)
		}
	}
	return list
}

func (s *Schema) VisibleIds() []uint16 {
	list := make([]uint16, 0, len(s.Fields))
	for _, f := range s.Fields {
		if f.IsVisible() {
			list = append(list, f.Id)
		}
	}
	return list
}

func (s *Schema) MetaIds() []uint16 {
	list := make([]uint16, 0, len(s.Fields))
	for _, f := range s.Fields {
		if f.IsMeta() && f.IsActive() {
			list = append(list, f.Id)
		}
	}
	return list
}

func (s *Schema) Find(name string) (f *Field, ok bool) {
	for _, v := range s.Fields {
		if v.Name == name && v.IsActive() {
			ok = true
			f = v
			break
		}
	}
	return
}

func (s *Schema) FindId(id uint16) (f *Field, ok bool) {
	for _, v := range s.Fields {
		if v.Id == id {
			ok = true
			f = v
			break
		}
	}
	return
}

func (s *Schema) Index(name string) (idx int, ok bool) {
	for i, f := range s.Fields {
		if f.Name == name && f.IsActive() {
			ok = true
			idx = i
			break
		}
	}
	return
}

func (s *Schema) IndexId(id uint16) (idx int, ok bool) {
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
	return &Field{}
}

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

func (s *Schema) RowId() *Field {
	for _, f := range s.Fields {
		if f.Id == MetaRid && f.IsMeta() && f.IsActive() {
			return f
		}
	}
	return &Field{}
}

func (s *Schema) RowIdIndex() int {
	for i, f := range s.Fields {
		if f.Id == MetaRid && f.IsMeta() && f.IsActive() {
			return i
		}
	}
	return -1
}

func (s *Schema) Clone() *Schema {
	clone := &Schema{
		Name:    s.Name,
		Fields:  slices.Clone(s.Fields),
		Indexes: slices.Clone(s.Indexes),
		Enums:   s.Enums,
		Version: s.Version,
	}
	for i := range clone.Fields {
		clone.Fields[i] = clone.Fields[i].Clone()
	}
	for i := range clone.Indexes {
		clone.Indexes[i].Base = clone
		for k, v := range clone.Indexes[i].Fields {
			clone.Indexes[i].Fields[k], _ = clone.FindId(v.Id)
		}
		for k, v := range clone.Indexes[i].Extra {
			clone.Indexes[i].Extra[k], _ = clone.FindId(v.Id)
		}
	}
	return clone
}

func (s *Schema) AddField(f *Field) (*Schema, error) {
	if err := f.Validate(); err != nil {
		return nil, err
	}
	// ensure field is unique
	if _, ok := s.Find(f.Name); ok {
		return nil, ErrDuplicateName
	}
	clone := s.Clone()
	f.Id = clone.nextFieldId()
	clone.Fields = append(clone.Fields, f)
	clone.Version++
	return clone.Finalize(), nil
}

func (s *Schema) DeleteId(id uint16) (*Schema, error) {
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
		// delete changes schema version
		clone := s.Clone()
		clone.Fields[i] = clone.Fields[i].Clone()
		clone.Fields[i].Flags |= types.FieldFlagDeleted
		clone.Version++
		return clone.Finalize(), nil
	}
	return nil, ErrInvalidField
}

func (s *Schema) RenameId(id uint16, name string) (*Schema, error) {
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
	fnew, ok := s.FindId(id)
	if !ok || fnew.Type != FT_U64 {
		return s, false
	}
	clone := s.Clone()
	fold := clone.Pk()
	fnew, _ = clone.FindId(id)
	// flip primary key flag
	// FIXME: changes schema hash (effect on catalog?)
	fold.Flags &^= types.FieldFlagPrimary
	fnew.Flags |= types.FieldFlagPrimary
	return clone, true
}

func (s *Schema) CanMatch(names ...string) bool {
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

func (s *Schema) Contains(names ...string) bool {
	if len(names) == 0 {
		return false
	}
	for _, v := range names {
		if _, ok := s.Find(v); !ok {
			return false
		}
	}
	return true
}

func (s *Schema) ContainsSchema(x *Schema) bool {
	if x == nil {
		return false
	}
	for _, xf := range x.Fields {
		sf, ok := s.Find(xf.Name)
		if !ok {
			return false
		}
		if xf.Type != sf.Type {
			return false
		}
	}
	return true
}

func (s *Schema) SelectSchema(x *Schema) (*Schema, error) {
	return s.SelectIds(x.ActiveIds()...)
}

func (s *Schema) SelectIds(fieldIds ...uint16) (*Schema, error) {
	ns := &Schema{
		Fields:      make([]*Field, 0, len(fieldIds)),
		IsFixedSize: true,
		Version:     s.Version,
		Name:        s.Name + "-select",
	}

	for _, fid := range fieldIds {
		f, ok := s.FindId(fid)
		if !ok || !f.IsActive() {
			return nil, fmt.Errorf("missing field id %d in schema %s", fid, s.Name)
		}
		ns.Fields = append(ns.Fields, f)
	}

	return ns.Finalize(), nil
}

func (s *Schema) Select(fields ...string) (*Schema, error) {
	ns := &Schema{
		Fields:      make([]*Field, 0, len(fields)),
		IsFixedSize: true,
		Version:     s.Version,
		Name:        s.Name + "-select",
	}

	for _, fname := range fields {
		f, ok := s.Find(fname)
		if !ok {
			return nil, fmt.Errorf("%w: missing field name %s in schema %s", ErrInvalidField, fname, s.Name)
		}
		ns.Fields = append(ns.Fields, f)
	}

	return ns.Finalize(), nil
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
func (s *Schema) MapSchema(dst *Schema) ([]int, error) {
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
	var firstTimebase *Field

	for _, f := range s.Fields {
		// fields must validate
		if err := f.Validate(); err != nil {
			return fmt.Errorf("schema %s: field %s: %v", s.Name, f.Name, err)
		}

		// check name uniqueness
		if _, ok := uniqueNames[f.Name]; ok {
			return fmt.Errorf("schema %s: duplicate field name %s", s.Name, f.Name)
		} else {
			uniqueNames[f.Name] = struct{}{}
		}

		// check id uniqueness
		if _, ok := uniqueIds[f.Id]; ok {
			return fmt.Errorf("schema %s: duplicate field id %d", s.Name, f.Id)
		} else {
			uniqueIds[f.Id] = struct{}{}
		}

		// check timebase flag is unique
		if f.Flags.Is(F_TIMEBASE) {
			if firstTimebase != nil {
				return fmt.Errorf("schema %s: timebase flag on multiple fields %q and %q",
					s.Name, firstTimebase.Name, f.Name)
			} else {
				firstTimebase = f
			}
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

	// validate indexes if defined
	clear(uniqueNames)
	for _, v := range s.Indexes {
		if _, ok := uniqueNames[v.Name]; ok {
			return fmt.Errorf("schema %s: duplicate index %s", s.Name, v.Name)
		}
		uniqueNames[v.Name] = struct{}{}
		if err := v.Validate(); err != nil {
			return fmt.Errorf("schema %s: %v", s.Name, err)
		}
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

			// hash: id, type, flags, fixed, scale (not: filter, compress, name)
			LE.PutUint16(b[:], f.Id)
			h.Write(b[:2])
			h.Write([]byte{byte(f.Type)})
			h.Write([]byte{byte(f.Flags)})
			LE.PutUint16(b[:], f.Fixed)
			h.Write(b[:2])
			h.Write([]byte{f.Scale})

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
		if f.Filter > 0 {
			if flags != "" {
				flags += ","
			}
			flags += f.Filter.String()
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
