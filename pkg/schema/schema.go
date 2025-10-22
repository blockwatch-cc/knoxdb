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
	name        string
	schemaHash  uint64
	fields      []Field
	exports     []*ExportedField
	enums       *EnumRegistry
	minWireSize int
	maxWireSize int
	isFixedSize bool
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

func (s *Schema) WithName(n string) *Schema {
	if len(n) > 0 {
		s.name = n
	}
	return s
}

func (s *Schema) As(alias string) *Schema {
	s.name = alias
	return s
}

func (s *Schema) WithVersion(v uint32) *Schema {
	if s.version < v {
		s.version = v
	}
	return s
}

func (s *Schema) WithField(f Field) *Schema {
	if f.IsValid() {
		f.id = s.nextFieldId()
		s.fields = append(s.fields, f)
		s.encode, s.decode = nil, nil
	}
	return s
}

func (s *Schema) WithEnums(r *EnumRegistry) *Schema {
	s.enums = r
	return s
}

func (s *Schema) Enums() *EnumRegistry {
	return s.enums
}

func (s *Schema) nextFieldId() uint16 {
	id := uint16(len(s.fields) + 1)
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
	return s.enums != nil
}

func (s *Schema) NewBuffer(sz int) *bytes.Buffer {
	return bytes.NewBuffer(make([]byte, 0, sz*s.maxWireSize))
}

func (s *Schema) IsValid() bool {
	return len(s.name) != 0 && len(s.fields) != 0 && len(s.encode) != 0
}

func (s *Schema) IsFixedSize() bool {
	return s.isFixedSize
}

func (s *Schema) Name() string {
	return s.name
}

func (s *Schema) Version() uint32 {
	return s.version
}

func (s *Schema) TypeLabel(vendor string) string {
	var b strings.Builder
	if vendor != "" {
		b.WriteString(vendor)
		b.WriteByte('.')
	}
	b.WriteString(s.name)
	b.WriteString(".v")
	b.WriteString(strconv.Itoa(int(s.version)))
	return b.String()
}

func (s *Schema) TaggedHash(tag types.ObjectTag) uint64 {
	return types.TaggedHash(tag, s.name)
}

func (s *Schema) Hash() uint64 {
	return s.schemaHash
}

func (s *Schema) EqualHash(x uint64) bool {
	return x == 0 || s.schemaHash == x
}

func (s *Schema) WireSize() int {
	return s.minWireSize
}

func (s *Schema) AverageSize() int {
	return s.maxWireSize
}

func (s *Schema) NumFields() int {
	return len(s.fields)
}

func (s *Schema) NumActiveFields() int {
	var n int
	for i := range s.fields {
		if s.fields[i].IsActive() {
			n++
		}
	}
	return n
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

func (s *Schema) NumInternalFields() int {
	var n int
	for i := range s.fields {
		if s.fields[i].IsInternal() && s.fields[i].IsActive() {
			n++
		}
	}
	return n
}

func (s *Schema) AllFieldNames() []string {
	list := make([]string, len(s.fields))
	for i := range s.fields {
		list[i] = s.fields[i].name
	}
	return list
}

func (s *Schema) ActiveFieldNames() []string {
	list := make([]string, 0, len(s.fields))
	for i := range s.fields {
		if s.fields[i].IsActive() {
			list = append(list, s.fields[i].name)
		}
	}
	return list
}

func (s *Schema) VisibleFieldNames() []string {
	list := make([]string, 0, len(s.fields))
	for i := range s.fields {
		if s.fields[i].IsVisible() {
			list = append(list, s.fields[i].name)
		}
	}
	return list
}

func (s *Schema) InternalFieldNames() []string {
	list := make([]string, 0, len(s.fields))
	for i := range s.fields {
		if s.fields[i].IsInternal() && s.fields[i].IsActive() {
			list = append(list, s.fields[i].name)
		}
	}
	return list
}

func (s *Schema) EnumFieldNames() []string {
	list := make([]string, 0)
	for i := range s.fields {
		if s.fields[i].IsEnum() {
			list = append(list, s.fields[i].name)
		}
	}
	return list
}

func (s *Schema) AllFieldIds() []uint16 {
	list := make([]uint16, len(s.fields))
	for i := range s.fields {
		list[i] = s.fields[i].id
	}
	return list
}

func (s *Schema) ActiveFieldIds() []uint16 {
	list := make([]uint16, 0, len(s.fields))
	for i := range s.fields {
		if s.fields[i].IsActive() {
			list = append(list, s.fields[i].id)
		}
	}
	return list
}

func (s *Schema) VisibleFieldIds() []uint16 {
	list := make([]uint16, 0, len(s.fields))
	for i := range s.fields {
		if s.fields[i].IsVisible() {
			list = append(list, s.fields[i].id)
		}
	}
	return list
}

func (s *Schema) InternalFieldIds() []uint16 {
	list := make([]uint16, 0, len(s.fields))
	for i := range s.fields {
		if s.fields[i].IsInternal() && s.fields[i].IsActive() {
			list = append(list, s.fields[i].id)
		}
	}
	return list
}

func (s *Schema) Fields() []Field {
	return s.fields
}

func (s *Schema) Field(i int) *Field {
	return &s.fields[i]
}

func (s *Schema) Exported() []*ExportedField {
	return s.exports
}

func (s *Schema) FieldByName(name string) (f Field, ok bool) {
	for _, v := range s.fields {
		if v.name == name && v.IsActive() {
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

func (s *Schema) FieldByIndex(i int) (f Field, ok bool) {
	if i < len(s.fields) {
		return s.fields[i], true
	}
	return
}

func (s *Schema) FieldIndexByName(name string) (idx int, ok bool) {
	for i, v := range s.fields {
		if v.name == name && v.IsActive() {
			ok = true
			idx = i
			break
		}
	}
	return
}

func (s *Schema) FieldIndexById(id uint16) (idx int, ok bool) {
	for i, v := range s.fields {
		if v.id == id {
			ok = true
			idx = i
			break
		}
	}
	return
}

func (s *Schema) Pk() *Field {
	for _, v := range s.fields {
		if v.IsPrimary() && v.IsActive() {
			return &v
		}
	}
	return &Field{}
}

func (s *Schema) CompositePk() []Field {
	res := make([]Field, 0)
	for _, v := range s.fields {
		if v.index.Is(types.IndexTypeComposite) && v.IsActive() {
			res = append(res, v)
		}
	}
	return res
}

func (s *Schema) PkId() uint16 {
	for _, v := range s.fields {
		if v.IsPrimary() && v.IsActive() {
			return v.Id()
		}
	}
	return 0
}

func (s *Schema) PkIndex() int {
	for i, v := range s.fields {
		if v.IsPrimary() && v.IsActive() {
			return i
		}
	}
	return -1
}

func (s *Schema) RowIdIndex() int {
	for i, v := range s.fields {
		if v.id == MetaRid && v.IsInternal() && v.IsActive() {
			return i
		}
	}
	return -1
}

func (s *Schema) Indexes() (list []Field) {
	for _, v := range s.fields {
		if v.IsIndexed() && v.IsActive() {
			list = append(list, v)
		}
	}
	return
}

func (s *Schema) Clone() *Schema {
	return &Schema{
		name:    s.name,
		fields:  slices.Clone(s.fields),
		enums:   s.enums,
		version: s.version,
	}
}

func (s *Schema) AddField(f Field) (*Schema, error) {
	if err := f.Validate(); err != nil {
		return nil, err
	}
	// ensure field is unique
	if _, ok := s.FieldByName(f.name); ok {
		return nil, ErrDuplicateName
	}
	clone := s.Clone()
	f.id = clone.nextFieldId()
	clone.fields = append(clone.fields, f)
	clone.version++
	return clone.Finalize(), nil
}

func (s *Schema) DeleteField(id uint16) (*Schema, error) {
	for i, v := range s.fields {
		if v.id != id {
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
		clone.fields[i].flags |= types.FieldFlagDeleted
		clone.version++
		return clone.Finalize(), nil
	}
	return nil, ErrInvalidField
}

func (s *Schema) RenameField(id uint16, name string) (*Schema, error) {
	// check pre-conditions
	var pos = -1
	for i, v := range s.fields {
		// ensure name is unique
		if v.name == name {
			return nil, ErrDuplicateName
		}
		if v.id == id {
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
	clone.fields[pos].name = name
	return clone.Finalize(), nil
}

// switch primary key field to id if exists
func (s *Schema) ResetPk(id uint16) (*Schema, bool) {
	oldPkIdx := s.PkIndex()
	newPkIdx, ok := s.FieldIndexById(id)
	if !ok || s.fields[newPkIdx].typ != FT_U64 {
		return s, false
	}
	// flip primary key flag
	if oldPkIdx >= 0 {
		s.fields[oldPkIdx].flags &^= types.FieldFlagPrimary
		s.exports[oldPkIdx].Flags &^= types.FieldFlagPrimary
	}
	s.fields[newPkIdx].flags |= types.FieldFlagPrimary
	s.exports[newPkIdx].Flags |= types.FieldFlagPrimary
	return s, true
}

func (s *Schema) CanMatchFields(names ...string) bool {
	if len(names) == 0 || len(names) > len(s.fields) {
		return false
	}
	for _, name := range names {
		var ok bool
		for i := range s.fields {
			if s.fields[i].name == name && s.fields[i].IsActive() {
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
	for _, xf := range x.fields {
		sf, ok := s.FieldByName(xf.name)
		if !ok {
			return fmt.Errorf("%w: missing field %s", ErrSchemaMismatch, xf.name)
		}
		if xf.typ != sf.typ {
			return fmt.Errorf("%w on field %s: type mismatch have=%s want=%s",
				ErrSchemaMismatch, xf.name, sf.typ, xf.typ)
		}
	}
	return nil
}

func (s *Schema) SelectSchema(x *Schema) (*Schema, error) {
	return s.SelectFields(x.ActiveFieldNames()...)
}

func (s *Schema) SelectFieldIds(fieldIds ...uint16) (*Schema, error) {
	ns := &Schema{
		fields:      make([]Field, 0, len(fieldIds)),
		isFixedSize: true,
		version:     s.version,
		name:        s.name + "-select",
	}

	for _, fid := range fieldIds {
		f, ok := s.FieldById(fid)
		if !ok || !f.IsActive() {
			return nil, fmt.Errorf("%w: missing field id %d in schema %s", ErrInvalidField, fid, s.name)
		}
		ns.fields = append(ns.fields, f)
	}

	return ns.Finalize(), nil
}

func (s *Schema) SelectFields(fields ...string) (*Schema, error) {
	ns := &Schema{
		fields:      make([]Field, 0, len(fields)),
		isFixedSize: true,
		version:     s.version,
		name:        s.name + "-select",
	}

	for _, fname := range fields {
		f, ok := s.FieldByName(fname)
		if !ok {
			return nil, fmt.Errorf("%w: missing field name %s in schema %s", ErrInvalidField, fname, s.name)
		}
		ns.fields = append(ns.fields, f)
	}

	return ns.Finalize(), nil
}

func (s *Schema) PkIndexSchema() (*Schema, error) {
	return s.SelectFieldIds(s.PkId(), MetaRid)
}

func (s *Schema) Sort() *Schema {
	sort.Slice(s.fields, func(i, j int) bool { return s.fields[i].id < s.fields[j].id })
	s.encode = nil
	s.decode = nil
	s.Finalize()
	return s
}

// Returns a field position mapping for child schema dst that maps child
// fields to source schema field positions. Iterating over child fields and
// using this mapping yields the order in which source schema data is encoded or
// layed out in storage containers (i.e. packages of blocks/vectors),
func (s *Schema) MapTo(dst *Schema) ([]int, error) {
	maps := make([]int, 0, len(dst.fields))
	for _, dstField := range dst.fields {
		var (
			srcField Field
			pos      = -1
		)
		for i, f := range s.fields {
			if dstField.name == f.name {
				srcField = f
				// hide inactive source fields
				if f.IsActive() {
					pos = i
				}
				break
			}
		}

		if pos > -1 {
			if srcField.typ != dstField.typ {
				return nil, fmt.Errorf("%w on %s: field %s type %s mismatch with source type %s",
					ErrSchemaMismatch, dst.name, dstField.name, dstField.typ, srcField.typ)
			}
			if srcField.fixed != dstField.fixed {
				return nil, fmt.Errorf("%w on %s: field %s fixed type mismatch",
					ErrSchemaMismatch, dst.name, dstField.name)
			}
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

	// count special fields, require no duplicate names and ids
	uniqueNames := make(map[string]struct{})
	uniqueIds := make(map[uint16]struct{})

	for i := range s.fields {
		// fields must validate
		if err := s.fields[i].Validate(); err != nil {
			return fmt.Errorf("schema %s: field %s: %v", s.name, s.fields[i].name, err)
		}

		// check name uniqueness
		n := s.fields[i].name
		if _, ok := uniqueNames[n]; ok {
			return fmt.Errorf("schema %s: duplicate field name %s", s.name, n)
		} else {
			uniqueNames[n] = struct{}{}
		}

		// check id uniqueness
		id := s.fields[i].id
		if _, ok := uniqueIds[id]; ok {
			return fmt.Errorf("schema %s: duplicate field id %d", s.name, id)
		} else {
			uniqueIds[id] = struct{}{}
		}
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

func (s Schema) MarshalBinary() ([]byte, error) {
	buf := bytes.NewBuffer(make([]byte, 0, 32*len(s.fields)+12+len(s.name)))

	// version: u32
	binary.Write(buf, LE, s.version)

	// name: string
	binary.Write(buf, LE, uint32(len(s.name)))
	buf.WriteString(s.name)

	// fields
	binary.Write(buf, LE, uint32(len(s.fields)))
	for i := range s.fields {
		s.fields[i].WriteTo(buf)
	}

	return buf.Bytes(), nil
}

func (s *Schema) UnmarshalBinary(b []byte) (err error) {
	if len(b) < 12 {
		return io.ErrShortBuffer
	}
	buf := bytes.NewBuffer(b)

	// version: u32
	err = binary.Read(buf, LE, &s.version)
	if err != nil {
		return
	}

	// name: string
	var l uint32
	err = binary.Read(buf, LE, &l)
	if err != nil {
		return
	}
	s.name = string(buf.Next(int(l)))
	if len(s.name) != int(l) {
		return io.ErrShortBuffer
	}

	// fields
	err = binary.Read(buf, LE, &l)
	if err != nil {
		return
	}
	s.fields = make([]Field, l)
	for i := range s.fields {
		err = s.fields[i].ReadFrom(buf)
		if err != nil {
			return
		}
	}

	// fill in computed fields
	s.Finalize()
	return nil
}

func (s *Schema) Finalize() *Schema {
	if len(s.encode) > 0 {
		return s
	}
	s.minWireSize = 0
	s.maxWireSize = 0
	s.isFixedSize = true
	s.schemaHash = 0

	var b [4]byte

	// generate schema hash from visible fields
	h := xxhash64.New()
	LE.PutUint32(b[:], s.version)
	h.Write(b[:])

	// check if we need to generate struct layout info
	var styp reflect.Type
	needLayout := len(s.fields) > 0 && s.fields[0].path == nil
	if needLayout {
		styp = s.StructType() // use logical types here
	}

	for i := range s.fields {
		// collect sizes from visible fields
		if s.fields[i].IsVisible() {
			sz := s.fields[i].WireSize()
			s.minWireSize += sz
			s.maxWireSize += sz
			if !s.fields[i].IsFixedSize() {
				s.isFixedSize = false
				s.maxWireSize += defaultVarFieldSize
			}

			// hash id, type
			LE.PutUint16(b[:], s.fields[i].id)
			h.Write(b[:2])
			h.Write([]byte{byte(s.fields[i].typ)})

			// fill struct type info
			if needLayout {
				sf := styp.Field(i)
				s.fields[i].path = sf.Index
				s.fields[i].offset = sf.Offset
			}
		}

		// try lookup enum from global registry using tag '0' or generate new enum
		if s.fields[i].Is(types.FieldFlagEnum) {
			if s.enums == nil {
				r := NewEnumRegistry()
				s.enums = &r
			}
			if _, ok := s.enums.Lookup(s.fields[i].name); !ok {
				if e, ok := LookupEnum(0, s.fields[i].name); ok {
					s.enums.Register(e)
				} else {
					s.enums.Register(NewEnumDictionary(s.fields[i].name))
				}
			}
		}
	}
	s.schemaHash = h.Sum64()
	s.encode, s.decode = compileCodecs(s)
	if s.name == "" {
		s.name = util.U64String(s.schemaHash).String()
	}

	// export all fields
	s.exports = make([]*ExportedField, len(s.fields))
	for i := range s.fields {
		s.exports[i] = &ExportedField{
			Name:       s.fields[i].name,
			Id:         s.fields[i].id,
			Type:       s.fields[i].typ,
			Flags:      s.fields[i].flags,
			Compress:   s.fields[i].compress,
			Index:      s.fields[i].index,
			IsVisible:  s.fields[i].IsVisible(),
			IsNullable: s.fields[i].IsNullable(),
			IsInternal: s.fields[i].IsInternal(),
			IsEnum:     s.fields[i].IsEnum(),
			Scale:      s.fields[i].scale,
			Fixed:      s.fields[i].fixed,
			Offset:     s.fields[i].offset,
			path:       s.fields[i].path,
		}
	}

	return s
}

func (s *Schema) String() string {
	var b strings.Builder
	if s.isFixedSize {
		fmt.Fprintf(&b, "%q fields=%d sz=%d (fixed)", s.name, len(s.fields), s.minWireSize)
	} else {
		fmt.Fprintf(&b, "%q fields=%d sz_min=%d sz_max=%d",
			s.name,
			len(s.fields),
			s.minWireSize,
			s.maxWireSize,
		)
	}
	var maxNameLen int
	for i := range s.fields {
		maxNameLen = max(maxNameLen, len(s.fields[i].name))
	}
	fmt.Fprintf(&b, "\n#  ID   %[1]*[2]s %-15s Flags", -maxNameLen-1, "Name", "Type")
	for i := range s.fields {
		f := &s.fields[i]
		var typ string
		switch f.typ {
		case FT_TIME, FT_TIMESTAMP:
			typ = f.typ.String() + "(" + TimeScale(f.scale).ShortName() + ")"
		case FT_D32, FT_D64, FT_D128, FT_D256:
			typ = f.typ.String() + "(" + strconv.Itoa(int(f.scale)) + ")"
		case FT_STRING, FT_BYTES:
			if f.fixed > 0 {
				typ = "[" + strconv.Itoa(int(f.fixed)) + "]" + f.typ.String()
			}
		}
		if typ == "" {
			typ = f.typ.String()
		}
		flags := f.flags.String()
		if f.index > 0 {
			flags += "," + f.index.String() + ":" + strconv.Itoa(int(f.scale))
		}
		fmt.Fprintf(&b, "\n%02d F#%02d %[3]*[4]s %-15s %s",
			i,
			f.id,
			-maxNameLen-1,
			f.name,
			typ,
			flags,
		)
	}
	return b.String()
}
