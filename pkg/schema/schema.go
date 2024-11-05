// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package schema

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"slices"
	"sort"
	"strconv"
	"strings"

	"blockwatch.cc/knoxdb/internal/hash/fnv"
	"blockwatch.cc/knoxdb/internal/types"
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
	minWireSize int
	maxWireSize int
	isFixedSize bool
	isInterface bool
	version     uint32
	encode      []OpCode
	decode      []OpCode
	enums       EnumRegistry
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

func (s *Schema) WithEnum(e *EnumDictionary) *Schema {
	if e != nil {
		for i, f := range s.fields {
			if !f.Is(types.FieldFlagEnum) {
				continue
			}
			if e.Name() != f.name {
				continue
			}
			s.fields[i].enum = e
			s.enums.Register(e)
		}
	}
	return s
}

func (s *Schema) WithEnumsFrom(r EnumRegistry) *Schema {
	for i, f := range s.fields {
		if !f.Is(types.FieldFlagEnum) {
			continue
		}
		e, ok := r.Lookup(f.name)
		if !ok {
			continue
		}
		s.fields[i].enum = e
		s.enums.Register(e)
	}
	return s
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
	if len(s.fields) < i {
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
		version: s.version,
		enums:   s.enums,
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
	var pos int = -1
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

func (s *Schema) MarkIndexField(id uint16, typ types.IndexType) (*Schema, error) {
	for i, v := range s.fields {
		if v.id != id {
			continue
		}
		if !v.IsActive() {
			return nil, ErrInvalidField
		}
		// clone but don't update version & hash
		clone := s.Clone()
		clone.fields[i] = clone.fields[i].WithIndex(typ)
		return clone.Finalize(), nil
	}
	return nil, ErrInvalidField
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

func (s *Schema) Sort() *Schema {
	sort.Slice(s.fields, func(i, j int) bool { return s.fields[i].id < s.fields[j].id })
	s.encode = nil
	s.decode = nil
	s.Finalize()
	return s
}

// Returns a field position mapping for a child schema that maps child schema
// fields to source schema field positions. Iterating over child fields and
// using this mapping yields the order in which source schema data is encoded or
// layed out in storage containers (i.e. packages of blocks/vectors),
func (s *Schema) MapTo(dst *Schema) ([]int, error) {
	maps := make([]int, 0, len(dst.fields))
	for _, dstField := range dst.fields {
		var (
			srcField Field
			pos      int = -1
		)
		for i, f := range s.fields {
			if dstField.name == f.name {
				srcField = f
				// hide invisible source fields
				if f.IsVisible() {
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
	buf := bytes.NewBuffer(make([]byte, 0, 32*len(s.fields)+8+len(s.name)))

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
	s.isInterface = false
	s.schemaHash = 0

	// generate schema hash from visible fields
	h := fnv.New64a()
	h.Write(Uint32Bytes(uint32(s.version)))

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
			h.Write(Uint16Bytes(s.fields[i].id))
			h.Write([]byte{byte(s.fields[i].typ)})

			// cache whether we need interface access
			s.isInterface = s.isInterface || s.fields[i].IsInterface()
		}

		// try lookup enum from global registry using tag '0' or generate new enum
		if s.fields[i].Is(types.FieldFlagEnum) {
			if s.fields[i].enum == nil {
				s.fields[i].enum, _ = LookupEnum(0, s.fields[i].name)
			}
			if s.fields[i].enum == nil {
				s.fields[i].enum = NewEnumDictionary(s.fields[i].name)
			}
			s.enums.Register(s.fields[i].enum)
		}
	}
	s.schemaHash = h.Sum64()
	s.encode, s.decode = compileCodecs(s)

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
			IsInternal: s.fields[i].IsInternal(),
			IsArray:    s.fields[i].isArray,
			Iface:      s.fields[i].iface,
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
	fmt.Fprintf(&b, "schema: %s minSz=%d maxSz=%d fixed=%t iface=%t enc/dec=%d/%d",
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
		fmt.Fprintf(&b, "\n  Field #%d: id=%d %s %s flags=%08b index=%d fixed=%d scale=%d arr=%t sz=%d iface=%08b enc=%s dec=%s",
			i,
			f.id,
			f.name,
			f.typ,
			f.flags,
			f.index,
			f.fixed,
			f.scale,
			f.isArray,
			f.wireSize,
			f.iface,
			s.encode[i],
			s.decode[i],
		)
	}
	return b.String()
}
