// Copyright (c) 2024-2026 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package schema

import (
	"bytes"
	"fmt"
	"iter"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"

	"blockwatch.cc/knoxdb/internal/hash"
	"blockwatch.cc/knoxdb/pkg/schema/enum"
)

type Schema struct {
	Fields      []*Field
	Enums       atomic.Pointer[enum.Registry]
	Name        string
	Hash        uint64
	MinWireSize int
	EstWireSize int
	Version     uint32
	IsFixedSize bool
}

func NewSchema(opts ...Option) *Schema {
	s := &Schema{
		Fields:      make([]*Field, 0),
		Version:     1,
		IsFixedSize: true,
	}
	for _, o := range opts {
		o(s)
	}
	return s
}

func (s *Schema) As(alias string) *Schema {
	s.Name = alias
	return s
}

func (s *Schema) UseEnums(r *enum.Registry) *Schema {
	s.Enums.Store(r)
	for _, f := range s.Fields {
		if f.Type == Enum {
			if e, ok := r.Find(f.Basename()); ok {
				f.Enum = e
			}
		}
	}
	return s
}

func (s *Schema) HasEnums() bool {
	return s.Enums.Load() != nil
}

func (s *Schema) NewBuffer(n int) *bytes.Buffer {
	return bytes.NewBuffer(make([]byte, 0, n*s.EstWireSize))
}

func (s *Schema) IsValid() bool {
	return len(s.Name) != 0 && len(s.Fields) != 0 && s.Hash > 0
}

func (s *Schema) Label() string {
	var b strings.Builder
	b.WriteString(s.Name)
	b.WriteString(".v")
	b.WriteString(strconv.Itoa(int(s.Version)))
	return b.String()
}

func (s *Schema) Equal(x *Schema) bool {
	return s != nil && x != nil && s.Hash == x.Hash
}

// Returns total number of fields across all nesting levels.
func (s *Schema) Len() int {
	return len(s.Fields)
}

// Returns the number of fields at top lesting level.
func (s *Schema) NumFields() int {
	lvl := s.Fields[0].Level
	n := 1
	for _, f := range s.Fields[1:] {
		if f.Level == lvl {
			n++
		}
	}
	return n
}

// Counts active fields across all nesting levels.
func (s *Schema) NumActive() int {
	var n int
	for _, f := range s.Fields {
		if f.IsActive() {
			n++
		}
	}
	return n
}

// Counts enum fields across all nesting levels.
func (s *Schema) NumEnums() int {
	var n int
	for _, f := range s.Fields {
		if f.Type == Enum {
			n++
		}
	}
	return n
}

// Counts visible (not deleted and not metadata) fields across
// all nesting levels.
func (s *Schema) NumVisible() int {
	var n int
	for _, f := range s.Fields {
		if f.IsVisible() {
			n++
		}
	}
	return n
}

// Returns all field names across all nesting levels.
func (s *Schema) Names() []string {
	list := make([]string, len(s.Fields))
	for i, f := range s.Fields {
		list[i] = f.Name
	}
	return list
}

func (s *Schema) EnumNames() []string {
	list := make([]string, 0)
	for _, f := range s.Fields {
		if f.Type == Enum {
			list = append(list, f.Basename())
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

// Visits all fields at top nesting level.
func (s *Schema) TopFields() iter.Seq2[int, *Field] {
	lvl := s.Fields[0].Level
	return func(yield func(int, *Field) bool) {
		var n int
		for _, f := range s.Fields {
			if f.Level != lvl {
				continue
			}
			if !yield(n, f) {
				return
			}
			n++
		}
	}
}

// Returns the i-th field at top nesting level.
func (s *Schema) Field(i int) *Field {
	lvl := s.Fields[0].Level
	n := 0
	for _, f := range s.Fields {
		if f.Level != lvl {
			continue
		}
		if n == i {
			return f
		}
		n++
	}
	return nil
}

func (s *Schema) Find(name string) (*Field, bool) {
	for _, f := range s.Fields {
		if f.Name == name && f.IsActive() {
			return f, true
		}
	}
	return nil, false
}

func (s *Schema) FindId(id uint16) (*Field, bool) {
	for _, f := range s.Fields {
		if f.Id == id {
			return f, true
		}
	}
	return nil, false
}

func (s *Schema) Index(name string) (int, bool) {
	for i, f := range s.Fields {
		if f.Name == name && f.IsActive() {
			return i, true
		}
	}
	return -1, false
}

func (s *Schema) IndexId(id uint16) (int, bool) {
	for i, f := range s.Fields {
		if f.Id == id {
			return i, true
		}
	}
	return -1, false
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

func (s *Schema) Timebase() *Field {
	for _, f := range s.Fields {
		if f.IsTimebase() && f.IsActive() {
			return f
		}
	}
	return &Field{}
}

func (s *Schema) TimebaseId() uint16 {
	for _, f := range s.Fields {
		if f.IsTimebase() && f.IsActive() {
			return f.Id
		}
	}
	return 0
}

func (s *Schema) TimebaseIndex() int {
	for i, f := range s.Fields {
		if f.IsTimebase() && f.IsActive() {
			return i
		}
	}
	return -1
}

func (s *Schema) Clone() *Schema {
	clone := &Schema{
		Name:    s.Name,
		Fields:  slices.Clone(s.Fields),
		Version: s.Version,
	}
	clone.Enums.Store(s.Enums.Load())

	// clone all fields
	for i, f := range clone.Fields {
		clone.Fields[i] = f.Clone()
	}

	// all nested child fields are known top-level, but field clone
	// does not relink them to the new pointers or create child schema
	// clones; we do that now and reassign field pointers

	// relink child schemas (assumes child fields have unique ids)
	// we only walk the list of fields once (not recursive) since
	// all nested fields are linearized
	for _, f := range clone.Fields {
		// skip when not a nested field
		if f.Child == nil {
			continue
		}

		// clone schema
		newChild := &Schema{
			Name:    f.Child.Name,
			Version: f.Child.Version,
			Fields:  slices.Clone(f.Child.Fields),
		}

		// relink fields
		if ok := newChild.relink(clone); ok {
			f.Child = newChild
		} else {
			panic(fmt.Errorf("schema %s: failed to relink nested field %q", s.Name, f.Name))
		}

		// clone cases
		if f.Cases != nil {
			newCases := make([]*Schema, len(*f.Cases))
			for i, cs := range *f.Cases {
				newSchema := &Schema{
					Name:    cs.Name,
					Version: cs.Version,
					Fields:  slices.Clone(cs.Fields),
				}
				// relink fields
				if ok := newSchema.relink(newChild); ok {
					newCases[i] = newSchema
				} else {
					panic(fmt.Errorf("schema %s: failed to relink nested variant field %q", s.Name, f.Name))
				}
			}
			f.Cases = &newCases
		}
	}

	// finalize children in reverse order to roll up hashes correctly
	// across nested children
	for _, f := range slices.Backward(clone.Fields) {
		if f.Child == nil {
			continue
		}
		f.Child.Finalize()

		// finalize case schemas after child fields are final
		if f.Cases != nil {
			for _, cs := range *f.Cases {
				cs.Finalize()
			}
		}
	}

	// finalize the outer schema
	return clone.Finalize()
}

// assigns the next available field id
func (s *Schema) nextFieldId() uint16 {
	id := uint16(len(s.Fields) + 1)
	for {
		if id == 1<<16-1 {
			panic("max number of fields reached")
		}
		_, ok := s.FindId(id)
		if !ok {
			return id
		}
		id++
	}
}

// relinks schema ids to field pointers found in dst schema.
// used on clone to rebuild the tree of nested fields.
func (s *Schema) relink(dst *Schema) bool {
	var (
		ok     bool
		fields = make([]*Field, len(s.Fields))
	)
	for i, f := range s.Fields {
		fields[i], ok = dst.FindId(f.Id)
		if !ok || fields[i].Type != f.Type {
			return false
		}
	}
	s.Fields = fields
	return true
}

// Relevel resets nesting levels and assigns parent field ids
// to all nested type fields. This method is recursive.
func (s *Schema) Relevel(lvl uint8, parent uint16) {
	// safeguard against circular dependencies and errors
	if lvl == 255 {
		panic(ErrMaxNested)
	}
	// reset level for all nested fields
	for _, f := range s.Fields {
		f.Level = lvl
		f.ParentId = parent
	}
	// assign new levels to nested children; since the type tree
	// is stored in pre-order and children are linked by pointers
	// we first assign new levels to upper layer nodes and then
	// trickle down towards leafs as we progress through the list.
	for _, f := range s.Fields {
		if f.Child == nil {
			continue
		}
		f.Child.Relevel(f.Level+1, f.Id)
	}
}

// AddField adds a new field to the schema. It creates a new field id
// and links the field to the given parent to support nesting.
// TODO:
// - handle variant case changes
// - support adding new variant cases
func (s *Schema) AddField(f *Field) (*Schema, error) {
	// require name and structure to be ok
	if err := f.Validate(); err != nil {
		return nil, err
	}

	// clone the incoming field
	f = f.Clone()

	// ensure field is unique, if nested check if it is unique
	// withing the nested type
	if f.ParentId == 0 {
		if _, ok := s.Find(f.Name); ok {
			return nil, ErrDuplicateName
		}
	} else {
		// parent must exist
		p, ok := s.FindId(f.ParentId)
		if !ok {
			return nil, ErrInvalidParent
		}

		// check if the type can hold more fields
		// TODO: better way to detect a nested struct type
		if p.Type != List || p.Child.NumFields() == 1 {
			return nil, ErrInvalidParent
		}

		// prefix field name with parent path and
		// check the extended name is unique within the parent
		f.Name = p.Name + "." + f.Name
		if _, ok := p.Child.Find(f.Name); ok {
			return nil, ErrDuplicateName
		}
	}

	// clone the schema and up version number
	clone := s.Clone()
	clone.Version++

	//  assign unique field id
	f.Id = clone.nextFieldId()
	clone.Fields = append(clone.Fields, f)

	// clone potential children
	if f.Child != nil {
		// clone child schema including all nested fields
		newChild := f.Child.Clone()

		// enforce all child schema versions are equal
		newChild.Version = s.Version

		// add child fields to main schema
		for _, c := range newChild.Fields {
			// always create new ids
			c.Id = clone.nextFieldId()
			clone.Fields = append(clone.Fields, c)
		}

		// replace child pointer
		f.Child = newChild
	}

	// if the new field has a parent link the field
	// ignore error because we have already checked above
	if f.ParentId > 0 {
		_ = clone.linkParent(f.ParentId, f)
	}

	// relevel the type tree in case the added field is
	// nested or changed a nested type
	clone.Relevel(0, 0)

	// finalize nested fields in reverse order to rollup leaf
	// child schema hashes first and update nested sizes
	for _, f := range slices.Backward(clone.Fields) {
		if f.Child != nil {
			f.Child.Finalize()
		}
		if f.Cases != nil {
			for _, cf := range *f.Cases {
				cf.Finalize()
			}
		}
	}

	// finalize the top-level schema and validate
	clone.Finalize()
	if err := clone.Validate(); err != nil {
		return nil, err
	}
	return clone, nil
}

// TODO: support deleting variant cases
func (s *Schema) DeleteId(id uint16) (*Schema, error) {
	// perform checks
	f, ok := s.FindId(id)
	if !ok {
		return nil, ErrInvalidField
	}
	if !f.IsActive() {
		return nil, ErrInvalidField
	}
	if f.IsPrimary() {
		return nil, ErrDeletePrimary
	}

	// delete changes schema version
	clone := s.Clone()
	clone.Version++
	f, _ = clone.FindId(id)
	f.Flags |= FlagDeleted

	// delete nested fields too and update versions
	if f.Child != nil {
		f.Child.Version = clone.Version
		for _, cf := range f.Child.Fields {
			cf.Flags |= FlagDeleted
			if cf.Child != nil {
				cf.Child.Version = clone.Version
			}
		}
	}

	// finalize nested types in reverse order to rollup
	// leaf child schema hashes first and update sizes
	if f.ParentId > 0 || f.Child != nil {
		for _, f := range slices.Backward(clone.Fields) {
			if f.Child != nil {
				f.Child.Finalize()
			}
			if f.Cases != nil {
				for _, cf := range *f.Cases {
					cf.Finalize()
				}
			}
		}
	}

	// fill in computed fields
	clone.Finalize()

	if err := clone.Validate(); err != nil {
		return nil, err
	}

	return clone, nil
}

func (s *Schema) RenameId(id uint16, name string) (*Schema, error) {
	// check pre-conditions
	f, ok := s.FindId(id)
	if !ok {
		return nil, ErrInvalidField
	}
	// cannot rename deleted fields
	if !f.IsActive() {
		return nil, ErrInvalidField
	}
	// enums are connected to named dictionaries and cannot be changed
	if f.Type == Enum {
		return nil, ErrRenameEnum
	}

	// ensure new name is unique
	if f.Level == 0 {
		if _, ok := s.Find(name); ok {
			return nil, ErrDuplicateName
		}
	} else {
		// new name must be unique at parent level
		if p, ok := s.FindId(f.ParentId); !ok {
			return nil, ErrInvalidParent
		} else {
			name = p.Name + "." + name
			if _, ok = s.Find(name); ok {
				return nil, ErrDuplicateName
			}
		}
	}

	// simple or nested name must be no longer than max
	if len(name) > MAX_NAME {
		return nil, ErrLongValue
	}

	// clone but don't update version
	// name change does not alter schema hash
	clone := s.Clone()
	f, _ = clone.FindId(id)

	// rename child fields by replacing the parent name prefix
	if f.Child != nil {
		for _, cf := range f.Child.Fields {
			cf.Name = name + strings.TrimPrefix(cf.Name, f.Name)
		}
	}

	// update field name
	f.Name = name

	// finalize the clone
	return clone.Finalize(), nil
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
			return nil, fmt.Errorf("schema %s: missing field id %d", s.Name, fid)
		}
		ns.Fields = append(ns.Fields, f)

		// add all nested child fields
		if f.Child != nil {
			ns.Fields = append(ns.Fields, f.Child.Fields...)
		}
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
			return nil, fmt.Errorf("schema %s: missing field name %s", s.Name, fname)
		}
		ns.Fields = append(ns.Fields, f)

		// add all nested child fields
		if f.Child != nil {
			ns.Fields = append(ns.Fields, f.Child.Fields...)
		}
	}

	return ns.Finalize(), nil
}

func (s *Schema) Sort() *Schema {
	sort.Slice(s.Fields, func(i, j int) bool {
		return s.Fields[i].Id < s.Fields[j].Id
	})
	s.Hash = 0
	return s.Finalize()
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
				// hide deleted source fields and metadata fields
				if f.IsVisible() {
					pos = i
				}
				break
			}
		}

		// assert mapping matches type conventions
		if pos > -1 {
			if srcField.Type != dstField.Type {
				return nil, fmt.Errorf("schema map [%s/%s] => [%s/%s]: type mismatch %s/%s",
					s.Name, srcField.Name,
					dst.Name, dstField.Name,
					srcField.Type, dstField.Type,
				)
			}
			if a, b := srcField.IsArray(), dstField.IsArray(); a != b {
				return nil, fmt.Errorf("schema map [%s/%s] => [%s/%s]: array mismatch %t/%t",
					s.Name, srcField.Name,
					dst.Name, dstField.Name,
					a, b,
				)
			}
			if srcField.Scale != dstField.Scale {
				return nil, fmt.Errorf("schema map [%s/%s] => [%s/%s]: scale mismatch %d/%d",
					s.Name, srcField.Name,
					dst.Name, dstField.Name,
					srcField.Scale, dstField.Scale,
				)
			}
		}
		maps = append(maps, pos)
	}
	return maps, nil
}

func (s *Schema) Validate() error {
	// require name between 1..255 bytes length
	if l := len(s.Name); l > 255 {
		return ErrLongName
	} else if l < 1 {
		return ErrNoName
	}

	// require fields
	if len(s.Fields) == 0 {
		return fmt.Errorf("schema %s: no supported fields found", s.Name)
	}

	// require sizes
	if s.MinWireSize == 0 || s.EstWireSize == 0 {
		return fmt.Errorf("schema %s: zero wire size", s.Name)
	}

	// TODO
	// - we could require strictly sorted fields by id

	// require no duplicate names, ids, pk or timebase fields
	uniqueNames := make(map[string]struct{})
	uniqueIds := make(map[uint16]struct{})
	var firstTimebase, firstPkField *Field

	for _, f := range s.Fields {
		// fields must validate including nested fields
		if err := f.Validate(true); err != nil {
			return fmt.Errorf("schema %s: %v", s.Name, err)
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

		// check pk field is unique
		if f.IsPrimary() {
			if firstPkField != nil {
				return fmt.Errorf("schema %s: pk flag on multiple fields %q and %q",
					s.Name, firstPkField.Name, f.Name)
			} else {
				firstPkField = f
			}
		}

		// check timebase field is unique
		if f.IsTimebase() {
			if firstTimebase != nil {
				return fmt.Errorf("schema %s: timebase flag on multiple fields %q and %q",
					s.Name, firstTimebase.Name, f.Name)
			} else {
				firstTimebase = f
			}
		}
	}

	return nil
}

func (s *Schema) Finalize(opts ...Option) *Schema {
	s.MinWireSize = 0
	s.EstWireSize = 0
	s.IsFixedSize = true
	s.Hash = 0

	// apply schema options
	for _, o := range opts {
		o(s)
	}

	// collect enums when used but no registry exists yet
	if s.NumEnums() > 0 && !s.HasEnums() {
		reg := enum.NewRegistry()
		for _, f := range s.Fields {
			if f.Type == Enum && f.Enum != nil {
				reg.Put(uint64(f.Id), f.Enum)
			}
		}
		s.Enums.Store(reg)
	}

	// generate schema hash from visible fields
	var (
		b   [8]byte
		lvl uint8
	)
	h := hash.New()
	LE.PutUint32(b[:], s.Version)
	h.Write(b[:4])

	// use the first field's level as top level (may be empty on invalid schema)
	if len(s.Fields) > 0 {
		lvl = s.Fields[0].Level
	}

	for _, f := range s.Fields {
		// collect sizes from visible fields only
		if !f.IsVisible() {
			continue
		}

		// skip sizes from nested fields inside containers
		// but count the top-level container header size
		if f.Level == lvl {
			sz := f.WireSize()
			s.MinWireSize += sz
			s.EstWireSize += sz
			if !f.IsFixedSize() {
				s.IsFixedSize = false
				s.EstWireSize += f.VarSizeEstimate()
			}
		}

		// hash: id, type, flags, scale, level, caseid
		// (not: filter, compress, name)
		LE.PutUint16(b[:], f.Id)
		h.Write(b[:2])
		h.Write([]byte{
			byte(f.Type),
			byte(f.Flags),
			f.Scale,
			f.Level,
			f.CaseId,
		})
	}

	s.Hash = h.Sum64()
	if s.Name == "" {
		s.Name = fmt.Sprintf("%016x", s.Hash)
	}

	return s
}

func (s *Schema) String() string {
	var b strings.Builder
	if s.IsFixedSize {
		fmt.Fprintf(&b, "%q 0x%016x fields=%d/%d sz=%d (fixed)",
			s.Name, s.Hash, s.NumFields(), len(s.Fields), s.MinWireSize)
	} else {
		fmt.Fprintf(&b, "%q 0x%016x fields=%d/%d sz_min=%d sz_max=%d",
			s.Name,
			s.Hash,
			s.NumFields(),
			len(s.Fields),
			s.MinWireSize,
			s.EstWireSize,
		)
	}
	var (
		maxNameLen, maxTypeLen, maxFlagLen, maxFilterLen = 4, 4, 5, 0
		isNested                                         bool
	)
	for _, f := range s.Fields {
		maxNameLen = max(maxNameLen, len(f.Name))
		maxTypeLen = max(maxTypeLen, len(f.Typename()))
		maxFlagLen = max(maxFlagLen, len(f.Flags.String()))
		maxFilterLen = max(maxFilterLen, len(f.Filter.String()))
		isNested = isNested || f.Child != nil
	}
	fmt.Fprintf(&b, "\n#  ID   %[1]*[2]s %[3]*[4]s %[5]*[6]s %[7]*[8]s %[9]*[10]s ",
		-maxNameLen, "Name",
		-maxTypeLen, "Type",
		-maxFlagLen, "Flags",
		-5, "Level",
		-4, "Case",
	)
	if maxFilterLen > 0 {
		fmt.Fprintf(&b, "%[1]*[2]s ", -maxFilterLen, "Filter")
	}
	if isNested {
		fmt.Fprintf(&b, "%-18s %s", "Child", "Links")
	}
	for i, f := range s.Fields {
		fmt.Fprintf(&b, "\n%02d F#%02d %[3]*[4]s %[5]*[6]s %[7]*[8]s %[9]*[10]d %[11]*[12]d ",
			i, f.Id,
			-maxNameLen, f.Name,
			-maxTypeLen, f.Typename(),
			-maxFlagLen, f.Flags.String(),
			-5, f.Level,
			-4, f.CaseId,
		)
		if maxFilterLen > 0 {
			fmt.Fprintf(&b, "%[1]*[2]s ", -maxFilterLen, f.Filter.String())
		}
		if f.Child != nil {
			fmt.Fprintf(&b, "0x%016x ", f.Child.Hash)
			for _, cf := range f.Child.Fields {
				fmt.Fprintf(&b, "%s(%d) ", cf.Name, cf.Id)
			}
		}
	}
	return b.String()
}
