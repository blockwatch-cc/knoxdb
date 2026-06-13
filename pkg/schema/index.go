// Copyright (c) 2024-2026 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package schema

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"

	"blockwatch.cc/knoxdb/internal/hash"
)

const (
	// default field name/suffix
	IndexName = "index"
)

type IndexType byte

const (
	InvalidIndex IndexType = iota
	HashIndex
	IntegerIndex
	PrimaryKeyIndex
	CompositeIndex
)

func (i IndexType) Is(f IndexType) bool {
	return i&f > 0
}

var (
	indexTypeString  = "__hash_int_pk_composite"
	indexTypeIdx     = [...]int8{0, 1, 7, 11, 14, 24}
	indexTypeReverse = map[string]IndexType{}
)

func init() {
	for t := InvalidIndex; t <= CompositeIndex; t++ {
		indexTypeReverse[t.String()] = t
	}
}

func (t IndexType) IsValid() bool {
	return t > InvalidIndex && t <= CompositeIndex
}

func (t IndexType) String() string {
	return indexTypeString[indexTypeIdx[t] : indexTypeIdx[t+1]-1]
}

func ParseIndexType(s string) (IndexType, error) {
	t, ok := indexTypeReverse[s]
	if ok {
		return t, nil
	}
	return 0, errors.New("invalid index type " + s)

}

// Knox index spec parsing
//
// Examples
//
// Id      uint64    `"knox:X,pk"`            // implies PK index type
// F1      int       `"knox:Y,index=hash"`
// F2      int       `"knox:Z,index=int,extra=X+Y"`
// _       struct{}  `"knox:idx,index=composite,fields=X+Y,extra=Z+X"`

type IndexSchema struct {
	Name   string    // index name
	Type   IndexType // index type: hash, int, composite
	Base   *Schema   // base schema
	Fields []*Field  // indexed fields in order
	Extra  []*Field  // extra (inline) fields
}

func NewIndexSchema(typ IndexType, base *Schema, opts ...IndexOption) *IndexSchema {
	if !typ.IsValid() {
		typ = InvalidIndex
	}
	ix := &IndexSchema{
		Name:   makeIndexName(typ, base),
		Type:   typ,
		Base:   base,
		Fields: make([]*Field, 0),
	}
	for _, o := range opts {
		o(ix)
	}
	return ix
}

func makeIndexName(typ IndexType, base *Schema, f ...*Field) string {
	if len(f) == 0 {
		return strings.Join([]string{base.Name, typ.String(), IndexName}, "_")
	}
	return strings.Join([]string{base.Name, f[0].Name, typ.String(), IndexName}, "_")
}

func (s *IndexSchema) IsValid() bool {
	return s.Type.IsValid() && len(s.Fields) > 0
}

func (s *IndexSchema) Clone() *IndexSchema {
	return &IndexSchema{
		Name:   s.Name,
		Type:   s.Type,
		Base:   s.Base,
		Fields: slices.Clone(s.Fields),
		Extra:  slices.Clone(s.Extra),
	}
}

func (s *IndexSchema) Rebase(base *Schema) (*IndexSchema, bool) {
	if !base.ContainsSchema(s.Base) {
		return nil, false
	}

	clone := &IndexSchema{
		Name:   s.Name,
		Type:   s.Type,
		Base:   base,
		Fields: make([]*Field, len(s.Fields)),
		Extra:  make([]*Field, len(s.Extra)),
	}

	var ok bool
	for i, fid := range s.FieldIds() {
		clone.Fields[i], ok = base.FindId(fid)
		if !ok {
			return nil, false
		}
	}
	for i, fid := range s.ExtraIds() {
		clone.Extra[i], ok = base.FindId(fid)
		if !ok {
			return nil, false
		}
	}

	return clone, true
}

// Hash returns a unique index schema hash.
func (s *IndexSchema) Hash() uint64 {
	h := hash.New()

	// index type
	h.Write([]byte{byte(s.Type)})

	// base schema hash
	var b [8]byte
	LE.PutUint64(b[:], s.Base.Hash)

	// hash: id, type, flags, fixed, scale (not: filter, compress, name)
	hashField := func(f *Field) {
		LE.PutUint16(b[:], f.Id)
		h.Write(b[:2])
		h.Write([]byte{
			byte(f.Type),
			byte(f.Flags),
			f.Scale,
		})
	}

	// index fields
	for _, f := range s.Fields {
		hashField(f)
	}

	// extra fields
	for _, f := range s.Fields {
		hashField(f)
	}

	return h.Sum64()
}

// AllIds returns an ordered list of all field ids required by this index.
func (s *IndexSchema) AllIds() []uint16 {
	ids := make([]uint16, 0, len(s.Fields)+len(s.Extra))
	for _, f := range s.Fields {
		ids = append(ids, f.Id)
	}
	for _, f := range s.Extra {
		ids = append(ids, f.Id)
	}
	slices.Sort(ids)
	return slices.Compact(ids)
}

func (s *IndexSchema) FieldIds() []uint16 {
	if len(s.Fields) == 0 {
		return nil
	}
	ids := make([]uint16, len(s.Fields))
	for k, f := range s.Fields {
		ids[k] = f.Id
	}
	return ids
}

func (s *IndexSchema) ExtraIds() []uint16 {
	if len(s.Extra) == 0 {
		return nil
	}
	ids := make([]uint16, len(s.Extra))
	for k, f := range s.Extra {
		ids[k] = f.Id
	}
	return ids
}

func (s *IndexSchema) FieldIndices() []int {
	ixs := make([]int, len(s.Fields))
	for k, f := range s.Fields {
		x, _ := s.Base.IndexId(f.Id)
		ixs[k] = x
	}
	return ixs
}

func (s *IndexSchema) ExtraIndices() []int {
	if len(s.Extra) == 0 {
		return nil
	}
	ixs := make([]int, len(s.Extra))
	for k, f := range s.Extra {
		x, _ := s.Base.IndexId(f.Id)
		ixs[k] = x
	}
	return ixs
}

// IndexSchema returns a sub-schema from base which contains all fields
// required by the index including index and extra fields. Allows the
// user to pass in additional ids.
func (s *IndexSchema) IndexSchema(other ...uint16) (*Schema, error) {
	ids := append(s.AllIds(), other...)
	slices.Sort(ids)
	ids = slices.Compact(ids)
	base, err := s.Base.SelectIds(ids...)
	if err != nil {
		return nil, err
	}
	return base.As(s.Name), nil
}

// Contains returns true if all named fields exist in order.
func (s *IndexSchema) Contains(names ...string) bool {
	if len(names) == 0 || len(names) > len(s.Fields) {
		return false
	}
	for k, n := range names {
		if s.Fields[k].Name == n {
			continue
		}
		return false
	}
	return true
}

func (s *IndexSchema) Validate() error {
	// require index type in range
	if s.Name == "" {
		return fmt.Errorf("index: empty name")
	}

	// require index type in range
	if !s.Type.IsValid() {
		return fmt.Errorf("index[%s]: invalid index type %d", s.Name, s.Type)
	}

	// requires at least 1 index field
	if len(s.Fields) == 0 {
		return fmt.Errorf("index[%s]: empty field list", s.Name)
	}

	// fields must be defined in base schema
	for _, f := range s.Fields {
		if _, ok := s.Base.FindId(f.Id); !ok {
			return fmt.Errorf("index[%s]: field %s (%d) not in base schema %s",
				s.Name, f.Name, f.Id, s.Base.Name)
		}
	}
	for _, f := range s.Extra {
		if _, ok := s.Base.FindId(f.Id); !ok {
			return fmt.Errorf("index[%s]: extra field %s (%d) not in base schema %s",
				s.Name, f.Name, f.Id, s.Base.Name)
		}
	}

	// fields and extra lists must not contain duplicate entries
	unique := make(map[uint16]struct{})
	for _, f := range s.Fields {
		if _, ok := unique[f.Id]; ok {
			return fmt.Errorf("index[%s]: duplicate index field %s (%d)", s.Name, f.Name, f.Id)
		}
		unique[f.Id] = struct{}{}
	}
	clear(unique)
	for _, f := range s.Extra {
		if _, ok := unique[f.Id]; ok {
			return fmt.Errorf("index[%s]: duplicate extra field %s (%d)", s.Name, f.Name, f.Id)
		}
		unique[f.Id] = struct{}{}
	}

	// check type-specific restrictions
	switch s.Type {
	case IntegerIndex:
		// requires single integer field
		if len(s.Fields) > 1 {
			return fmt.Errorf("index[%s]: integer index requires single field", s.Name)
		}
		f := s.Fields[0]
		switch f.Type {
		case Timestamp, Time, Date,
			Int64, Int32, Int16, Int8,
			Uint64, Uint32, Uint16, Uint8:
			// ok
		default:
			return fmt.Errorf("index[%s]: unsupported integer index on field %s type %s",
				s.Name, f.Name, f.Type)
		}

	case PrimaryKeyIndex:
		// requires single integer field
		if len(s.Fields) > 1 {
			return fmt.Errorf("index[%s]: primary index requires single field", s.Name)
		}
		// require pk index on pk field only
		f := s.Fields[0]
		if f.Type != Uint64 || f.Flags&FlagPrimary == 0 {
			return fmt.Errorf("field[%s]: pk index on unsupported field %s type %s",
				s.Name, f.Name, f.Type)
		}

	case HashIndex:
		// requires single field
		if len(s.Fields) > 1 {
			return fmt.Errorf("index[%s]: hash index requires single field", s.Name)
		}

	case CompositeIndex:
		// requires multiple fields
		if len(s.Fields) < 2 {
			return fmt.Errorf("index[%s]: composite index requires at least 2 fields", s.Name)
		}
	}

	return nil
}

func (s IndexSchema) MarshalBinary() ([]byte, error) {
	buf := bytes.NewBuffer(make([]byte, 0, 22+len(s.Name)+32*(len(s.Fields)+len(s.Extra))))

	// version: byte
	buf.WriteByte(1)

	// type: byte
	buf.WriteByte(byte(s.Type))

	// base schema hash: u64
	binary.Write(buf, LE, s.Base.Hash)

	// name: string
	binary.Write(buf, LE, uint32(len(s.Name)))
	buf.WriteString(s.Name)

	// fields
	binary.Write(buf, LE, uint32(len(s.Fields)))
	for _, f := range s.Fields {
		f.WriteTo(buf)
	}

	// extra
	binary.Write(buf, LE, uint32(len(s.Extra)))
	for _, f := range s.Extra {
		f.WriteTo(buf)
	}

	return buf.Bytes(), nil
}

func (s *IndexSchema) UnmarshalBinary(b []byte) (err error) {
	if len(b) < 22 {
		return io.ErrShortBuffer
	}

	// version
	if b[0] != 1 {
		return fmt.Errorf("invalid index schema version %d", b[0])
	}

	// type
	s.Type = IndexType(b[1])
	if !s.Type.IsValid() {
		return fmt.Errorf("invalid index type %d", b[1])
	}

	buf := bytes.NewBuffer(b[2:])

	// base schema hash: u64
	s.Base = &Schema{}
	err = binary.Read(buf, LE, &s.Base.Hash)
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

	// extra fields
	err = binary.Read(buf, LE, &l)
	if err != nil {
		return
	}
	s.Extra = make([]*Field, l)
	for i := range s.Extra {
		f := &Field{}
		if err = f.ReadFrom(buf); err != nil {
			return
		}
		s.Extra[i] = f
	}

	// Note: although not strictly required, users may want to resolve
	// base schema from its hash

	return nil
}
