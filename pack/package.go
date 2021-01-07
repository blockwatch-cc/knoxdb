// Copyright (c) 2018-2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package pack

import (
	"bytes"
	"encoding"
	"fmt"
	"math"
	"reflect"
	"sort"
	"time"

	"blockwatch.cc/knoxdb/encoding/block"
	"blockwatch.cc/knoxdb/util"

	. "blockwatch.cc/knoxdb/encoding/decimal"
	. "blockwatch.cc/knoxdb/vec"
)

type Package struct {
	key      uint32 // identity
	nFields  int
	nValues  int
	blocks   []*block.Block // embedded blocks
	fields   FieldList      // shared with table
	tinfo    *typeInfo      // Go typeinfo
	pkindex  int            // field index of primary key (optional)
	dirty    bool           // pack is updated, needs to be written
	cached   bool           // pack is cached
	stripped bool           // some blocks are ignored, don't store this pack
	sizeHint int            // block size hint
	size     int            // storage size
}

func (p *Package) Key() []byte {
	var b [4]byte
	bigEndian.PutUint32(b[:], p.key)
	return b[:]
}

func (p *Package) SetKey(key []byte) {
	p.key = bigEndian.Uint32(key)
}

func NewPackage(sz int) *Package {
	return &Package{
		pkindex:  -1,
		sizeHint: sz,
	}
}

func (p *Package) IsDirty() bool {
	return p.dirty
}

func (p *Package) Cols() int {
	return p.nFields
}

func (p *Package) Len() int {
	return p.nValues
}

func (p *Package) Cap() int {
	if p.pkindex > 0 {
		return p.blocks[p.pkindex].Cap()
	}
	if p.nFields > 0 {
		return p.blocks[0].Cap()
	}
	return 0
}

func (p *Package) FieldIndex(name string) int {
	for i, v := range p.fields {
		if v.Name == name || v.Alias == name {
			return i
		}
	}
	return -1
}

func (p *Package) FieldByName(name string) Field {
	for _, v := range p.fields {
		if v.Name == name || v.Alias == name {
			return v
		}
	}
	return Field{Index: -1}
}

func (p *Package) FieldById(idx int) Field {
	if idx < 0 {
		return Field{Index: -1}
	}
	return p.fields[idx]
}

func (p *Package) Contains(fields FieldList) bool {
	for _, v := range fields {
		for _, vv := range p.fields {
			if vv.Name != v.Name {
				return false
			}
		}
	}
	return true
}

// Go type is required to write using reflect inside Push(),
func (p *Package) InitType(v interface{}) error {
	if p.tinfo != nil && p.tinfo.gotype {
		return nil
	}
	tinfo, err := getTypeInfo(v)
	if err != nil {
		return err
	}
	if len(tinfo.fields) > 256 {
		return fmt.Errorf("pack: cannot handle more than 256 fields")
	}
	p.tinfo = tinfo
	if p.pkindex < 0 {
		p.pkindex = tinfo.PkColumn()
	}
	if len(p.fields) == 0 {
		// extract fields from Go type
		fields, err := Fields(v)
		if err != nil {
			return err
		}
		// if pack has been loaded, check if field types match block types
		if p.nFields > 0 && len(p.blocks) > 0 {
			if len(fields) > len(p.blocks) {
				return fmt.Errorf("pack: inconsistent Go type for loaded pack: %d fields, %d blocks", len(fields), len(p.blocks))
			}
			for i, f := range fields {
				b := p.blocks[i]
				if b.Type() != f.Type.BlockType() {
					return fmt.Errorf("pack: invalid block type %s for %s field %d", b.Type(), f.Type, i)
				}
			}
		}
		p.fields = fields
		p.nFields = len(p.fields)
	} else {
		if p.nFields != len(tinfo.fields) {
			return fmt.Errorf("pack: invalid Go type %s with %d fields for pack with %d fields",
				tinfo.name, len(tinfo.fields), p.nFields)
		}
	}
	if len(p.blocks) == 0 {
		p.blocks = make([]*block.Block, p.nFields)
		for i, f := range p.fields {
			p.blocks[i] = f.NewBlock(p.sizeHint)
		}
	} else {
		// make sure we use the correct compression (empty blocks are stored without)
		for i := range p.blocks {
			p.blocks[i].SetCompression(p.fields[i].Flags.Compression())
		}
	}
	return nil
}

func (p *Package) ResetType() {
	p.tinfo = nil
}

// Init from field list when Go type is unavailable
func (p *Package) InitFields(fields FieldList, tinfo *typeInfo) error {
	if len(fields) > 256 {
		return fmt.Errorf("pack: cannot handle more than 256 fields")
	}
	if len(p.fields) > 0 {
		return fmt.Errorf("pack: already initialized")
	}
	// if pack has been loaded, check if field types match block types
	if p.nFields > 0 && len(p.blocks) > 0 {
		if len(fields) > len(p.blocks) {
			return fmt.Errorf("pack: inconsistent Go type for loaded pack: %d fields, %d blocks", len(fields), len(p.blocks))
		}
		for i, f := range fields {
			b := p.blocks[i]
			if b.Type() != f.Type.BlockType() {
				return fmt.Errorf("pack: mismatch block type %s for %s field %d", b.Type(), f.Type, i)
			}
		}
	}

	p.fields = fields
	p.nFields = len(fields)
	p.pkindex = fields.PkIndex()
	p.tinfo = tinfo

	if len(p.blocks) == 0 {
		p.blocks = make([]*block.Block, p.nFields)
		for i, f := range fields {
			p.blocks[i] = f.NewBlock(p.sizeHint)
		}
	} else {
		// make sure we use the correct compression (empty blocks are stored without)
		for i := range p.blocks {
			p.blocks[i].SetCompression(fields[i].Flags.Compression())
		}
	}
	return nil
}

func (p *Package) Clone(copydata bool, sz int) (*Package, error) {
	clone := &Package{
		nFields:  p.nFields,
		nValues:  0,
		fields:   p.fields,
		key:      0, // cloned pack has no identity yet
		dirty:    true,
		stripped: p.stripped, // cloning a stripped pack is allowed
		tinfo:    p.tinfo,    // share static type info
		pkindex:  p.pkindex,
		sizeHint: p.sizeHint,
	}

	if len(p.blocks) > 0 {
		clone.blocks = make([]*block.Block, p.nFields)
		// create new empty blocks
		for i, b := range p.blocks {
			var err error
			clone.blocks[i], err = b.Clone(sz, copydata)
			if err != nil {
				return nil, err
			}
			// overwrite compression (empty journal blocks get saved without)
			clone.blocks[i].SetCompression(p.fields[i].Flags.Compression())
		}
		if copydata {
			clone.nValues = p.nValues
		}
	}
	return clone, nil
}

func (p *Package) KeepFields(fields FieldList) *Package {
	if len(fields) == 0 {
		return p
	}
	for i, v := range p.fields {
		if !fields.Contains(v.Name) {
			p.blocks[i].SetIgnore()
			p.stripped = true
		}
	}
	return p
}

// clones field list and sets new aliase names
func (p *Package) UpdateAliasesFrom(fields FieldList) *Package {
	if len(fields) == 0 {
		return p
	}
	// clone our field list (since it may be shared with table)
	prevfields := p.fields
	p.fields = make([]Field, len(prevfields))
	for i := range p.fields {
		field := prevfields[i]
		updated := fields.Find(field.Name)
		if updated.IsValid() {
			field.Alias = updated.Alias
		}
		p.fields[i] = field
	}
	return p
}

// Push append a new row to all columns. Requires a type that strictly defines
// all columns in this pack! Column mapping uses the default struct tag `pack`,
// hence the fields name only (not the fields alias).
func (p *Package) Push(v interface{}) error {
	if err := p.InitType(v); err != nil {
		return err
	}
	val := reflect.Indirect(reflect.ValueOf(v))
	if !val.IsValid() {
		return fmt.Errorf("pack: push: invalid value of type %T", v)
	}
	for _, fi := range p.tinfo.fields {
		if fi.blockid < 0 {
			continue
		}
		b := p.blocks[fi.blockid]
		field := p.fields[fi.blockid]
		// skip early
		if b.IsIgnore() {
			continue
		}
		f := fi.value(val)

		// log.Infof("Push to field %d %s type=%s block=%s struct val %s (%s) finfo=%s",
		// 	fi.blockid, field.Name, field.Type, b.Type(),
		// 	f.Type().String(), f.Kind(), fi)

		switch field.Type {
		case FieldTypeBytes:
			var buf []byte
			// check if type implements BinaryMarshaler
			if f.CanInterface() && f.Type().Implements(binaryMarshalerType) {
				var err error
				if buf, err = f.Interface().(encoding.BinaryMarshaler).MarshalBinary(); err != nil {
					return err
				}
			} else {
				src := f.Bytes()
				buf = make([]byte, len(src))
				copy(buf, src)
			}
			b.Bytes = append(b.Bytes, buf)
		case FieldTypeString:
			b.Strings = append(b.Strings, f.String())
		case FieldTypeDatetime:
			b.Int64 = append(b.Int64, f.Interface().(time.Time).UnixNano())
		case FieldTypeBoolean:
			l := b.Bits.Len()
			b.Bits.Grow(l + 1)
			if f.Bool() {
				b.Bits.Set(l)
			}
		case FieldTypeFloat64:
			b.Float64 = append(b.Float64, f.Float())
		case FieldTypeFloat32:
			b.Float32 = append(b.Float32, float32(f.Float()))
		case FieldTypeInt256:
			b.Int256 = append(b.Int256, f.Interface().(Int256))
		case FieldTypeInt128:
			b.Int128 = append(b.Int128, f.Interface().(Int128))
		case FieldTypeInt64:
			b.Int64 = append(b.Int64, f.Int())
		case FieldTypeInt32:
			b.Int32 = append(b.Int32, int32(f.Int()))
		case FieldTypeInt16:
			b.Int16 = append(b.Int16, int16(f.Int()))
		case FieldTypeInt8:
			b.Int8 = append(b.Int8, int8(f.Int()))
		case FieldTypeUint64:
			b.Uint64 = append(b.Uint64, f.Uint())
		case FieldTypeUint32:
			b.Uint32 = append(b.Uint32, uint32(f.Uint()))
		case FieldTypeUint16:
			b.Uint16 = append(b.Uint16, uint16(f.Uint()))
		case FieldTypeUint8:
			b.Uint8 = append(b.Uint8, uint8(f.Uint()))
		case FieldTypeDecimal256:
			switch {
			case field.Flags.Contains(flagUintType):
				b.Int256 = append(b.Int256, Int256{0, 0, 0, f.Uint()})
			case field.Flags.Contains(flagIntType):
				b.Int256 = append(b.Int256, Int256{0, 0, 0, uint64(f.Int())})
			case field.Flags.Contains(flagFloatType):
				dec := Decimal256{}
				dec.SetFloat64(f.Float(), field.Scale)
				b.Int256 = append(b.Int256, dec.Int256())
			default:
				b.Int256 = append(b.Int256, f.Interface().(Decimal256).Quantize(field.Scale).Int256())
			}
		case FieldTypeDecimal128:
			switch {
			case field.Flags.Contains(flagUintType):
				b.Int128 = append(b.Int128, Int128{0, f.Uint()})
			case field.Flags.Contains(flagIntType):
				b.Int128 = append(b.Int128, Int128{0, uint64(f.Int())})
			case field.Flags.Contains(flagFloatType):
				dec := Decimal128{}
				dec.SetFloat64(f.Float(), field.Scale)
				b.Int128 = append(b.Int128, dec.Int128())
			default:
				b.Int128 = append(b.Int128, f.Interface().(Decimal128).Quantize(field.Scale).Int128())
			}
		case FieldTypeDecimal64:
			switch {
			case field.Flags.Contains(flagUintType):
				b.Int64 = append(b.Int64, int64(f.Uint()))
			case field.Flags.Contains(flagIntType):
				b.Int64 = append(b.Int64, f.Int())
			case field.Flags.Contains(flagFloatType):
				dec := Decimal64{}
				dec.SetFloat64(f.Float(), field.Scale)
				b.Int64 = append(b.Int64, dec.Int64())
			default:
				b.Int64 = append(b.Int64, f.Interface().(Decimal64).Quantize(field.Scale).Int64())
			}
		case FieldTypeDecimal32:
			switch {
			case field.Flags.Contains(flagUintType):
				b.Int32 = append(b.Int32, int32(f.Uint()))
			case field.Flags.Contains(flagIntType):
				b.Int32 = append(b.Int32, int32(f.Int()))
			case field.Flags.Contains(flagFloatType):
				dec := Decimal32{}
				dec.SetFloat64(f.Float(), field.Scale)
				b.Int32 = append(b.Int32, dec.Int32())
			default:
				b.Int32 = append(b.Int32, f.Interface().(Decimal32).Quantize(field.Scale).Int32())
			}
		default:
			return fmt.Errorf("pack: pushed unsupported value type %s (%v) for %s field %d",
				f.Type().String(), f.Kind(), field.Type, fi.blockid)
		}
		b.SetDirty()
	}
	p.nValues++
	p.dirty = true
	return nil
}

// ReplaceAt replaces a row at offset pos across all columns. Requires a type
// that strictly defines all columns in this pack! Column mapping uses the
// default struct tag `pack`,  hence the fields name only (not the fields alias).
func (p *Package) ReplaceAt(pos int, v interface{}) error {
	if err := p.InitType(v); err != nil {
		return err
	}
	if p.nValues <= pos {
		return fmt.Errorf("pack: invalid pack offset %d (max %d)", pos, p.nValues)
	}
	val := reflect.Indirect(reflect.ValueOf(v))
	if !val.IsValid() {
		return fmt.Errorf("pack: invalid value of type %T", v)
	}
	for _, fi := range p.tinfo.fields {
		if fi.blockid < 0 {
			continue
		}
		b := p.blocks[fi.blockid]
		field := p.fields[fi.blockid]

		// skip early
		if b.IsIgnore() {
			continue
		}
		f := fi.value(val)

		switch field.Type {
		case FieldTypeBytes:
			var buf []byte
			// check if type implements BinaryMarshaler
			if f.CanInterface() && f.Type().Implements(binaryMarshalerType) {
				var err error
				if buf, err = f.Interface().(encoding.BinaryMarshaler).MarshalBinary(); err != nil {
					return err
				}
			} else {
				src := f.Bytes()
				buf = make([]byte, len(src))
				copy(buf, src)
			}
			b.Bytes[pos] = buf

		case FieldTypeString:
			b.Strings[pos] = f.String()

		case FieldTypeDatetime:
			b.Int64[pos] = f.Interface().(time.Time).UnixNano()

		case FieldTypeBoolean:
			if f.Bool() {
				b.Bits.Set(pos)
			} else {
				b.Bits.Clear(pos)
			}

		case FieldTypeFloat64:
			b.Float64[pos] = f.Float()

		case FieldTypeFloat32:
			b.Float32[pos] = float32(f.Float())

		case FieldTypeInt256:
			b.Int256[pos] = f.Interface().(Int256)

		case FieldTypeInt128:
			b.Int128[pos] = f.Interface().(Int128)

		case FieldTypeInt64:
			b.Int64[pos] = f.Int()

		case FieldTypeInt32:
			b.Int32[pos] = int32(f.Int())

		case FieldTypeInt16:
			b.Int16[pos] = int16(f.Int())

		case FieldTypeInt8:
			b.Int8[pos] = int8(f.Int())

		case FieldTypeUint64:
			b.Uint64[pos] = f.Uint()

		case FieldTypeUint32:
			b.Uint32[pos] = uint32(f.Uint())

		case FieldTypeUint16:
			b.Uint16[pos] = uint16(f.Uint())

		case FieldTypeUint8:
			b.Uint8[pos] = uint8(f.Uint())

		case FieldTypeDecimal256:
			switch {
			case field.Flags.Contains(flagUintType):
				b.Int256[pos] = Int256{0, 0, 0, f.Uint()}
			case field.Flags.Contains(flagIntType):
				b.Int256[pos] = Int256{0, 0, 0, uint64(f.Int())}
			case field.Flags.Contains(flagFloatType):
				dec := Decimal256{}
				dec.SetFloat64(f.Float(), field.Scale)
				b.Int256[pos] = dec.Int256()
			default:
				b.Int256[pos] = f.Interface().(Decimal256).Quantize(field.Scale).Int256()
			}

		case FieldTypeDecimal128:
			switch {
			case field.Flags.Contains(flagUintType):
				b.Int128[pos] = Int128{0, f.Uint()}
			case field.Flags.Contains(flagIntType):
				b.Int128[pos] = Int128{0, uint64(f.Int())}
			case field.Flags.Contains(flagFloatType):
				dec := Decimal128{}
				dec.SetFloat64(f.Float(), field.Scale)
				b.Int128[pos] = dec.Int128()
			default:
				b.Int128[pos] = f.Interface().(Decimal128).Quantize(field.Scale).Int128()
			}

		case FieldTypeDecimal64:
			switch {
			case field.Flags.Contains(flagUintType):
				b.Int64[pos] = int64(f.Uint())
			case field.Flags.Contains(flagIntType):
				b.Int64[pos] = f.Int()
			case field.Flags.Contains(flagFloatType):
				dec := Decimal64{}
				dec.SetFloat64(f.Float(), field.Scale)
				b.Int64[pos] = dec.Int64()
			default:
				b.Int64[pos] = f.Interface().(Decimal64).Quantize(field.Scale).Int64()
			}

		case FieldTypeDecimal32:
			switch {
			case field.Flags.Contains(flagUintType):
				b.Int32[pos] = int32(f.Uint())
			case field.Flags.Contains(flagIntType):
				b.Int32[pos] = int32(f.Int())
			case field.Flags.Contains(flagFloatType):
				dec := Decimal32{}
				dec.SetFloat64(f.Float(), field.Scale)
				b.Int32[pos] = dec.Int32()
			default:
				b.Int32[pos] = f.Interface().(Decimal32).Quantize(field.Scale).Int32()
			}

		default:
			return fmt.Errorf("pack: replace unsupported value type %s (%v) for %s field %d",
				f.Type().String(), f.Kind(), field.Type, fi.blockid)
		}
		b.SetDirty()
	}
	p.dirty = true
	return nil
}

// ReadAtWithInfo reads a row at offset pos and unmarshals values into an arbitrary
// type described by tinfo. This method has better performance than ReadAt when
// calls are very frequent, e.g. walking all rows in a pack.
// Will set struct fields based on name and alias as defined by struct tags `pack`
// and `json`.
func (p *Package) ReadAtWithInfo(pos int, v interface{}, tinfo *typeInfo) error {
	if p.nValues <= pos {
		return nil
	}
	val := derefValue(reflect.ValueOf(v))
	if !val.IsValid() {
		return fmt.Errorf("pack: invalid value of type %T", v)
	}
	for _, fi := range tinfo.fields {
		// Note: field to block mapping is required to be initialized in tinfo!
		// this happens once for every new type used in Result.DecodeAt(),
		// and assumes all packs have the same internal structure!
		if fi.blockid < 0 {
			continue
		}
		// skip early
		b := p.blocks[fi.blockid]
		field := p.fields[fi.blockid]
		if b.IsIgnore() {
			continue
		}

		dst := fi.value(val)
		if !dst.IsValid() {
			continue
		}
		if dst.Kind() == reflect.Ptr {
			if dst.IsNil() && dst.CanSet() {
				dst.Set(reflect.New(dst.Type().Elem()))
			}
			dst = dst.Elem()
		}

		switch field.Type {
		case FieldTypeBytes:
			if dst.CanAddr() {
				pv := dst.Addr()
				if pv.CanInterface() && pv.Type().Implements(binaryUnmarshalerType) {
					if err := pv.Interface().(encoding.BinaryUnmarshaler).UnmarshalBinary(b.Bytes[pos]); err != nil {
						return err
					}
					break
				}
			}
			// copy to avoid memleaks of large blocks
			buf := make([]byte, len(b.Bytes[pos]))
			copy(buf, b.Bytes[pos])
			dst.SetBytes(buf)

		case FieldTypeString:
			dst.SetString(b.Strings[pos])

		case FieldTypeDatetime:
			dst.Set(reflect.ValueOf(time.Unix(0, b.Int64[pos]).UTC()))

		case FieldTypeBoolean:
			dst.SetBool(b.Bits.IsSet(pos))

		case FieldTypeFloat64:
			dst.SetFloat(b.Float64[pos])

		case FieldTypeFloat32:
			dst.SetFloat(float64(b.Float32[pos]))

		case FieldTypeInt256:
			dst.Set(reflect.ValueOf(b.Int256[pos]))

		case FieldTypeInt128:
			dst.Set(reflect.ValueOf(b.Int128[pos]))

		case FieldTypeInt64:
			dst.SetInt(b.Int64[pos])

		case FieldTypeInt32:
			dst.SetInt(int64(b.Int32[pos]))

		case FieldTypeInt16:
			dst.SetInt(int64(b.Int16[pos]))

		case FieldTypeInt8:
			dst.SetInt(int64(b.Int8[pos]))

		case FieldTypeUint64:
			dst.SetUint(b.Uint64[pos])

		case FieldTypeUint32:
			dst.SetUint(uint64(b.Uint32[pos]))

		case FieldTypeUint16:
			dst.SetUint(uint64(b.Uint16[pos]))

		case FieldTypeUint8:
			dst.SetUint(uint64(b.Uint8[pos]))

		case FieldTypeDecimal256:
			switch {
			case field.Flags.Contains(flagUintType):
				dst.SetUint(uint64(b.Int256[pos].Int64()))
			case field.Flags.Contains(flagIntType):
				dst.SetInt(b.Int256[pos].Int64())
			case field.Flags.Contains(flagFloatType):
				dst.SetFloat(NewDecimal256(b.Int256[pos], field.Scale).Float64())
			default:
				val := NewDecimal256(b.Int256[pos], field.Scale)
				dst.Set(reflect.ValueOf(val))
			}

		case FieldTypeDecimal128:
			switch {
			case field.Flags.Contains(flagUintType):
				dst.SetUint(uint64(b.Int128[pos].Int64()))
			case field.Flags.Contains(flagIntType):
				dst.SetInt(b.Int128[pos].Int64())
			case field.Flags.Contains(flagFloatType):
				dst.SetFloat(NewDecimal128(b.Int128[pos], field.Scale).Float64())
			default:
				val := NewDecimal128(b.Int128[pos], field.Scale)
				dst.Set(reflect.ValueOf(val))
			}

		case FieldTypeDecimal64:
			switch {
			case field.Flags.Contains(flagUintType):
				dst.SetUint(uint64(b.Int64[pos]))
			case field.Flags.Contains(flagIntType):
				dst.SetInt(b.Int64[pos])
			case field.Flags.Contains(flagFloatType):
				dst.SetFloat(NewDecimal64(b.Int64[pos], field.Scale).Float64())
			default:
				val := NewDecimal64(b.Int64[pos], field.Scale)
				dst.Set(reflect.ValueOf(val))
			}

		case FieldTypeDecimal32:
			switch {
			case field.Flags.Contains(flagUintType):
				dst.SetUint(uint64(b.Int32[pos]))
			case field.Flags.Contains(flagIntType):
				dst.SetInt(int64(b.Int32[pos]))
			case field.Flags.Contains(flagFloatType):
				dst.SetFloat(NewDecimal32(b.Int32[pos], field.Scale).Float64())
			default:
				val := NewDecimal32(b.Int32[pos], field.Scale)
				dst.Set(reflect.ValueOf(val))
			}

		default:
			return fmt.Errorf("pack: unsupported field type %s", field.Type)
		}
	}
	return nil
}

func (p *Package) FieldAt(index, pos int) (interface{}, error) {
	if p.nFields <= index {
		return nil, fmt.Errorf("pack: invalid field index %d (max=%d)", index, p.nFields)
	}
	if p.nValues <= pos {
		return nil, fmt.Errorf("pack: invalid pos index %d (max=%d)", pos, p.nValues)
	}

	b := p.blocks[index]
	field := p.fields[index]
	if b.IsIgnore() {
		return nil, fmt.Errorf("pack: skipped block %d (%s)", index, field.Type)
	}

	switch field.Type {
	case FieldTypeBytes:
		return b.Bytes[pos], nil

	case FieldTypeString:
		return b.Strings[pos], nil

	case FieldTypeDatetime:
		val := time.Unix(0, b.Int64[pos]).UTC()
		return val, nil

	case FieldTypeBoolean:
		return b.Bits.IsSet(pos), nil

	case FieldTypeFloat64:
		return b.Float64[pos], nil

	case FieldTypeFloat32:
		return b.Float32[pos], nil

	case FieldTypeInt256:
		return b.Int256[pos], nil

	case FieldTypeInt128:
		return b.Int128[pos], nil

	case FieldTypeInt64:
		return b.Int64[pos], nil

	case FieldTypeInt32:
		return b.Int32[pos], nil

	case FieldTypeInt16:
		return b.Int16[pos], nil

	case FieldTypeInt8:
		return b.Int8[pos], nil

	case FieldTypeUint64:
		return b.Uint64[pos], nil

	case FieldTypeUint32:
		return b.Uint32[pos], nil

	case FieldTypeUint16:
		return b.Uint16[pos], nil

	case FieldTypeUint8:
		return b.Uint8[pos], nil

	case FieldTypeDecimal256:
		val := NewDecimal256(b.Int256[pos], field.Scale)
		return val, nil

	case FieldTypeDecimal128:
		val := NewDecimal128(b.Int128[pos], field.Scale)
		return val, nil

	case FieldTypeDecimal64:
		val := NewDecimal64(b.Int64[pos], field.Scale)
		return val, nil

	case FieldTypeDecimal32:
		val := NewDecimal32(b.Int32[pos], field.Scale)
		return val, nil

	default:
		return nil, fmt.Errorf("pack: unsupported field type %s", field.Type)
	}
}

func (p *Package) SetFieldAt(index, pos int, v interface{}) error {
	if p.nFields <= index {
		return fmt.Errorf("pack: invalid field index %d (max=%d)", index, p.nFields)
	}
	if p.nValues <= pos {
		return fmt.Errorf("pack: invalid pos index %d (max=%d)", pos, p.nValues)
	}
	b := p.blocks[index]
	field := p.fields[index]
	if b.IsIgnore() {
		return fmt.Errorf("pack: skipped block %d (%s)", index, field.Type)
	}
	val := reflect.Indirect(reflect.ValueOf(v))
	if !val.IsValid() {
		return fmt.Errorf("pack: invalid value of type %T", v)
	}

	switch field.Type {
	case FieldTypeBytes:
		var buf []byte
		// check if type implements BinaryMarshaler
		if val.CanInterface() && val.Type().Implements(binaryMarshalerType) {
			var err error
			if buf, err = val.Interface().(encoding.BinaryMarshaler).MarshalBinary(); err != nil {
				return err
			}
		} else {
			src := val.Bytes()
			buf = make([]byte, len(src))
			copy(buf, src)
		}
		b.Bytes[pos] = buf

	case FieldTypeString:
		b.Strings[pos] = val.String()

	case FieldTypeDatetime:
		b.Int64[pos] = val.Interface().(time.Time).UnixNano()

	case FieldTypeBoolean:
		if val.Bool() {
			b.Bits.Set(pos)
		} else {
			b.Bits.Clear(pos)
		}

	case FieldTypeFloat64:
		b.Float64[pos] = val.Float()

	case FieldTypeFloat32:
		b.Float32[pos] = float32(val.Float())

	case FieldTypeInt256:
		b.Int256[pos] = val.Interface().(Int256)

	case FieldTypeInt128:
		b.Int128[pos] = val.Interface().(Int128)

	case FieldTypeInt64:
		b.Int64[pos] = val.Int()

	case FieldTypeInt32:
		b.Int32[pos] = int32(val.Int())

	case FieldTypeInt16:
		b.Int16[pos] = int16(val.Int())

	case FieldTypeInt8:
		b.Int8[pos] = int8(val.Int())

	case FieldTypeUint64:
		b.Uint64[pos] = val.Uint()

	case FieldTypeUint32:
		b.Uint32[pos] = uint32(val.Uint())

	case FieldTypeUint16:
		b.Uint16[pos] = uint16(val.Uint())

	case FieldTypeUint8:
		b.Uint8[pos] = uint8(val.Uint())

	case FieldTypeDecimal256:
		b.Int256[pos] = val.Interface().(Decimal256).Quantize(field.Scale).Int256()

	case FieldTypeDecimal128:
		b.Int128[pos] = val.Interface().(Decimal128).Quantize(field.Scale).Int128()

	case FieldTypeDecimal64:
		b.Int64[pos] = val.Interface().(Decimal64).Quantize(field.Scale).Int64()

	case FieldTypeDecimal32:
		b.Int32[pos] = val.Interface().(Decimal32).Quantize(field.Scale).Int32()

	default:
		return fmt.Errorf("pack: unsupported field type %s", field.Type)
	}
	b.SetDirty()
	p.dirty = true
	return nil
}

func (p *Package) isValidAt(index, pos int, typ FieldType) error {
	if index < 0 || p.nFields <= index {
		return ErrNoField
	}
	if p.nValues <= pos {
		return ErrNoColumn
	}
	if p.fields[index].Type != typ {
		return ErrInvalidType
	}
	if p.blocks[index].Type() != typ.BlockType() {
		return ErrInvalidType
	}
	if p.blocks[index].IsIgnore() {
		return fmt.Errorf("pack: skipped block %d (%s)", index, p.fields[index].Type)
	}
	return nil
}

func (p *Package) Uint64At(index, pos int) (uint64, error) {
	if err := p.isValidAt(index, pos, FieldTypeUint64); err != nil {
		return 0, err
	}
	return p.blocks[index].Uint64[pos], nil
}

func (p *Package) Uint32At(index, pos int) (uint32, error) {
	if err := p.isValidAt(index, pos, FieldTypeUint32); err != nil {
		return 0, err
	}
	return p.blocks[index].Uint32[pos], nil
}

func (p *Package) Uint16At(index, pos int) (uint16, error) {
	if err := p.isValidAt(index, pos, FieldTypeUint16); err != nil {
		return 0, err
	}
	return p.blocks[index].Uint16[pos], nil
}

func (p *Package) Uint8At(index, pos int) (uint8, error) {
	if err := p.isValidAt(index, pos, FieldTypeUint8); err != nil {
		return 0, err
	}
	return p.blocks[index].Uint8[pos], nil
}

func (p *Package) Int256At(index, pos int) (Int256, error) {
	if err := p.isValidAt(index, pos, FieldTypeInt256); err != nil {
		return Int256{}, err
	}
	return p.blocks[index].Int256[pos], nil
}

func (p *Package) Int128At(index, pos int) (Int128, error) {
	if err := p.isValidAt(index, pos, FieldTypeInt128); err != nil {
		return Int128{}, err
	}
	return p.blocks[index].Int128[pos], nil
}

func (p *Package) Int64At(index, pos int) (int64, error) {
	if err := p.isValidAt(index, pos, FieldTypeInt64); err != nil {
		return 0, err
	}
	return p.blocks[index].Int64[pos], nil
}

func (p *Package) Int32At(index, pos int) (int32, error) {
	if err := p.isValidAt(index, pos, FieldTypeInt32); err != nil {
		return 0, err
	}
	return p.blocks[index].Int32[pos], nil
}

func (p *Package) Int16At(index, pos int) (int16, error) {
	if err := p.isValidAt(index, pos, FieldTypeInt16); err != nil {
		return 0, err
	}
	return p.blocks[index].Int16[pos], nil
}

func (p *Package) Int8At(index, pos int) (int8, error) {
	if err := p.isValidAt(index, pos, FieldTypeInt8); err != nil {
		return 0, err
	}
	return p.blocks[index].Int8[pos], nil
}

func (p *Package) Float64At(index, pos int) (float64, error) {
	if err := p.isValidAt(index, pos, FieldTypeFloat64); err != nil {
		return 0.0, err
	}
	return p.blocks[index].Float64[pos], nil
}

func (p *Package) Float32At(index, pos int) (float32, error) {
	if err := p.isValidAt(index, pos, FieldTypeFloat32); err != nil {
		return 0.0, err
	}
	return p.blocks[index].Float32[pos], nil
}

func (p *Package) StringAt(index, pos int) (string, error) {
	if err := p.isValidAt(index, pos, FieldTypeString); err != nil {
		return "", err
	}
	return p.blocks[index].Strings[pos], nil
}

func (p *Package) BytesAt(index, pos int) ([]byte, error) {
	if err := p.isValidAt(index, pos, FieldTypeBytes); err != nil {
		return nil, err
	}
	return p.blocks[index].Bytes[pos], nil
}

func (p *Package) BoolAt(index, pos int) (bool, error) {
	if err := p.isValidAt(index, pos, FieldTypeBoolean); err != nil {
		return false, err
	}
	return p.blocks[index].Bits.IsSet(pos), nil
}

func (p *Package) TimeAt(index, pos int) (time.Time, error) {
	if err := p.isValidAt(index, pos, FieldTypeDatetime); err != nil {
		return zeroTime, err
	}
	return time.Unix(0, p.blocks[index].Int64[pos]).UTC(), nil
}

func (p *Package) Decimal32At(index, pos int) (Decimal32, error) {
	if err := p.isValidAt(index, pos, FieldTypeDecimal32); err != nil {
		return Decimal32{}, err
	}
	return NewDecimal32(p.blocks[index].Int32[pos], p.fields[index].Scale), nil
}

func (p *Package) Decimal64At(index, pos int) (Decimal64, error) {
	if err := p.isValidAt(index, pos, FieldTypeDecimal64); err != nil {
		return Decimal64{}, err
	}
	return NewDecimal64(p.blocks[index].Int64[pos], p.fields[index].Scale), nil
}

func (p *Package) Decimal128At(index, pos int) (Decimal128, error) {
	if err := p.isValidAt(index, pos, FieldTypeDecimal128); err != nil {
		return Decimal128{}, err
	}
	return NewDecimal128(p.blocks[index].Int128[pos], p.fields[index].Scale), nil
}

func (p *Package) Decimal256At(index, pos int) (Decimal256, error) {
	if err := p.isValidAt(index, pos, FieldTypeDecimal256); err != nil {
		return Decimal256{}, err
	}
	return NewDecimal256(p.blocks[index].Int256[pos], p.fields[index].Scale), nil
}

func (p *Package) IsZeroAt(index, pos int) bool {
	if p.nFields <= index || p.nValues <= pos {
		return false
	}
	if p.blocks[index].IsIgnore() {
		return false
	}
	field := p.fields[index]
	switch field.Type {
	case FieldTypeInt256,
		FieldTypeInt128,
		FieldTypeInt64,
		FieldTypeInt32,
		FieldTypeInt16,
		FieldTypeInt8,
		FieldTypeUint64,
		FieldTypeUint32,
		FieldTypeUint16,
		FieldTypeUint8,
		FieldTypeDecimal256,
		FieldTypeDecimal128,
		FieldTypeDecimal64,
		FieldTypeDecimal32,
		FieldTypeBoolean:
		// cannot be zero because 0 value has a meaning
		return false
	case FieldTypeFloat64:
		v := p.blocks[index].Float64[pos]
		return math.IsNaN(v) || math.IsInf(v, 0)
	case FieldTypeFloat32:
		v := float64(p.blocks[index].Float32[pos])
		return math.IsNaN(v) || math.IsInf(v, 0)
	case FieldTypeString:
		return len(p.blocks[index].Strings[pos]) == 0
	case FieldTypeBytes:
		return len(p.blocks[index].Bytes[pos]) == 0
	case FieldTypeDatetime:
		val := p.blocks[index].Int64[pos]
		return val == 0 || time.Unix(0, val).IsZero()
	}
	return true
}

func (p *Package) Column(index int) (interface{}, error) {
	if index < 0 || p.nFields <= index {
		return nil, ErrNoField
	}
	b := p.blocks[index]
	field := p.fields[index]
	if b.IsIgnore() {
		return nil, fmt.Errorf("pack: skipped block %d (%s)", index, field.Type)
	}
	slice := b.RawSlice()

	switch field.Type {
	case FieldTypeBytes,
		FieldTypeString,
		FieldTypeFloat64,
		FieldTypeFloat32,
		FieldTypeInt256,
		FieldTypeInt128,
		FieldTypeInt64,
		FieldTypeInt32,
		FieldTypeInt16,
		FieldTypeInt8,
		FieldTypeUint64,
		FieldTypeUint32,
		FieldTypeUint16,
		FieldTypeUint8:
		// direct access, no copy
		return slice, nil

	case FieldTypeDatetime:
		// materialize
		res := make([]time.Time, len(b.Int64))
		for i, v := range b.Int64 {
			res[i] = time.Unix(0, v).UTC()
		}
		return res, nil

	case FieldTypeBoolean:
		// materialized from bitset
		return slice, nil

	case FieldTypeDecimal256:
		// materialize
		return Decimal256Slice{b.Int256, field.Scale}, nil

	case FieldTypeDecimal128:
		// materialize
		return Decimal128Slice{b.Int128, field.Scale}, nil

	case FieldTypeDecimal64:
		// materialize
		return Decimal64Slice{b.Int64, field.Scale}, nil

	case FieldTypeDecimal32:
		// materialize
		return Decimal32Slice{b.Int32, field.Scale}, nil

	default:
		return nil, fmt.Errorf("pack: unsupported type %s", field.Type)
	}
}

func (p *Package) RowAt(pos int) ([]interface{}, error) {
	if p.nValues <= pos {
		return nil, fmt.Errorf("pack: invalid pack offset %d (max %d)", pos, p.nValues)
	}
	// copy one full row of values
	out := make([]interface{}, p.nFields)
	for i, b := range p.blocks {
		// skip
		if b.IsIgnore() {
			continue
		}
		field := p.fields[i]

		switch field.Type {
		case FieldTypeBytes:
			buf := make([]byte, len(b.Bytes[pos]))
			copy(buf, b.Bytes[pos])
			out[i] = buf
		case FieldTypeString:
			str := b.Strings[pos]
			out[i] = str
		case FieldTypeDatetime:
			// materialize
			out[i] = time.Unix(0, b.Int64[pos]).UTC()
		case FieldTypeBoolean:
			out[i] = b.Bits.IsSet(pos)
		case FieldTypeFloat64:
			out[i] = b.Float64[pos]
		case FieldTypeFloat32:
			out[i] = b.Float32[pos]
		case FieldTypeInt256:
			out[i] = b.Int256[pos]
		case FieldTypeInt128:
			out[i] = b.Int128[pos]
		case FieldTypeInt64:
			out[i] = b.Int64[pos]
		case FieldTypeInt32:
			out[i] = b.Int32[pos]
		case FieldTypeInt16:
			out[i] = b.Int16[pos]
		case FieldTypeInt8:
			out[i] = b.Int8[pos]
		case FieldTypeUint64:
			out[i] = b.Uint64[pos]
		case FieldTypeUint32:
			out[i] = b.Uint32[pos]
		case FieldTypeUint16:
			out[i] = b.Uint16[pos]
		case FieldTypeUint8:
			out[i] = b.Uint8[pos]
		case FieldTypeDecimal256:
			// materialize
			out[i] = NewDecimal256(b.Int256[pos], field.Scale)
		case FieldTypeDecimal128:
			// materialize
			out[i] = NewDecimal128(b.Int128[pos], field.Scale)
		case FieldTypeDecimal64:
			// materialize
			out[i] = NewDecimal64(b.Int64[pos], field.Scale)
		case FieldTypeDecimal32:
			// materialize
			out[i] = NewDecimal32(b.Int32[pos], field.Scale)
		default:
			return nil, fmt.Errorf("pack: unsupported type %s", field.Type)
		}
	}
	return out, nil
}

func (p *Package) RangeAt(index, start, end int) (interface{}, error) {
	if p.nFields <= index {
		return nil, fmt.Errorf("pack: invalid field index %d (max=%d)", index, p.nFields)
	}
	if p.nValues <= start || p.nValues <= end {
		return nil, fmt.Errorf("pack: invalid range %d:%d (max=%d)", start, end, p.nValues)
	}
	b := p.blocks[index]
	field := p.fields[index]
	if b.IsIgnore() {
		return nil, fmt.Errorf("pack: skipped block %d (%s)", index, field.Type)
	}

	switch field.Type {
	case FieldTypeBytes:
		// Note: does not copy data; don't reference!
		return b.Bytes[start:end], nil
	case FieldTypeString:
		return b.Strings[start:end], nil
	case FieldTypeDatetime:
		// materialize
		res := make([]time.Time, end-start)
		for i, v := range b.Int64[start:end] {
			res[i+start] = time.Unix(0, v).UTC()
		}
		return res, nil
	case FieldTypeBoolean:
		return b.Bits.SubSlice(start, end-start), nil
	case FieldTypeFloat64:
		return b.Float64[start:end], nil
	case FieldTypeFloat32:
		return b.Float32[start:end], nil
	case FieldTypeInt256:
		return b.Int256[start:end], nil
	case FieldTypeInt128:
		return b.Int128[start:end], nil
	case FieldTypeInt64:
		return b.Int64[start:end], nil
	case FieldTypeInt32:
		return b.Int32[start:end], nil
	case FieldTypeInt16:
		return b.Int16[start:end], nil
	case FieldTypeInt8:
		return b.Int8[start:end], nil
	case FieldTypeUint64:
		return b.Uint64[start:end], nil
	case FieldTypeUint32:
		return b.Uint32[start:end], nil
	case FieldTypeUint16:
		return b.Uint16[start:end], nil
	case FieldTypeUint8:
		return b.Uint8[start:end], nil
	case FieldTypeDecimal256:
		// materialize
		return Decimal256Slice{b.Int256[start:end], field.Scale}, nil
	case FieldTypeDecimal128:
		// materialize
		return Decimal128Slice{b.Int128[start:end], field.Scale}, nil
	case FieldTypeDecimal64:
		// materialize
		return Decimal64Slice{b.Int64[start:end], field.Scale}, nil
	case FieldTypeDecimal32:
		// materialize
		return Decimal32Slice{b.Int32[start:end], field.Scale}, nil
	default:
		return nil, fmt.Errorf("pack: unsupported type %s", field.Type)
	}
}

// ReplaceFrom replaces at most srcLen rows from the current package starting at
// offset dstPos with rows from package src starting at pos srcPos.
// Both packages must have same block order.
func (p *Package) ReplaceFrom(srcPack *Package, dstPos, srcPos, srcLen int) error {
	if srcPack.nFields != p.nFields {
		return fmt.Errorf("pack: invalid src/dst field count %d/%d", srcPack.nFields, p.nFields)
	}
	if srcPack.nValues <= srcPos {
		return fmt.Errorf("pack: invalid source pack offset %d (max %d)", srcPos, srcPack.nValues)
	}
	if srcPack.nValues < srcPos+srcLen {
		return fmt.Errorf("pack: invalid source pack offset %d len %d (max %d)", srcPos, srcLen, srcPack.nValues)
	}
	if p.nValues <= dstPos {
		return fmt.Errorf("pack: invalid dest pack offset %d (max %d)", dstPos, p.nValues)
	}
	// copy at most N rows without overflowing dst
	n := util.Min(p.Len()-dstPos, srcLen)
	for i, dst := range p.blocks {
		src := srcPack.blocks[i]
		// skip
		if dst.IsIgnore() || src.IsIgnore() {
			continue
		}
		srcField := srcPack.fields[i]
		dstField := p.fields[i]
		if srcField.Index != dstField.Index || srcField.Type != dstField.Type {
			return fmt.Errorf("pack: replace from: field mismatch %d (%s) != %d (%s)",
				srcField.Index, srcField.Type, dstField.Index, dstField.Type)
		}

		switch dstField.Type {
		case FieldTypeBytes:
			for j, v := range src.Bytes[srcPos : srcPos+n] {
				if cap(dst.Bytes[dstPos+j]) < len(v) {
					buf := make([]byte, len(v))
					copy(buf, v)
					dst.Bytes[dstPos+j] = buf
				} else {
					dst.Bytes[dstPos+j] = dst.Bytes[dstPos+j][:len(v)]
					copy(dst.Bytes[dstPos+j], v)
				}
			}

		case FieldTypeString:
			copy(dst.Strings[dstPos:], src.Strings[srcPos:srcPos+n])

		case FieldTypeBoolean:
			dst.Bits.Replace(src.Bits, srcPos, n, dstPos)

		case FieldTypeFloat64:
			copy(dst.Float64[dstPos:], src.Float64[srcPos:srcPos+n])

		case FieldTypeFloat32:
			copy(dst.Float32[dstPos:], src.Float32[srcPos:srcPos+n])

		case FieldTypeInt256:
			copy(dst.Int256[dstPos:], src.Int256[srcPos:srcPos+n])

		case FieldTypeInt128:
			copy(dst.Int128[dstPos:], src.Int128[srcPos:srcPos+n])

		case FieldTypeInt64, FieldTypeDatetime:
			copy(dst.Int64[dstPos:], src.Int64[srcPos:srcPos+n])

		case FieldTypeInt32:
			copy(dst.Int32[dstPos:], src.Int32[srcPos:srcPos+n])

		case FieldTypeInt16:
			copy(dst.Int16[dstPos:], src.Int16[srcPos:srcPos+n])

		case FieldTypeInt8:
			copy(dst.Int8[dstPos:], src.Int8[srcPos:srcPos+n])

		case FieldTypeUint64:
			copy(dst.Uint64[dstPos:], src.Uint64[srcPos:srcPos+n])

		case FieldTypeUint32:
			copy(dst.Uint32[dstPos:], src.Uint32[srcPos:srcPos+n])

		case FieldTypeUint16:
			copy(dst.Uint16[dstPos:], src.Uint16[srcPos:srcPos+n])

		case FieldTypeUint8:
			copy(dst.Uint8[dstPos:], src.Uint8[srcPos:srcPos+n])

		case FieldTypeDecimal256:
			sc, dc := srcField.Scale, dstField.Scale
			if sc == dc {
				copy(dst.Int256[dstPos:], src.Int256[srcPos:srcPos+n])
			} else {
				for j, v := range src.Int256[srcPos : srcPos+n] {
					dst.Int256[dstPos+j] = NewDecimal256(v, sc).Quantize(dc).Int256()
				}
			}

		case FieldTypeDecimal128:
			sc, dc := srcField.Scale, dstField.Scale
			if sc == dc {
				copy(dst.Int128[dstPos:], src.Int128[srcPos:srcPos+n])
			} else {
				for j, v := range src.Int128[srcPos : srcPos+n] {
					dst.Int128[dstPos+j] = NewDecimal128(v, sc).Quantize(dc).Int128()
				}
			}

		case FieldTypeDecimal64:
			sc, dc := srcField.Scale, dstField.Scale
			if sc == dc {
				copy(dst.Int64[dstPos:], src.Int64[srcPos:srcPos+n])
			} else {
				for j, v := range src.Int64[srcPos : srcPos+n] {
					dst.Int64[dstPos+j] = NewDecimal64(v, sc).Quantize(dc).Int64()
				}
			}

		case FieldTypeDecimal32:
			sc, dc := srcField.Scale, dstField.Scale
			if sc == dc {
				copy(dst.Int32[dstPos:], src.Int32[srcPos:srcPos+n])
			} else {
				for j, v := range src.Int32[srcPos : srcPos+n] {
					dst.Int32[dstPos+j] = NewDecimal32(v, sc).Quantize(dc).Int32()
				}
			}

		default:
			return fmt.Errorf("pack: invalid data type %s", dstField.Type)
		}
		dst.SetDirty()
	}
	p.dirty = true
	return nil
}

// note: will panic on package schema mismatch
func (p *Package) AppendFrom(srcPack *Package, srcPos, srcLen int, safecopy bool) error {
	if srcPack.nFields != p.nFields {
		return fmt.Errorf("pack: invalid src/dst field count %d/%d", srcPack.nFields, p.nFields)
	}
	if srcPack.nValues <= srcPos {
		return fmt.Errorf("pack: invalid source pack offset %d (max %d)", srcPos, srcPack.nValues)
	}
	if srcPack.nValues < srcPos+srcLen {
		return fmt.Errorf("pack: invalid source pack offset %d len %d (max %d)", srcPos, srcLen, srcPack.nValues)
	}
	for i, dst := range p.blocks {
		src := srcPack.blocks[i]
		if dst.IsIgnore() || src.IsIgnore() {
			continue
		}
		srcField := srcPack.fields[i]
		dstField := p.fields[i]
		if srcField.Index != dstField.Index || srcField.Type != dstField.Type {
			return fmt.Errorf("pack: replace from: field mismatch %d (%s) != %d (%s)",
				srcField.Index, srcField.Type, dstField.Index, dstField.Type)
		}

		switch dstField.Type {
		case FieldTypeBytes:
			if safecopy {
				for _, v := range src.Bytes[srcPos : srcPos+srcLen] {
					buf := make([]byte, len(v))
					copy(buf, v)
					dst.Bytes = append(dst.Bytes, buf)
				}
			} else {
				dst.Bytes = append(dst.Bytes, src.Bytes[srcPos:srcPos+srcLen]...)
			}

		case FieldTypeString:
			dst.Strings = append(dst.Strings, src.Strings[srcPos:srcPos+srcLen]...)

		case FieldTypeBoolean:
			dst.Bits.Append(src.Bits, srcPos, srcLen)

		case FieldTypeFloat64:
			dst.Float64 = append(dst.Float64, src.Float64[srcPos:srcPos+srcLen]...)

		case FieldTypeFloat32:
			dst.Float32 = append(dst.Float32, src.Float32[srcPos:srcPos+srcLen]...)

		case FieldTypeInt256:
			dst.Int256 = append(dst.Int256, src.Int256[srcPos:srcPos+srcLen]...)

		case FieldTypeInt128:
			dst.Int128 = append(dst.Int128, src.Int128[srcPos:srcPos+srcLen]...)

		case FieldTypeInt64, FieldTypeDatetime:
			dst.Int64 = append(dst.Int64, src.Int64[srcPos:srcPos+srcLen]...)

		case FieldTypeInt32:
			dst.Int32 = append(dst.Int32, src.Int32[srcPos:srcPos+srcLen]...)

		case FieldTypeInt16:
			dst.Int16 = append(dst.Int16, src.Int16[srcPos:srcPos+srcLen]...)

		case FieldTypeInt8:
			dst.Int8 = append(dst.Int8, src.Int8[srcPos:srcPos+srcLen]...)

		case FieldTypeUint64:
			dst.Uint64 = append(dst.Uint64, src.Uint64[srcPos:srcPos+srcLen]...)

		case FieldTypeUint32:
			dst.Uint32 = append(dst.Uint32, src.Uint32[srcPos:srcPos+srcLen]...)

		case FieldTypeUint16:
			dst.Uint16 = append(dst.Uint16, src.Uint16[srcPos:srcPos+srcLen]...)

		case FieldTypeUint8:
			dst.Uint8 = append(dst.Uint8, src.Uint8[srcPos:srcPos+srcLen]...)

		case FieldTypeDecimal256:
			sc, dc := srcField.Scale, dstField.Scale
			if sc == dc {
				dst.Int256 = append(dst.Int256, src.Int256[srcPos:srcPos+srcLen]...)
			} else {
				for _, v := range src.Int256[srcPos : srcPos+srcLen] {
					dst.Int256 = append(dst.Int256, NewDecimal256(v, sc).Quantize(dc).Int256())
				}
			}

		case FieldTypeDecimal128:
			sc, dc := srcField.Scale, dstField.Scale
			if sc == dc {
				dst.Int128 = append(dst.Int128, src.Int128[srcPos:srcPos+srcLen]...)
			} else {
				for _, v := range src.Int128[srcPos : srcPos+srcLen] {
					dst.Int128 = append(dst.Int128, NewDecimal128(v, sc).Quantize(dc).Int128())
				}
			}

		case FieldTypeDecimal64:
			sc, dc := srcField.Scale, dstField.Scale
			if sc == dc {
				dst.Int64 = append(dst.Int64, src.Int64[srcPos:srcPos+srcLen]...)
			} else {
				for _, v := range src.Int64[srcPos : srcPos+srcLen] {
					dst.Int64 = append(dst.Int64, NewDecimal64(v, sc).Quantize(dc).Int64())
				}
			}

		case FieldTypeDecimal32:
			sc, dc := srcField.Scale, dstField.Scale
			if sc == dc {
				dst.Int32 = append(dst.Int32, src.Int32[srcPos:srcPos+srcLen]...)
			} else {
				for _, v := range src.Int32[srcPos : srcPos+srcLen] {
					dst.Int32 = append(dst.Int32, NewDecimal32(v, sc).Quantize(dc).Int32())
				}
			}

		default:
			return fmt.Errorf("pack: invalid data type %s", dstField.Type)
		}
		dst.SetDirty()
	}
	p.nValues += srcLen
	p.dirty = true
	return nil
}

// appends an empty row with default/zero values
func (p *Package) Append() error {
	for i, b := range p.blocks {
		if b.IsIgnore() {
			continue
		}
		field := p.fields[i]

		switch field.Type {
		case FieldTypeBytes:
			b.Bytes = append(b.Bytes, []byte{})

		case FieldTypeString:
			b.Strings = append(b.Strings, "")

		case FieldTypeBoolean:
			b.Bits.Grow(b.Bits.Len() + 1)

		case FieldTypeFloat64:
			b.Float64 = append(b.Float64, 0)

		case FieldTypeFloat32:
			b.Float32 = append(b.Float32, 0)

		case FieldTypeInt256, FieldTypeDecimal256:
			b.Int256 = append(b.Int256, ZeroInt256)

		case FieldTypeInt128, FieldTypeDecimal128:
			b.Int128 = append(b.Int128, ZeroInt128)

		case FieldTypeInt64, FieldTypeDatetime, FieldTypeDecimal64:
			b.Int64 = append(b.Int64, 0)

		case FieldTypeInt32, FieldTypeDecimal32:
			b.Int32 = append(b.Int32, 0)

		case FieldTypeInt16:
			b.Int16 = append(b.Int16, 0)

		case FieldTypeInt8:
			b.Int8 = append(b.Int8, 0)

		case FieldTypeUint64:
			b.Uint64 = append(b.Uint64, 0)

		case FieldTypeUint32:
			b.Uint32 = append(b.Uint32, 0)

		case FieldTypeUint16:
			b.Uint16 = append(b.Uint16, 0)

		case FieldTypeUint8:
			b.Uint8 = append(b.Uint8, 0)

		default:
			return fmt.Errorf("pack: invalid data type %s", field.Type)
		}
		b.SetDirty()
	}
	p.nValues++
	p.dirty = true
	return nil
}

// append n empty rows with default/zero values
func (p *Package) Grow(n int) error {
	if n <= 0 {
		return fmt.Errorf("pack: grow requires positive value")
	}
	for i, b := range p.blocks {
		if b.IsIgnore() {
			continue
		}
		field := p.fields[i]

		switch field.Type {
		case FieldTypeBytes:
			b.Bytes = append(b.Bytes, make([][]byte, n)...)

		case FieldTypeString:
			b.Strings = append(b.Strings, make([]string, n)...)

		case FieldTypeBoolean:
			b.Bits.Grow(b.Bits.Len() + n)

		case FieldTypeFloat64:
			b.Float64 = append(b.Float64, make([]float64, n)...)

		case FieldTypeFloat32:
			b.Float32 = append(b.Float32, make([]float32, n)...)

		case FieldTypeInt256, FieldTypeDecimal256:
			b.Int256 = append(b.Int256, make([]Int256, n)...)

		case FieldTypeInt128, FieldTypeDecimal128:
			b.Int128 = append(b.Int128, make([]Int128, n)...)

		case FieldTypeInt64, FieldTypeDatetime, FieldTypeDecimal64:
			b.Int64 = append(b.Int64, make([]int64, n)...)

		case FieldTypeInt32, FieldTypeDecimal32:
			b.Int32 = append(b.Int32, make([]int32, n)...)

		case FieldTypeInt16:
			b.Int16 = append(b.Int16, make([]int16, n)...)

		case FieldTypeInt8:
			b.Int8 = append(b.Int8, make([]int8, n)...)

		case FieldTypeUint64:
			b.Uint64 = append(b.Uint64, make([]uint64, n)...)

		case FieldTypeUint32:
			b.Uint32 = append(b.Uint32, make([]uint32, n)...)

		case FieldTypeUint16:
			b.Uint16 = append(b.Uint16, make([]uint16, n)...)

		case FieldTypeUint8:
			b.Uint8 = append(b.Uint8, make([]uint8, n)...)

		default:
			return fmt.Errorf("pack: invalid data type %s", field.Type)
		}
		b.SetDirty()
	}
	p.nValues += n
	p.dirty = true
	return nil
}

func (p *Package) Delete(pos, n int) error {
	if n <= 0 {
		return nil
	}
	if p.nValues <= pos {
		return fmt.Errorf("pack: invalid pack offset %d (max %d)", pos, p.nValues)
	}
	n = util.Min(p.Len()-pos, n)
	for i, b := range p.blocks {
		if b.IsIgnore() {
			continue
		}
		field := p.fields[i]

		switch field.Type {
		case FieldTypeBytes:
			// avoid mem leaks
			for j, l := pos, pos+n; j < l; j++ {
				b.Bytes[j] = nil
			}
			b.Bytes = append(b.Bytes[:pos], b.Bytes[pos+n:]...)

		case FieldTypeString:
			// avoid mem leaks
			for j, l := pos, pos+n; j < l; j++ {
				b.Strings[j] = ""
			}
			b.Strings = append(b.Strings[:pos], b.Strings[pos+n:]...)

		case FieldTypeBoolean:
			b.Bits.Delete(pos, n)

		case FieldTypeFloat64:
			b.Float64 = append(b.Float64[:pos], b.Float64[pos+n:]...)

		case FieldTypeFloat32:
			b.Float32 = append(b.Float32[:pos], b.Float32[pos+n:]...)

		case FieldTypeInt256, FieldTypeDecimal256:
			b.Int256 = append(b.Int256[:pos], b.Int256[pos+n:]...)

		case FieldTypeInt128, FieldTypeDecimal128:
			b.Int128 = append(b.Int128[:pos], b.Int128[pos+n:]...)

		case FieldTypeInt64, FieldTypeDatetime, FieldTypeDecimal64:
			b.Int64 = append(b.Int64[:pos], b.Int64[pos+n:]...)

		case FieldTypeInt32, FieldTypeDecimal32:
			b.Int32 = append(b.Int32[:pos], b.Int32[pos+n:]...)

		case FieldTypeInt16:
			b.Int16 = append(b.Int16[:pos], b.Int16[pos+n:]...)

		case FieldTypeInt8:
			b.Int8 = append(b.Int8[:pos], b.Int8[pos+n:]...)

		case FieldTypeUint64:
			b.Uint64 = append(b.Uint64[:pos], b.Uint64[pos+n:]...)

		case FieldTypeUint32:
			b.Uint32 = append(b.Uint32[:pos], b.Uint32[pos+n:]...)

		case FieldTypeUint16:
			b.Uint16 = append(b.Uint16[:pos], b.Uint16[pos+n:]...)

		case FieldTypeUint8:
			b.Uint8 = append(b.Uint8[:pos], b.Uint8[pos+n:]...)

		default:
			return fmt.Errorf("pack: invalid data type %s", field.Type)
		}
		b.SetDirty()
	}
	p.nValues -= n
	p.dirty = true
	return nil
}

func (p *Package) Clear() {
	for _, v := range p.blocks {
		v.Clear()
	}
	// Note: we keep all type-related data and blocks
	// also keep pack key to avoid clearing journal/tombstone identity
	p.nValues = 0
	p.dirty = true
	p.cached = false
	p.size = 0
}

func (p *Package) Release() {
	for _, v := range p.blocks {
		v.Release()
	}
	p.nFields = 0
	p.nValues = 0
	p.blocks = nil
	p.key = 0
	p.tinfo = nil
	p.pkindex = -1
	p.dirty = false
	p.cached = false
	p.stripped = false
	p.size = 0
}

func (p *Package) HeapSize() int {
	var sz int
	for _, v := range p.blocks {
		sz += v.HeapSize()
	}
	return sz
}

// Searches id in primary key column and return index or -1 when not found
// This function is only safe to use when packs are sorted!
func (p *Package) PkIndex(id uint64, last int) int {
	// primary key field required
	if p.pkindex < 0 || p.Len() <= last {
		return -1
	}

	// search for id value in pk block (always an uint64) starting at last index
	// this helps limiting search space when ids are pre-sorted
	slice := p.blocks[p.pkindex].Uint64[last:]
	l := len(slice)
	min, max := slice[0], slice[l-1]
	if id < min || id > max {
		return -1
	}

	// for dense packs (pk's are continuous) compute offset directly
	if l == int(max-min)+1 {
		return int(id-min) + last
	}

	// for sparse pk spaces, use binary search on sorted slices
	idx := sort.Search(l, func(i int) bool { return slice[i] >= id })
	if idx < l && slice[idx] == id {
		return idx + last
	}
	return -1
}

// Searches id in primary key column and return index or -1 when not found,
// use this function when pack is unsorted as when updates/inserts are out of order.
func (p *Package) PkIndexUnsorted(id uint64, last int) int {
	// primary key field required
	if p.pkindex < 0 || p.Len() <= last {
		return -1
	}

	// search for id value in pk block (always an uint64) starting at last index
	// this helps limiting search space when ids are pre-sorted
	slice := p.blocks[p.pkindex].Uint64[last:]

	// run full scan on unsorted slices
	for i, v := range slice {
		if v != id {
			continue
		}
		return i + last
	}
	return -1
}

type PackageSorter struct {
	*Package
	col int
	typ FieldType
	b   *block.Block
}

func (p *PackageSorter) Len() int { return p.Package.Len() }

func (p *PackageSorter) Less(i, j int) bool {
	switch p.typ {
	case FieldTypeBytes:
		return bytes.Compare(p.b.Bytes[i], p.b.Bytes[j]) < 0

	case FieldTypeString:
		return p.b.Strings[i] < p.b.Strings[j]

	case FieldTypeBoolean:
		return !p.b.Bits.IsSet(i) && p.b.Bits.IsSet(j)

	case FieldTypeFloat64:
		return p.b.Float64[i] < p.b.Float64[j]

	case FieldTypeFloat32:
		return p.b.Float32[i] < p.b.Float32[j]

	case FieldTypeInt256, FieldTypeDecimal256:
		return p.b.Int256[i].Lt(p.b.Int256[j])

	case FieldTypeInt128, FieldTypeDecimal128:
		return p.b.Int128[i].Lt(p.b.Int128[j])

	case FieldTypeInt64, FieldTypeDatetime, FieldTypeDecimal64:
		return p.b.Int64[i] < p.b.Int64[j]

	case FieldTypeInt32, FieldTypeDecimal32:
		return p.b.Int32[i] < p.b.Int32[j]

	case FieldTypeInt16:
		return p.b.Int16[i] < p.b.Int16[j]

	case FieldTypeInt8:
		return p.b.Int8[i] < p.b.Int8[j]

	case FieldTypeUint64:
		return p.b.Uint64[i] < p.b.Uint64[j]

	case FieldTypeUint32:
		return p.b.Uint32[i] < p.b.Uint32[j]

	case FieldTypeUint16:
		return p.b.Uint16[i] < p.b.Uint16[j]

	case FieldTypeUint8:
		return p.b.Uint8[i] < p.b.Uint8[j]

	default:
		return false
	}
}

func (p *PackageSorter) Swap(i, j int) {
	for n := 0; n < p.Package.nFields; n++ {
		b := p.Package.blocks[n]
		if b.IsIgnore() {
			continue
		}

		switch p.Package.fields[n].Type {
		case FieldTypeBytes:
			b.Bytes[i], b.Bytes[j] = b.Bytes[j], b.Bytes[i]

		case FieldTypeString:
			b.Strings[i], b.Strings[j] = b.Strings[j], b.Strings[i]

		case FieldTypeBoolean:
			b.Bits.Swap(i, j)

		case FieldTypeFloat64:
			b.Float64[i], b.Float64[j] = b.Float64[j], b.Float64[i]

		case FieldTypeFloat32:
			b.Float32[i], b.Float32[j] = b.Float32[j], b.Float32[i]

		case FieldTypeInt256, FieldTypeDecimal256:
			b.Int256[i], b.Int256[j] = b.Int256[j], b.Int256[i]

		case FieldTypeInt128, FieldTypeDecimal128:
			b.Int128[i], b.Int128[j] = b.Int128[j], b.Int128[i]

		case FieldTypeInt64, FieldTypeDatetime, FieldTypeDecimal64:
			b.Int64[i], b.Int64[j] = b.Int64[j], b.Int64[i]

		case FieldTypeInt32, FieldTypeDecimal32:
			b.Int32[i], b.Int32[j] = b.Int32[j], b.Int32[i]

		case FieldTypeInt16:
			b.Int16[i], b.Int16[j] = b.Int16[j], b.Int16[i]

		case FieldTypeInt8:
			b.Int8[i], b.Int8[j] = b.Int8[j], b.Int8[i]

		case FieldTypeUint64:
			b.Uint64[i], b.Uint64[j] = b.Uint64[j], b.Uint64[i]

		case FieldTypeUint32:
			b.Uint32[i], b.Uint32[j] = b.Uint32[j], b.Uint32[i]

		case FieldTypeUint16:
			b.Uint16[i], b.Uint16[j] = b.Uint16[j], b.Uint16[i]

		case FieldTypeUint8:
			b.Uint8[i], b.Uint8[j] = b.Uint8[j], b.Uint8[i]

		}
	}
}

func (p *Package) PkSort() error {
	if p.pkindex < 0 {
		return fmt.Errorf("pack: missing primary key field")
	}

	if p.Len() == 0 {
		return nil
	}

	spkg := &PackageSorter{
		Package: p,
		col:     p.pkindex,
		typ:     p.fields[p.pkindex].Type,
		b:       p.blocks[p.pkindex],
	}
	if !sort.IsSorted(spkg) {
		sort.Sort(spkg)
		p.dirty = true
	}
	return nil
}
