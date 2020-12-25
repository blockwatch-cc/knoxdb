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

const (
	packageStorageFormatVersionV1 = 1 // OSS: same as V3
	packageStorageFormatVersionV2 = 2 // PRO: compress & precision stored in pack header
	packageStorageFormatVersionV3 = 3 // PRO: current, per-block compression & precision
	packageStorageFormatVersionV4 = 4 // PRO: extended data types
)

type Package struct {
	used    int64          // atomic reference counter, can be recycled when 0
	version byte           // 8bit
	nFields int            // 8bit
	nValues int            // 32bit
	offsets []int          // nFields * 32bit block offsets (starting after header)
	names   []string       // field names (optional) null-terminated strings
	blocks  []*block.Block // compressed blocks, one per field

	// not stored
	types      []FieldType // field types
	namemap    map[string]int
	key        []byte
	tinfo      *typeInfo
	pkindex    int
	pkmap      map[uint64]int // lazy-generated map
	packedsize int            // zipped total size including header
	rawsize    int            // unzipped size after snappy decompression
	dirty      bool
	cached     bool
	stripped   bool // some blocks are ignored, don't store!
}

func (p *Package) Key() []byte {
	return p.key
}

func (p *Package) SetKey(key []byte) {
	p.key = key
}

func (p *Package) PkMap() map[uint64]int {
	if p.pkmap != nil {
		return p.pkmap
	}
	if p.pkindex < 0 {
		return nil
	}
	p.pkmap = make(map[uint64]int, p.nValues)
	for i, v := range p.blocks[p.pkindex].Uint64 {
		p.pkmap[v] = i
	}
	return p.pkmap
}

func NewPackage() *Package {
	return &Package{
		version: packageStorageFormatVersionV4,
		pkindex: -1,
		namemap: make(map[string]int),
	}
}

func (p *Package) HasNames() bool {
	return len(p.names) > 0
}

func (p *Package) IsDirty() bool {
	return p.dirty
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
	if i, ok := p.namemap[name]; ok {
		return int(i)
	}
	return -1
}

func (p *Package) FieldByName(name string) Field {
	return p.FieldById(p.FieldIndex(name))
}

func (p *Package) FieldById(idx int) Field {
	if idx < 0 {
		return Field{Index: -1}
	}
	return Field{
		Index: idx,
		Name:  p.names[idx],
		Type:  p.types[idx],
		Alias: p.tinfo.fields[idx].alias,
		Flags: p.tinfo.fields[idx].flags,
		Scale: p.blocks[idx].Scale(),
	}
}

func (p *Package) Contains(fields FieldList) bool {
	for _, v := range fields {
		if _, ok := p.namemap[v.Name]; !ok {
			return false
		}
	}
	return true
}

func (p *Package) initType(v interface{}) error {
	if p.tinfo != nil && p.tinfo.gotype {
		return nil
	}
	tinfo, err := getTypeInfo(v)
	if err != nil {
		return err
	}
	p.tinfo = tinfo
	if p.pkindex < 0 {
		p.pkindex = tinfo.PkColumn()
	}
	return nil
}

// Init from Go type
func (p *Package) Init(v interface{}, sz int) error {
	// detect and map Go type
	err := p.initType(v)
	if err != nil {
		return err
	}

	// extract fields from Go type
	fields, err := Fields(v)
	if err != nil {
		return err
	}

	if len(fields) > 256 {
		return fmt.Errorf("pack: cannot handle more than 256 fields")
	}

	// create pack
	p.nFields = len(fields)
	p.blocks = make([]*block.Block, p.nFields)
	p.offsets = make([]int, p.nFields)
	p.names = make([]string, p.nFields)
	p.types = make([]FieldType, p.nFields)
	p.namemap = make(map[string]int)
	p.dirty = true

	// create blocks
	for i, field := range fields {
		p.names[i] = field.Name
		p.types[i] = field.Type
		p.namemap[field.Name] = i
		p.namemap[field.Alias] = i
		p.blocks[i], err = block.NewBlock(
			field.Type.BlockType(),
			sz,
			field.Flags.Compression(),
			field.Scale,
			field.Flags.BlockFlags(),
		)
		if err != nil {
			return err
		}
	}
	return err
}

// init from field list when Go type is unavailable
func (p *Package) InitFields(fields FieldList, sz int) error {
	var err error
	if len(fields) > 256 {
		return fmt.Errorf("pack: cannot handle more than 256 fields")
	}

	// create pack
	p.nFields = len(fields)
	p.blocks = make([]*block.Block, p.nFields)
	p.offsets = make([]int, p.nFields)
	p.names = make([]string, p.nFields)
	p.types = make([]FieldType, p.nFields)
	p.namemap = make(map[string]int)
	p.dirty = true

	// fill type info from fields
	p.tinfo = &typeInfo{
		fields: make([]fieldInfo, p.nFields),
		gotype: false,
	}
	for i, field := range fields {
		// fill type info from fields
		if field.Flags&FlagPrimary > 0 {
			p.pkindex = i
		}
		p.tinfo.fields[i].name = field.Name
		p.tinfo.fields[i].alias = field.Alias
		p.tinfo.fields[i].flags = field.Flags
		p.tinfo.fields[i].blockid = i

		// register field
		p.names[i] = field.Name
		p.types[i] = field.Type
		p.namemap[field.Name] = i
		p.namemap[field.Alias] = i

		// alloc block
		p.blocks[i], err = block.NewBlock(
			field.Type.BlockType(),
			sz,
			field.Flags.Compression(),
			field.Scale,
			field.Flags.BlockFlags(),
		)
		if err != nil {
			return err
		}
	}
	return err
}

func (p *Package) Clone(copydata bool, sz int) (*Package, error) {
	np := &Package{
		version:  p.version,
		nFields:  p.nFields,
		nValues:  0,
		offsets:  make([]int, p.nFields),
		names:    p.names, // share static field names
		types:    p.types, // share static field types
		namemap:  make(map[string]int),
		blocks:   make([]*block.Block, p.nFields),
		key:      nil, // cloned pack has no identity yet
		dirty:    true,
		stripped: p.stripped, // cloning a stripped pack is allowed
		tinfo:    p.tinfo,    // share static type info
		pkindex:  p.pkindex,
	}

	// create new empty block slices
	for i, b := range p.blocks {
		var err error
		np.blocks[i], err = b.Clone(sz, copydata)
		if err != nil {
			return nil, err
		}
		np.namemap[np.names[i]] = i
	}

	if copydata {
		np.nValues = p.nValues
	}
	return np, nil
}

func (p *Package) KeepFields(fields FieldList) *Package {
	if len(fields) == 0 {
		return p
	}
	for i, v := range p.names {
		if !fields.Contains(v) {
			p.blocks[i].Release()
			p.blocks[i].SetIgnore()
			p.stripped = true
		}
	}
	return p
}

// removes old aliases and sets new alias names
func (p *Package) UpdateAliasesFrom(fields FieldList) *Package {
	if len(fields) == 0 {
		return p
	}
	for _, v := range fields {
		if v.Index < 0 || v.Index-1 > p.nFields {
			continue
		}
		delete(p.namemap, v.Alias)
		p.namemap[v.Alias] = v.Index
	}
	return p
}

// adds new alias names
func (p *Package) UpdateAliases(aliases []string) *Package {
	if len(aliases) == 0 {
		return p
	}
	for i, v := range aliases {
		if i >= p.nFields {
			continue
		}
		p.namemap[v] = i
	}
	return p
}

// Push append a new row to all columns. Requires a type that strictly defines
// all columns in this pack! Column mapping uses the default struct tag `pack`,
// hence the fields name only (not the fields alias).
func (p *Package) Push(v interface{}) error {
	if err := p.initType(v); err != nil {
		return err
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
		// skip early
		if b.IsIgnore() {
			continue
		}
		f := fi.value(val)

		switch p.types[fi.blockid] {
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
			b.Bools = append(b.Bools, f.Bool())
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
			b.Int256 = append(b.Int256, f.Interface().(Decimal256).Quantize(b.Scale()).Int256())
		case FieldTypeDecimal128:
			b.Int128 = append(b.Int128, f.Interface().(Decimal128).Quantize(b.Scale()).Int128())
		case FieldTypeDecimal64:
			b.Int64 = append(b.Int64, f.Interface().(Decimal64).Quantize(b.Scale()).Int64())
		case FieldTypeDecimal32:
			b.Int32 = append(b.Int32, f.Interface().(Decimal32).Quantize(b.Scale()).Int32())
		default:
			return fmt.Errorf("pack: unsupported type %s (%v)", f.Type().String(), f.Kind())
		}
		b.SetDirty()
	}
	p.nValues++
	p.dirty = true
	p.pkmap = nil
	return nil
}

// ReplaceAt replaces a row at offset pos across all columns. Requires a type
// that strictly defines all columns in this pack! Column mapping uses the
// default struct tag `pack`,  hence the fields name only (not the fields alias).
func (p *Package) ReplaceAt(pos int, v interface{}) error {
	if err := p.initType(v); err != nil {
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
		// skip early
		if b.IsIgnore() {
			continue
		}
		f := fi.value(val)

		switch p.types[fi.blockid] {
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
			b.Bools[pos] = f.Bool()

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
			b.Int256[pos] = f.Interface().(Decimal256).Quantize(b.Scale()).Int256()

		case FieldTypeDecimal128:
			b.Int128[pos] = f.Interface().(Decimal128).Quantize(b.Scale()).Int128()

		case FieldTypeDecimal64:
			b.Int64[pos] = f.Interface().(Decimal64).Quantize(b.Scale()).Int64()

		case FieldTypeDecimal32:
			b.Int32[pos] = f.Interface().(Decimal32).Quantize(b.Scale()).Int32()

		default:
			return fmt.Errorf("pack: unsupported type %s (%v)", f.Type().String(), f.Kind())
		}
		// set flag to indicate we must reparse min/max values when storing the pack
		b.SetDirty()
	}
	p.dirty = true
	p.pkmap = nil
	return nil
}

// ReadAt reads a row at offset pos and unmarshals values into an arbitrary type.
// Will set struct fields based on name and alias as defined by struct tags `pack`
// and `json`.
func (p *Package) ReadAt(pos int, v interface{}) error {
	if p.tinfo == nil || !p.tinfo.gotype {
		tinfo, err := getTypeInfo(v)
		if err != nil {
			return err
		}
		p.tinfo = tinfo
	}
	return p.ReadAtWithInfo(pos, v, p.tinfo)
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
		if b.IsIgnore() {
			continue
		}

		dst := fi.value(val)
		if !dst.IsValid() {
			continue
		}
		dst0 := dst
		if dst.Kind() == reflect.Ptr {
			if dst.IsNil() && dst.CanSet() {
				dst.Set(reflect.New(dst.Type().Elem()))
			}
			dst = dst.Elem()
		}

		switch p.types[fi.blockid] {
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
			dst.SetBool(b.Bools[pos])

		case FieldTypeFloat64:
			dst.SetFloat(b.Float64[pos])

		case FieldTypeFloat32:
			dst.SetFloat(float64(b.Float32[pos]))

		case FieldTypeInt256:
			dst.Set(reflect.ValueOf(b.Int256))

		case FieldTypeInt128:
			dst.Set(reflect.ValueOf(b.Int128))

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
			val := NewDecimal256(b.Int256[pos], b.Scale())
			dst.Set(reflect.ValueOf(val))

		case FieldTypeDecimal128:
			val := NewDecimal128(b.Int128[pos], b.Scale())
			dst.Set(reflect.ValueOf(val))

		case FieldTypeDecimal64:
			val := NewDecimal64(b.Int64[pos], b.Scale())
			dst.Set(reflect.ValueOf(val))

		case FieldTypeDecimal32:
			val := NewDecimal32(b.Int32[pos], b.Scale())
			dst.Set(reflect.ValueOf(val))

		default:
			return fmt.Errorf("pack: unsupported type %s (%v)", dst0.Type().String(), dst0.Kind())
		}
	}
	return nil
}

func (p *Package) ForEach(proto interface{}, fn func(i int, val interface{}) error) error {
	if p.tinfo == nil || !p.tinfo.gotype {
		tinfo, err := getTypeInfo(proto)
		if err != nil {
			return err
		}
		p.tinfo = tinfo
	}
	typ := derefIndirect(proto).Type()
	for i := 0; i < p.nValues; i++ {
		// create new empty value for interface prototype
		val := reflect.New(typ)
		if err := p.ReadAtWithInfo(i, val.Interface(), p.tinfo); err != nil {
			return err
		}
		if err := fn(i, val.Interface()); err != nil {
			return err
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
	if b.IsIgnore() {
		return nil, fmt.Errorf("pack: skipped block %d (%s)", index, p.types[index])
	}

	switch p.types[index] {
	case FieldTypeBytes:
		return b.Bytes[pos], nil

	case FieldTypeString:
		return b.Strings[pos], nil

	case FieldTypeDatetime:
		val := time.Unix(0, b.Int64[pos]).UTC()
		return val, nil

	case FieldTypeBoolean:
		return b.Bools[pos], nil

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
		val := NewDecimal256(b.Int256[pos], b.Scale())
		return val, nil

	case FieldTypeDecimal128:
		val := NewDecimal128(b.Int128[pos], b.Scale())
		return val, nil

	case FieldTypeDecimal64:
		val := NewDecimal64(b.Int64[pos], b.Scale())
		return val, nil

	case FieldTypeDecimal32:
		val := NewDecimal32(b.Int32[pos], b.Scale())
		return val, nil

	default:
		return nil, fmt.Errorf("pack: unsupported type %s", p.types[index])
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
	if b.IsIgnore() {
		return fmt.Errorf("pack: skipped block %d (%s)", index, p.types[index])
	}
	val := reflect.Indirect(reflect.ValueOf(v))
	if !val.IsValid() {
		return fmt.Errorf("pack: invalid value of type %T", v)
	}

	switch p.types[index] {
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
		b.Bools[pos] = val.Bool()

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
		b.Int256[pos] = val.Interface().(Decimal256).Quantize(b.Scale()).Int256()

	case FieldTypeDecimal128:
		b.Int128[pos] = val.Interface().(Decimal128).Quantize(b.Scale()).Int128()

	case FieldTypeDecimal64:
		b.Int64[pos] = val.Interface().(Decimal64).Quantize(b.Scale()).Int64()

	case FieldTypeDecimal32:
		b.Int32[pos] = val.Interface().(Decimal32).Quantize(b.Scale()).Int32()

	default:
		return fmt.Errorf("pack: unsupported type %s", p.types[index])
	}
	b.SetDirty()
	p.dirty = true
	if p.pkindex == index {
		p.pkmap = nil
	}
	return nil
}

func (p *Package) isValidAt(index, pos int, typ FieldType) error {
	if index < 0 || p.nFields <= index {
		return ErrNoField
	}
	if p.nValues <= pos {
		return ErrNoColumn
	}
	if p.types[index] != typ {
		return ErrInvalidType
	}
	if p.blocks[index].Type() != typ.BlockType() {
		return ErrInvalidType
	}
	if p.blocks[index].IsIgnore() {
		return fmt.Errorf("pack: skipped block %d (%s)", index, p.types[index])
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
	return p.blocks[index].Bools[pos], nil
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
	return NewDecimal32(p.blocks[index].Int32[pos], p.blocks[index].Scale()), nil
}

func (p *Package) Decimal64At(index, pos int) (Decimal64, error) {
	if err := p.isValidAt(index, pos, FieldTypeDecimal64); err != nil {
		return Decimal64{}, err
	}
	return NewDecimal64(p.blocks[index].Int64[pos], p.blocks[index].Scale()), nil
}

func (p *Package) Decimal128At(index, pos int) (Decimal128, error) {
	if err := p.isValidAt(index, pos, FieldTypeDecimal128); err != nil {
		return Decimal128{}, err
	}
	return NewDecimal128(p.blocks[index].Int128[pos], p.blocks[index].Scale()), nil
}

func (p *Package) Decimal256At(index, pos int) (Decimal256, error) {
	if err := p.isValidAt(index, pos, FieldTypeDecimal256); err != nil {
		return Decimal256{}, err
	}
	return NewDecimal256(p.blocks[index].Int256[pos], p.blocks[index].Scale()), nil
}

func (p *Package) IsZeroAt(index, pos int) bool {
	if p.nFields <= index || p.nValues <= pos {
		return false
	}
	if p.blocks[index].IsIgnore() {
		return false
	}
	switch p.types[index] {
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
	if b.IsIgnore() {
		return nil, fmt.Errorf("pack: skipped block %d (%s)", index, p.types[index])
	}
	slice := b.RawSlice()

	switch p.types[index] {
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
		// TODO: materialize when using bitset
		return slice, nil

	case FieldTypeDecimal256:
		// materialize
		return Decimal256Slice{b.Int256, b.Scale()}, nil

	case FieldTypeDecimal128:
		// materialize
		return Decimal128Slice{b.Int128, b.Scale()}, nil

	case FieldTypeDecimal64:
		// materialize
		return Decimal64Slice{b.Int64, b.Scale()}, nil

	case FieldTypeDecimal32:
		// materialize
		return Decimal32Slice{b.Int32, b.Scale()}, nil

	default:
		return nil, fmt.Errorf("pack: unsupported type %s", p.types[index])
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

		switch p.types[i] {
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
			out[i] = b.Bools[pos]
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
			out[i] = NewDecimal256(b.Int256[pos], b.Scale())
		case FieldTypeDecimal128:
			// materialize
			out[i] = NewDecimal128(b.Int128[pos], b.Scale())
		case FieldTypeDecimal64:
			// materialize
			out[i] = NewDecimal64(b.Int64[pos], b.Scale())
		case FieldTypeDecimal32:
			// materialize
			out[i] = NewDecimal32(b.Int32[pos], b.Scale())
		default:
			return nil, fmt.Errorf("pack: unsupported type %s", p.types[i])
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
	if b.IsIgnore() {
		return nil, fmt.Errorf("pack: skipped block %d (%s)", index, p.types[index])
	}

	switch p.types[index] {
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
		return b.Bools[start:end], nil
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
		return Decimal256Slice{b.Int256[start:end], b.Scale()}, nil
	case FieldTypeDecimal128:
		// materialize
		return Decimal128Slice{b.Int128[start:end], b.Scale()}, nil
	case FieldTypeDecimal64:
		// materialize
		return Decimal64Slice{b.Int64[start:end], b.Scale()}, nil
	case FieldTypeDecimal32:
		// materialize
		return Decimal32Slice{b.Int32[start:end], b.Scale()}, nil
	default:
		return nil, fmt.Errorf("pack: unsupported type %s", p.types[index])
	}
}

// CopyFrom replaces at most srcLen rows from the current package starting at
// offset dstPos with rows from package src starting at pos srcPos.
// Both packages must have same block order.
func (p *Package) CopyFrom(srcPack *Package, dstPos, srcPos, srcLen int) error {
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

		switch p.types[i] {
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
			copy(dst.Bools[dstPos:], src.Bools[srcPos:srcPos+n])

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
			sc, dc := src.Scale(), dst.Scale()
			if sc == dc {
				copy(dst.Int256[dstPos:], src.Int256[srcPos:srcPos+n])
			} else {
				for j, v := range src.Int256[srcPos : srcPos+n] {
					dst.Int256[dstPos+j] = NewDecimal256(v, sc).Quantize(dc).Int256()
				}
			}

		case FieldTypeDecimal128:
			sc, dc := src.Scale(), dst.Scale()
			if sc == dc {
				copy(dst.Int128[dstPos:], src.Int128[srcPos:srcPos+n])
			} else {
				for j, v := range src.Int128[srcPos : srcPos+n] {
					dst.Int128[dstPos+j] = NewDecimal128(v, sc).Quantize(dc).Int128()
				}
			}

		case FieldTypeDecimal64:
			sc, dc := src.Scale(), dst.Scale()
			if sc == dc {
				copy(dst.Int64[dstPos:], src.Int64[srcPos:srcPos+n])
			} else {
				for j, v := range src.Int64[srcPos : srcPos+n] {
					dst.Int64[dstPos+j] = NewDecimal64(v, sc).Quantize(dc).Int64()
				}
			}

		case FieldTypeDecimal32:
			sc, dc := src.Scale(), dst.Scale()
			if sc == dc {
				copy(dst.Int32[dstPos:], src.Int32[srcPos:srcPos+n])
			} else {
				for j, v := range src.Int32[srcPos : srcPos+n] {
					dst.Int32[dstPos+j] = NewDecimal32(v, sc).Quantize(dc).Int32()
				}
			}

		default:
			return fmt.Errorf("pack: invalid data type %d", p.types[i])
		}
		dst.SetDirty()
	}
	p.dirty = true
	p.pkmap = nil
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

		switch p.types[i] {
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
			dst.Bools = append(dst.Bools, src.Bools[srcPos:srcPos+srcLen]...)

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
			sc, dc := src.Scale(), dst.Scale()
			if sc == dc {
				dst.Int256 = append(dst.Int256, src.Int256[srcPos:srcPos+srcLen]...)
			} else {
				for _, v := range src.Int256[srcPos : srcPos+srcLen] {
					dst.Int256 = append(dst.Int256, NewDecimal256(v, sc).Quantize(dc).Int256())
				}
			}

		case FieldTypeDecimal128:
			sc, dc := src.Scale(), dst.Scale()
			if sc == dc {
				dst.Int128 = append(dst.Int128, src.Int128[srcPos:srcPos+srcLen]...)
			} else {
				for _, v := range src.Int128[srcPos : srcPos+srcLen] {
					dst.Int128 = append(dst.Int128, NewDecimal128(v, sc).Quantize(dc).Int128())
				}
			}

		case FieldTypeDecimal64:
			sc, dc := src.Scale(), dst.Scale()
			if sc == dc {
				dst.Int64 = append(dst.Int64, src.Int64[srcPos:srcPos+srcLen]...)
			} else {
				for _, v := range src.Int64[srcPos : srcPos+srcLen] {
					dst.Int64 = append(dst.Int64, NewDecimal64(v, sc).Quantize(dc).Int64())
				}
			}

		case FieldTypeDecimal32:
			sc, dc := src.Scale(), dst.Scale()
			if sc == dc {
				dst.Int32 = append(dst.Int32, src.Int32[srcPos:srcPos+srcLen]...)
			} else {
				for _, v := range src.Int32[srcPos : srcPos+srcLen] {
					dst.Int32 = append(dst.Int32, NewDecimal32(v, sc).Quantize(dc).Int32())
				}
			}

		default:
			return fmt.Errorf("pack: invalid data type %d", p.types[i])
		}
		dst.SetDirty()
	}
	p.nValues += srcLen
	p.dirty = true
	p.pkmap = nil
	return nil
}

// appends an empty row with default/zero values
func (p *Package) Append() error {
	for i, b := range p.blocks {
		if b.IsIgnore() {
			continue
		}

		switch p.types[i] {
		case FieldTypeBytes:
			b.Bytes = append(b.Bytes, []byte{})

		case FieldTypeString:
			b.Strings = append(b.Strings, "")

		case FieldTypeBoolean:
			b.Bools = append(b.Bools, false)

		case FieldTypeFloat64:
			b.Float64 = append(b.Float64, 0)

		case FieldTypeFloat32:
			b.Float32 = append(b.Float32, 0)

		case FieldTypeInt256, FieldTypeDecimal256:
			b.Int256 = append(b.Int256, Int256Zero)

		case FieldTypeInt128, FieldTypeDecimal128:
			b.Int128 = append(b.Int128, Int128Zero)

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
			return fmt.Errorf("pack: invalid data type %d", p.types[i])
		}
		b.SetDirty()
	}
	p.nValues++
	p.dirty = true
	p.pkmap = nil
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

		switch p.types[i] {
		case FieldTypeBytes:
			b.Bytes = append(b.Bytes, make([][]byte, n)...)

		case FieldTypeString:
			b.Strings = append(b.Strings, make([]string, n)...)

		case FieldTypeBoolean:
			b.Bools = append(b.Bools, make([]bool, n)...)

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
			return fmt.Errorf("pack: invalid data type %d", p.types[i])
		}
		b.SetDirty()
	}
	p.nValues += n
	p.dirty = true
	p.pkmap = nil
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

		switch p.types[i] {
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
			b.Bools = append(b.Bools[:pos], b.Bools[pos+n:]...)

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
			return fmt.Errorf("pack: invalid data type %d", p.types[i])
		}
		b.SetDirty()
	}
	p.nValues -= n
	p.dirty = true
	p.pkmap = nil
	return nil
}

func (p *Package) Clear() {
	for _, v := range p.blocks {
		v.Clear()
	}
	// we keep all type-related data like names, type info and blocks
	// keep pack name to avoid clearing journal/tombstone names
	p.version = packageStorageFormatVersionV4
	p.nValues = 0
	p.pkmap = nil
	p.offsets = nil
	p.dirty = true
	p.cached = false
	p.packedsize = 0
	p.rawsize = 0
}

func (p *Package) Release() {
	for _, v := range p.blocks {
		v.Release()
	}
	p.version = 0
	p.nFields = 0
	p.nValues = 0
	p.offsets = nil
	p.names = nil
	p.types = nil
	p.blocks = nil
	p.namemap = nil
	p.key = nil
	p.tinfo = nil
	p.pkindex = -1
	p.pkmap = nil
	p.packedsize = 0
	p.rawsize = 0
	p.dirty = false
	p.cached = false
	p.stripped = false
}

func (p *Package) Size() int {
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

	// if pk map exists, use it
	if p.pkmap != nil {
		idx, ok := p.pkmap[id]
		if ok {
			return idx
		}
		return -1
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

	// if pk map exists, use it
	if p.pkmap != nil {
		idx, ok := p.pkmap[id]
		if ok {
			return idx
		}
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
}

func (p *PackageSorter) Len() int { return p.Package.Len() }

func (p *PackageSorter) Less(i, j int) bool {
	// we assume the block exists, check must happen outside this hot path
	b := p.Package.blocks[p.col]

	// early abort if we ignore this block
	if b.IsIgnore() {
		return true
	}

	switch p.Package.types[p.col] {
	case FieldTypeBytes:
		return bytes.Compare(b.Bytes[i], b.Bytes[j]) < 0

	case FieldTypeString:
		return b.Strings[i] < b.Strings[j]

	case FieldTypeBoolean:
		return !b.Bools[i] && b.Bools[j]

	case FieldTypeFloat64:
		return b.Float64[i] < b.Float64[j]

	case FieldTypeFloat32:
		return b.Float32[i] < b.Float32[j]

	case FieldTypeInt256, FieldTypeDecimal256:
		return b.Int256[i].Lt(b.Int256[j])

	case FieldTypeInt128, FieldTypeDecimal128:
		return b.Int128[i].Lt(b.Int128[j])

	case FieldTypeInt64, FieldTypeDatetime, FieldTypeDecimal64:
		return b.Int64[i] < b.Int64[j]

	case FieldTypeInt32, FieldTypeDecimal32:
		return b.Int32[i] < b.Int32[j]

	case FieldTypeInt16:
		return b.Int16[i] < b.Int16[j]

	case FieldTypeInt8:
		return b.Int8[i] < b.Int8[j]

	case FieldTypeUint64:
		return b.Uint64[i] < b.Uint64[j]

	case FieldTypeUint32:
		return b.Uint32[i] < b.Uint32[j]

	case FieldTypeUint16:
		return b.Uint16[i] < b.Uint16[j]

	case FieldTypeUint8:
		return b.Uint8[i] < b.Uint8[j]

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

		switch p.Package.types[p.col] {
		case FieldTypeBytes:
			b.Bytes[i], b.Bytes[j] = b.Bytes[j], b.Bytes[i]

		case FieldTypeString:
			b.Strings[i], b.Strings[j] = b.Strings[j], b.Strings[i]

		case FieldTypeBoolean:
			b.Bools[i], b.Bools[j] = b.Bools[j], b.Bools[i]

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

	spkg := &PackageSorter{Package: p, col: p.pkindex}
	if !sort.IsSorted(spkg) {
		sort.Sort(spkg)
		p.dirty = true
		p.pkmap = nil
	}
	return nil
}
