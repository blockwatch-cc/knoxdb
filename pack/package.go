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
		f := fi.value(val)
		// TODO: migrate to p.types[fi.blockid] FieldType
		//
		// Think about new interface to let blocks manage their own contents
		// -> block.AppendInt64(f) // for all int types
		// -> block.AppendUint64(f)  // for all uint types
		// -> block.AppendBool(f)
		// -> block.AppendFloat64(f) // for all float types
		// -> block.AppendString(f)
		// -> block.AppendBytes(f) // also for i128/1256, then scatter
		// - or -
		// -> block.Append(interface{})
		switch p.blocks[fi.blockid].Type() {
		case block.BlockTime:
			p.blocks[fi.blockid].Int64 = append(p.blocks[fi.blockid].Int64, f.Interface().(time.Time).UnixNano())

		case block.BlockInt64:
			p.blocks[fi.blockid].Int64 = append(p.blocks[fi.blockid].Int64, f.Int())

		case block.BlockInt32:
			p.blocks[fi.blockid].Int32 = append(p.blocks[fi.blockid].Int32, int32(f.Int()))

		case block.BlockInt16:
			p.blocks[fi.blockid].Int16 = append(p.blocks[fi.blockid].Int16, int16(f.Int()))

		case block.BlockInt8:
			p.blocks[fi.blockid].Int8 = append(p.blocks[fi.blockid].Int8, int8(f.Int()))

		case block.BlockUint64:
			p.blocks[fi.blockid].Uint64 = append(p.blocks[fi.blockid].Uint64, f.Uint())

		case block.BlockUint32:
			p.blocks[fi.blockid].Uint32 = append(p.blocks[fi.blockid].Uint32, uint32(f.Uint()))

		case block.BlockUint16:
			p.blocks[fi.blockid].Uint16 = append(p.blocks[fi.blockid].Uint16, uint16(f.Uint()))

		case block.BlockUint8:
			p.blocks[fi.blockid].Uint8 = append(p.blocks[fi.blockid].Uint8, uint8(f.Uint()))

		case block.BlockFloat64:
			p.blocks[fi.blockid].Float64 = append(p.blocks[fi.blockid].Float64, f.Float())

		case block.BlockFloat32:
			p.blocks[fi.blockid].Float32 = append(p.blocks[fi.blockid].Float32, float32(f.Float()))

		case block.BlockString:
			p.blocks[fi.blockid].Strings = append(p.blocks[fi.blockid].Strings, f.String())

		case block.BlockBytes:
			var amount []byte
			// check if type implements BinaryMarshaler
			if f.CanInterface() && f.Type().Implements(binaryMarshalerType) {
				if b, err := f.Interface().(encoding.BinaryMarshaler).MarshalBinary(); err != nil {
					return err
				} else {
					amount = b
				}
			} else {
				buf := f.Bytes()
				amount = make([]byte, len(buf))
				copy(amount, buf)
			}
			p.blocks[fi.blockid].Bytes = append(p.blocks[fi.blockid].Bytes, amount)

		case block.BlockBool:
			p.blocks[fi.blockid].Bools = append(p.blocks[fi.blockid].Bools, f.Bool())

		// case block.BlockDecimal32:
		// 	p.blocks[fi.blockid].Int32 = append(p.blocks[fi.blockid].Int32, int32(f.Interface().(Decimal32).Int64()))

		// case block.BlockDecimal64:
		// 	p.blocks[fi.blockid].Int64 = append(p.blocks[fi.blockid].Int64, f.Interface().(Decimal64).Int64())

		// case block.BlockDecimal128:
		// 	p.blocks[fi.blockid].Int128 = append(p.blocks[fi.blockid].Int128, f.Interface().(Decimal128).Int128())

		// case block.BlockDecimal256:
		// 	p.blocks[fi.blockid].Int256 = append(p.blocks[fi.blockid].Int256, f.Interface().(Decimal256).Int256())

		case block.BlockIgnore:

		default:
			return fmt.Errorf("pack: unsupported type %s (%v)", f.Type().String(), f.Kind())
		}
		p.blocks[fi.blockid].SetDirty()
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
		f := fi.value(val)
		// TODO: migrate to p.types[fi.blockid] FieldType
		switch p.blocks[fi.blockid].Type() {
		case block.BlockInt64:
			amount := f.Int()
			p.blocks[fi.blockid].Int64[pos] = amount

		case block.BlockInt32:
			amount := int32(f.Int())
			p.blocks[fi.blockid].Int32[pos] = amount

		case block.BlockInt16:
			amount := int16(f.Int())
			p.blocks[fi.blockid].Int16[pos] = amount

		case block.BlockInt8:
			amount := int8(f.Int())
			p.blocks[fi.blockid].Int8[pos] = amount

		case block.BlockUint64:
			amount := f.Uint()
			p.blocks[fi.blockid].Uint64[pos] = amount

		case block.BlockUint32:
			amount := uint32(f.Uint())
			p.blocks[fi.blockid].Uint32[pos] = amount

		case block.BlockUint16:
			amount := uint16(f.Uint())
			p.blocks[fi.blockid].Uint16[pos] = amount

		case block.BlockUint8:
			amount := uint8(f.Uint())
			p.blocks[fi.blockid].Uint8[pos] = amount

		case block.BlockFloat64:
			amount := f.Float()
			p.blocks[fi.blockid].Float64[pos] = amount

		case block.BlockFloat32:
			amount := float32(f.Float())
			p.blocks[fi.blockid].Float32[pos] = amount

		case block.BlockString:
			amount := f.String()
			p.blocks[fi.blockid].Strings[pos] = amount

		case block.BlockBytes:
			// check if type implements BinaryMarshaler
			var amount []byte
			if f.CanInterface() && f.Type().Implements(binaryMarshalerType) {
				if b, err := f.Interface().(encoding.BinaryMarshaler).MarshalBinary(); err != nil {
					return err
				} else {
					amount = b
				}
			} else {
				buf := f.Bytes()
				amount = make([]byte, len(buf))
				copy(amount, buf)
			}
			p.blocks[fi.blockid].Bytes[pos] = amount

		case block.BlockBool:
			amount := f.Bool()
			p.blocks[fi.blockid].Bools[pos] = amount

		case block.BlockTime:
			amount := f.Interface().(time.Time)
			p.blocks[fi.blockid].Int64[pos] = amount.UnixNano()

		// case block.BlockDecimal32:
		// 	amount := f.Interface().(Decimal32)
		// 	p.blocks[fi.blockid].Int32[pos] = int32(amount.Int64())

		// case block.BlockDecimal64:
		// 	amount := f.Interface().(Decimal64)
		// 	p.blocks[fi.blockid].Int64[pos] = amount.Int64()

		// case block.BlockDecimal128:
		// 	amount := f.Interface().(Decimal128)
		// 	p.blocks[fi.blockid].Int128[pos] = amount.Int128()

		// case block.BlockDecimal256:
		// 	amount := f.Interface().(Decimal256)
		// 	p.blocks[fi.blockid].Int256[pos] = amount.Int256()

		case block.BlockIgnore:

		default:
			return fmt.Errorf("pack: unsupported type %s (%v)", f.Type().String(), f.Kind())
		}
		// set flag to indicate we must reparse min/max values when storing the pack
		p.blocks[fi.blockid].SetDirty()
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
		b := p.blocks[fi.blockid]
		// TODO: migrate to p.types[fi.blockid] FieldType
		switch b.Type() {
		case block.BlockInt64:
			dst.SetInt(b.Int64[pos])

		case block.BlockInt32:
			dst.SetInt(int64(b.Int32[pos]))

		case block.BlockInt16:
			dst.SetInt(int64(b.Int16[pos]))

		case block.BlockInt8:
			dst.SetInt(int64(b.Int8[pos]))

		case block.BlockUint64:
			dst.SetUint(b.Uint64[pos])

		case block.BlockUint32:
			dst.SetUint(uint64(b.Uint32[pos]))

		case block.BlockUint16:
			dst.SetUint(uint64(b.Uint16[pos]))

		case block.BlockUint8:
			dst.SetUint(uint64(b.Uint8[pos]))

		case block.BlockFloat64:
			dst.SetFloat(b.Float64[pos])

		case block.BlockFloat32:
			dst.SetFloat(float64(b.Float32[pos]))

		case block.BlockString:
			dst.SetString(b.Strings[pos])

		case block.BlockBytes:
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

		case block.BlockBool:
			dst.SetBool(b.Bools[pos])

		case block.BlockTime:
			dst.Set(reflect.ValueOf(time.Unix(0, b.Int64[pos]).UTC()))

		// TODO
		// case block.BlockDecimal32:
		// 	// if dst.CanAddr() {
		// 	// 	pv := dst.Addr()
		// 	// 	if pv.CanInterface() && pv.Type().Implements(decimalType) {
		// 	// 		if err := pv.Interface().(Decimal).SetInt64(int64(b.Int32[pos]), fi.scale); err != nil {
		// 	// 			return err
		// 	// 		}
		// 	// 		break
		// 	// 	}
		// 	// }
		// 	dec, err := NewDecimal32(b.Int32[pos], fi.scale)
		// 	if err != nil {
		// 		return err
		// 	}
		// 	dst.Set(reflect.ValueOf(dec))

		// case block.BlockDecimal64:
		// 	// if dst.CanAddr() {
		// 	// 	pv := dst.Addr()
		// 	// 	if pv.CanInterface() && pv.Type().Implements(decimalType) {
		// 	// 		if err := pv.Interface().(Decimal).SetInt64(b.Int64[pos], fi.scale); err != nil {
		// 	// 			return err
		// 	// 		}
		// 	// 		break
		// 	// 	}
		// 	// }
		// 	dec, err := NewDecimal64(b.Int64[pos], fi.scale)
		// 	if err != nil {
		// 		return err
		// 	}
		// 	dst.Set(reflect.ValueOf(dec))

		// case block.BlockDecimal128:
		// 	dec, err := NewDecimal128(b.Int128[pos], fi.scale)
		// 	if err != nil {
		// 		return err
		// 	}
		// 	dst.Set(reflect.ValueOf(dec))

		// case block.BlockDecimal256:
		// 	dec, err := NewDecimal256(b.Int256[pos], fi.scale)
		// 	if err != nil {
		// 		return err
		// 	}
		// 	dst.Set(reflect.ValueOf(dec))

		case block.BlockIgnore:

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
	// TODO: migrate to p.types[fi.blockid] FieldType
	switch p.blocks[index].Type() {
	case block.BlockInt64:
		val := p.blocks[index].Int64[pos]
		return val, nil
	case block.BlockInt32:
		val := p.blocks[index].Int32[pos]
		return val, nil
	case block.BlockInt16:
		val := p.blocks[index].Int16[pos]
		return val, nil
	case block.BlockInt8:
		val := p.blocks[index].Int8[pos]
		return val, nil
	case block.BlockUint64:
		val := p.blocks[index].Uint64[pos]
		return val, nil
	case block.BlockUint32:
		val := p.blocks[index].Uint32[pos]
		return val, nil
	case block.BlockUint16:
		val := p.blocks[index].Uint16[pos]
		return val, nil
	case block.BlockUint8:
		val := p.blocks[index].Uint8[pos]
		return val, nil
	case block.BlockFloat64:
		val := p.blocks[index].Float64[pos]
		return val, nil
	case block.BlockFloat32:
		val := p.blocks[index].Float32[pos]
		return val, nil
	case block.BlockString:
		val := p.blocks[index].Strings[pos]
		return val, nil
	case block.BlockBytes:
		val := p.blocks[index].Bytes[pos]
		return val, nil
	case block.BlockBool:
		val := p.blocks[index].Bools[pos]
		return val, nil
	case block.BlockTime:
		val := time.Unix(0, p.blocks[index].Int64[pos]).UTC()
		return val, nil
	// case block.BlockDecimal32:
	// 	return NewDecimal32(p.blocks[index].Int32[pos], p.blocks[index].Scale)
	// case block.BlockDecimal64:
	// 	return NewDecimal64(p.blocks[index].Int64[pos], p.blocks[index].Scale)
	// case block.BlockDecimal128:
	// 	return NewDecimal128(p.blocks[index].Int128[pos], p.blocks[index].Scale)
	// case block.BlockDecimal256:
	// 	return NewDecimal256(p.blocks[index].Int256[pos], p.blocks[index].Scale)
	default:
		return nil, fmt.Errorf("pack: invalid data type %d", p.blocks[index].Type)
	}
}

func (p *Package) SetFieldAt(index, pos int, v interface{}) error {
	if p.nFields <= index {
		return fmt.Errorf("pack: invalid field index %d (max=%d)", index, p.nFields)
	}
	if p.nValues <= pos {
		return fmt.Errorf("pack: invalid pos index %d (max=%d)", pos, p.nValues)
	}
	val := reflect.Indirect(reflect.ValueOf(v))
	if !val.IsValid() {
		return fmt.Errorf("pack: invalid value of type %T", v)
	}
	// TODO: migrate to p.types[fi.blockid] FieldType
	switch p.blocks[index].Type() {
	case block.BlockInt64:
		p.blocks[index].Int64[pos] = val.Int()
	case block.BlockInt32:
		p.blocks[index].Int32[pos] = int32(val.Int())
	case block.BlockInt16:
		p.blocks[index].Int16[pos] = int16(val.Int())
	case block.BlockInt8:
		p.blocks[index].Int8[pos] = int8(val.Int())
	case block.BlockUint64:
		p.blocks[index].Uint64[pos] = val.Uint()
	case block.BlockUint32:
		p.blocks[index].Uint32[pos] = uint32(val.Uint())
	case block.BlockUint16:
		p.blocks[index].Uint16[pos] = uint16(val.Uint())
	case block.BlockUint8:
		p.blocks[index].Uint8[pos] = uint8(val.Uint())
	case block.BlockFloat64:
		amount := val.Float()
		p.blocks[index].Float64[pos] = amount
	case block.BlockFloat32:
		amount := float32(val.Float())
		p.blocks[index].Float32[pos] = amount
	case block.BlockString:
		amount := val.String()
		p.blocks[index].Strings[pos] = amount
	case block.BlockBytes:
		var amount []byte
		if val.CanInterface() && val.Type().Implements(binaryMarshalerType) {
			if b, err := val.Interface().(encoding.BinaryMarshaler).MarshalBinary(); err != nil {
				return err
			} else {
				amount = b
			}
		} else {
			buf := val.Bytes()
			amount = make([]byte, len(buf))
			copy(amount, buf)
		}
		p.blocks[index].Bytes[pos] = amount
	case block.BlockBool:
		amount := val.Bool()
		p.blocks[index].Bools[pos] = amount
	case block.BlockTime:
		amount := val.Interface().(time.Time)
		p.blocks[index].Int64[pos] = amount.UnixNano()
	// case block.BlockDecimal32:
	// 	amount := val.Interface().(Decimal32)
	// 	p.blocks[index].Int32[pos] = int32(amount.Int64())
	// case block.BlockDecimal64:
	// 	amount := val.Interface().(Decimal64)
	// 	p.blocks[index].Int64[pos] = amount.Int64()
	// case block.BlockDecimal128:
	// 	amount := val.Interface().(Decimal128)
	// 	p.blocks[index].Int128[pos] = amount.Int128()
	// case block.BlockDecimal256:
	// 	amount := val.Interface().(Decimal256)
	// 	p.blocks[index].Int256[pos] = amount.Int256()

	default:
		return fmt.Errorf("pack: invalid data type %d", p.blocks[index].Type)
	}
	p.blocks[index].SetDirty()
	p.dirty = true
	if p.pkindex == index {
		p.pkmap = nil
	}
	return nil
}

// TODO: migrate to  FieldType
func (p *Package) isValidAt(index, pos int, typ block.BlockType) error {
	if index < 0 || p.nFields <= index {
		return ErrNoField
	}
	if p.nValues <= pos {
		return ErrNoColumn
	}
	if p.blocks[index].Type() != typ { // check FieldType.BlockType too
		return ErrInvalidType
	}
	return nil
}

// TODO: migrate to  FieldType
func (p *Package) Uint64At(index, pos int) (uint64, error) {
	if err := p.isValidAt(index, pos, block.BlockUint64); err != nil {
		return 0, err
	}
	return p.blocks[index].Uint64[pos], nil
}

func (p *Package) Uint32At(index, pos int) (uint32, error) {
	if err := p.isValidAt(index, pos, block.BlockUint32); err != nil {
		return 0, err
	}
	return p.blocks[index].Uint32[pos], nil
}

func (p *Package) Uint16At(index, pos int) (uint16, error) {
	if err := p.isValidAt(index, pos, block.BlockUint16); err != nil {
		return 0, err
	}
	return p.blocks[index].Uint16[pos], nil
}

func (p *Package) Uint8At(index, pos int) (uint8, error) {
	if err := p.isValidAt(index, pos, block.BlockUint8); err != nil {
		return 0, err
	}
	return p.blocks[index].Uint8[pos], nil
}

func (p *Package) Int64At(index, pos int) (int64, error) {
	if err := p.isValidAt(index, pos, block.BlockInt64); err != nil {
		return 0, err
	}
	return p.blocks[index].Int64[pos], nil
}

func (p *Package) Int32At(index, pos int) (int32, error) {
	if err := p.isValidAt(index, pos, block.BlockInt32); err != nil {
		return 0, err
	}
	return p.blocks[index].Int32[pos], nil
}

func (p *Package) Int16At(index, pos int) (int16, error) {
	if err := p.isValidAt(index, pos, block.BlockInt16); err != nil {
		return 0, err
	}
	return p.blocks[index].Int16[pos], nil
}

func (p *Package) Int8At(index, pos int) (int8, error) {
	if err := p.isValidAt(index, pos, block.BlockInt8); err != nil {
		return 0, err
	}
	return p.blocks[index].Int8[pos], nil
}

func (p *Package) Float64At(index, pos int) (float64, error) {
	if err := p.isValidAt(index, pos, block.BlockFloat64); err != nil {
		return 0.0, err
	}
	return p.blocks[index].Float64[pos], nil
}

func (p *Package) Float32At(index, pos int) (float32, error) {
	if err := p.isValidAt(index, pos, block.BlockFloat32); err != nil {
		return 0.0, err
	}
	return p.blocks[index].Float32[pos], nil
}

func (p *Package) StringAt(index, pos int) (string, error) {
	if err := p.isValidAt(index, pos, block.BlockString); err != nil {
		return "", err
	}
	return p.blocks[index].Strings[pos], nil
}

func (p *Package) BytesAt(index, pos int) ([]byte, error) {
	if err := p.isValidAt(index, pos, block.BlockBytes); err != nil {
		return nil, err
	}
	return p.blocks[index].Bytes[pos], nil
}

func (p *Package) BoolAt(index, pos int) (bool, error) {
	if err := p.isValidAt(index, pos, block.BlockBool); err != nil {
		return false, err
	}
	return p.blocks[index].Bools[pos], nil
}

func (p *Package) TimeAt(index, pos int) (time.Time, error) {
	if err := p.isValidAt(index, pos, block.BlockTime); err != nil {
		return zeroTime, err
	}
	return time.Unix(0, p.blocks[index].Int64[pos]).UTC(), nil
}

func (p *Package) Decimal32At(index, pos int) (Decimal32, error) {
	if err := p.isValidAt(index, pos, block.BlockInt32); err != nil {
		return Decimal32{}, err
	}
	return NewDecimal32(p.blocks[index].Int32[pos], p.blocks[index].Scale()), nil
}

func (p *Package) Decimal64At(index, pos int) (Decimal64, error) {
	if err := p.isValidAt(index, pos, block.BlockInt64); err != nil {
		return Decimal64{}, err
	}
	return NewDecimal64(p.blocks[index].Int64[pos], p.blocks[index].Scale()), nil
}

func (p *Package) Decimal128At(index, pos int) (Decimal128, error) {
	if err := p.isValidAt(index, pos, block.BlockInt128); err != nil {
		return Decimal128{}, err
	}
	return NewDecimal128(p.blocks[index].Int128[pos], p.blocks[index].Scale()), nil
}

func (p *Package) Decimal256At(index, pos int) (Decimal256, error) {
	if err := p.isValidAt(index, pos, block.BlockInt256); err != nil {
		return Decimal256{}, err
	}
	return NewDecimal256(p.blocks[index].Int256[pos], p.blocks[index].Scale()), nil
}

func (p *Package) Int128At(index, pos int) (Int128, error) {
	if err := p.isValidAt(index, pos, block.BlockInt128); err != nil {
		return Int128{}, err
	}
	return p.blocks[index].Int128[pos], nil
}

func (p *Package) Int256At(index, pos int) (Int256, error) {
	if err := p.isValidAt(index, pos, block.BlockInt256); err != nil {
		return Int256{}, err
	}
	return p.blocks[index].Int256[pos], nil
}

func (p *Package) IsZeroAt(index, pos int) bool {
	if p.nFields <= index || p.nValues <= pos {
		return false
	}
	// TODO: migrate to  FieldType
	switch p.blocks[index].Type() {
	case block.BlockInt64, block.BlockInt32, block.BlockInt16, block.BlockInt8:
		// cannot be zero because 0 value has a meaning
		return false
	case block.BlockUint64, block.BlockUint32, block.BlockUint16, block.BlockUint8, block.BlockBool:
		// cannot be zero because 0 value has a meaning
		return false
	case block.BlockFloat64:
		v := p.blocks[index].Float64[pos]
		return math.IsNaN(v) || math.IsInf(v, 0)
	case block.BlockFloat32:
		v := float64(p.blocks[index].Float32[pos])
		return math.IsNaN(v) || math.IsInf(v, 0)
	case block.BlockString:
		return len(p.blocks[index].Strings[pos]) == 0
	case block.BlockBytes:
		return len(p.blocks[index].Bytes[pos]) == 0
	case block.BlockTime:
		val := p.blocks[index].Int64[pos]
		return val == 0 || time.Unix(0, val).IsZero()
	}
	return true
}

func (p *Package) Column(index int) (interface{}, error) {
	if index < 0 || p.nFields <= index {
		return nil, ErrNoField
	}
	// TODO: migrate to  FieldType?
	switch p.blocks[index].Type() {
	case block.BlockInt64, block.BlockTime:
		return p.blocks[index].Int64, nil
	case block.BlockInt32:
		return p.blocks[index].Int32, nil
	case block.BlockInt16:
		return p.blocks[index].Int16, nil
	case block.BlockInt8:
		return p.blocks[index].Int8, nil
	case block.BlockUint64:
		return p.blocks[index].Uint64, nil
	case block.BlockUint32:
		return p.blocks[index].Uint32, nil
	case block.BlockUint16:
		return p.blocks[index].Uint16, nil
	case block.BlockUint8:
		return p.blocks[index].Uint8, nil
	case block.BlockFloat64:
		return p.blocks[index].Float64, nil
	case block.BlockFloat32:
		return p.blocks[index].Float32, nil
	case block.BlockString:
		return p.blocks[index].Strings, nil
	case block.BlockBytes:
		return p.blocks[index].Bytes, nil
	case block.BlockBool:
		return p.blocks[index].Bools, nil
	case block.BlockInt128:
		return p.blocks[index].Int128, nil
	case block.BlockInt256:
		return p.blocks[index].Int256, nil
	default:
		return nil, fmt.Errorf("pack: invalid data type %d", p.blocks[index].Type)
	}
}

func (p *Package) RowAt(pos int) ([]interface{}, error) {
	if p.nValues <= pos {
		return nil, fmt.Errorf("pack: invalid pack offset %d (max %d)", pos, p.nValues)
	}
	// copy one full row of values
	out := make([]interface{}, p.nFields)
	for i, b := range p.blocks {
		// TODO: migrate to  FieldType
		switch b.Type() {
		case block.BlockInt256:
			out[i] = b.Int256[pos]
		case block.BlockInt128:
			out[i] = b.Int128[pos]
		case block.BlockInt64, block.BlockTime:
			out[i] = b.Int64[pos]
		case block.BlockInt32:
			out[i] = b.Int32[pos]
		case block.BlockInt16:
			out[i] = b.Int16[pos]
		case block.BlockInt8:
			out[i] = b.Int8[pos]
		case block.BlockUint64:
			out[i] = b.Uint64[pos]
		case block.BlockUint32:
			out[i] = b.Uint32[pos]
		case block.BlockUint16:
			out[i] = b.Uint16[pos]
		case block.BlockUint8:
			out[i] = b.Uint8[pos]
		case block.BlockFloat64:
			out[i] = b.Float64[pos]
		case block.BlockFloat32:
			out[i] = b.Float32[pos]
		case block.BlockString:
			str := b.Strings[pos]
			out[i] = str
		case block.BlockBytes:
			buf := make([]byte, len(b.Bytes[pos]))
			copy(buf, b.Bytes[pos])
			out[i] = buf
		case block.BlockBool:
			out[i] = b.Bools[pos]
		case block.BlockIgnore:
		default:
			return nil, fmt.Errorf("pack: invalid data type %d", b.Type)
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
	// TODO: migrate to  FieldType
	switch p.blocks[index].Type() {
	case block.BlockInt256:
		return p.blocks[index].Int256[start:end], nil
	case block.BlockInt128:
		return p.blocks[index].Int128[start:end], nil
	case block.BlockInt64, block.BlockTime:
		return p.blocks[index].Int64[start:end], nil
	case block.BlockInt32:
		return p.blocks[index].Int32[start:end], nil
	case block.BlockInt16:
		return p.blocks[index].Int16[start:end], nil
	case block.BlockInt8:
		return p.blocks[index].Int8[start:end], nil
	case block.BlockUint64:
		return p.blocks[index].Uint64[start:end], nil
	case block.BlockUint32:
		return p.blocks[index].Uint32[start:end], nil
	case block.BlockUint16:
		return p.blocks[index].Uint16[start:end], nil
	case block.BlockUint8:
		return p.blocks[index].Uint8[start:end], nil
	case block.BlockFloat64:
		return p.blocks[index].Float64[start:end], nil
	case block.BlockFloat32:
		return p.blocks[index].Float32[start:end], nil
	case block.BlockString:
		return p.blocks[index].Strings[start:end], nil
	case block.BlockBytes:
		return p.blocks[index].Bytes[start:end], nil
	case block.BlockBool:
		return p.blocks[index].Bools[start:end], nil
	default:
		return nil, fmt.Errorf("pack: invalid data type %d", p.blocks[index].Type)
	}
}

// CopyFrom replaces at most srcLen rows from the corrent package starting at
// offset dstPos with rows from package src starting at pos srcPos.
// Both packages must have same columns order.
func (p *Package) CopyFrom(src *Package, dstPos, srcPos, srcLen int) error {
	if src.nFields != p.nFields {
		return fmt.Errorf("pack: invalid src/dst field count %d/%d", src.nFields, p.nFields)
	}
	if src.nValues <= srcPos {
		return fmt.Errorf("pack: invalid source pack offset %d (max %d)", srcPos, src.nValues)
	}
	if src.nValues < srcPos+srcLen {
		return fmt.Errorf("pack: invalid source pack offset %d len %d (max %d)", srcPos, srcLen, src.nValues)
	}
	if p.nValues <= dstPos {
		return fmt.Errorf("pack: invalid dest pack offset %d (max %d)", dstPos, p.nValues)
	}
	// copy at most N rows without overflowing dst
	n := util.Min(p.Len()-dstPos, srcLen)
	for i, _ := range p.blocks {
		// TODO: migrate to  FieldType
		switch src.blocks[i].Type() {
		case block.BlockIgnore:
			// skip
		case block.BlockInt256:
			copy(p.blocks[i].Int256[dstPos:], src.blocks[i].Int256[srcPos:srcPos+n])
		case block.BlockInt128:
			copy(p.blocks[i].Int128[dstPos:], src.blocks[i].Int128[srcPos:srcPos+n])
		case block.BlockInt64, block.BlockTime:
			copy(p.blocks[i].Int64[dstPos:], src.blocks[i].Int64[srcPos:srcPos+n])
		case block.BlockInt32:
			copy(p.blocks[i].Int32[dstPos:], src.blocks[i].Int32[srcPos:srcPos+n])
		case block.BlockInt16:
			copy(p.blocks[i].Int16[dstPos:], src.blocks[i].Int16[srcPos:srcPos+n])
		case block.BlockInt8:
			copy(p.blocks[i].Int8[dstPos:], src.blocks[i].Int8[srcPos:srcPos+n])
		case block.BlockUint64:
			copy(p.blocks[i].Uint64[dstPos:], src.blocks[i].Uint64[srcPos:srcPos+n])
		case block.BlockUint32:
			copy(p.blocks[i].Uint32[dstPos:], src.blocks[i].Uint32[srcPos:srcPos+n])
		case block.BlockUint16:
			copy(p.blocks[i].Uint16[dstPos:], src.blocks[i].Uint16[srcPos:srcPos+n])
		case block.BlockUint8:
			copy(p.blocks[i].Uint8[dstPos:], src.blocks[i].Uint8[srcPos:srcPos+n])
		case block.BlockFloat64:
			copy(p.blocks[i].Float64[dstPos:], src.blocks[i].Float64[srcPos:srcPos+n])
		case block.BlockFloat32:
			copy(p.blocks[i].Float32[dstPos:], src.blocks[i].Float32[srcPos:srcPos+n])
		case block.BlockString:
			copy(p.blocks[i].Strings[dstPos:], src.blocks[i].Strings[srcPos:srcPos+n])
		case block.BlockBytes:
			for j, v := range src.blocks[i].Bytes[srcPos : srcPos+n] {
				// always allocate new slice because underlying block slice is shared
				if len(p.blocks[i].Bytes[dstPos+j]) < len(v) {
					buf := make([]byte, len(v))
					copy(buf, v)
					p.blocks[i].Bytes[dstPos+j] = buf
				} else {
					p.blocks[i].Bytes[dstPos+j] = p.blocks[i].Bytes[dstPos+j][:len(v)]
					copy(p.blocks[i].Bytes[dstPos+j], v)
				}
			}
		case block.BlockBool:
			copy(p.blocks[i].Bools[dstPos:], src.blocks[i].Bools[srcPos:srcPos+n])
		default:
			return fmt.Errorf("pack: invalid data type %d", p.blocks[i].Type)
		}
		p.blocks[i].SetDirty()
	}
	p.dirty = true
	p.pkmap = nil
	return nil
}

// note: will panic on package schema mismatch
func (p *Package) AppendFrom(src *Package, srcPos, srcLen int, safecopy bool) error {
	if src.nFields != p.nFields {
		return fmt.Errorf("pack: invalid src/dst field count %d/%d", src.nFields, p.nFields)
	}
	if src.nValues <= srcPos {
		return fmt.Errorf("pack: invalid source pack offset %d (max %d)", srcPos, src.nValues)
	}
	if src.nValues < srcPos+srcLen {
		return fmt.Errorf("pack: invalid source pack offset %d len %d (max %d)", srcPos, srcLen, src.nValues)
	}
	for i, _ := range p.blocks {
		// TODO: migrate to  FieldType (match decimal scale)
		switch src.blocks[i].Type() {
		case block.BlockIgnore:
			// skip
		case block.BlockInt256:
			p.blocks[i].Int256 = append(p.blocks[i].Int256, src.blocks[i].Int256[srcPos:srcPos+srcLen]...)
		case block.BlockInt128:
			p.blocks[i].Int128 = append(p.blocks[i].Int128, src.blocks[i].Int128[srcPos:srcPos+srcLen]...)
		case block.BlockInt64, block.BlockTime:
			p.blocks[i].Int64 = append(p.blocks[i].Int64, src.blocks[i].Int64[srcPos:srcPos+srcLen]...)
		case block.BlockInt32:
			p.blocks[i].Int32 = append(p.blocks[i].Int32, src.blocks[i].Int32[srcPos:srcPos+srcLen]...)
		case block.BlockInt16:
			p.blocks[i].Int16 = append(p.blocks[i].Int16, src.blocks[i].Int16[srcPos:srcPos+srcLen]...)
		case block.BlockInt8:
			p.blocks[i].Int8 = append(p.blocks[i].Int8, src.blocks[i].Int8[srcPos:srcPos+srcLen]...)
		case block.BlockUint64:
			p.blocks[i].Uint64 = append(p.blocks[i].Uint64, src.blocks[i].Uint64[srcPos:srcPos+srcLen]...)
		case block.BlockUint32:
			p.blocks[i].Uint32 = append(p.blocks[i].Uint32, src.blocks[i].Uint32[srcPos:srcPos+srcLen]...)
		case block.BlockUint16:
			p.blocks[i].Uint16 = append(p.blocks[i].Uint16, src.blocks[i].Uint16[srcPos:srcPos+srcLen]...)
		case block.BlockUint8:
			p.blocks[i].Uint8 = append(p.blocks[i].Uint8, src.blocks[i].Uint8[srcPos:srcPos+srcLen]...)
		case block.BlockFloat64:
			p.blocks[i].Float64 = append(p.blocks[i].Float64, src.blocks[i].Float64[srcPos:srcPos+srcLen]...)
		case block.BlockFloat32:
			p.blocks[i].Float32 = append(p.blocks[i].Float32, src.blocks[i].Float32[srcPos:srcPos+srcLen]...)
		case block.BlockString:
			p.blocks[i].Strings = append(p.blocks[i].Strings, src.blocks[i].Strings[srcPos:srcPos+srcLen]...)
		case block.BlockBytes:
			if safecopy {
				for _, v := range src.blocks[i].Bytes[srcPos : srcPos+srcLen] {
					buf := make([]byte, len(v))
					copy(buf, v)
					p.blocks[i].Bytes = append(p.blocks[i].Bytes, buf)
				}
			} else {
				p.blocks[i].Bytes = append(p.blocks[i].Bytes, src.blocks[i].Bytes[srcPos:srcPos+srcLen]...)
			}
		case block.BlockBool:
			p.blocks[i].Bools = append(p.blocks[i].Bools, src.blocks[i].Bools[srcPos:srcPos+srcLen]...)
		default:
			return fmt.Errorf("pack: invalid data type %d", p.blocks[i].Type)
		}
		p.blocks[i].SetDirty()
	}
	p.nValues += srcLen
	p.dirty = true
	p.pkmap = nil
	return nil
}

// appends an empty row with default/zero values
func (p *Package) Append() error {
	for i, _ := range p.blocks {
		switch p.blocks[i].Type() {
		case block.BlockIgnore:
			// skip
		case block.BlockInt256:
			p.blocks[i].Int256 = append(p.blocks[i].Int256, Int256Zero)
		case block.BlockInt128:
			p.blocks[i].Int128 = append(p.blocks[i].Int128, Int128Zero)
		case block.BlockInt64, block.BlockTime:
			p.blocks[i].Int64 = append(p.blocks[i].Int64, 0)
		case block.BlockInt32:
			p.blocks[i].Int32 = append(p.blocks[i].Int32, 0)
		case block.BlockInt16:
			p.blocks[i].Int16 = append(p.blocks[i].Int16, 0)
		case block.BlockInt8:
			p.blocks[i].Int8 = append(p.blocks[i].Int8, 0)
		case block.BlockUint64:
			p.blocks[i].Uint64 = append(p.blocks[i].Uint64, 0)
		case block.BlockUint32:
			p.blocks[i].Uint32 = append(p.blocks[i].Uint32, 0)
		case block.BlockUint16:
			p.blocks[i].Uint16 = append(p.blocks[i].Uint16, 0)
		case block.BlockUint8:
			p.blocks[i].Uint8 = append(p.blocks[i].Uint8, 0)
		case block.BlockFloat64:
			p.blocks[i].Float64 = append(p.blocks[i].Float64, 0)
		case block.BlockFloat32:
			p.blocks[i].Float32 = append(p.blocks[i].Float32, 0)
		case block.BlockString:
			p.blocks[i].Strings = append(p.blocks[i].Strings, "")
		case block.BlockBytes:
			p.blocks[i].Bytes = append(p.blocks[i].Bytes, []byte{})
		case block.BlockBool:
			p.blocks[i].Bools = append(p.blocks[i].Bools, false)
		default:
			return fmt.Errorf("pack: invalid data type %d", p.blocks[i].Type)
		}
		p.blocks[i].SetDirty()
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
	for i, _ := range p.blocks {
		switch p.blocks[i].Type() {
		case block.BlockIgnore:
			// skip
		case block.BlockInt256:
			p.blocks[i].Int256 = append(p.blocks[i].Int256, make([]Int256, n)...)
		case block.BlockInt128:
			p.blocks[i].Int128 = append(p.blocks[i].Int128, make([]Int128, n)...)
		case block.BlockInt64, block.BlockTime:
			p.blocks[i].Int64 = append(p.blocks[i].Int64, make([]int64, n)...)
		case block.BlockInt32:
			p.blocks[i].Int32 = append(p.blocks[i].Int32, make([]int32, n)...)
		case block.BlockInt16:
			p.blocks[i].Int16 = append(p.blocks[i].Int16, make([]int16, n)...)
		case block.BlockInt8:
			p.blocks[i].Int8 = append(p.blocks[i].Int8, make([]int8, n)...)
		case block.BlockUint64:
			p.blocks[i].Uint64 = append(p.blocks[i].Uint64, make([]uint64, n)...)
		case block.BlockUint32:
			p.blocks[i].Uint32 = append(p.blocks[i].Uint32, make([]uint32, n)...)
		case block.BlockUint16:
			p.blocks[i].Uint16 = append(p.blocks[i].Uint16, make([]uint16, n)...)
		case block.BlockUint8:
			p.blocks[i].Uint8 = append(p.blocks[i].Uint8, make([]uint8, n)...)
		case block.BlockFloat64:
			p.blocks[i].Float64 = append(p.blocks[i].Float64, make([]float64, n)...)
		case block.BlockFloat32:
			p.blocks[i].Float32 = append(p.blocks[i].Float32, make([]float32, n)...)
		case block.BlockString:
			p.blocks[i].Strings = append(p.blocks[i].Strings, make([]string, n)...)
		case block.BlockBytes:
			p.blocks[i].Bytes = append(p.blocks[i].Bytes, make([][]byte, n)...)
		case block.BlockBool:
			p.blocks[i].Bools = append(p.blocks[i].Bools, make([]bool, n)...)
		default:
			return fmt.Errorf("pack: invalid data type %d", p.blocks[i].Type)
		}
		p.blocks[i].SetDirty()
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
	for i, _ := range p.blocks {
		switch p.blocks[i].Type() {
		case block.BlockIgnore:
			// skip
		case block.BlockInt256:
			p.blocks[i].Int256 = append(p.blocks[i].Int256[:pos], p.blocks[i].Int256[pos+n:]...)
		case block.BlockInt128:
			p.blocks[i].Int128 = append(p.blocks[i].Int128[:pos], p.blocks[i].Int128[pos+n:]...)
		case block.BlockInt64, block.BlockTime:
			p.blocks[i].Int64 = append(p.blocks[i].Int64[:pos], p.blocks[i].Int64[pos+n:]...)
		case block.BlockInt32:
			p.blocks[i].Int32 = append(p.blocks[i].Int32[:pos], p.blocks[i].Int32[pos+n:]...)
		case block.BlockInt16:
			p.blocks[i].Int16 = append(p.blocks[i].Int16[:pos], p.blocks[i].Int16[pos+n:]...)
		case block.BlockInt8:
			p.blocks[i].Int8 = append(p.blocks[i].Int8[:pos], p.blocks[i].Int8[pos+n:]...)
		case block.BlockUint64:
			p.blocks[i].Uint64 = append(p.blocks[i].Uint64[:pos], p.blocks[i].Uint64[pos+n:]...)
		case block.BlockUint32:
			p.blocks[i].Uint32 = append(p.blocks[i].Uint32[:pos], p.blocks[i].Uint32[pos+n:]...)
		case block.BlockUint16:
			p.blocks[i].Uint16 = append(p.blocks[i].Uint16[:pos], p.blocks[i].Uint16[pos+n:]...)
		case block.BlockUint8:
			p.blocks[i].Uint8 = append(p.blocks[i].Uint8[:pos], p.blocks[i].Uint8[pos+n:]...)
		case block.BlockFloat64:
			p.blocks[i].Float64 = append(p.blocks[i].Float64[:pos], p.blocks[i].Float64[pos+n:]...)
		case block.BlockFloat32:
			p.blocks[i].Float32 = append(p.blocks[i].Float32[:pos], p.blocks[i].Float32[pos+n:]...)
		case block.BlockString:
			// avoid mem leaks
			for j, l := pos, pos+n; j < l; j++ {
				p.blocks[i].Strings[j] = ""
			}
			p.blocks[i].Strings = append(p.blocks[i].Strings[:pos], p.blocks[i].Strings[pos+n:]...)
		case block.BlockBytes:
			// avoid mem leaks
			for j, l := pos, pos+n; j < l; j++ {
				p.blocks[i].Bytes[j] = nil
			}
			p.blocks[i].Bytes = append(p.blocks[i].Bytes[:pos], p.blocks[i].Bytes[pos+n:]...)
		case block.BlockBool:
			p.blocks[i].Bools = append(p.blocks[i].Bools[:pos], p.blocks[i].Bools[pos+n:]...)
		default:
			return fmt.Errorf("pack: invalid data type %d", p.blocks[i].Type)
		}
		p.blocks[i].SetDirty()
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
	switch p.Package.blocks[p.col].Type() {
	case block.BlockIgnore:
		return true
	case block.BlockInt256:
		return p.Package.blocks[p.col].Int256[i].Lt(p.Package.blocks[p.col].Int256[j])
	case block.BlockInt128:
		return p.Package.blocks[p.col].Int128[i].Lt(p.Package.blocks[p.col].Int128[j])
	case block.BlockInt64, block.BlockTime:
		return p.Package.blocks[p.col].Int64[i] < p.Package.blocks[p.col].Int64[j]
	case block.BlockInt32:
		return p.Package.blocks[p.col].Int32[i] < p.Package.blocks[p.col].Int32[j]
	case block.BlockInt16:
		return p.Package.blocks[p.col].Int16[i] < p.Package.blocks[p.col].Int16[j]
	case block.BlockInt8:
		return p.Package.blocks[p.col].Int8[i] < p.Package.blocks[p.col].Int8[j]
	case block.BlockUint64:
		return p.Package.blocks[p.col].Uint64[i] < p.Package.blocks[p.col].Uint64[j]
	case block.BlockUint32:
		return p.Package.blocks[p.col].Uint32[i] < p.Package.blocks[p.col].Uint32[j]
	case block.BlockUint16:
		return p.Package.blocks[p.col].Uint16[i] < p.Package.blocks[p.col].Uint16[j]
	case block.BlockUint8:
		return p.Package.blocks[p.col].Uint8[i] < p.Package.blocks[p.col].Uint8[j]
	case block.BlockFloat64:
		return p.Package.blocks[p.col].Float64[i] < p.Package.blocks[p.col].Float64[j]
	case block.BlockFloat32:
		return p.Package.blocks[p.col].Float32[i] < p.Package.blocks[p.col].Float32[j]
	case block.BlockString:
		return p.Package.blocks[p.col].Strings[i] < p.Package.blocks[p.col].Strings[j]
	case block.BlockBytes:
		return bytes.Compare(p.Package.blocks[p.col].Bytes[i], p.Package.blocks[p.col].Bytes[j]) < 0
	case block.BlockBool:
		return !p.Package.blocks[p.col].Bools[i] && p.Package.blocks[p.col].Bools[j]
	default:
		return false
	}
}

func (p *PackageSorter) Swap(i, j int) {
	for n := 0; n < p.Package.nFields; n++ {
		switch p.Package.blocks[n].Type() {
		case block.BlockInt256:
			p.Package.blocks[n].Int256[i], p.Package.blocks[n].Int256[j] =
				p.Package.blocks[n].Int256[j], p.Package.blocks[n].Int256[i]
		case block.BlockInt128:
			p.Package.blocks[n].Int128[i], p.Package.blocks[n].Int128[j] =
				p.Package.blocks[n].Int128[j], p.Package.blocks[n].Int128[i]
		case block.BlockInt64, block.BlockTime:
			p.Package.blocks[n].Int64[i], p.Package.blocks[n].Int64[j] =
				p.Package.blocks[n].Int64[j], p.Package.blocks[n].Int64[i]
		case block.BlockInt32:
			p.Package.blocks[n].Int32[i], p.Package.blocks[n].Int32[j] =
				p.Package.blocks[n].Int32[j], p.Package.blocks[n].Int32[i]
		case block.BlockInt16:
			p.Package.blocks[n].Int16[i], p.Package.blocks[n].Int16[j] =
				p.Package.blocks[n].Int16[j], p.Package.blocks[n].Int16[i]
		case block.BlockInt8:
			p.Package.blocks[n].Int8[i], p.Package.blocks[n].Int8[j] =
				p.Package.blocks[n].Int8[j], p.Package.blocks[n].Int8[i]
		case block.BlockUint64:
			p.Package.blocks[n].Uint64[i], p.Package.blocks[n].Uint64[j] =
				p.Package.blocks[n].Uint64[j], p.Package.blocks[n].Uint64[i]
		case block.BlockUint32:
			p.Package.blocks[n].Uint32[i], p.Package.blocks[n].Uint32[j] =
				p.Package.blocks[n].Uint32[j], p.Package.blocks[n].Uint32[i]
		case block.BlockUint16:
			p.Package.blocks[n].Uint16[i], p.Package.blocks[n].Uint16[j] =
				p.Package.blocks[n].Uint16[j], p.Package.blocks[n].Uint16[i]
		case block.BlockUint8:
			p.Package.blocks[n].Uint8[i], p.Package.blocks[n].Uint8[j] =
				p.Package.blocks[n].Uint8[j], p.Package.blocks[n].Uint8[i]
		case block.BlockFloat64:
			p.Package.blocks[n].Float64[i], p.Package.blocks[n].Float64[j] =
				p.Package.blocks[n].Float64[j], p.Package.blocks[n].Float64[i]
		case block.BlockFloat32:
			p.Package.blocks[n].Float32[i], p.Package.blocks[n].Float32[j] =
				p.Package.blocks[n].Float32[j], p.Package.blocks[n].Float32[i]
		case block.BlockString:
			p.Package.blocks[n].Strings[i], p.Package.blocks[n].Strings[j] =
				p.Package.blocks[n].Strings[j], p.Package.blocks[n].Strings[i]
		case block.BlockBytes:
			p.Package.blocks[n].Bytes[i], p.Package.blocks[n].Bytes[j] =
				p.Package.blocks[n].Bytes[j], p.Package.blocks[n].Bytes[i]
		case block.BlockBool:
			p.Package.blocks[n].Bools[i], p.Package.blocks[n].Bools[j] =
				p.Package.blocks[n].Bools[j], p.Package.blocks[n].Bools[i]
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
