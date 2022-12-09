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
	"sync"
	"sync/atomic"
	"time"

	"blockwatch.cc/knoxdb/encoding/block"
	"blockwatch.cc/knoxdb/encoding/compress"
	"blockwatch.cc/knoxdb/util"

	. "blockwatch.cc/knoxdb/encoding/decimal"
	"blockwatch.cc/knoxdb/vec"
)

type Package struct {
	refCount int64
	key      uint32 // identity
	nFields  int
	nValues  int
	blocks   []*block.Block // embedded blocks
	fields   FieldList      // shared with table
	tinfo    *typeInfo      // Go typeinfo
	pkindex  int            // field index of primary key (optional)
	dirty    bool           // pack is updated, needs to be written
	capHint  int            // block size hint
	size     int            // storage size
	pool     *sync.Pool
}

func (p *Package) IncRef() int64 {
	return atomic.AddInt64(&p.refCount, 1)
}

func (p *Package) DecRef() int64 {
	val := atomic.AddInt64(&p.refCount, -1)
	if val == 0 {
		p.recycle()
	}
	return val
}

func (p *Package) recycle() {
	// don't recycle stripped or oversized packs
	c := p.Cap()
	if p.pool == nil || c <= 0 || c > p.capHint {
		p.Release()
		return
	}
	p.Clear()
	p.pool.Put(p)
}

func (p *Package) Key() []byte {
	return encodePackKey(p.key)
}

func (p *Package) WithKey(k uint32) *Package {
	p.key = k
	return p
}

func (p Package) IsJournal() bool {
	return p.key == journalKey
}

func (p Package) IsTomb() bool {
	return p.key == tombstoneKey
}

func (p Package) IsResult() bool {
	return p.key == resultKey
}

func (p *Package) SetKey(key []byte) {
	switch {
	case bytes.Equal(key, []byte("_journal")):
		p.key = journalKey
	case bytes.Equal(key, []byte("_tombstone")):
		p.key = tombstoneKey
	case bytes.Equal(key, []byte("_result")):
		p.key = resultKey
	default:
		p.key = bigEndian.Uint32(key)
	}
}

func encodePackKey(key uint32) []byte {
	switch key {
	case journalKey:
		return []byte("_journal")
	case tombstoneKey:
		return []byte("_tombstone")
	case resultKey:
		return []byte("_result")
	default:
		var buf [4]byte
		bigEndian.PutUint32(buf[:], key)
		return buf[:]
	}
}

func encodeBlockKey(packkey uint32, col int) uint64 {
	return (uint64(packkey) << 32) | uint64(col&0xffffffff)
}

func NewPackage(sz int, pool *sync.Pool) *Package {
	return &Package{
		pkindex: -1,
		capHint: sz,
		dirty:   true,
		pool:    pool,
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
	return p.capHint
}

func (p *Package) IsFull() bool {
	return p.nValues == p.capHint
}

func (p *Package) CanGrow(n int) bool {
	if p.IsJournal() || p.IsResult() || p.IsTomb() {
		return true
	}
	return p.nValues+n <= p.capHint
}

func (p *Package) FieldIndex(name string) int {
	for i, v := range p.fields {
		if v.Name == name || v.Alias == name {
			return i
		}
	}
	return -1
}

func (p *Package) FieldByName(name string) *Field {
	for _, v := range p.fields {
		if v.Name == name || v.Alias == name {
			return v
		}
	}
	return &Field{Index: -1}
}

func (p *Package) PkField() *Field {
	return p.fields.Pk()
}

func (p *Package) Fields() FieldList {
	return p.fields
}

func (p *Package) Blocks() []*block.Block {
	return p.blocks
}

func (p *Package) FieldById(idx int) *Field {
	if idx < 0 {
		return &Field{Index: -1}
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
func (p *Package) InitType(proto interface{}) error {
	if p.tinfo != nil && p.tinfo.gotype {
		return nil
	}
	tinfo, err := getTypeInfo(proto)
	if err != nil {
		return err
	}
	if len(tinfo.fields) > 256 {
		return fmt.Errorf("pack: cannot handle more than 256 fields")
	}
	if len(tinfo.fields) == 0 {
		return fmt.Errorf("pack: empty type (there are no exported fields)")
	}
	p.tinfo = tinfo
	if p.pkindex < 0 {
		p.pkindex = tinfo.PkColumn()
	}
	if len(p.fields) == 0 {
		// extract fields from Go type
		fields, err := Fields(proto)
		if err != nil {
			return err
		}
		// require pk field
		if fields.PkIndex() < 0 {
			return fmt.Errorf("pack: missing primary key field in type %T", proto)
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
			p.blocks[i] = f.NewBlock(p.capHint)
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
	if len(fields) == 0 {
		return fmt.Errorf("pack: empty fields")
	}
	if len(p.fields) > 0 {
		return fmt.Errorf("pack: already initialized")
	}
	// require pk field
	if fields.PkIndex() < 0 {
		return fmt.Errorf("pack: missing primary key field")
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
			p.blocks[i] = f.NewBlock(p.capHint)
		}
	} else {
		// make sure we use the correct compression (empty blocks are stored without)
		for i := range p.blocks {
			p.blocks[i].SetCompression(fields[i].Flags.Compression())
		}
	}
	return nil
}

// Init from field list when Go type is unavailable
func (p *Package) InitFieldsEmpty(fields FieldList, tinfo *typeInfo) error {
	if len(fields) > 256 {
		return fmt.Errorf("pack: cannot handle more than 256 fields")
	}
	if len(fields) == 0 {
		return fmt.Errorf("pack: empty fields")
	}
	if len(p.fields) > 0 {
		return fmt.Errorf("pack: already initialized")
	}
	// require pk field
	if fields.PkIndex() < 0 {
		return fmt.Errorf("pack: missing primary key field")
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
		// Keep the blocks empty
	} else {
		// make sure we use the correct compression (empty blocks are stored without)
		for i := range p.blocks {
			p.blocks[i].SetCompression(fields[i].Flags.Compression())
		}
	}
	return nil
}

func (p *Package) InitFieldsFrom(src *Package) error {
	return p.InitFields(src.fields, src.tinfo)
}

func (p *Package) InitFieldsFromEmpty(src *Package) error {
	return p.InitFieldsEmpty(src.fields, src.tinfo)
}

// may be called from Join, no pk required
func (p *Package) InitResultFields(fields FieldList, tinfo *typeInfo) error {
	if len(fields) > 256 {
		return fmt.Errorf("pack: cannot handle more than 256 fields")
	}
	if len(fields) == 0 {
		return fmt.Errorf("pack: empty fields")
	}
	if len(p.fields) > 0 {
		return fmt.Errorf("pack: already initialized")
	}

	p.fields = fields
	p.nFields = len(fields)
	p.pkindex = fields.PkIndex()
	p.tinfo = tinfo

	if len(p.blocks) == 0 {
		p.blocks = make([]*block.Block, p.nFields)
		for i, f := range fields {
			p.blocks[i] = f.NewBlock(p.capHint)
		}
	}
	return nil
}

func (p *Package) Clone(capacity int) (*Package, error) {
	// cloned pack has no identity yet
	// cloning a stripped pack is allowed
	clone := NewPackage(capacity, p.pool)
	if err := clone.InitFieldsEmpty(p.fields, p.tinfo); err != nil {
		return nil, err
	}

	clone.key = p.key
	clone.nValues = p.nValues
	clone.size = p.size

	for i, src := range p.blocks {
		if src == nil {
			continue
		}
		clone.blocks[i] = block.NewBlock(src.Type(), src.Compression(), capacity)
		clone.blocks[i].Copy(src)
	}
	return clone, nil
}

func (dst *Package) MergeCols(src *Package) error {
	if src == nil {
		return nil
	}
	if dst.nValues != src.nValues {
		return fmt.Errorf("pack: size mismatch on merge: src[%x]=%d dst[%x]=%d",
			src.key, src.nValues, dst.key, dst.nValues)
	}
	for i := range dst.blocks {
		if i > len(src.blocks) {
			break
		}
		if dst.blocks[i] == nil && src.blocks[i] != nil {
			dst.blocks[i] = src.blocks[i]
			src.blocks[i] = nil
		}
	}
	return nil
}

func (p *Package) Optimize() {
	if p.key == journalKey {
		return
	}
	for _, b := range p.blocks {
		if b.IsIgnore() {
			continue
		}
		if b.Type() == block.BlockBytes && !b.Bytes.IsOptimized() {
			// log.Infof("Pack %d: optimize %T rows=%d len=%d cap=%d", p.key, b.Bytes, p.nValues, b.Bytes.Len(), b.Bytes.Cap())
			opt := b.Bytes.Optimize()
			b.Bytes.Release()
			b.Bytes = opt
			// log.Infof("Pack %d: optimized to %T len=%d cap=%d", p.key, b.Bytes, b.Bytes.Len(), b.Bytes.Cap())
		}
	}
}

func (p *Package) Materialize() {
	if p.key == journalKey {
		return
	}
	for _, b := range p.blocks {
		if b.IsIgnore() {
			continue
		}
		if b.Type() == block.BlockBytes && !b.Bytes.IsMaterialized() {
			// log.Infof("Pack %d: materialize %T rows=%d len=%d cap=%d", p.key, b.Bytes, p.nValues, b.Bytes.Len(), b.Bytes.Cap())
			mat := b.Bytes.Materialize()
			b.Bytes.Release()
			b.Bytes = mat
			// log.Infof("Pack %d: materialized to %T len=%d cap=%d", p.key, b.Bytes, b.Bytes.Len(), b.Bytes.Cap())
		}
	}
}

func (p *Package) PopulateFields(fields FieldList) *Package {
	if len(fields) == 0 {
		fields = p.fields
	}
	for i, v := range p.fields {
		if p.blocks[i] == nil {
			if fields.Contains(v.Name) {
				p.blocks[i] = v.NewBlock(p.capHint)
			}
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
	p.fields = make(FieldList, len(prevfields))
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

// Push appends a new row to all columns. Requires a type that strictly defines
// all columns in this pack! Column mapping uses the default struct tag `knox`.
func (p *Package) Push(v interface{}) error {
	if err := p.InitType(v); err != nil {
		return err
	}
	val := reflect.Indirect(reflect.ValueOf(v))
	if !val.IsValid() {
		return fmt.Errorf("pack: pushed invalid value of type %T", v)
	}
	if !p.CanGrow(1) {
		panic(fmt.Errorf("pack: overflow on push into pack 0x%x with %d/%d rows", p.key, p.nValues, p.capHint))
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
			if fi.flags.Contains(flagBinaryMarshalerType) {
				buf, err := f.Interface().(encoding.BinaryMarshaler).MarshalBinary()
				if err != nil {
					return err
				}
				b.Bytes.Append(buf)
			} else {
				b.Bytes.Append(f.Bytes())
			}

		case FieldTypeString:
			if fi.flags.Contains(flagTextMarshalerType) {
				buf, err := f.Interface().(encoding.TextMarshaler).MarshalText()
				if err != nil {
					return err
				}
				b.Bytes.Append(buf)
			} else if fi.flags.Contains(flagStringerType) {
				b.Bytes.Append(compress.UnsafeGetBytes(f.Interface().(fmt.Stringer).String()))
			} else {
				b.Bytes.Append(compress.UnsafeGetBytes(f.String()))
			}
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
			b.Int256.Append(f.Interface().(vec.Int256))
		case FieldTypeInt128:
			b.Int128.Append(f.Interface().(vec.Int128))
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
				b.Int256.Append(vec.Int256{0, 0, 0, f.Uint()})
			case field.Flags.Contains(flagIntType):
				b.Int256.Append(vec.Int256{0, 0, 0, uint64(f.Int())})
			case field.Flags.Contains(flagFloatType):
				dec := Decimal256{}
				dec.SetFloat64(f.Float(), field.Scale)
				b.Int256.Append(dec.Int256())
			default:
				b.Int256.Append(f.Interface().(Decimal256).Quantize(field.Scale).Int256())
			}
		case FieldTypeDecimal128:
			switch {
			case field.Flags.Contains(flagUintType):
				b.Int128.Append(vec.Int128{0, f.Uint()})
			case field.Flags.Contains(flagIntType):
				b.Int128.Append(vec.Int128{0, uint64(f.Int())})
			case field.Flags.Contains(flagFloatType):
				dec := Decimal128{}
				dec.SetFloat64(f.Float(), field.Scale)
				b.Int128.Append(dec.Int128())
			default:
				b.Int128.Append(f.Interface().(Decimal128).Quantize(field.Scale).Int128())
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
			if fi.flags.Contains(flagBinaryMarshalerType) {
				buf, err := f.Interface().(encoding.BinaryMarshaler).MarshalBinary()
				if err != nil {
					return err
				}
				b.Bytes.Set(pos, buf)
			} else {
				b.Bytes.Set(pos, f.Bytes())
			}

		case FieldTypeString:
			if fi.flags.Contains(flagTextMarshalerType) {
				buf, err := f.Interface().(encoding.TextMarshaler).MarshalText()
				if err != nil {
					return err
				}
				b.Bytes.Set(pos, buf)
			} else if fi.flags.Contains(flagStringerType) {
				b.Bytes.Set(pos, compress.UnsafeGetBytes(f.Interface().(fmt.Stringer).String()))
			} else {
				b.Bytes.Set(pos, compress.UnsafeGetBytes(f.String()))
			}

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
			b.Int256.Set(pos, f.Interface().(vec.Int256))

		case FieldTypeInt128:
			b.Int128.Set(pos, f.Interface().(vec.Int128))

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
				b.Int256.Set(pos, vec.Int256{0, 0, 0, f.Uint()})
			case field.Flags.Contains(flagIntType):
				b.Int256.Set(pos, vec.Int256{0, 0, 0, uint64(f.Int())})
			case field.Flags.Contains(flagFloatType):
				dec := Decimal256{}
				dec.SetFloat64(f.Float(), field.Scale)
				b.Int256.Set(pos, dec.Int256())
			default:
				b.Int256.Set(pos, f.Interface().(Decimal256).Quantize(field.Scale).Int256())
			}

		case FieldTypeDecimal128:
			switch {
			case field.Flags.Contains(flagUintType):
				b.Int128.Set(pos, vec.Int128{0, f.Uint()})
			case field.Flags.Contains(flagIntType):
				b.Int128.Set(pos, vec.Int128{0, uint64(f.Int())})
			case field.Flags.Contains(flagFloatType):
				dec := Decimal128{}
				dec.SetFloat64(f.Float(), field.Scale)
				b.Int128.Set(pos, dec.Int128())
			default:
				b.Int128.Set(pos, f.Interface().(Decimal128).Quantize(field.Scale).Int128())
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
	// log.Debugf("Reading %s at pkg %d pos %d", tinfo.name, p.key, pos)

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
		if dst.Kind() == reflect.Ptr {
			if dst.IsNil() && dst.CanSet() {
				dst.Set(reflect.New(dst.Type().Elem()))
			}
			dst = dst.Elem()
		}

		field := p.fields[fi.blockid]
		switch field.Type {
		case FieldTypeBytes:
			if fi.flags.Contains(flagBinaryMarshalerType) {
				// decode using unmarshaler, requires the unmarshaler makes a copy
				if err := dst.Addr().Interface().(encoding.BinaryUnmarshaler).UnmarshalBinary(b.Bytes.Elem(pos)); err != nil {
					return err
				}
			} else {
				// copy to avoid memleaks of large blocks
				elm := b.Bytes.Elem(pos)
				buf := make([]byte, len(elm))
				copy(buf, elm)
				dst.SetBytes(buf)
			}

		case FieldTypeString:
			if fi.flags.Contains(flagTextMarshalerType) {
				if err := dst.Addr().Interface().(encoding.TextUnmarshaler).UnmarshalText(b.Bytes.Elem(pos)); err != nil {
					return err
				}
			} else {
				// copy to avoid memleaks of large blocks
				// dst.SetString(compress.UnsafeGetString(b.Bytes.Elem(pos)))
				dst.SetString(string(b.Bytes.Elem(pos)))
			}

		case FieldTypeDatetime:
			if ts := b.Int64[pos]; ts > 0 {
				dst.Set(reflect.ValueOf(time.Unix(0, ts)))
			} else {
				dst.Set(reflect.ValueOf(zeroTime))
			}

		case FieldTypeBoolean:
			dst.SetBool(b.Bits.IsSet(pos))

		case FieldTypeFloat64:
			dst.SetFloat(b.Float64[pos])

		case FieldTypeFloat32:
			dst.SetFloat(float64(b.Float32[pos]))

		case FieldTypeInt256:
			dst.Set(reflect.ValueOf(b.Int256.Elem(pos)))

		case FieldTypeInt128:
			dst.Set(reflect.ValueOf(b.Int128.Elem(pos)))

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
				dst.SetUint(uint64(b.Int256.Elem(pos).Int64()))
			case field.Flags.Contains(flagIntType):
				dst.SetInt(b.Int256.Elem(pos).Int64())
			case field.Flags.Contains(flagFloatType):
				dst.SetFloat(NewDecimal256(b.Int256.Elem(pos), field.Scale).Float64())
			default:
				val := NewDecimal256(b.Int256.Elem(pos), field.Scale)
				dst.Set(reflect.ValueOf(val))
			}

		case FieldTypeDecimal128:
			switch {
			case field.Flags.Contains(flagUintType):
				dst.SetUint(uint64(b.Int128.Elem(pos).Int64()))
			case field.Flags.Contains(flagIntType):
				dst.SetInt(b.Int128.Elem(pos).Int64())
			case field.Flags.Contains(flagFloatType):
				dst.SetFloat(NewDecimal128(b.Int128.Elem(pos), field.Scale).Float64())
			default:
				val := NewDecimal128(b.Int128.Elem(pos), field.Scale)
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
		return b.Bytes.Elem(pos), nil

	case FieldTypeString:
		return compress.UnsafeGetString(b.Bytes.Elem(pos)), nil

	case FieldTypeDatetime:
		if ts := b.Int64[pos]; ts > 0 {
			return time.Unix(0, ts), nil
		} else {
			return zeroTime, nil
		}

	case FieldTypeBoolean:
		return b.Bits.IsSet(pos), nil

	case FieldTypeFloat64:
		return b.Float64[pos], nil

	case FieldTypeFloat32:
		return b.Float32[pos], nil

	case FieldTypeInt256:
		return b.Int256.Elem(pos), nil

	case FieldTypeInt128:
		return b.Int128.Elem(pos), nil

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
		val := NewDecimal256(b.Int256.Elem(pos), field.Scale)
		return val, nil

	case FieldTypeDecimal128:
		val := NewDecimal128(b.Int128.Elem(pos), field.Scale)
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
		// explicit check if type implements Marshaler (v != struct type)
		if val.CanInterface() && val.Type().Implements(binaryMarshalerType) {
			buf, err := val.Interface().(encoding.BinaryMarshaler).MarshalBinary()
			if err != nil {
				return err
			}
			b.Bytes.Set(pos, buf)
		} else {
			b.Bytes.Set(pos, val.Bytes())
		}

	case FieldTypeString:
		// explicit check if type implements Marshaler (v != struct type)
		if val.CanInterface() && val.Type().Implements(textMarshalerType) {
			buf, err := val.Interface().(encoding.TextMarshaler).MarshalText()
			if err != nil {
				return err
			}
			b.Bytes.Set(pos, buf)
		} else if val.CanInterface() && val.Type().Implements(stringerType) {
			b.Bytes.Set(pos, compress.UnsafeGetBytes(val.Interface().(fmt.Stringer).String()))
		} else {
			b.Bytes.Set(pos, compress.UnsafeGetBytes(val.String()))
		}

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
		b.Int256.Set(pos, val.Interface().(vec.Int256))

	case FieldTypeInt128:
		b.Int128.Set(pos, val.Interface().(vec.Int128))

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
		b.Int256.Set(pos, val.Interface().(Decimal256).Quantize(field.Scale).Int256())

	case FieldTypeDecimal128:
		b.Int128.Set(pos, val.Interface().(Decimal128).Quantize(field.Scale).Int128())

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
	if pos < 0 || p.nValues <= pos {
		return ErrNoColumn
	}
	if p.fields[index].Type != typ {
		return ErrInvalidType
	}
	// expensive (call overhead)
	// if p.blocks[index].Type() != typ.BlockType() {
	// 	return ErrInvalidType
	// }
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

func (p *Package) Int256At(index, pos int) (vec.Int256, error) {
	if err := p.isValidAt(index, pos, FieldTypeInt256); err != nil {
		return vec.Int256{}, err
	}
	return p.blocks[index].Int256.Elem(pos), nil
}

func (p *Package) Int128At(index, pos int) (vec.Int128, error) {
	if err := p.isValidAt(index, pos, FieldTypeInt128); err != nil {
		return vec.Int128{}, err
	}
	return p.blocks[index].Int128.Elem(pos), nil
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
	return compress.UnsafeGetString(p.blocks[index].Bytes.Elem(pos)), nil
}

func (p *Package) BytesAt(index, pos int) ([]byte, error) {
	if err := p.isValidAt(index, pos, FieldTypeBytes); err != nil {
		return nil, err
	}
	return p.blocks[index].Bytes.Elem(pos), nil
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
	if ts := p.blocks[index].Int64[pos]; ts == 0 {
		return zeroTime, nil
	} else {
		return time.Unix(0, ts), nil
	}
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
	return NewDecimal128(p.blocks[index].Int128.Elem(pos), p.fields[index].Scale), nil
}

func (p *Package) Decimal256At(index, pos int) (Decimal256, error) {
	if err := p.isValidAt(index, pos, FieldTypeDecimal256); err != nil {
		return Decimal256{}, err
	}
	return NewDecimal256(p.blocks[index].Int256.Elem(pos), p.fields[index].Scale), nil
}

func (p *Package) IsZeroAt(index, pos int, zeroIsNull bool) bool {
	if p.nFields <= index || p.nValues <= pos {
		return true
	}
	if p.blocks[index].IsIgnore() {
		return true
	}
	field := p.fields[index]
	switch field.Type {
	case FieldTypeInt256, FieldTypeDecimal256:
		return zeroIsNull && p.blocks[index].Int256.Elem(pos).IsZero()
	case FieldTypeInt128, FieldTypeDecimal128:
		return zeroIsNull && p.blocks[index].Int128.Elem(pos).IsZero()
	case FieldTypeInt64, FieldTypeDecimal64:
		return zeroIsNull && p.blocks[index].Int64[pos] == 0
	case FieldTypeInt32, FieldTypeDecimal32:
		return zeroIsNull && p.blocks[index].Int32[pos] == 0
	case FieldTypeInt16:
		return zeroIsNull && p.blocks[index].Int16[pos] == 0
	case FieldTypeInt8:
		return zeroIsNull && p.blocks[index].Int8[pos] == 0
	case FieldTypeUint64:
		return zeroIsNull && p.blocks[index].Uint64[pos] == 0
	case FieldTypeUint32:
		return zeroIsNull && p.blocks[index].Uint32[pos] == 0
	case FieldTypeUint16:
		return zeroIsNull && p.blocks[index].Uint16[pos] == 0
	case FieldTypeUint8:
		return zeroIsNull && p.blocks[index].Uint8[pos] == 0
	case FieldTypeBoolean:
		return zeroIsNull && !p.blocks[index].Bits.IsSet(pos)
	case FieldTypeFloat64:
		v := p.blocks[index].Float64[pos]
		return math.IsNaN(v) || math.IsInf(v, 0) || (zeroIsNull && v == 0.0)
	case FieldTypeFloat32:
		v := float64(p.blocks[index].Float32[pos])
		return math.IsNaN(v) || math.IsInf(v, 0) || (zeroIsNull && v == 0.0)
	case FieldTypeString, FieldTypeBytes:
		return len(p.blocks[index].Bytes.Elem(pos)) == 0
	case FieldTypeDatetime:
		val := p.blocks[index].Int64[pos]
		return val == 0 || (zeroIsNull && time.Unix(0, val).IsZero())
	}
	return false
}

// Block allows raw access to the underlying block for a field. Use this for
// implementing efficient matching algorithms that can work with optimized data
// vectors used at the block layer.
func (p *Package) Block(index int) (*block.Block, error) {
	if index < 0 || p.nFields <= index {
		return nil, ErrNoField
	}
	return p.blocks[index], nil
}

// Column returns a typed slice containing materialized values for the requested
// field index. This function has higher cost than direct block access because
// optimized representation of data vectors like timestamps, bitsets and decimals
// are unpacked into a temporary slice and type-cast.
func (p *Package) Column(index int) (interface{}, error) {
	if index < 0 || p.nFields <= index {
		return nil, ErrNoField
	}
	b := p.blocks[index]
	field := p.fields[index]
	if b.IsIgnore() {
		return nil, fmt.Errorf("pack: skipped block %d (%s)", index, field.Type)
	}

	switch field.Type {
	case FieldTypeBytes,
		FieldTypeString,
		FieldTypeFloat64,
		FieldTypeFloat32,
		FieldTypeInt256,
		FieldTypeInt64,
		FieldTypeInt32,
		FieldTypeInt16,
		FieldTypeInt8,
		FieldTypeUint64,
		FieldTypeUint32,
		FieldTypeUint16,
		FieldTypeUint8:
		// direct access, no copy
		return b.RawSlice(), nil

	case FieldTypeInt128:
		// materialized from Int128LLSlice
		return b.RawSlice(), nil

	case FieldTypeDatetime:
		// materialize
		res := make([]time.Time, len(b.Int64))
		for i, v := range b.Int64 {
			if v > 0 {
				res[i] = time.Unix(0, v)
			} else {
				res[i] = zeroTime
			}
		}
		return res, nil

	case FieldTypeBoolean:
		// materialized from bitset
		return b.RawSlice(), nil

	case FieldTypeDecimal256:
		// materialize
		return Decimal256Slice{Int256: b.Int256.Int256Slice(), Scale: field.Scale}, nil

	case FieldTypeDecimal128:
		// materialize
		return Decimal128Slice{Int128: b.Int128.Int128Slice(), Scale: field.Scale}, nil

	case FieldTypeDecimal64:
		// materialize
		return Decimal64Slice{Int64: b.Int64, Scale: field.Scale}, nil

	case FieldTypeDecimal32:
		// materialize
		return Decimal32Slice{Int32: b.Int32, Scale: field.Scale}, nil

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
			out[i] = b.Bytes.Elem(pos)
		case FieldTypeString:
			out[i] = compress.UnsafeGetString(b.Bytes.Elem(pos))
		case FieldTypeDatetime:
			// materialize
			if ts := b.Int64[pos]; ts > 0 {
				out[i] = time.Unix(0, ts)
			} else {
				out[i] = time.Time{}
			}
		case FieldTypeBoolean:
			out[i] = b.Bits.IsSet(pos)
		case FieldTypeFloat64:
			out[i] = b.Float64[pos]
		case FieldTypeFloat32:
			out[i] = b.Float32[pos]
		case FieldTypeInt256:
			out[i] = b.Int256.Elem(pos)
		case FieldTypeInt128:
			out[i] = b.Int128.Elem(pos)
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
			out[i] = NewDecimal256(b.Int256.Elem(pos), field.Scale)
		case FieldTypeDecimal128:
			// materialize
			out[i] = NewDecimal128(b.Int128.Elem(pos), field.Scale)
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
		return b.Bytes.Subslice(start, end), nil
	case FieldTypeString:
		// Note: does not copy data; don't reference!
		s := make([]string, end-start+1)
		for i, v := range b.Bytes.Subslice(start, end) {
			s[i] = compress.UnsafeGetString(v)
		}
		return s, nil
	case FieldTypeDatetime:
		// materialize
		res := make([]time.Time, end-start+1)
		for i, v := range b.Int64[start:end] {
			if v > 0 {
				res[i+start] = time.Unix(0, v)
			} else {
				res[i+start] = zeroTime
			}
		}
		return res, nil
	case FieldTypeBoolean:
		return b.Bits.SubSlice(start, end-start+1), nil
	case FieldTypeFloat64:
		return b.Float64[start:end], nil
	case FieldTypeFloat32:
		return b.Float32[start:end], nil
	case FieldTypeInt256:
		return b.Int256.Subslice(start, end).Int256Slice(), nil
	case FieldTypeInt128:
		return b.Int128.Subslice(start, end).Int128Slice(), nil
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
		return Decimal256Slice{Int256: b.Int256.Subslice(start, end).Int256Slice(), Scale: field.Scale}, nil
	case FieldTypeDecimal128:
		// materialize
		return Decimal128Slice{Int128: b.Int128.Subslice(start, end).Int128Slice(), Scale: field.Scale}, nil
	case FieldTypeDecimal64:
		// materialize
		return Decimal64Slice{Int64: b.Int64[start:end], Scale: field.Scale}, nil
	case FieldTypeDecimal32:
		// materialize
		return Decimal32Slice{Int32: b.Int32[start:end], Scale: field.Scale}, nil
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
		case FieldTypeBytes, FieldTypeString:
			dst.Bytes.Copy(src.Bytes, dstPos, srcPos, n)

		case FieldTypeBoolean:
			dst.Bits.Replace(src.Bits, srcPos, n, dstPos)

		case FieldTypeFloat64:
			copy(dst.Float64[dstPos:], src.Float64[srcPos:srcPos+n])

		case FieldTypeFloat32:
			copy(dst.Float32[dstPos:], src.Float32[srcPos:srcPos+n])

		case FieldTypeInt256:
			dst.Int256.Copy(src.Int256, dstPos, srcPos, n)

		case FieldTypeInt128:
			dst.Int128.Copy(src.Int128, dstPos, srcPos, n)

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
				dst.Int256.Copy(src.Int256, dstPos, srcPos, n)
			} else {
				for j := 0; j < n; j++ {
					dst.Int256.Set(dstPos+j, NewDecimal256(src.Int256.Elem(srcPos+j), sc).Quantize(dc).Int256())
				}
			}

		case FieldTypeDecimal128:
			sc, dc := srcField.Scale, dstField.Scale
			if sc == dc {
				dst.Int128.Copy(src.Int128, dstPos, srcPos, n)
			} else {
				for j := 0; j < n; j++ {
					dst.Int128.Set(dstPos+j, NewDecimal128(src.Int128.Elem(srcPos+j), sc).Quantize(dc).Int128())
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

func (p *Package) AppendFrom(srcPack *Package, srcPos, srcLen int) error {
	if srcPack.nFields != p.nFields {
		return fmt.Errorf("pack: invalid src/dst field count %d/%d", srcPack.nFields, p.nFields)
	}
	if srcPack.nValues <= srcPos {
		return fmt.Errorf("pack: invalid source pack offset %d (max %d)", srcPos, srcPack.nValues)
	}
	if srcPack.nValues < srcPos+srcLen {
		return fmt.Errorf("pack: invalid source pack offset %d len %d (max %d)", srcPos, srcLen, srcPack.nValues)
	}
	if !p.CanGrow(srcLen) {
		panic(fmt.Errorf("pack: overflow on append %d rows into pack 0x%x with %d/%d rows (first block %d/%d)",
			srcLen, p.key, p.nValues, p.capHint, p.blocks[0].Len(), p.blocks[0].Cap()))
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
		case FieldTypeBytes, FieldTypeString:
			if srcLen == 1 {
				dst.Bytes.Append(src.Bytes.Elem(srcPos))
			} else {
				dst.Bytes.Append(src.Bytes.Subslice(srcPos, srcPos+srcLen)...)
			}

		case FieldTypeBoolean:
			dst.Bits.Append(src.Bits, srcPos, srcLen)

		case FieldTypeFloat64:
			dst.Float64 = append(dst.Float64, src.Float64[srcPos:srcPos+srcLen]...)

		case FieldTypeFloat32:
			dst.Float32 = append(dst.Float32, src.Float32[srcPos:srcPos+srcLen]...)

		case FieldTypeInt256:
			dst.Int256.AppendFrom(src.Int256.Subslice(srcPos, srcPos+srcLen))

		case FieldTypeInt128:
			dst.Int128.AppendFrom(src.Int128.Subslice(srcPos, srcPos+srcLen))

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
				dst.Int256.AppendFrom(src.Int256.Subslice(srcPos, srcPos+srcLen))
			} else {
				for j := 0; j < srcLen; j++ {
					dst.Int256.Append(NewDecimal256(src.Int256.Elem(srcPos+j), sc).Quantize(dc).Int256())
				}
			}

		case FieldTypeDecimal128:
			sc, dc := srcField.Scale, dstField.Scale
			if sc == dc {
				dst.Int128.AppendFrom(src.Int128.Subslice(srcPos, srcPos+srcLen))
			} else {
				for j := 0; j < srcLen; j++ {
					dst.Int128.Append(NewDecimal128(src.Int128.Elem(srcPos+j), sc).Quantize(dc).Int128())
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

func (p *Package) InsertFrom(srcPack *Package, dstPos, srcPos, srcLen int) error {
	if srcPack.nFields != p.nFields {
		return fmt.Errorf("pack: invalid src/dst field count %d/%d", srcPack.nFields, p.nFields)
	}
	if srcPack.nValues <= srcPos {
		return fmt.Errorf("pack: invalid source pack offset %d (max %d)", srcPos, srcPack.nValues)
	}
	if srcPack.nValues < srcPos+srcLen {
		return fmt.Errorf("pack: invalid source pack offset %d len %d (max %d)", srcPos, srcLen, srcPack.nValues)
	}
	if !p.CanGrow(srcLen) {
		panic(fmt.Errorf("pack: overflow on insert %d rows into pack 0x%x with %d/%d rows", srcLen, p.key, p.nValues, p.capHint))
	}
	n := util.Min(srcPack.Len()-srcPos, srcLen)
	for i, dst := range p.blocks {
		src := srcPack.blocks[i]
		if dst.IsIgnore() || src.IsIgnore() {
			continue
		}
		srcField := srcPack.fields[i]
		dstField := p.fields[i]
		if srcField.Index != dstField.Index || srcField.Type != dstField.Type {
			return fmt.Errorf("pack: insert from: field mismatch %d (%s) != %d (%s)",
				srcField.Index, srcField.Type, dstField.Index, dstField.Type)
		}

		switch dstField.Type {
		case FieldTypeBytes, FieldTypeString:
			dst.Bytes.Insert(dstPos, src.Bytes.Subslice(srcPos, srcPos+n)...)

		case FieldTypeBoolean:
			dst.Bits.Insert(src.Bits, srcPos, srcLen, dstPos)

		case FieldTypeFloat64:
			dst.Float64 = vec.Float64.Insert(dst.Float64, dstPos, src.Float64[srcPos:srcPos+n]...)

		case FieldTypeFloat32:
			dst.Float32 = vec.Float32.Insert(dst.Float32, dstPos, src.Float32[srcPos:srcPos+n]...)

		case FieldTypeInt256:
			dst.Int256.Insert(dstPos, src.Int256.Subslice(srcPos, srcPos+n))

		case FieldTypeInt128:
			dst.Int128.Insert(dstPos, src.Int128.Subslice(srcPos, srcPos+n))

		case FieldTypeInt64, FieldTypeDatetime:
			dst.Int64 = vec.Int64.Insert(dst.Int64, dstPos, src.Int64[srcPos:srcPos+n]...)

		case FieldTypeInt32:
			dst.Int32 = vec.Int32.Insert(dst.Int32, dstPos, src.Int32[srcPos:srcPos+n]...)

		case FieldTypeInt16:
			dst.Int16 = vec.Int16.Insert(dst.Int16, dstPos, src.Int16[srcPos:srcPos+n]...)

		case FieldTypeInt8:
			dst.Int8 = vec.Int8.Insert(dst.Int8, dstPos, src.Int8[srcPos:srcPos+n]...)

		case FieldTypeUint64:
			dst.Uint64 = vec.Uint64.Insert(dst.Uint64, dstPos, src.Uint64[srcPos:srcPos+n]...)

		case FieldTypeUint32:
			dst.Uint32 = vec.Uint32.Insert(dst.Uint32, dstPos, src.Uint32[srcPos:srcPos+n]...)

		case FieldTypeUint16:
			dst.Uint16 = vec.Uint16.Insert(dst.Uint16, dstPos, src.Uint16[srcPos:srcPos+n]...)

		case FieldTypeUint8:
			dst.Uint8 = vec.Uint8.Insert(dst.Uint8, dstPos, src.Uint8[srcPos:srcPos+n]...)

		case FieldTypeDecimal256:
			sc, dc := srcField.Scale, dstField.Scale
			if sc == dc {
				dst.Int256.Insert(dstPos, src.Int256.Subslice(srcPos, srcPos+n))
			} else {
				cp := vec.MakeInt256LLSlice(n)
				for i := 0; i < n; i++ {
					cp.Set(i, NewDecimal256(src.Int256.Elem(srcPos+n), sc).Quantize(dc).Int256())
				}
				dst.Int256.Insert(dstPos, cp)
			}

		case FieldTypeDecimal128:
			sc, dc := srcField.Scale, dstField.Scale
			if sc == dc {
				dst.Int128.Insert(dstPos, src.Int128.Subslice(srcPos, srcPos+n))
			} else {
				cp := vec.MakeInt128LLSlice(n)
				for i := 0; i < n; i++ {
					cp.Set(i, NewDecimal128(src.Int128.Elem(srcPos+n), sc).Quantize(dc).Int128())
				}
				dst.Int128.Insert(dstPos, cp)
			}

		case FieldTypeDecimal64:
			sc, dc := srcField.Scale, dstField.Scale
			if sc == dc {
				vec.Int64.Insert(dst.Int64, dstPos, src.Int64[srcPos:srcPos+n]...)
			} else {
				cp := make([]int64, n)
				for i, v := range src.Int64[srcPos : srcPos+srcLen] {
					cp[i] = NewDecimal64(v, sc).Quantize(dc).Int64()
				}
				vec.Int64.Insert(dst.Int64, dstPos, cp...)
			}

		case FieldTypeDecimal32:
			sc, dc := srcField.Scale, dstField.Scale
			if sc == dc {
				vec.Int32.Insert(dst.Int32, dstPos, src.Int32[srcPos:srcPos+n]...)
			} else {
				cp := make([]int32, n)
				for i, v := range src.Int32[srcPos : srcPos+srcLen] {
					cp[i] = NewDecimal32(v, sc).Quantize(dc).Int32()
				}
				vec.Int32.Insert(dst.Int32, dstPos, cp...)
			}

		default:
			return fmt.Errorf("pack: invalid data type %s", dstField.Type)
		}
		dst.SetDirty()
	}
	p.nValues += n
	p.dirty = true
	return nil
}

// append n empty rows with default/zero values
func (p *Package) Grow(n int) error {
	if n <= 0 {
		return fmt.Errorf("pack: grow requires positive value")
	}
	if !p.CanGrow(n) {
		panic(fmt.Errorf("pack: overflow on grow %d rows in pack 0x%x with %d/%d rows", n, p.key, p.nValues, p.capHint))
	}
	for i, b := range p.blocks {
		if b.IsIgnore() {
			continue
		}
		field := p.fields[i]

		switch field.Type {
		case FieldTypeBytes, FieldTypeString:
			b.Bytes.Append(make([][]byte, n)...)

		case FieldTypeBoolean:
			b.Bits.Grow(b.Bits.Len() + n)

		case FieldTypeFloat64:
			b.Float64 = append(b.Float64, make([]float64, n)...)

		case FieldTypeFloat32:
			b.Float32 = append(b.Float32, make([]float32, n)...)

		case FieldTypeInt256, FieldTypeDecimal256:
			b.Int256.AppendFrom(vec.MakeInt256LLSlice(n))

		case FieldTypeInt128, FieldTypeDecimal128:
			b.Int128.AppendFrom(vec.MakeInt128LLSlice(n))

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
		case FieldTypeBytes, FieldTypeString:
			b.Bytes.Delete(pos, n)

		case FieldTypeBoolean:
			b.Bits.Delete(pos, n)

		case FieldTypeFloat64:
			b.Float64 = append(b.Float64[:pos], b.Float64[pos+n:]...)

		case FieldTypeFloat32:
			b.Float32 = append(b.Float32[:pos], b.Float32[pos+n:]...)

		case FieldTypeInt256, FieldTypeDecimal256:
			b.Int256 = b.Int256.Delete(pos, n)

		case FieldTypeInt128, FieldTypeDecimal128:
			b.Int128 = b.Int128.Delete(pos, n)

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
	for i := range p.blocks {
		p.blocks[i].Clear()
	}
	// Note: we keep all type-related data and blocks, pool reference
	// also keep pack key to avoid clearing journal/tombstone identity
	p.nValues = 0
	p.dirty = true
	p.size = 0
}

func (p *Package) Release() {
	for i := range p.blocks {
		p.blocks[i].Release()
		p.blocks[i] = nil
	}
	p.refCount = 0
	p.nValues = 0
	p.key = 0
	p.dirty = false
	p.size = 0
	if p.pool != nil {
		p.pool.Put(p)
	}
}

func (p *Package) HeapSize() int {
	var sz int = szPackage
	sz += 8 * len(p.blocks)
	for _, v := range p.blocks {
		sz += v.HeapSize()
	}
	return sz
}

func (p *Package) PkColumn() []uint64 {
	if p.pkindex < 0 {
		return []uint64{}
	}
	return p.blocks[p.pkindex].Uint64
}

// Searches id in primary key column and return index or -1 when not found
// This function is only safe to use when packs are sorted!
func (p *Package) PkIndex(id uint64, last int) (int, int) {
	// primary key field required
	if p.pkindex < 0 || last >= p.nValues {
		return -1, p.nValues
	}

	// search for id value in pk block (always an uint64) starting at last index
	// this helps limiting search space when ids are pre-sorted
	slice := p.blocks[p.pkindex].Uint64[last:]
	l := len(slice)

	// for sparse pk spaces, use binary search on sorted slices
	idx := sort.Search(l, func(i int) bool { return slice[i] >= id })
	last += idx
	if idx < l && slice[idx] == id {
		return last, last
	}
	return -1, last
}

// Searches id in primary key column and return index or -1 when not found,
// use this function when pack is unsorted as when updates/inserts are out of order.
func (p *Package) PkIndexUnsorted(id uint64, last int) int {
	// primary key field required
	if p.pkindex < 0 || p.nValues <= last {
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
	block *block.Block
}

func NewPackageSorter(p *Package, col int) (*PackageSorter, error) {
	if col < 0 || col > p.nFields {
		return nil, fmt.Errorf("pack: invalid sort field %d", col)
	}
	return &PackageSorter{
		Package: p,
		block:   p.blocks[col],
	}, nil
}

// Sorts the package by defined column and returns if the package was changed,
// i.e. returns false if the package was sorted before
func (s *PackageSorter) Sort() bool {
	if sort.IsSorted(s) {
		return false
	}
	sort.Sort(s)
	return true
}

func (s *PackageSorter) Len() int { return s.Package.Len() }

func (s *PackageSorter) Less(i, j int) bool { return s.block.Less(i, j) }

func (s *PackageSorter) Swap(i, j int) {
	for _, b := range s.Package.blocks {
		if b.IsIgnore() {
			continue
		}
		b.Swap(i, j)
	}
}

func (p *Package) PkSort() error {
	if p.Len() == 0 {
		return nil
	}

	// sort by primary key index
	sorter, err := NewPackageSorter(p, p.pkindex)
	if err != nil {
		return err
	}

	// update dirty state when package has changed
	updated := sorter.Sort()
	p.dirty = p.dirty || updated

	return nil
}
