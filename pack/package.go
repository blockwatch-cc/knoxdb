// Copyright (c) 2018-2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package pack

import (
	"bytes"
	"fmt"
	"reflect"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"blockwatch.cc/knoxdb/encoding/bignum"
	"blockwatch.cc/knoxdb/encoding/block"
	"blockwatch.cc/knoxdb/encoding/compress"
	"blockwatch.cc/knoxdb/util"

	. "blockwatch.cc/knoxdb/encoding/decimal"
)

type Package struct {
	refCount int64
	key      uint32 // identity
	nFields  int
	nValues  int
	blocks   []block.Block // embedded blocks
	fields   FieldList     // shared with table
	tinfo    *typeInfo     // Go typeinfo
	pkindex  int           // field index of primary key (optional)
	dirty    bool          // pack is updated, needs to be written
	capHint  int           // block size hint
	size     int           // storage size
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

func (p *Package) KeyUint32() uint32 {
	return p.key
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

func EncodeBlockKey(packkey uint32, col int) uint64 {
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

func (p *Package) Blocks() []block.Block {
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
		p.blocks = make([]block.Block, p.nFields)
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
		p.blocks = make([]block.Block, p.nFields)
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
		p.blocks = make([]block.Block, p.nFields)
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
		p.blocks = make([]block.Block, p.nFields)
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
		clone.blocks[i] = block.NewBlock(src.Type(), src.Compression(), capacity, src.Scale(), src.Flags())
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
		if b == nil {
			continue
		}
		if b.Type() == block.BlockTypeBytes {
			b.Optimize()
		}
	}
}

func (p *Package) Materialize() {
	if p.key == journalKey {
		return
	}
	for _, b := range p.blocks {
		if b == nil {
			continue
		}
		if b.Type() == block.BlockTypeBytes {
			b.Materialize()
		}
	}
}

func (p *Package) PopulateFields(fields FieldList) *Package {
	// FIXME: This is Quick and Dirty Hack
	if p.nFields == 1 && len(p.blocks) == 1 { // seems to be tombstone
		p.blocks[0] = block.NewBlock(block.BlockTypeUint64, block.NoCompression, p.capHint, 0, 0)
		return p
	}
	if p.nFields == 2 && len(p.blocks) == 2 { // seems to be index
		p.blocks[0] = block.NewBlock(block.BlockTypeUint64, block.NoCompression, p.capHint, 0, 0)
		p.blocks[1] = block.NewBlock(block.BlockTypeUint64, block.NoCompression, p.capHint, 0, 0)
		return p
	}
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
		// field := p.fields[fi.blockid]
		// skip early
		if b == nil {
			continue
		}
		f := fi.value(val)

		// log.Infof("Push to field %d %s type=%s block=%s struct val %s (%s) finfo=%s",
		// 	fi.blockid, field.Name, field.Type, b.Type(),
		// 	f.Type().String(), f.Kind(), fi)

		if err := b.Append(f); err != nil {
			return err
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

		// skip early
		if b == nil {
			continue
		}
		f := fi.value(val)

		if err := b.SetWithCast(pos, f); err != nil {
			return err
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
		if b == nil {
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

		if err := b.ReadAtWithInfo(pos, dst); err != nil {
			return err
		}
	}

	return nil
}

// FIXME: compare with master branch, maybe use reflect.SetInt etc
func (p *Package) FieldAt(index, pos int) (interface{}, error) {
	if p.nFields <= index {
		return nil, fmt.Errorf("pack: invalid field index %d (max=%d)", index, p.nFields)
	}
	if p.nValues <= pos {
		return nil, fmt.Errorf("pack: invalid pos index %d (max=%d)", pos, p.nValues)
	}

	b := p.blocks[index]
	field := p.fields[index]
	if b == nil {
		return nil, fmt.Errorf("pack: skipped block %d (%s)", index, field.Type)
	}

	return b.FieldAt(pos), nil
}

func (p *Package) SetFieldAt(index, pos int, v interface{}) error {
	if p.nFields <= index {
		return fmt.Errorf("pack: invalid field index %d (max=%d)", index, p.nFields)
	}
	if p.nValues <= pos {
		return fmt.Errorf("pack: invalid pos index %d (max=%d)", pos, p.nValues)
	}
	b := p.blocks[index]
	if b == nil {
		return fmt.Errorf("pack: skipped block %d (%s)", index, p.fields[index].Type)
	}
	val := reflect.Indirect(reflect.ValueOf(v))
	if !val.IsValid() {
		return fmt.Errorf("pack: invalid value of type %T", v)
	}

	if err := b.SetFieldAt(pos, val); err != nil {
		return err
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
	if p.blocks[index] == nil {
		return fmt.Errorf("pack: skipped block %d (%s)", index, p.fields[index].Type)
	}
	return nil
}

func (p *Package) Uint64At(index, pos int) (uint64, error) {
	if err := p.isValidAt(index, pos, FieldTypeUint64); err != nil {
		return 0, err
	}
	return p.blocks[index].Uint64At(pos), nil
}

func (p *Package) Uint32At(index, pos int) (uint32, error) {
	if err := p.isValidAt(index, pos, FieldTypeUint32); err != nil {
		return 0, err
	}
	return p.blocks[index].Uint32At(pos), nil
}

func (p *Package) Uint16At(index, pos int) (uint16, error) {
	if err := p.isValidAt(index, pos, FieldTypeUint16); err != nil {
		return 0, err
	}
	return p.blocks[index].Uint16At(pos), nil
}

func (p *Package) Uint8At(index, pos int) (uint8, error) {
	if err := p.isValidAt(index, pos, FieldTypeUint8); err != nil {
		return 0, err
	}
	return p.blocks[index].Uint8At(pos), nil
}

func (p *Package) Int256At(index, pos int) (bignum.Int256, error) {
	if err := p.isValidAt(index, pos, FieldTypeInt256); err != nil {
		return bignum.Int256{}, err
	}
	return p.blocks[index].Int256At(pos), nil
}

func (p *Package) Int128At(index, pos int) (bignum.Int128, error) {
	if err := p.isValidAt(index, pos, FieldTypeInt128); err != nil {
		return bignum.Int128{}, err
	}
	return p.blocks[index].Int128At(pos), nil
}

func (p *Package) Int64At(index, pos int) (int64, error) {
	if err := p.isValidAt(index, pos, FieldTypeInt64); err != nil {
		return 0, err
	}
	return p.blocks[index].Int64At(pos), nil
}

func (p *Package) Int32At(index, pos int) (int32, error) {
	if err := p.isValidAt(index, pos, FieldTypeInt32); err != nil {
		return 0, err
	}
	return p.blocks[index].Int32At(pos), nil
}

func (p *Package) Int16At(index, pos int) (int16, error) {
	if err := p.isValidAt(index, pos, FieldTypeInt16); err != nil {
		return 0, err
	}
	return p.blocks[index].Int16At(pos), nil
}

func (p *Package) Int8At(index, pos int) (int8, error) {
	if err := p.isValidAt(index, pos, FieldTypeInt8); err != nil {
		return 0, err
	}
	return p.blocks[index].Int8At(pos), nil
}

func (p *Package) Float64At(index, pos int) (float64, error) {
	if err := p.isValidAt(index, pos, FieldTypeFloat64); err != nil {
		return 0.0, err
	}
	return p.blocks[index].Float64At(pos), nil
}

func (p *Package) Float32At(index, pos int) (float32, error) {
	if err := p.isValidAt(index, pos, FieldTypeFloat32); err != nil {
		return 0.0, err
	}
	return p.blocks[index].Float32At(pos), nil
}

func (p *Package) StringAt(index, pos int) (string, error) {
	if err := p.isValidAt(index, pos, FieldTypeString); err != nil {
		return "", err
	}
	return compress.UnsafeGetString(p.blocks[index].BytesAt(pos)), nil
}

func (p *Package) BytesAt(index, pos int) ([]byte, error) {
	if err := p.isValidAt(index, pos, FieldTypeBytes); err != nil {
		return nil, err
	}
	return p.blocks[index].BytesAt(pos), nil
}

func (p *Package) BoolAt(index, pos int) (bool, error) {
	if err := p.isValidAt(index, pos, FieldTypeBoolean); err != nil {
		return false, err
	}
	return p.blocks[index].BoolAt(pos), nil
}

func (p *Package) TimeAt(index, pos int) (time.Time, error) {
	if err := p.isValidAt(index, pos, FieldTypeDatetime); err != nil {
		return zeroTime, err
	}
	if ts := p.blocks[index].Int64At(pos); ts == 0 {
		return zeroTime, nil
	} else {
		return time.Unix(0, ts), nil
	}
}

func (p *Package) Decimal32At(index, pos int) (Decimal32, error) {
	if err := p.isValidAt(index, pos, FieldTypeDecimal32); err != nil {
		return Decimal32{}, err
	}
	return NewDecimal32(p.blocks[index].Int32At(pos), p.fields[index].Scale), nil
}

func (p *Package) Decimal64At(index, pos int) (Decimal64, error) {
	if err := p.isValidAt(index, pos, FieldTypeDecimal64); err != nil {
		return Decimal64{}, err
	}
	return NewDecimal64(p.blocks[index].Int64At(pos), p.fields[index].Scale), nil
}

func (p *Package) Decimal128At(index, pos int) (Decimal128, error) {
	if err := p.isValidAt(index, pos, FieldTypeDecimal128); err != nil {
		return Decimal128{}, err
	}
	return NewDecimal128(p.blocks[index].Int128At(pos), p.fields[index].Scale), nil
}

func (p *Package) Decimal256At(index, pos int) (Decimal256, error) {
	if err := p.isValidAt(index, pos, FieldTypeDecimal256); err != nil {
		return Decimal256{}, err
	}
	return NewDecimal256(p.blocks[index].Int256At(pos), p.fields[index].Scale), nil
}

func (p *Package) IsZeroAt(index, pos int, zeroIsNull bool) bool {
	if p.nFields <= index || p.nValues <= pos {
		return true
	}
	if p.blocks[index] == nil {
		return true
	}

	return p.blocks[index].IsZeroAt(pos, zeroIsNull)

}

// Block allows raw access to the underlying block for a field. Use this for
// implementing efficient matching algorithms that can work with optimized data
// vectors used at the block layer.
func (p *Package) Block(index int) (block.Block, error) {
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
	if b == nil {
		return nil, fmt.Errorf("pack: skipped block %d (%s)", index, field.Type)
	}

	return b.Slice(), nil
}

func (p *Package) RowAt(pos int) ([]interface{}, error) {
	if p.nValues <= pos {
		return nil, fmt.Errorf("pack: invalid pack offset %d (max %d)", pos, p.nValues)
	}
	// copy one full row of values
	out := make([]interface{}, p.nFields)
	for i, b := range p.blocks {
		// skip
		if b == nil {
			continue
		}

		out[i] = b.FieldAt(i)
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
	if b == nil {
		return nil, fmt.Errorf("pack: skipped block %d (%s)", index, field.Type)
	}

	return b.RangeSlice(start, end), nil
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
		if dst == nil || src == nil {
			continue
		}
		srcField := srcPack.fields[i]
		dstField := p.fields[i]
		if srcField.Index != dstField.Index || srcField.Type != dstField.Type {
			return fmt.Errorf("pack: replace from: field mismatch %d (%s) != %d (%s)",
				srcField.Index, srcField.Type, dstField.Index, dstField.Type)
		}

		dst.ReplaceFrom(src, srcPos, dstPos, n)
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
		if dst == nil || src == nil {
			continue
		}
		srcField := srcPack.fields[i]
		dstField := p.fields[i]
		if srcField.Index != dstField.Index || srcField.Type != dstField.Type {
			return fmt.Errorf("pack: replace from: field mismatch %d (%s) != %d (%s)",
				srcField.Index, srcField.Type, dstField.Index, dstField.Type)
		}
		dst.AppendFrom(src, srcPos, srcLen)
		//dst.AppendFromPtr(src.SlicePtr(), srcPos, srcLen)
		//fmt.Printf("AppendFrom: %s, pos = %d, len = %d\n  dst.len = %d\n",
		//	srcField.Type.String(), srcPos, srcLen, dst.Len())
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
		if dst == nil || src == nil {
			continue
		}
		srcField := srcPack.fields[i]
		dstField := p.fields[i]
		if srcField.Index != dstField.Index || srcField.Type != dstField.Type {
			return fmt.Errorf("pack: insert from: field mismatch %d (%s) != %d (%s)",
				srcField.Index, srcField.Type, dstField.Index, dstField.Type)
		}

		dst.InsertFrom(src, srcPos, dstPos, n)
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
	for _, b := range p.blocks {
		if b == nil {
			continue
		}

		b.Grow(n)
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
	for _, b := range p.blocks {
		if b == nil {
			continue
		}

		b.Delete(pos, n)
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
		if p.blocks[i] == nil {
			continue
		}
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
		if v != nil {
			sz += v.HeapSize()
		}
	}
	return sz
}

func (p *Package) PkColumn() []uint64 {
	if p.pkindex < 0 {
		return []uint64{}
	}
	return p.blocks[p.pkindex].Slice().([]uint64)
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
	slice := p.blocks[p.pkindex].Slice().([]uint64)[last:]
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
	slice := p.blocks[p.pkindex].Slice().([]uint64)[last:]

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
	block block.Block
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
		if b == nil {
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
