// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package table

import (
	"bytes"
	"iter"
	"time"

	"blockwatch.cc/knoxdb/internal/engine"
	"blockwatch.cc/knoxdb/internal/pack"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/assert"
	"blockwatch.cc/knoxdb/pkg/num"
	"blockwatch.cc/knoxdb/pkg/schema"
)

// Columnar (pack-based) results

var (
	_ engine.QueryResult = (*Result)(nil)
	_ engine.QueryRow    = (*Row)(nil)

	_ QueryResultConsumer = (*Result)(nil)
	_ QueryResultConsumer = (*CountResult)(nil)
	_ QueryResultConsumer = (*StreamResult)(nil)

	ErrResultClosed = engine.ErrResultClosed
)

type QueryResultConsumer interface {
	AppendRange(*pack.Package, int, int) error
}

type CountResult struct {
	n uint64
}

func NewCountResult() *CountResult {
	return &CountResult{}
}

func (r *CountResult) AppendRange(_ *pack.Package, i, j int) error {
	r.n += uint64(j - i)
	return nil
}

func (r *CountResult) Count() uint64 {
	return r.n
}

func (r *CountResult) Len() int {
	return int(r.n)
}

type StreamCallback func(engine.QueryRow) error

type StreamResult struct {
	r  *Result
	fn StreamCallback
	n  int
}

// QueryResultConsumer interface
func (r *StreamResult) AppendRange(pkg *pack.Package, i, j int) error {
	r.r.pkg = pkg
	r.n++
	return r.fn(r.r.Row(i))
}

func (r *StreamResult) Len() int {
	return r.n
}

func (r *StreamResult) Close() {
	r.r.pkg = nil
	r.fn = nil
	r.r.Close()
	r.r = nil
}

func NewStreamResult(fn StreamCallback) *StreamResult {
	sr := &StreamResult{
		r:  NewResult(nil),
		fn: fn,
	}
	return sr
}

type Result struct {
	pkg *pack.Package
	row *Row // row cache
}

func NewResult(pkg *pack.Package) *Result {
	return &Result{
		pkg: pkg,
	}
}

func (r *Result) IsValid() bool {
	return r.pkg != nil
}

func (r *Result) Schema() *schema.Schema {
	return r.pkg.Schema()
}

func (r *Result) Len() int {
	return r.pkg.Len()
}

func (r *Result) Row(row int) engine.QueryRow {
	if r.row == nil {
		r.row = &Row{}
	}
	r.row.res = r
	r.row.row = row
	return r.row
}

func (r *Result) EncodeRecord(row int) []byte {
	buf, err := r.pkg.ReadWire(row)
	assert.Always(err == nil, "pack wire encode failed", "err", err)
	return buf
}

func (r *Result) Close() {
	if r == nil {
		return
	}
	if r.pkg != nil {
		r.pkg.Release()
	}
	r.pkg = nil
	r.row = nil
}

func (r *Result) Err() error {
	if !r.IsValid() {
		return ErrResultClosed
	}
	return nil
}

func (r *Result) Encode() []byte {
	buf := bytes.NewBuffer(make([]byte, 0, r.pkg.Schema().WireSize()))
	for i, l := 0, r.pkg.Len(); i < l; i++ {
		_ = r.pkg.ReadWireBuffer(buf, i)
	}
	return buf.Bytes()
}

func (r *Result) SortBy(name string, order types.OrderType) {
	if !r.IsValid() {
		return
	}
	if r.pkg.Len() == 0 {
		return
	}
	idx, ok := r.pkg.Schema().FieldIndexByName(name)
	if !ok {
		return
	}
	pack.NewPackageSorter([]int{idx}, []types.OrderType{order}).Sort(r.pkg)
}

func (r *Result) Iterator() iter.Seq2[int, engine.QueryRow] {
	return func(fn func(int, engine.QueryRow) bool) {
		for i := range r.Len() {
			if !fn(i, r.Row(i)) {
				return
			}
		}
	}
}

// non public
func (r *Result) AppendRange(pkg *pack.Package, i, j int) error {
	r.pkg.AppendRange(pkg, i, j)
	return nil
}

// non public
// func (r *Result) PkColumn() []uint64 {
// 	return r.pkg.PkColumn()
// }

// TODO: replace by vector accessor
// func (r *Result) Column(name string) (any, error) {
// 	if !r.IsValid() {
// 		return nil, ErrResultClosed
// 	}
// 	f, ok := r.pkg.Schema().FieldByName(name)
// 	if !ok {
// 		return nil, schema.ErrInvalidField
// 	}
// 	return r.pkg.ReadCol(int(f.Id())), nil
// }

// Pack row
type Row struct {
	res    *Result        // result including query result schema
	row    int            // row offset in result package
	schema *schema.Schema // decode target struct schema (i.e. with Go interfaces)
	maps   []int          // field mapping from result schema to struct schema
}

func (r *Row) Schema() *schema.Schema {
	return r.res.pkg.Schema()
}

func (r *Row) Encode() []byte {
	return r.res.EncodeRecord(r.row)
}

func (r *Row) Decode(val any) error {
	if !r.res.IsValid() {
		return ErrResultClosed
	}

	// detect and cache struct schema
	s, err := schema.SchemaOf(val)
	if err != nil {
		return err
	}
	if r.schema == nil || r.schema != s {
		maps, err := r.res.Schema().MapTo(s)
		if err != nil {
			return err
		}
		r.maps = maps
		r.schema = s.WithEnums(r.res.Schema().Enums())
	}
	return r.res.pkg.ReadStruct(r.row, val, r.schema, r.maps)
}

func (r *Row) Field(name string) (any, error) {
	if !r.res.IsValid() {
		return nil, ErrResultClosed
	}
	f, ok := r.res.Schema().FieldByName(name)
	if !ok {
		return nil, schema.ErrInvalidField
	}
	return r.res.pkg.ReadValue(int(f.Id()), r.row, f.Type(), f.Scale()), nil
}

func (r *Row) Index(i int) (any, error) {
	if r.res.pkg == nil {
		return nil, ErrResultClosed
	}
	f, ok := r.res.Schema().FieldById(uint16(i))
	if !ok {
		return nil, schema.ErrInvalidField
	}
	return r.res.pkg.ReadValue(int(f.Id()), r.row, f.Type(), f.Scale()), nil
}

// non public
func (r *Row) Uint64(col int) uint64 {
	return r.res.pkg.Uint64(col, r.row)
}

func (r *Row) Uint32(col int) uint32 {
	return r.res.pkg.Uint32(col, r.row)
}

func (r *Row) Uint16(col int) uint16 {
	return r.res.pkg.Uint16(col, r.row)
}

func (r *Row) Uint8(col int) uint8 {
	return r.res.pkg.Uint8(col, r.row)
}

func (r *Row) Int256(col int) num.Int256 {
	return r.res.pkg.Int256(col, r.row)
}

func (r *Row) Int128(col int) num.Int128 {
	return r.res.pkg.Int128(col, r.row)
}

func (r *Row) Int64(col int) int64 {
	return r.res.pkg.Int64(col, r.row)
}

func (r *Row) Int32(col int) int32 {
	return r.res.pkg.Int32(col, r.row)
}

func (r *Row) Int16(col int) int16 {
	return r.res.pkg.Int16(col, r.row)
}

func (r *Row) Int8(col int) int8 {
	return r.res.pkg.Int8(col, r.row)
}

func (r *Row) Decimal256(col int) num.Decimal256 {
	return r.res.pkg.Decimal256(col, r.row)
}

func (r *Row) Decimal128(col int) num.Decimal128 {
	return r.res.pkg.Decimal128(col, r.row)
}

func (r *Row) Decimal64(col int) num.Decimal64 {
	return r.res.pkg.Decimal64(col, r.row)
}

func (r *Row) Decimal32(col int) num.Decimal32 {
	return r.res.pkg.Decimal32(col, r.row)
}

func (r *Row) Float64(col int) float64 {
	return r.res.pkg.Float64(col, r.row)
}

func (r *Row) Float32(col int) float32 {
	return r.res.pkg.Float32(col, r.row)
}

func (r *Row) String(col int) string {
	return r.res.pkg.String(col, r.row)
}

func (r *Row) Bytes(col int) []byte {
	return r.res.pkg.Bytes(col, r.row)
}

func (r *Row) Bool(col int) bool {
	return r.res.pkg.Bool(col, r.row)
}

func (r *Row) Time(col int) time.Time {
	return r.res.pkg.Time(col, r.row)
}

func (r *Row) Enum(col int) string {
	f, ok := r.schema.FieldByIndex(col)
	if !ok {
		return ""
	}
	enums := r.schema.Enums()
	if enums == nil {
		return ""
	}
	enum, ok := enums.Lookup(f.Name())
	if !ok {
		return ""
	}
	val, _ := enum.Value(r.res.pkg.Uint16(col, r.row))
	return val
}
