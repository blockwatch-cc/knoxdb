// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package pack

import (
	"bytes"
	"time"

	"blockwatch.cc/knoxdb/internal/engine"
	"blockwatch.cc/knoxdb/internal/pack"
	"blockwatch.cc/knoxdb/internal/query"
	"blockwatch.cc/knoxdb/pkg/assert"
	"blockwatch.cc/knoxdb/pkg/num"
	"blockwatch.cc/knoxdb/pkg/schema"
)

var (
	_ engine.QueryResult = (*Result)(nil)
	_ engine.QueryRow    = (*Row)(nil)

	_ QueryResultConsumer = (*Result)(nil)
	_ QueryResultConsumer = (*CountResult)(nil)
	_ QueryResultConsumer = (*StreamResult)(nil)

	ErrResultClosed = engine.ErrResultClosed
)

type QueryResultConsumer interface {
	Append(*pack.Package, int, int) error
}

type CountResult struct {
	n uint64
}

func NewCountResult() *CountResult {
	return &CountResult{}
}

func (r *CountResult) Append(_ *pack.Package, _, n int) error {
	r.n += uint64(n)
	return nil
}

func (r *CountResult) Count() uint64 {
	return r.n
}

type StreamCallback func(engine.QueryRow) error

type StreamResult struct {
	r  *Result
	fn StreamCallback
}

// QueryResultConsumer interface
func (r *StreamResult) Append(pkg *pack.Package, idx, _ int) error {
	r.r.pkg = pkg
	return r.fn(r.r.Row(idx))
}

func (r *StreamResult) Close() {
	r.r.pkg = nil
	r.fn = nil
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

func (r *Result) Rows() int {
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

func (r *Result) Record(row int) []byte {
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
}

func (r *Result) Bytes() []byte {
	buf := bytes.NewBuffer(make([]byte, 0, r.pkg.Schema().WireSize()))
	for i, l := 0, r.pkg.Len(); i < l; i++ {
		_ = r.pkg.ReadWireBuffer(buf, i)
	}
	return buf.Bytes()
}

func (r *Result) SortBy(name string, order query.OrderType) {
	if !r.IsValid() {
		return
	}
	if r.pkg.Len() == 0 {
		return
	}
	f, ok := r.pkg.Schema().FieldByName(name)
	if !ok {
		return
	}
	pack.NewPackageSorter(r.pkg, f.Id()).SortOrder(order)
}

func (r *Result) ForEach(fn func(r engine.QueryRow) error) error {
	if !r.IsValid() {
		return ErrResultClosed
	}
	for i, l := 0, r.Rows(); i < l; i++ {
		if err := fn(r.Row(i)); err != nil {
			return err
		}
	}
	return nil
}

// non public
func (r *Result) Append(pkg *pack.Package, idx, n int) error {
	return r.pkg.AppendPack(pkg, idx, n)
}

// non public
func (r *Result) PkColumn() []uint64 {
	return r.pkg.PkColumn()
}

// non public
func (r *Result) Column(name string) (any, error) {
	if !r.IsValid() {
		return nil, ErrResultClosed
	}
	f, ok := r.pkg.Schema().FieldByName(name)
	if !ok {
		return nil, schema.ErrInvalidField
	}
	return r.pkg.ReadCol(int(f.Id())), nil
}

// Pack row
type Row struct {
	res    *Result
	row    int
	schema *schema.Schema
	maps   []int
}

func (r *Row) Schema() *schema.Schema {
	return r.res.pkg.Schema()
}

func (r *Row) Bytes() []byte {
	return r.res.Record(r.row)
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
		maps, err := r.res.pkg.Schema().MapTo(s)
		if err != nil {
			return err
		}
		r.maps = maps
		r.schema = s
	}

	return r.res.pkg.ReadStruct(r.row, val, r.schema, r.maps)
}

func (r *Row) Field(name string) (any, error) {
	if !r.res.IsValid() {
		return nil, ErrResultClosed
	}
	f, ok := r.res.pkg.Schema().FieldByName(name)
	if !ok {
		return nil, schema.ErrInvalidField
	}
	return r.res.pkg.ReadValue(int(f.Id()), r.row, f.Type(), f.Scale()), nil
}

func (r *Row) Index(i int) (any, error) {
	if r.res.pkg == nil {
		return nil, ErrResultClosed
	}
	f, ok := r.res.pkg.Schema().FieldById(uint16(i))
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

func (r *Row) ByteSlice(col int) []byte {
	return r.res.pkg.Bytes(col, r.row)
}

func (r *Row) Bool(col int) bool {
	return r.res.pkg.Bool(col, r.row)
}

func (r *Row) Time(col int) time.Time {
	return r.res.pkg.Time(col, r.row)
}
