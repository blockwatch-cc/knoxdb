// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package query

import (
	"bytes"
	"context"
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

type QueryResultConsumer = engine.QueryResultConsumer

var (
	_ engine.QueryResult         = (*Result)(nil)
	_ engine.QueryRow            = (*Row)(nil)
	_ engine.QueryResultConsumer = (*Result)(nil)
	_ engine.QueryResultConsumer = (*CountResult)(nil)
	_ engine.QueryResultConsumer = (*StreamResult)(nil)

	ErrResultClosed   = engine.ErrResultClosed
	ErrResultOverflow = engine.ErrResultOverflow
)

type CountResult struct {
	n uint64
}

func NewCountResult() *CountResult {
	return &CountResult{}
}

func (r *CountResult) Append(_ context.Context, src *pack.Package) error {
	if sel := src.Selected(); sel != nil {
		r.n += uint64(len(sel))
	} else {
		r.n += uint64(src.Len())
	}
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
	r      *Result
	fn     StreamCallback
	n      uint32
	limit  uint32
	offset uint32
}

func NewStreamResult(fn StreamCallback) *StreamResult {
	sr := &StreamResult{
		r:  NewResult(nil),
		fn: fn,
	}
	return sr
}

func (r *StreamResult) WithLimit(l uint32) *StreamResult {
	r.limit = l
	return r
}

func (r *StreamResult) WithOffset(o uint32) *StreamResult {
	r.offset = o
	return r
}

// QueryResultConsumer interface
func (r *StreamResult) Append(_ context.Context, pkg *pack.Package) error {
	r.r.pkg = pkg
	sel := pkg.Selected()
	if sel == nil {
		for i := range pkg.Len() {
			// skip offset
			if r.offset > 0 {
				r.offset--
				continue
			}
			r.n++
			if err := r.fn(r.r.Row(i)); err != nil {
				return err
			}
			// apply limit
			if r.limit > 0 && r.n >= r.limit {
				return types.EndStream
			}
		}
	} else {
		for _, v := range sel {
			// skip offset
			if r.offset > 0 {
				r.offset--
				continue
			}
			r.n++
			if err := r.fn(r.r.Row(int(v))); err != nil {
				return err
			}
			// apply limit
			if r.limit > 0 && r.n >= r.limit {
				return types.EndStream
			}
		}
	}
	return nil
}

func (r *StreamResult) Len() int {
	return int(r.n)
}

func (r *StreamResult) Close() {
	r.r.pkg = nil
	r.fn = nil
	r.r.Close()
	r.r = nil
	r.limit = 0
	r.offset = 0
}

type Result struct {
	pkg    *pack.Package
	row    *Row // row cache
	limit  uint32
	offset uint32
}

func NewResult(pkg *pack.Package) *Result {
	return &Result{
		pkg: pkg,
	}
}

func (r *Result) WithLimit(l uint32) *Result {
	r.limit = l
	return r
}

func (r *Result) WithOffset(o uint32) *Result {
	r.offset = o
	return r
}

func (r *Result) Append(_ context.Context, src *pack.Package) error {
	// read selection info
	sel := src.Selected()
	nsel := uint32(src.NumSelected())

	// apply offset and limit to selection vector, generate selection vector if necessary
	if r.offset > 0 || r.limit > 0 {
		// skip offset records
		if r.offset > 0 {
			if r.offset > nsel {
				// skip the entire src pack
				r.offset -= nsel
				return nil
			}
			if sel != nil {
				// skip offset elements from existing selection vector
				sel = sel[r.offset:]
				nsel -= r.offset
			} else {
				// create selection vector for some tail portion of src
				sel = types.NewRange(r.offset, nsel).AsSelection()
				nsel = uint32(src.Len()) - r.offset
			}
			r.offset = 0
		}

		// apply limit
		if r.limit > 0 {
			free := uint32(r.pkg.FreeSpace())
			if nsel > free {
				if sel != nil {
					// shorten selection vector
					sel = sel[:free]
				} else {
					// create selection vector
					sel = types.NewRange(0, free).AsSelection()
				}
			}
		}
	}

	// append selected elements (note: without src selection, limit and offset
	// sel is nil here)
	src.AppendTo(r.pkg, sel)

	// stop when limit is reached
	if r.limit > 0 && r.pkg.Len() == int(r.limit) {
		return types.EndStream
	}
	return nil
}

func (r *Result) Reset() {
	r.pkg.Clear()
}

func (r *Result) IsValid() bool {
	return r.pkg != nil
}

func (r *Result) Schema() *schema.Schema {
	return r.pkg.Schema()
}

func (r *Result) Pack() *pack.Package {
	return r.pkg
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
	r.row = nil
}

func (r *Result) Err() error {
	if !r.IsValid() {
		return ErrResultClosed
	}
	return nil
}

func (r *Result) Encode() []byte {
	sz := r.pkg.Len() * r.pkg.Schema().WireSize()
	buf := bytes.NewBuffer(make([]byte, 0, sz))
	for i := range r.pkg.Len() {
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

func (r *Result) Value(row, col int) any {
	return r.pkg.Block(col).Get(row)
}

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

func (r *Row) Record() []byte {
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
		maps, err := r.res.Schema().MapTo(s)
		if err != nil {
			return err
		}
		r.maps = maps
		r.schema = s.WithEnums(r.res.Schema().Enums())
	}
	return r.res.pkg.ReadStruct(r.row, val, r.schema, r.maps)
}

// debug only
// func (r *Row) Field(name string) (any, error) {
//  if !r.res.IsValid() {
//      return nil, ErrResultClosed
//  }
//  f, ok := r.res.Schema().FieldByName(name)
//  if !ok {
//      return nil, schema.ErrInvalidField
//  }
//  return r.res.pkg.ReadValue(int(f.Id()), r.row, f.Type(), f.Scale()), nil
// }

func (r *Row) Get(i int) any {
	return r.res.pkg.Block(i).Get(r.row)
}

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

func (r *Row) Big(col int) num.Big {
	return r.res.pkg.Big(col, r.row)
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
