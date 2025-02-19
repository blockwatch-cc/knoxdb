// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package query

import (
	"bytes"
	"encoding/binary"
	"strings"
	"time"

	"blockwatch.cc/knoxdb/internal/engine"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/num"
	"blockwatch.cc/knoxdb/pkg/schema"
	"blockwatch.cc/knoxdb/pkg/util"
	"golang.org/x/exp/slices"
)

var (
	_ engine.QueryResult = (*Result)(nil)
	_ engine.QueryRow    = (*Row)(nil)

	ErrResultClosed = engine.ErrResultClosed
)

type QueryResultConsumer interface {
	Append(buf []byte, isZeroCopy bool) error
	Len() int
}

type StreamCallback func(engine.QueryRow) error

type StreamResult struct {
	r  *Result
	fn StreamCallback
	n  int
}

// QueryResultConsumer interface
func (r *StreamResult) Append(buf []byte, _ bool) error {
	r.r.values = buf
	r.n++
	return r.fn(r.r.Row(0))
}

func (r *StreamResult) Len() int {
	return r.n
}

func NewStreamResult(s *schema.Schema, fn StreamCallback) *StreamResult {
	sr := &StreamResult{
		r:  NewResult(s, 1),
		fn: fn,
	}
	sr.r.offsets = sr.r.offsets[:1]
	return sr
}

type Result struct {
	schema  *schema.Schema // result schema
	row     *Row           // row cache
	values  []byte
	offsets []int32
	sorted  []int32
	desc    bool
}

func NewResult(s *schema.Schema, szs ...int) *Result {
	sz := 1024
	if len(szs) > 0 {
		sz = szs[0]
	}
	return &Result{
		schema:  s,
		offsets: make([]int32, 0, sz),
		values:  make([]byte, 0, sz*s.WireSize()),
	}
}

func (r *Result) Reset() {
	r.values = r.values[:0]
	r.offsets = r.offsets[:0]
	r.sorted = r.sorted[:0]
}

func (r *Result) IsValid() bool {
	return r.values != nil
}

func (r *Result) Schema() *schema.Schema {
	return r.schema
}

func (r *Result) Len() int {
	return len(r.offsets)
}

func (r *Result) Row(row int) engine.QueryRow {
	if r.row == nil {
		r.row = &Row{}
	}
	r.row.res = r
	r.row.row = row
	return r.row
}

func (r *Result) Record(n int) []byte {
	olen := len(r.offsets)
	if r.values == nil || olen < n {
		return nil
	}
	if r.sorted != nil {
		if r.desc {
			n = int(r.sorted[olen-n])
		} else {
			n = int(r.sorted[n])
		}
	}
	start, end := r.offsets[n], len(r.values)
	if n < olen-1 {
		end = int(r.offsets[n+1])
	}
	return r.values[start:end]
}

func (r *Result) Close() {
	r.values = nil
	r.offsets = nil
}

func (r *Result) Bytes() []byte {
	return r.values
}

func (r *Result) SortBy(name string, order types.OrderType) {
	col, err := r.Column(name)
	if err != nil {
		return
	}

	// prepare sort lookup table
	n := len(r.values)
	if cap(r.sorted) < n {
		r.sorted = make([]int32, n)
	}
	r.sorted = r.sorted[:n]
	for i := range r.sorted {
		r.sorted[i] = int32(i)
	}

	// remember order
	switch order {
	case types.OrderDesc, types.OrderDescCaseInsensitive:
		r.desc = true
	}

	// sort indirect by column
	switch c := col.(type) {
	case []int8:
		slices.SortStableFunc(r.sorted, func(i, j int32) int { return util.Cmp(c[i], c[j]) })
	case []int16:
		slices.SortStableFunc(r.sorted, func(i, j int32) int { return util.Cmp(c[i], c[j]) })
	case []int32:
		slices.SortStableFunc(r.sorted, func(i, j int32) int { return util.Cmp(c[i], c[j]) })
	case []int64:
		slices.SortStableFunc(r.sorted, func(i, j int32) int { return util.Cmp(c[i], c[j]) })
	case []uint8:
		slices.SortStableFunc(r.sorted, func(i, j int32) int { return util.Cmp(c[i], c[j]) })
	case []uint16:
		slices.SortStableFunc(r.sorted, func(i, j int32) int { return util.Cmp(c[i], c[j]) })
	case []uint32:
		slices.SortStableFunc(r.sorted, func(i, j int32) int { return util.Cmp(c[i], c[j]) })
	case []uint64:
		slices.SortStableFunc(r.sorted, func(i, j int32) int { return util.Cmp(c[i], c[j]) })
	case []float32:
		slices.SortStableFunc(r.sorted, func(i, j int32) int { return util.Cmp(c[i], c[j]) })
	case []float64:
		slices.SortStableFunc(r.sorted, func(i, j int32) int { return util.Cmp(c[i], c[j]) })
	case [][]byte:
		slices.SortStableFunc(r.sorted, func(i, j int32) int { return bytes.Compare(c[i], c[j]) })
	case []string:
		switch order {
		case types.OrderAscCaseInsensitive, types.OrderDescCaseInsensitive:
			slices.SortStableFunc(r.sorted, func(i, j int32) int { return util.CmpCaseInsensitive(c[i], c[j]) })
		default:
			slices.SortStableFunc(r.sorted, func(i, j int32) int { return strings.Compare(c[i], c[j]) })
		}
	case []time.Time:
		slices.SortStableFunc(r.sorted, func(i, j int32) int { return util.CmpTime(c[i], c[j]) })
	case []num.Int128:
		slices.SortStableFunc(r.sorted, func(i, j int32) int { return c[i].Cmp(c[j]) })
	case []num.Int256:
		slices.SortStableFunc(r.sorted, func(i, j int32) int { return c[i].Cmp(c[j]) })
	case num.Decimal32Slice:
		slices.SortStableFunc(r.sorted, func(i, j int32) int { return util.Cmp(c.Int32[i], c.Int32[j]) })
	case num.Decimal64Slice:
		slices.SortStableFunc(r.sorted, func(i, j int32) int { return util.Cmp(c.Int64[i], c.Int64[j]) })
	case num.Decimal128Slice:
		slices.SortStableFunc(r.sorted, func(i, j int32) int { return c.Int128[i].Cmp(c.Int128[j]) })
	case num.Decimal256Slice:
		slices.SortStableFunc(r.sorted, func(i, j int32) int { return c.Int256[i].Cmp(c.Int256[j]) })
	}
}

func (r *Result) ForEach(fn func(val engine.QueryRow) error) error {
	if !r.IsValid() {
		return ErrResultClosed
	}
	for i, l := 0, r.Len(); i < l; i++ {
		if err := fn(r.Row(i)); err != nil {
			return err
		}
	}
	return nil
}

// QueryResultConsumer interface
func (r *Result) Append(buf []byte, isZeroCopy bool) error {
	if isZeroCopy {
		buf = bytes.Clone(buf)
	}
	r.offsets = append(r.offsets, int32(len(r.values)))
	r.values = append(r.values, buf...)
	return nil
}

// not public
func (r *Result) Column(name string) (any, error) {
	if !r.IsValid() {
		return nil, ErrResultClosed
	}
	idx, ok := r.schema.FieldIndexByName(name)
	if !ok {
		return nil, engine.ErrNoField
	}
	view := schema.NewView(r.schema)
	var vals any
	for i := range r.offsets {
		view.Reset(r.Record(i))
		vals = view.Append(vals, idx)
	}
	return vals, nil
}

type Row struct {
	res  *Result
	row  int
	conv *schema.Converter
	dec  *schema.Decoder
	view *schema.View
}

func (r *Row) Schema() *schema.Schema {
	return r.res.Schema()
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
	if r.conv == nil || r.conv.Schema() != s {
		r.conv = schema.NewConverter(r.res.schema, s, binary.NativeEndian)
		r.dec = schema.NewDecoder(s.WithEnums(r.res.schema.Enums()))
	}

	return r.dec.Decode(r.conv.Extract(r.Bytes()), val)
}

func (r *Row) Field(name string) (any, error) {
	if !r.res.IsValid() {
		return nil, ErrResultClosed
	}
	f, ok := r.res.schema.FieldByName(name)
	if !ok {
		return nil, schema.ErrInvalidField
	}
	if r.view == nil {
		r.view = schema.NewView(r.res.schema)
	}
	r.view.Reset(r.Bytes())
	val, ok := r.view.Get(int(f.Id()))
	if !ok {
		return nil, schema.ErrInvalidField
	}
	return val, nil
}

func (r *Row) Index(i int) (any, error) {
	if !r.res.IsValid() {
		return nil, ErrResultClosed
	}
	f, ok := r.res.schema.FieldById(uint16(i))
	if !ok {
		return nil, schema.ErrInvalidField
	}
	if r.view == nil {
		r.view = schema.NewView(r.res.schema)
	}
	r.view.Reset(r.Bytes())
	val, ok := r.view.Get(int(f.Id()))
	if !ok {
		return nil, schema.ErrInvalidField
	}
	return val, nil
}
