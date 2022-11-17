// Copyright (c) 2018-2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package pack

import (
	"reflect"
	"time"

	. "blockwatch.cc/knoxdb/encoding/decimal"
	. "blockwatch.cc/knoxdb/vec"
)

type Result struct {
	fields FieldList
	pkg    *Package
	table  *Table
	tinfo  *typeInfo
}

type Row struct {
	res *Result
	n   int
}

func (r Row) Decode(val interface{}) error {
	return r.res.DecodeAt(r.n, val)
}

func (r Row) Result() *Result {
	return r.res
}

func (r Row) N() int {
	return r.n
}

func (r Row) Field(name string) (interface{}, error) {
	if r.res.pkg == nil {
		return nil, ErrResultClosed
	}
	i := r.res.pkg.FieldIndex(name)
	if i < 0 {
		return nil, ErrNoField
	}
	return r.res.pkg.FieldAt(i, r.n)
}

func (r *Result) Close() {
	r.pkg.recycleNew()
	r.table = nil
	r.pkg = nil
	r.tinfo = nil
}

func (r *Result) Fields() FieldList {
	return r.fields
}

func (r *Result) SortByField(name string) error {
	if r.pkg == nil {
		return ErrResultClosed
	}
	i := r.pkg.FieldIndex(name)
	if i < 0 {
		return ErrNoField
	}
	if r.pkg.Len() == 0 {
		return nil
	}
	sorter, err := NewPackageSorter(r.pkg, i)
	if err != nil {
		return err
	}

	// update dirty state when package has changed
	updated := sorter.Sort()
	r.pkg.dirty = r.pkg.dirty || updated
	return nil
}

func (r *Result) Cols() int {
	if r.pkg == nil {
		return 0
	}
	return r.pkg.nFields
}

func (r *Result) Rows() int {
	if r.pkg == nil {
		return 0
	}
	return r.pkg.nValues
}

func (r *Result) DecodeAt(n int, val interface{}) error {
	if r.pkg == nil {
		return ErrResultClosed
	}
	if r.tinfo == nil {
		if err := r.buildTypeInfo(val); err != nil {
			return err
		}
	}
	return r.pkg.ReadAtWithInfo(n, val, r.tinfo)
}

func (r *Result) buildTypeInfo(val interface{}) error {
	sharedTypeInfo, err := getTypeInfo(val)
	if err != nil {
		return err
	}
	r.tinfo = sharedTypeInfo.Clone()
	for i, v := range r.tinfo.fields {
		r.tinfo.fields[i].blockid = r.pkg.FieldIndex(v.name)
	}
	return nil
}

func (r *Result) DecodeRange(start, end int, proto interface{}) (interface{}, error) {
	if r.pkg == nil {
		return nil, ErrResultClosed
	}
	if r.tinfo == nil {
		if err := r.buildTypeInfo(proto); err != nil {
			return nil, err
		}
	}
	typ := reflect.Indirect(reflect.ValueOf(proto)).Type()
	slice := reflect.MakeSlice(reflect.SliceOf(typ), end-start, end-start)
	for i := start; i < end; i++ {
		if err := r.pkg.ReadAtWithInfo(i, slice.Index(i-start).Interface(), r.tinfo); err != nil {
			return nil, err
		}
	}
	return slice.Interface(), nil
}

func (r *Result) Walk(fn func(r Row) error) error {
	if r.pkg == nil {
		return ErrResultClosed
	}
	for i, l := 0, r.pkg.Len(); i < l; i++ {
		if err := fn(Row{res: r, n: i}); err != nil {
			return err
		}
	}
	return nil
}

func (r *Result) ForEach(proto interface{}, fn func(i int, val interface{}) error) error {
	if r.pkg == nil {
		return ErrResultClosed
	}
	if r.tinfo == nil {
		if err := r.buildTypeInfo(proto); err != nil {
			return err
		}
	}
	typ := derefIndirect(proto).Type()
	for i := 0; i < r.pkg.nValues; i++ {
		// create new empty value for interface prototype
		val := reflect.New(typ)
		// unmarshal and map
		if err := r.pkg.ReadAtWithInfo(i, val.Interface(), r.tinfo); err != nil {
			return err
		}
		// forward to callback
		if err := fn(i, val.Interface()); err != nil {
			return err
		}
	}
	return nil
}

func (r *Result) PkColumn() []uint64 {
	return r.pkg.PkColumn()
}

func (r *Result) Column(colname string) (interface{}, error) {
	if r.pkg == nil {
		return nil, ErrResultClosed
	}
	i := r.pkg.FieldIndex(colname)
	if i < 0 {
		return nil, ErrNoField
	}
	return r.pkg.Column(i)
}

func (r *Result) Range(colname string, start, end int) (interface{}, error) {
	if r.pkg == nil {
		return nil, ErrResultClosed
	}
	i := r.pkg.FieldIndex(colname)
	if i < 0 {
		return nil, ErrNoField
	}
	return r.pkg.RangeAt(i, start, end)
}

func (r *Result) Uint64At(index, pos int) (uint64, error) {
	if r.pkg == nil {
		return 0, ErrResultClosed
	}
	return r.pkg.Uint64At(index, pos)
}

func (r *Result) Uint32At(index, pos int) (uint32, error) {
	if r.pkg == nil {
		return 0, ErrResultClosed
	}
	return r.pkg.Uint32At(index, pos)
}

func (r *Result) Uint16At(index, pos int) (uint16, error) {
	if r.pkg == nil {
		return 0, ErrResultClosed
	}
	return r.pkg.Uint16At(index, pos)
}

func (r *Result) Uint8At(index, pos int) (uint8, error) {
	if r.pkg == nil {
		return 0, ErrResultClosed
	}
	return r.pkg.Uint8At(index, pos)
}

func (r *Result) Int256At(index, pos int) (Int256, error) {
	if r.pkg == nil {
		return ZeroInt256, ErrResultClosed
	}
	return r.pkg.Int256At(index, pos)
}

func (r *Result) Int128At(index, pos int) (Int128, error) {
	if r.pkg == nil {
		return ZeroInt128, ErrResultClosed
	}
	return r.pkg.Int128At(index, pos)
}

func (r *Result) Int64At(index, pos int) (int64, error) {
	if r.pkg == nil {
		return 0, ErrResultClosed
	}
	return r.pkg.Int64At(index, pos)
}

func (r *Result) Int32At(index, pos int) (int32, error) {
	if r.pkg == nil {
		return 0, ErrResultClosed
	}
	return r.pkg.Int32At(index, pos)
}

func (r *Result) Int16At(index, pos int) (int16, error) {
	if r.pkg == nil {
		return 0, ErrResultClosed
	}
	return r.pkg.Int16At(index, pos)
}

func (r *Result) Int8At(index, pos int) (int8, error) {
	if r.pkg == nil {
		return 0, ErrResultClosed
	}
	return r.pkg.Int8At(index, pos)
}

func (r *Result) Decimal256At(index, pos int) (Decimal256, error) {
	if r.pkg == nil {
		return Decimal256Zero, ErrResultClosed
	}
	return r.pkg.Decimal256At(index, pos)
}

func (r *Result) Decimal128At(index, pos int) (Decimal128, error) {
	if r.pkg == nil {
		return Decimal128Zero, ErrResultClosed
	}
	return r.pkg.Decimal128At(index, pos)
}

func (r *Result) Decimal64At(index, pos int) (Decimal64, error) {
	if r.pkg == nil {
		return Decimal64Zero, ErrResultClosed
	}
	return r.pkg.Decimal64At(index, pos)
}

func (r *Result) Decimal32At(index, pos int) (Decimal32, error) {
	if r.pkg == nil {
		return Decimal32Zero, ErrResultClosed
	}
	return r.pkg.Decimal32At(index, pos)
}

func (r *Result) Float64At(index, pos int) (float64, error) {
	if r.pkg == nil {
		return 0, ErrResultClosed
	}
	return r.pkg.Float64At(index, pos)
}

func (r *Result) Float32At(index, pos int) (float32, error) {
	if r.pkg == nil {
		return 0, ErrResultClosed
	}
	return r.pkg.Float32At(index, pos)
}

func (r *Result) StringAt(index, pos int) (string, error) {
	if r.pkg == nil {
		return "", ErrResultClosed
	}
	return r.pkg.StringAt(index, pos)
}

func (r *Result) BytesAt(index, pos int) ([]byte, error) {
	if r.pkg == nil {
		return nil, ErrResultClosed
	}
	return r.pkg.BytesAt(index, pos)
}

func (r *Result) BoolAt(index, pos int) (bool, error) {
	if r.pkg == nil {
		return false, ErrResultClosed
	}
	return r.pkg.BoolAt(index, pos)
}

func (r *Result) TimeAt(index, pos int) (time.Time, error) {
	if r.pkg == nil {
		return time.Time{}, ErrResultClosed
	}
	return r.pkg.TimeAt(index, pos)
}

func (r *Result) TimeColumn(colname string) ([]time.Time, error) {
	col, err := r.Column(colname)
	if err != nil {
		return nil, err
	}
	tcol, ok := col.([]int64)
	if !ok {
		return nil, ErrTypeMismatch
	}
	res := make([]time.Time, len(tcol))
	for i := range tcol {
		res[i] = time.Unix(0, tcol[i]).UTC()
	}
	return res, nil
}

func (r *Result) Uint64Column(colname string) ([]uint64, error) {
	col, err := r.Column(colname)
	if err != nil {
		return nil, err
	}
	tcol, ok := col.([]uint64)
	if !ok {
		return nil, ErrTypeMismatch
	}
	return tcol, nil
}

func (r *Result) Uint32Column(colname string) ([]uint32, error) {
	col, err := r.Column(colname)
	if err != nil {
		return nil, err
	}
	tcol, ok := col.([]uint32)
	if !ok {
		return nil, ErrTypeMismatch
	}
	return tcol, nil
}

func (r *Result) Uint16Column(colname string) ([]uint16, error) {
	col, err := r.Column(colname)
	if err != nil {
		return nil, err
	}
	tcol, ok := col.([]uint16)
	if !ok {
		return nil, ErrTypeMismatch
	}
	return tcol, nil
}

func (r *Result) Uint8Column(colname string) ([]uint8, error) {
	col, err := r.Column(colname)
	if err != nil {
		return nil, err
	}
	tcol, ok := col.([]uint8)
	if !ok {
		return nil, ErrTypeMismatch
	}
	return tcol, nil
}

func (r *Result) Int256Column(colname string) ([]Int256, error) {
	col, err := r.Column(colname)
	if err != nil {
		return nil, err
	}
	tcol, ok := col.([]Int256)
	if !ok {
		return nil, ErrTypeMismatch
	}
	return tcol, nil
}

func (r *Result) Int128Column(colname string) ([]Int128, error) {
	col, err := r.Column(colname)
	if err != nil {
		return nil, err
	}
	tcol, ok := col.([]Int128)
	if !ok {
		return nil, ErrTypeMismatch
	}
	return tcol, nil
}

func (r *Result) Int64Column(colname string) ([]int64, error) {
	col, err := r.Column(colname)
	if err != nil {
		return nil, err
	}
	tcol, ok := col.([]int64)
	if !ok {
		return nil, ErrTypeMismatch
	}
	return tcol, nil
}

func (r *Result) Int32Column(colname string) ([]int32, error) {
	col, err := r.Column(colname)
	if err != nil {
		return nil, err
	}
	tcol, ok := col.([]int32)
	if !ok {
		return nil, ErrTypeMismatch
	}
	return tcol, nil
}

func (r *Result) Int16Column(colname string) ([]int16, error) {
	col, err := r.Column(colname)
	if err != nil {
		return nil, err
	}
	tcol, ok := col.([]int16)
	if !ok {
		return nil, ErrTypeMismatch
	}
	return tcol, nil
}

func (r *Result) Int8Column(colname string) ([]int8, error) {
	col, err := r.Column(colname)
	if err != nil {
		return nil, err
	}
	tcol, ok := col.([]int8)
	if !ok {
		return nil, ErrTypeMismatch
	}
	return tcol, nil
}

func (r *Result) Decimal256Column(colname string) (Decimal256Slice, error) {
	col, err := r.Column(colname)
	if err != nil {
		return Decimal256Slice{}, err
	}
	tcol, ok := col.(Decimal256Slice)
	if !ok {
		return Decimal256Slice{}, ErrTypeMismatch
	}
	return tcol, nil
}

func (r *Result) Decimal128Column(colname string) (Decimal128Slice, error) {
	col, err := r.Column(colname)
	if err != nil {
		return Decimal128Slice{}, err
	}
	tcol, ok := col.(Decimal128Slice)
	if !ok {
		return Decimal128Slice{}, ErrTypeMismatch
	}
	return tcol, nil
}

func (r *Result) Decimal64Column(colname string) (Decimal64Slice, error) {
	col, err := r.Column(colname)
	if err != nil {
		return Decimal64Slice{}, err
	}
	tcol, ok := col.(Decimal64Slice)
	if !ok {
		return Decimal64Slice{}, ErrTypeMismatch
	}
	return tcol, nil
}

func (r *Result) Decimal32Column(colname string) (Decimal32Slice, error) {
	col, err := r.Column(colname)
	if err != nil {
		return Decimal32Slice{}, err
	}
	tcol, ok := col.(Decimal32Slice)
	if !ok {
		return Decimal32Slice{}, ErrTypeMismatch
	}
	return tcol, nil
}

func (r *Result) Float64Column(colname string) ([]float64, error) {
	col, err := r.Column(colname)
	if err != nil {
		return nil, err
	}
	tcol, ok := col.([]float64)
	if !ok {
		return nil, ErrTypeMismatch
	}
	return tcol, nil
}

func (r *Result) Float32Column(colname string) ([]float32, error) {
	col, err := r.Column(colname)
	if err != nil {
		return nil, err
	}
	tcol, ok := col.([]float32)
	if !ok {
		return nil, ErrTypeMismatch
	}
	return tcol, nil
}

func (r *Result) StringColumn(colname string) ([]string, error) {
	col, err := r.Column(colname)
	if err != nil {
		return nil, err
	}
	tcol, ok := col.([]string)
	if !ok {
		return nil, ErrTypeMismatch
	}
	return tcol, nil
}

func (r *Result) BytesColumn(colname string) ([][]byte, error) {
	col, err := r.Column(colname)
	if err != nil {
		return nil, err
	}
	tcol, ok := col.([][]byte)
	if !ok {
		return nil, ErrTypeMismatch
	}
	return tcol, nil
}

func (r *Result) BoolColumn(colname string) ([]bool, error) {
	col, err := r.Column(colname)
	if err != nil {
		return nil, err
	}
	tcol, ok := col.([]bool)
	if !ok {
		return nil, ErrTypeMismatch
	}
	return tcol, nil
}
