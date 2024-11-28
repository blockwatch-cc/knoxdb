// Copyright (c) 2018-2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc
//go:build ignore
// +build ignore

package pack

import (
	"fmt"
	"reflect"
	"time"

	"blockwatch.cc/knoxdb/encoding/decimal"
	"blockwatch.cc/knoxdb/vec"
)

// TODO: migrate to interface and implement PackResult + KeyValueResult
type Result struct {
	fields  FieldList
	pkg     *Package
	tinfo   *typeInfo
	values  []byte
	offsets []int
}

func (r *Result) Close() {
	if r == nil {
		return
	}
	if r.pkg != nil {
		r.pkg.Release()
	}
	r.pkg = nil
	r.values = nil
	r.offsets = nil
	r.tinfo = nil
}

func (r *Result) Fields() FieldList {
	return r.fields
}

func (r *Result) IsValid() bool {
	return r.pkg != nil || r.values != nil
}

func (r *Result) Cols() int {
	if !r.IsValid() {
		return 0
	}
	return len(r.fields)
}

func (r *Result) Rows() int {
	switch {
	case r.pkg != nil:
		return r.pkg.Len()
	case r.offsets != nil:
		return len(r.offsets)
	default:
		return 0
	}
}

func (r *Result) Row(n int) Row {
	return Row{r, n}
}

func (r *Result) Record(n int) []byte {
	olen := len(r.offsets)
	if r.values == nil || olen < n {
		return nil
	}
	start, end := r.offsets[n], len(r.values)
	if n < olen-1 {
		end = r.offsets[n+1]
	}
	return r.values[start:end]
}

// TODO: extend to value type results
func (r *Result) PkColumn() []uint64 {
	return r.pkg.PkColumn()
}

// TODO: extend to value type results
func (r *Result) Column(colname string) (interface{}, error) {
	if !r.IsValid() {
		return nil, ErrResultClosed
	}
	i := r.pkg.FieldIndex(colname)
	if i < 0 {
		return nil, ErrNoField
	}
	return r.pkg.Column(i)
}

// TODO: extend to value type results
func (r *Result) SortByField(name string) error {
	if !r.IsValid() {
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

func (r *Result) DecodeAt(n int, val interface{}) error {
	if !r.IsValid() {
		return ErrResultClosed
	}
	if r.tinfo == nil {
		if err := r.buildTypeInfo(val); err != nil {
			return err
		}
	}
	if r.pkg != nil {
		return r.pkg.ReadAtWithInfo(n, val, r.tinfo)
	}
	return r.fields.DecodeWithInfo(r.Record(n), val, r.tinfo)
}

func (r *Result) buildTypeInfo(val interface{}) error {
	sharedTypeInfo, err := getTypeInfo(val)
	if err != nil {
		return err
	}
	r.tinfo = sharedTypeInfo.Clone()
	if r.pkg != nil {
		for i, v := range r.tinfo.fields {
			r.tinfo.fields[i].blockid = r.pkg.FieldIndex(v.name)
		}
	} else {
		r.fields = r.fields.Clone()
		for i := range r.fields {
			r.fields[i].Index = -1
		}
		for i, v := range r.tinfo.fields {
			r.fields.Find(v.name).Index = i
		}
	}
	return nil
}

func (r *Result) buildTypeInfoReflect(typ reflect.Type) error {
	if typ.Kind() == reflect.Ptr {
		typ = typ.Elem()
	}
	return r.buildTypeInfo(reflect.New(typ).Interface())
}

func (r *Result) Decode(val interface{}) error {
	v := reflect.ValueOf(val)
	if v.Kind() != reflect.Ptr {
		return fmt.Errorf("pack: non-pointer passed to Decode")
	}
	if !r.IsValid() {
		return ErrResultClosed
	}
	v = reflect.Indirect(v)
	switch v.Kind() {
	case reflect.Slice:
		// get slice element type
		typ := v.Type().Elem()
		err := r.buildTypeInfoReflect(typ)
		if err != nil {
			return err
		}
		for i := 0; i < r.pkg.Len(); i++ {
			// create new slice element (may be a pointer to struct)
			e := reflect.New(typ)
			ev := e

			// if element is ptr to struct, allocate the underlying struct
			if e.Elem().Kind() == reflect.Ptr {
				ev.Elem().Set(reflect.New(e.Elem().Type().Elem()))
				ev = reflect.Indirect(e)
			}

			// decode the struct element (re-use our interface-based methods)
			if r.pkg != nil {
				err = r.pkg.ReadAtWithInfo(i, ev.Interface(), r.tinfo)
			} else {
				err = r.fields.DecodeWithInfo(r.Record(i), ev.Interface(), r.tinfo)
			}
			if err != nil {
				return err
			}
			// append slice element
			v.Set(reflect.Append(v, e.Elem()))
		}
		return nil
	case reflect.Struct:
		return r.DecodeAt(0, val)
	default:
		return fmt.Errorf("pack: non-slice/struct passed to Decode")
	}
}

func (r *Result) DecodeRange(start, end int, proto interface{}) (interface{}, error) {
	if !r.IsValid() {
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
		var err error
		if r.pkg != nil {
			err = r.pkg.ReadAtWithInfo(i, slice.Index(i-start).Interface(), r.tinfo)
		} else {
			err = r.fields.DecodeWithInfo(r.Record(i), slice.Index(i-start).Interface(), r.tinfo)
		}
		if err != nil {
			return nil, err
		}
	}
	return slice.Interface(), nil
}

func (r *Result) Walk(fn func(r Row) error) error {
	if !r.IsValid() {
		return ErrResultClosed
	}
	for i, l := 0, r.Rows(); i < l; i++ {
		if err := fn(Row{res: r, n: i}); err != nil {
			return err
		}
	}
	return nil
}

func (r *Result) ForEach(proto interface{}, fn func(i int, val interface{}) error) error {
	if !r.IsValid() {
		return ErrResultClosed
	}
	if r.tinfo == nil {
		if err := r.buildTypeInfo(proto); err != nil {
			return err
		}
	}
	typ := derefIndirect(proto).Type()
	for i, l := 0, r.Rows(); i < l; i++ {
		var err error
		// create new empty value for interface prototype
		val := reflect.New(typ)
		// unmarshal and map
		if r.pkg != nil {
			err = r.pkg.ReadAtWithInfo(i, val.Interface(), r.tinfo)
		} else {
			err = r.fields.DecodeWithInfo(r.Record(i), val.Interface(), r.tinfo)
		}
		if err != nil {
			return err
		}
		// forward to callback
		if err := fn(i, val.Interface()); err != nil {
			return err
		}
	}
	return nil
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
	if !r.res.IsValid() {
		return nil, ErrResultClosed
	}
	if r.res.pkg != nil {
		i := r.res.pkg.FieldIndex(name)
		if i < 0 {
			return nil, ErrNoField
		}
		return r.res.pkg.FieldAt(i, r.n)
	} else {
		f := r.res.fields.Find(name)
		if f.Index < 0 {
			return nil, ErrNoField
		}
		val, ok := NewValue(r.res.fields).Reset(r.res.Record(r.n)).Get(f.Index)
		if !ok {
			return nil, ErrNoColumn
		}
		return val, nil
	}
}

// TODO: extend to value type results
func (r Row) Index(i int) (interface{}, error) {
	if r.res.pkg == nil {
		return nil, ErrResultClosed
	}
	return r.res.pkg.FieldAt(i, r.n)
}

func (r Row) Uint64(index int) (uint64, error) {
	return r.res.pkg.Uint64At(index, r.n)
}

func (r Row) Uint32(index int) (uint32, error) {
	return r.res.pkg.Uint32At(index, r.n)
}

func (r Row) Uint16(index int) (uint16, error) {
	return r.res.pkg.Uint16At(index, r.n)
}

func (r Row) Uint8(index int) (uint8, error) {
	return r.res.pkg.Uint8At(index, r.n)
}

func (r Row) Int256(index int) (vec.Int256, error) {
	return r.res.pkg.Int256At(index, r.n)
}

func (r Row) Int128(index int) (vec.Int128, error) {
	return r.res.pkg.Int128At(index, r.n)
}

func (r Row) Int64(index int) (int64, error) {
	return r.res.pkg.Int64At(index, r.n)
}

func (r Row) Int32(index int) (int32, error) {
	return r.res.pkg.Int32At(index, r.n)
}

func (r Row) Int16(index int) (int16, error) {
	return r.res.pkg.Int16At(index, r.n)
}

func (r Row) Int8(index int) (int8, error) {
	return r.res.pkg.Int8At(index, r.n)
}

func (r Row) Decimal256(index int) (decimal.Decimal256, error) {
	return r.res.pkg.Decimal256At(index, r.n)
}

func (r Row) Decimal128(index int) (decimal.Decimal128, error) {
	return r.res.pkg.Decimal128At(index, r.n)
}

func (r Row) Decimal64(index int) (decimal.Decimal64, error) {
	return r.res.pkg.Decimal64At(index, r.n)
}

func (r Row) Decimal32(index int) (decimal.Decimal32, error) {
	return r.res.pkg.Decimal32At(index, r.n)
}

func (r Row) Float64(index int) (float64, error) {
	return r.res.pkg.Float64At(index, r.n)
}

func (r Row) Float32(index int) (float32, error) {
	return r.res.pkg.Float32At(index, r.n)
}

func (r Row) String(index int) (string, error) {
	return r.res.pkg.StringAt(index, r.n)
}

func (r Row) Bytes(index int) ([]byte, error) {
	return r.res.pkg.BytesAt(index, r.n)
}

func (r Row) Bool(index int) (bool, error) {
	return r.res.pkg.BoolAt(index, r.n)
}

func (r Row) Time(index int) (time.Time, error) {
	return r.res.pkg.TimeAt(index, r.n)
}
