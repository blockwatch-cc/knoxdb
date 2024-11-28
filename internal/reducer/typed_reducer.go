// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package reducer

import (
	"math"
	"reflect"
	"time"
)

func NewTypedReducer(typ reflect.Type, fn ReducerFunc) TypedReducer {
	val := reflect.New(typ).Interface().(Aggregatable)
	switch fn {
	case ReducerFuncSum:
		return &TypedSumReducer{v: val}
	case ReducerFuncFirst:
		return &TypedFirstReducer{v: val}
	case ReducerFuncLast:
		return &TypedLastReducer{v: val}
	case ReducerFuncMin:
		return &TypedMinReducer{v: val}
	case ReducerFuncMax:
		return &TypedMaxReducer{v: val}
	case ReducerFuncMean:
		return &TypedMeanReducer{v: val}
	case ReducerFuncVar:
		return &TypedVarReducer{v: val}
	case ReducerFuncStd:
		return &TypedStdReducer{v: val}
	case ReducerFuncFirstJoin:
		return &TypedFirstJoinReducer{v: val}
	case ReducerFuncLastJoin:
		return &TypedLastJoinReducer{v: val}
	case ReducerFuncMinJoin:
		return &TypedMinJoinReducer{v: val}
	case ReducerFuncMaxJoin:
		return &TypedMaxJoinReducer{v: val}
	case ReducerFuncMeanJoin:
		return &TypedMeanJoinReducer{v: val}
	case ReducerFuncVarJoin:
		return &TypedVarJoinReducer{v: val}
	case ReducerFuncStdJoin:
		return &TypedStdJoinReducer{v: val}
	default:
		return nil
	}
}

type TypedReducer interface {
	Reduce(Aggregatable, time.Time, bool)
	Reset()
	Init(Aggregatable)
	Value() (Aggregatable, bool)
	Config() Aggregatable
	Time() time.Time
	Type() ReducerFunc
}

// SUM
type TypedSumReducer struct {
	t time.Time
	v Aggregatable
}

func (r *TypedSumReducer) Reduce(v Aggregatable, t time.Time, _ bool) {
	if r.t.IsZero() {
		r.t = t
	}
	r.v = r.v.Add(v)
}

func (r *TypedSumReducer) Reset() {
	r.t = time.Time{}
	r.v = r.v.Zero()
}

func (r *TypedSumReducer) Init(v Aggregatable) {
	r.v = r.v.Zero()
	r.v.Init(v)
}

func (r *TypedSumReducer) Config() Aggregatable {
	return r.v
}

func (r *TypedSumReducer) Value() (Aggregatable, bool) {
	return r.v, !r.t.IsZero()
}

func (r *TypedSumReducer) Time() time.Time {
	return r.t
}

func (r *TypedSumReducer) Type() ReducerFunc {
	return ReducerFuncSum
}

// FIRST
type TypedFirstReducer struct {
	t time.Time
	v Aggregatable
}

func (r *TypedFirstReducer) Reduce(v Aggregatable, t time.Time, _ bool) {
	if r.t.IsZero() {
		r.t = t
		v.Init(r.v)
		r.v = v
	}
}

func (r *TypedFirstReducer) Reset() {
	r.t = time.Time{}
	r.v = r.v.Zero()
}

func (r *TypedFirstReducer) Init(v Aggregatable) {
	r.v = v
}

func (r *TypedFirstReducer) Config() Aggregatable {
	return r.v
}

func (r *TypedFirstReducer) Value() (Aggregatable, bool) {
	return r.v, !r.t.IsZero()
}

func (r *TypedFirstReducer) Time() time.Time {
	return r.t
}

func (r *TypedFirstReducer) Type() ReducerFunc {
	return ReducerFuncFirst
}

// LAST
type TypedLastReducer struct {
	t time.Time
	v Aggregatable
}

func (r *TypedLastReducer) Reduce(v Aggregatable, t time.Time, _ bool) {
	r.t = t
	v.Init(r.v)
	r.v = v
}

func (r *TypedLastReducer) Reset() {
	r.t = time.Time{}
	r.v = r.v.Zero()
}

func (r *TypedLastReducer) Init(v Aggregatable) {
	r.v = v
}

func (r *TypedLastReducer) Config() Aggregatable {
	return r.v
}

func (r *TypedLastReducer) Value() (Aggregatable, bool) {
	return r.v, !r.t.IsZero()

}
func (r *TypedLastReducer) Time() time.Time {
	return r.t
}

func (r *TypedLastReducer) Type() ReducerFunc {
	return ReducerFuncLast
}

// MAX
type TypedMaxReducer struct {
	t time.Time
	v Aggregatable
}

func (r *TypedMaxReducer) Reduce(v Aggregatable, t time.Time, _ bool) {
	if r.t.IsZero() || r.v.Cmp(v) < 0 {
		r.t = t
		v.Init(r.v)
		r.v = v
	}
}

func (r *TypedMaxReducer) Reset() {
	r.t = time.Time{}
	r.v = r.v.Zero()
}

func (r *TypedMaxReducer) Init(v Aggregatable) {
	r.v = v
}

func (r *TypedMaxReducer) Config() Aggregatable {
	return r.v
}

func (r *TypedMaxReducer) Value() (Aggregatable, bool) {
	return r.v, !r.t.IsZero()

}
func (r *TypedMaxReducer) Time() time.Time {
	return r.t
}

func (r *TypedMaxReducer) Type() ReducerFunc {
	return ReducerFuncMax
}

// MIN
type TypedMinReducer struct {
	t time.Time
	v Aggregatable
}

func (r *TypedMinReducer) Reduce(v Aggregatable, t time.Time, _ bool) {
	if r.t.IsZero() || r.v.Cmp(v) > 0 {
		r.t = t
		v.Init(r.v)
		r.v = v
	}
}

func (r *TypedMinReducer) Reset() {
	r.t = time.Time{}
	r.v = r.v.Zero()
}

func (r *TypedMinReducer) Init(v Aggregatable) {
	r.v = v
}

func (r *TypedMinReducer) Config() Aggregatable {
	return r.v
}

func (r *TypedMinReducer) Value() (Aggregatable, bool) {
	return r.v, !r.t.IsZero()

}
func (r *TypedMinReducer) Time() time.Time {
	return r.t
}

func (r *TypedMinReducer) Type() ReducerFunc {
	return ReducerFuncMin
}

// MEAN
// Welford's Online algorithm, see
// https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance
type TypedMeanReducer struct {
	t    time.Time
	n    int
	mean float64
	v    Aggregatable
}

func (r *TypedMeanReducer) Reduce(v Aggregatable, t time.Time, _ bool) {
	if r.n == 0 {
		r.t = t
	}
	r.n++
	delta := v.Float64() - r.mean
	r.mean += delta / float64(r.n)
}

func (r *TypedMeanReducer) Reset() {
	r.t = time.Time{}
	r.mean = 0
	r.n = 0
}

func (r *TypedMeanReducer) Init(v Aggregatable) {
	r.v = v
}

func (r *TypedMeanReducer) Config() Aggregatable {
	return r.v
}

func (r *TypedMeanReducer) Value() (Aggregatable, bool) {
	v := r.v.Zero()
	v.SetFloat64(r.mean)
	return v, r.n > 0

}
func (r *TypedMeanReducer) Time() time.Time {
	return r.t
}

func (r *TypedMeanReducer) Type() ReducerFunc {
	return ReducerFuncMean
}

// VAR
type TypedVarReducer struct {
	t    time.Time
	n    int
	mean float64 // mean aggregator
	m2   float64 // variance aggregator
	v    Aggregatable
}

func (r *TypedVarReducer) Reduce(v Aggregatable, t time.Time, _ bool) {
	if r.n == 0 {
		r.t = t
	}
	r.n++
	f64 := v.Float64()
	delta := f64 - r.mean
	r.mean += delta / float64(r.n)
	r.m2 += delta * (f64 - r.mean)
}

func (r *TypedVarReducer) Reset() {
	r.t = time.Time{}
	r.n = 0
	r.mean = 0
	r.m2 = 0
}

func (r *TypedVarReducer) Init(v Aggregatable) {
	r.v = v
}

func (r *TypedVarReducer) Config() Aggregatable {
	return r.v
}

func (r *TypedVarReducer) Value() (Aggregatable, bool) {
	v := r.v.Zero()
	if r.n < 2 {
		return v, r.n > 0
	}
	v.SetFloat64(r.m2 / float64(r.n-1))
	return v, r.n > 0
}

func (r *TypedVarReducer) Time() time.Time {
	return r.t
}

func (r *TypedVarReducer) Type() ReducerFunc {
	return ReducerFuncVar
}

// STD
type TypedStdReducer struct {
	t    time.Time
	n    int
	mean float64 // mean aggregator
	m2   float64 // variance aggregator
	v    Aggregatable
}

func (r *TypedStdReducer) Reduce(v Aggregatable, t time.Time, _ bool) {
	if r.n == 0 {
		r.t = t
	}
	r.n++
	f64 := v.Float64()
	delta := f64 - r.mean
	r.mean += delta / float64(r.n)
	r.m2 += delta * (f64 - r.mean)
}

func (r *TypedStdReducer) Reset() {
	r.t = time.Time{}
	r.n = 0
	r.mean = 0
	r.m2 = 0
}

func (r *TypedStdReducer) Init(v Aggregatable) {
	r.v = v
}

func (r *TypedStdReducer) Config() Aggregatable {
	return r.v
}

func (r *TypedStdReducer) Value() (Aggregatable, bool) {
	v := r.v.Zero()
	if r.n < 2 {
		return v, r.n > 0
	}
	v.SetFloat64(math.Sqrt(r.m2 / float64(r.n-1)))
	return v, r.n > 0
}

func (r *TypedStdReducer) Time() time.Time {
	return r.t
}

func (r *TypedStdReducer) Type() ReducerFunc {
	return ReducerFuncStd
}

// FIRST JOIN
type TypedFirstJoinReducer struct {
	t    time.Time
	v    Aggregatable
	j    Aggregatable
	done bool
}

func (r *TypedFirstJoinReducer) Reduce(v Aggregatable, t time.Time, join bool) {
	switch {
	case r.t.IsZero():
		r.t = t
		v.Init(r.v)
		r.v = v
	case join && !r.done:
		r.v = r.v.Add(v)
	default:
		r.done = true
	}
}

func (r *TypedFirstJoinReducer) Reset() {
	r.t = time.Time{}
	r.v = r.v.Zero()
	r.j = r.j.Zero()
	r.done = false
}

func (r *TypedFirstJoinReducer) Init(v Aggregatable) {
	r.v = v
	r.j = v.Zero()
}

func (r *TypedFirstJoinReducer) Config() Aggregatable {
	return r.v
}

func (r *TypedFirstJoinReducer) Value() (Aggregatable, bool) {
	return r.v, !r.t.IsZero()
}

func (r *TypedFirstJoinReducer) Time() time.Time {
	return r.t
}

func (r *TypedFirstJoinReducer) Type() ReducerFunc {
	return ReducerFuncFirstJoin
}

// LAST JOIN
type TypedLastJoinReducer struct {
	t time.Time
	v Aggregatable
	j Aggregatable
}

func (r *TypedLastJoinReducer) Reduce(v Aggregatable, t time.Time, join bool) {
	r.t = t
	if join {
		r.v = r.v.Add(v)
	} else {
		v.Init(r.v)
		r.v = v
	}
}

func (r *TypedLastJoinReducer) Reset() {
	r.t = time.Time{}
	r.v = r.v.Zero()
	r.j = r.j.Zero()
}

func (r *TypedLastJoinReducer) Init(v Aggregatable) {
	r.v = v
	r.j = v.Zero()
}

func (r *TypedLastJoinReducer) Config() Aggregatable {
	return r.v
}

func (r *TypedLastJoinReducer) Value() (Aggregatable, bool) {
	return r.v, !r.t.IsZero()

}
func (r *TypedLastJoinReducer) Time() time.Time {
	return r.t
}

func (r *TypedLastJoinReducer) Type() ReducerFunc {
	return ReducerFuncLastJoin
}

// MAX JOIN
type TypedMaxJoinReducer struct {
	t time.Time
	v Aggregatable
	j Aggregatable
}

func (r *TypedMaxJoinReducer) Reduce(v Aggregatable, t time.Time, join bool) {
	if r.t.IsZero() {
		r.t = t
		r.j = v
		return
	}

	if join {
		r.j = r.j.Add(v)
	} else {
		if r.v.Cmp(r.j) < 0 {
			r.j.Init(r.v)
			r.v = r.j
		}
		r.j = v
	}
}

func (r *TypedMaxJoinReducer) Reset() {
	r.t = time.Time{}
	r.v = r.v.Zero()
	r.j = r.j.Zero()
}

func (r *TypedMaxJoinReducer) Init(v Aggregatable) {
	r.v = v
	r.j = v.Zero()
}

func (r *TypedMaxJoinReducer) Config() Aggregatable {
	return r.v
}

func (r *TypedMaxJoinReducer) Value() (Aggregatable, bool) {
	r.Reduce(r.v.Zero(), r.t, false)
	return r.v, !r.t.IsZero()

}
func (r *TypedMaxJoinReducer) Time() time.Time {
	return r.t
}

func (r *TypedMaxJoinReducer) Type() ReducerFunc {
	return ReducerFuncMaxJoin
}

// MIN JOIN
type TypedMinJoinReducer struct {
	t time.Time
	v Aggregatable
	j Aggregatable
}

func (r *TypedMinJoinReducer) Reduce(v Aggregatable, t time.Time, join bool) {
	if r.t.IsZero() {
		r.t = t
		r.j = v
		return
	}

	if join {
		r.j = r.j.Add(v)
	} else {
		if r.v.Cmp(r.j) > 0 {
			r.j.Init(r.v)
			r.v = r.j
		}
		r.j = v
	}
}

func (r *TypedMinJoinReducer) Reset() {
	r.t = time.Time{}
	r.v = r.v.Zero()
	r.j = r.j.Zero()
}

func (r *TypedMinJoinReducer) Init(v Aggregatable) {
	r.v = v
	r.j = v.Zero()
}

func (r *TypedMinJoinReducer) Config() Aggregatable {
	return r.v
}

func (r *TypedMinJoinReducer) Value() (Aggregatable, bool) {
	r.Reduce(r.j, r.t, false)
	return r.v, !r.t.IsZero()

}
func (r *TypedMinJoinReducer) Time() time.Time {
	return r.t
}

func (r *TypedMinJoinReducer) Type() ReducerFunc {
	return ReducerFuncMinJoin
}

// MEAN JOIN
// Welford's Online algorithm, see
// https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance
type TypedMeanJoinReducer struct {
	t    time.Time
	n    int
	mean float64
	v    Aggregatable
	j    Aggregatable
}

func (r *TypedMeanJoinReducer) Reduce(v Aggregatable, t time.Time, join bool) {
	if r.t.IsZero() {
		r.t = t
		r.j = v
		return
	}
	if join {
		r.j = r.j.Add(v)
	} else {
		r.n++
		r.j.Init(r.v)
		delta := r.j.Float64() - r.mean
		r.mean += delta / float64(r.n)
		r.j = v
	}
}

func (r *TypedMeanJoinReducer) Reset() {
	r.t = time.Time{}
	r.mean = 0
	r.n = 0
	r.v = r.v.Zero()
	r.j = r.j.Zero()
}

func (r *TypedMeanJoinReducer) Init(v Aggregatable) {
	r.v = v
	r.j = v.Zero()
}

func (r *TypedMeanJoinReducer) Config() Aggregatable {
	return r.v
}

func (r *TypedMeanJoinReducer) Value() (Aggregatable, bool) {
	r.Reduce(r.v.Zero(), r.t, false)
	v := r.v.Zero()
	v.SetFloat64(r.mean)
	return v, r.n > 0

}
func (r *TypedMeanJoinReducer) Time() time.Time {
	return r.t
}

func (r *TypedMeanJoinReducer) Type() ReducerFunc {
	return ReducerFuncMeanJoin
}

// VAR JOIN
type TypedVarJoinReducer struct {
	t    time.Time
	n    int
	mean float64 // mean aggregator
	m2   float64 // variance aggregator
	v    Aggregatable
	j    Aggregatable
}

func (r *TypedVarJoinReducer) Reduce(v Aggregatable, t time.Time, join bool) {
	if r.t.IsZero() {
		r.t = t
		r.j = v
		return
	}
	if join {
		r.j = r.j.Add(v)
	} else {
		r.n++
		r.j.Init(r.v)
		f64 := r.j.Float64()
		delta := f64 - r.mean
		r.mean += delta / float64(r.n)
		r.m2 += delta * (f64 - r.mean)
		r.j = v
	}
}

func (r *TypedVarJoinReducer) Reset() {
	r.t = time.Time{}
	r.n = 0
	r.mean = 0
	r.m2 = 0
	r.v = r.v.Zero()
	r.j = r.j.Zero()
}

func (r *TypedVarJoinReducer) Init(v Aggregatable) {
	r.v = v
	r.j = v.Zero()
}

func (r *TypedVarJoinReducer) Config() Aggregatable {
	return r.v
}

func (r *TypedVarJoinReducer) Value() (Aggregatable, bool) {
	r.Reduce(r.v.Zero(), r.t, false)
	v := r.v.Zero()
	if r.n < 2 {
		return v, r.n > 0
	}
	v.SetFloat64(r.m2 / float64(r.n-1))
	return v, r.n > 0
}

func (r *TypedVarJoinReducer) Time() time.Time {
	return r.t
}

func (r *TypedVarJoinReducer) Type() ReducerFunc {
	return ReducerFuncVarJoin
}

// STD JOIN
type TypedStdJoinReducer struct {
	t    time.Time
	n    int
	mean float64 // mean aggregator
	m2   float64 // variance aggregator
	v    Aggregatable
	j    Aggregatable
}

func (r *TypedStdJoinReducer) Reduce(v Aggregatable, t time.Time, join bool) {
	if r.t.IsZero() {
		r.t = t
		r.j = v
		return
	}
	if join {
		r.j = r.j.Add(v)
	} else {
		r.n++
		r.j.Init(r.v)
		f64 := r.j.Float64()
		delta := f64 - r.mean
		r.mean += delta / float64(r.n)
		r.m2 += delta * (f64 - r.mean)
		r.j = v
	}
}

func (r *TypedStdJoinReducer) Reset() {
	r.t = time.Time{}
	r.n = 0
	r.mean = 0
	r.m2 = 0
	r.v = r.v.Zero()
	r.j = r.j.Zero()
}

func (r *TypedStdJoinReducer) Init(v Aggregatable) {
	r.v = v
	r.j = v.Zero()
}

func (r *TypedStdJoinReducer) Config() Aggregatable {
	return r.v
}

func (r *TypedStdJoinReducer) Value() (Aggregatable, bool) {
	r.Reduce(r.v.Zero(), r.t, false)
	v := r.v.Zero()
	if r.n < 2 {
		return v, r.n > 0
	}
	v.SetFloat64(math.Sqrt(r.m2 / float64(r.n-1)))
	return v, r.n > 0
}

func (r *TypedStdJoinReducer) Time() time.Time {
	return r.t
}

func (r *TypedStdJoinReducer) Type() ReducerFunc {
	return ReducerFuncStdJoin
}
