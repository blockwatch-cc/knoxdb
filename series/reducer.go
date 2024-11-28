// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package series

import (
	"golang.org/x/exp/constraints"

	"fmt"
	"math"
	"strings"
	"time"
)

type Number interface {
	constraints.Integer | constraints.Float
}

type Signed = constraints.Signed
type Unsigned = constraints.Unsigned
type Float = constraints.Float

type ReducerFunc string

const (
	ReducerFuncInvalid ReducerFunc = ""
	ReducerFuncSum     ReducerFunc = "sum"
	ReducerFuncFirst   ReducerFunc = "first"
	ReducerFuncLast    ReducerFunc = "last"
	ReducerFuncMin     ReducerFunc = "min"
	ReducerFuncMax     ReducerFunc = "max"
	ReducerFuncMean    ReducerFunc = "mean"
	ReducerFuncVar     ReducerFunc = "var"
	ReducerFuncStd     ReducerFunc = "std"
	ReducerFuncCount   ReducerFunc = "count"

	// sum same timestamp items, then apply reducer
	ReducerFuncFirstJoin ReducerFunc = "first_join"
	ReducerFuncLastJoin  ReducerFunc = "last_join"
	ReducerFuncMinJoin   ReducerFunc = "min_join"
	ReducerFuncMaxJoin   ReducerFunc = "max_join"
	ReducerFuncMeanJoin  ReducerFunc = "mean_join"
	ReducerFuncVarJoin   ReducerFunc = "var_join"
	ReducerFuncStdJoin   ReducerFunc = "std_join"

	// ReducerFuncHist    ReducerFunc = "hist"
	// ReducerFuncMode = "mode"
	// ReducerFuncMedian = "median"
)

func ParseReducerFunc(s string) ReducerFunc {
	switch f := ReducerFunc(strings.ToLower(s)); f {
	case ReducerFuncSum, ReducerFuncFirst, ReducerFuncLast, ReducerFuncMin, ReducerFuncMax,
		ReducerFuncMean, ReducerFuncVar, ReducerFuncStd, ReducerFuncCount:
		return f

	case ReducerFuncFirstJoin, ReducerFuncLastJoin, ReducerFuncMinJoin, ReducerFuncMaxJoin,
		ReducerFuncMeanJoin, ReducerFuncVarJoin, ReducerFuncStdJoin:
		return f
	// case ReducerFuncHist:
	// 	return f
	default:
		return ReducerFuncInvalid
	}
}

func (f ReducerFunc) IsValid() bool {
	return f != ReducerFuncInvalid
}

func (f ReducerFunc) String() string {
	return string(f)
}

func (f ReducerFunc) MarshalText() ([]byte, error) {
	return []byte(f.String()), nil
}

func (f *ReducerFunc) UnmarshalText(data []byte) error {
	fn := ParseReducerFunc(string(data))
	if !fn.IsValid() {
		return fmt.Errorf("invalid reducer function '%s'", string(data))
	}
	*f = fn
	return nil
}

func NewReducer[T Number](fn ReducerFunc) Reducer[T] {
	switch fn {
	case ReducerFuncSum:
		return &SumReducer[T]{}
	case ReducerFuncFirst:
		return &FirstReducer[T]{}
	case ReducerFuncLast:
		return &LastReducer[T]{}
	case ReducerFuncMin:
		return &MinReducer[T]{}
	case ReducerFuncMax:
		return &MaxReducer[T]{}
	case ReducerFuncMean:
		return &MeanReducer[T]{}
	case ReducerFuncVar:
		return &VarReducer[T]{}
	case ReducerFuncStd:
		return &StdReducer[T]{}
	case ReducerFuncCount:
		return &CountReducer[T]{}
	case ReducerFuncFirstJoin:
		return &FirstJoinReducer[T]{}
	case ReducerFuncLastJoin:
		return &LastJoinReducer[T]{}
	case ReducerFuncMinJoin:
		return &MinJoinReducer[T]{}
	case ReducerFuncMaxJoin:
		return &MaxJoinReducer[T]{}
	case ReducerFuncMeanJoin:
		return &MeanJoinReducer[T]{}
	case ReducerFuncVarJoin:
		return &VarJoinReducer[T]{}
	case ReducerFuncStdJoin:
		return &StdJoinReducer[T]{}
	// case ReducerFuncHist:
	// 	return &HistReducer[T]{}
	default:
		return nil
	}
}

type Reducer[T Number] interface {
	Reduce(T, time.Time, bool)
	Reset()
	Value() (T, bool)
	Time() time.Time
	Type() ReducerFunc
}

// COUNT
type CountReducer[T Number] struct {
	t time.Time
	v T
}

func (r *CountReducer[T]) Reduce(_ T, t time.Time, _ bool) {
	if r.t.IsZero() {
		r.t = t
	}
	r.v++
}

func (r *CountReducer[T]) Reset() {
	r.t = time.Time{}
	r.v = 0
}

func (r *CountReducer[T]) Value() (T, bool) {
	return r.v, !r.t.IsZero()
}

func (r *CountReducer[T]) Time() time.Time {
	return r.t
}

func (r *CountReducer[T]) Type() ReducerFunc {
	return ReducerFuncCount
}

// SUM
type SumReducer[T Number] struct {
	t time.Time
	v T
}

func (r *SumReducer[T]) Reduce(v T, t time.Time, _ bool) {
	if r.t.IsZero() {
		r.t = t
	}
	r.v += v
}

func (r *SumReducer[T]) Reset() {
	r.t = time.Time{}
	r.v = 0
}

func (r *SumReducer[T]) Value() (T, bool) {
	return r.v, !r.t.IsZero()
}

func (r *SumReducer[T]) Time() time.Time {
	return r.t
}

func (r *SumReducer[T]) Type() ReducerFunc {
	return ReducerFuncSum
}

// FIRST
type FirstReducer[T Number] struct {
	t time.Time
	v T
}

func (r *FirstReducer[T]) Reduce(v T, t time.Time, _ bool) {
	if r.t.IsZero() {
		r.t = t
		r.v = v
	}
}

func (r *FirstReducer[T]) Reset() {
	r.t = time.Time{}
	r.v = 0
}

func (r *FirstReducer[T]) Value() (T, bool) {
	return r.v, !r.t.IsZero()

}
func (r *FirstReducer[T]) Time() time.Time {
	return r.t
}

func (r *FirstReducer[T]) Type() ReducerFunc {
	return ReducerFuncFirst
}

// LAST
type LastReducer[T Number] struct {
	t time.Time
	v T
}

func (r *LastReducer[T]) Reduce(v T, t time.Time, _ bool) {
	r.t = t
	r.v = v
}

func (r *LastReducer[T]) Reset() {
	r.t = time.Time{}
	r.v = 0
}

func (r *LastReducer[T]) Value() (T, bool) {
	return r.v, !r.t.IsZero()

}
func (r *LastReducer[T]) Time() time.Time {
	return r.t
}

func (r *LastReducer[T]) Type() ReducerFunc {
	return ReducerFuncLast
}

// MAX
type MaxReducer[T Number] struct {
	t time.Time
	v T
}

func (r *MaxReducer[T]) Reduce(v T, t time.Time, _ bool) {
	if r.t.IsZero() || r.v < v {
		r.t = t
		r.v = v
	}
}

func (r *MaxReducer[T]) Reset() {
	r.t = time.Time{}
	r.v = 0
}

func (r *MaxReducer[T]) Value() (T, bool) {
	return r.v, !r.t.IsZero()

}
func (r *MaxReducer[T]) Time() time.Time {
	return r.t
}

func (r *MaxReducer[T]) Type() ReducerFunc {
	return ReducerFuncMax
}

// MIN
type MinReducer[T Number] struct {
	t time.Time
	v T
}

func (r *MinReducer[T]) Reduce(v T, t time.Time, _ bool) {
	if r.t.IsZero() || r.v > v {
		r.t = t
		r.v = v
	}
}

func (r *MinReducer[T]) Reset() {
	r.t = time.Time{}
	r.v = 0
}

func (r *MinReducer[T]) Value() (T, bool) {
	return r.v, !r.t.IsZero()

}
func (r *MinReducer[T]) Time() time.Time {
	return r.t
}

func (r *MinReducer[T]) Type() ReducerFunc {
	return ReducerFuncMin
}

// MEAN
// Welford's Online algorithm, see
// https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance
type MeanReducer[T Number] struct {
	t    time.Time
	n    int
	mean float64
}

func (r *MeanReducer[T]) Reduce(v T, t time.Time, _ bool) {
	if r.n == 0 {
		r.t = t
	}
	r.n++
	delta := float64(v) - r.mean
	r.mean += delta / float64(r.n)
}

func (r *MeanReducer[T]) Reset() {
	r.t = time.Time{}
	r.mean = 0
	r.n = 0
}

func (r *MeanReducer[T]) Value() (T, bool) {
	return T(r.mean), r.n > 0

}
func (r *MeanReducer[T]) Time() time.Time {
	return r.t
}

func (r *MeanReducer[T]) Type() ReducerFunc {
	return ReducerFuncMean
}

// VAR
type VarReducer[T Number] struct {
	t    time.Time
	n    int
	mean float64 // mean aggregator
	m2   float64 // variance aggregator
}

func (r *VarReducer[T]) Reduce(v T, t time.Time, _ bool) {
	if r.n == 0 {
		r.t = t
	}
	r.n++
	delta := float64(v) - r.mean
	r.mean += delta / float64(r.n)
	r.m2 += delta * (float64(v) - r.mean)
}

func (r *VarReducer[T]) Reset() {
	r.t = time.Time{}
	r.n = 0
	r.mean = 0
	r.m2 = 0
}

func (r *VarReducer[T]) Value() (T, bool) {
	if r.n < 2 {
		return T(math.NaN()), r.n > 0
	}
	return T(r.m2 / float64(r.n-1)), r.n > 0
}

func (r *VarReducer[T]) Time() time.Time {
	return r.t
}

func (r *VarReducer[T]) Type() ReducerFunc {
	return ReducerFuncVar
}

// STD
type StdReducer[T Number] struct {
	t    time.Time
	n    int
	mean float64 // mean aggregator
	m2   float64 // variance aggregator
}

func (r *StdReducer[T]) Reduce(v T, t time.Time, _ bool) {
	if r.n == 0 {
		r.t = t
	}
	r.n++
	delta := float64(v) - r.mean
	r.mean += delta / float64(r.n)
	r.m2 += delta * (float64(v) - r.mean)
}

func (r *StdReducer[T]) Reset() {
	r.t = time.Time{}
	r.n = 0
	r.mean = 0
	r.m2 = 0
}

func (r *StdReducer[T]) Value() (T, bool) {
	if r.n < 2 {
		return T(math.NaN()), r.n > 0
	}
	return T(math.Sqrt(r.m2 / float64(r.n-1))), r.n > 0
}

func (r *StdReducer[T]) Time() time.Time {
	return r.t
}

func (r *StdReducer[T]) Type() ReducerFunc {
	return ReducerFuncStd
}

// FIRST JOIN
type FirstJoinReducer[T Number] struct {
	t    time.Time
	v    T
	done bool
}

func (r *FirstJoinReducer[T]) Reduce(v T, t time.Time, join bool) {
	switch {
	case r.t.IsZero():
		r.t = t
		r.v = v
	case join && !r.done:
		r.v += v
	default:
		r.done = true
	}
}

func (r *FirstJoinReducer[T]) Reset() {
	r.t = time.Time{}
	r.v = 0
	r.done = false
}

func (r *FirstJoinReducer[T]) Value() (T, bool) {
	return r.v, !r.t.IsZero()

}
func (r *FirstJoinReducer[T]) Time() time.Time {
	return r.t
}

func (r *FirstJoinReducer[T]) Type() ReducerFunc {
	return ReducerFuncFirstJoin
}

// LAST JOIN
type LastJoinReducer[T Number] struct {
	t time.Time
	v T
}

func (r *LastJoinReducer[T]) Reduce(v T, t time.Time, join bool) {
	r.t = t
	if join {
		r.v += v
	} else {
		r.v = v
	}
}

func (r *LastJoinReducer[T]) Reset() {
	r.t = time.Time{}
	r.v = 0
}

func (r *LastJoinReducer[T]) Value() (T, bool) {
	return r.v, !r.t.IsZero()

}
func (r *LastJoinReducer[T]) Time() time.Time {
	return r.t
}

func (r *LastJoinReducer[T]) Type() ReducerFunc {
	return ReducerFuncLastJoin
}

// MAX JOIN
type MaxJoinReducer[T Number] struct {
	t time.Time
	v T
	j T
}

func (r *MaxJoinReducer[T]) Reduce(v T, t time.Time, join bool) {
	if r.t.IsZero() {
		r.t = t
		r.j = v
		return
	}

	if join {
		r.j += v
	} else {
		if r.v < r.j {
			r.v = r.j
		}
		r.j = v
	}
}

func (r *MaxJoinReducer[T]) Reset() {
	r.t = time.Time{}
	r.v = 0
	r.j = 0
}

func (r *MaxJoinReducer[T]) Value() (T, bool) {
	r.Reduce(0, r.t, false)
	return r.v, !r.t.IsZero()

}
func (r *MaxJoinReducer[T]) Time() time.Time {
	return r.t
}

func (r *MaxJoinReducer[T]) Type() ReducerFunc {
	return ReducerFuncMaxJoin
}

// MIN JOIN
type MinJoinReducer[T Number] struct {
	t time.Time
	v T
	j T
}

func (r *MinJoinReducer[T]) Reduce(v T, t time.Time, join bool) {
	if r.t.IsZero() {
		r.t = t
		r.j = v
		return
	}

	if join {
		r.j += v
	} else {
		if r.j < r.v {
			r.v = r.j
		}
		r.j = v
	}
}

func (r *MinJoinReducer[T]) Reset() {
	r.t = time.Time{}
	r.v = 0
	r.j = 0
}

func (r *MinJoinReducer[T]) Value() (T, bool) {
	r.Reduce(r.j, r.t, false)
	return r.v, !r.t.IsZero()

}
func (r *MinJoinReducer[T]) Time() time.Time {
	return r.t
}

func (r *MinJoinReducer[T]) Type() ReducerFunc {
	return ReducerFuncMinJoin
}

// MEAN JOIN
// Welford's Online algorithm, see
// https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance
type MeanJoinReducer[T Number] struct {
	t    time.Time
	j    T
	n    int
	mean float64
}

func (r *MeanJoinReducer[T]) Reduce(v T, t time.Time, join bool) {
	if r.t.IsZero() {
		r.t = t
		r.j = v
		return
	}

	if join {
		r.j += v
	} else {
		r.n++
		delta := float64(r.j) - r.mean
		r.mean += delta / float64(r.n)
		r.j = v
	}
}

func (r *MeanJoinReducer[T]) Reset() {
	r.t = time.Time{}
	r.j = 0
	r.mean = 0
	r.n = 0
}

func (r *MeanJoinReducer[T]) Value() (T, bool) {
	r.Reduce(0, r.t, false)
	return T(r.mean), r.n > 0

}
func (r *MeanJoinReducer[T]) Time() time.Time {
	return r.t
}

func (r *MeanJoinReducer[T]) Type() ReducerFunc {
	return ReducerFuncMeanJoin
}

// VAR JOIN
type VarJoinReducer[T Number] struct {
	t    time.Time
	j    T
	n    int
	mean float64 // mean aggregator
	m2   float64 // variance aggregator
}

func (r *VarJoinReducer[T]) Reduce(v T, t time.Time, join bool) {
	if r.t.IsZero() {
		r.t = t
		r.j = v
		return
	}
	if join {
		r.j += v
	} else {
		r.n++
		delta := float64(r.j) - r.mean
		r.mean += delta / float64(r.n)
		r.m2 += delta * (float64(r.j) - r.mean)
		r.j = v
	}
}

func (r *VarJoinReducer[T]) Reset() {
	r.t = time.Time{}
	r.j = 0
	r.n = 0
	r.mean = 0
	r.m2 = 0
}

func (r *VarJoinReducer[T]) Value() (T, bool) {
	r.Reduce(0, r.t, false)
	if r.n < 2 {
		return T(math.NaN()), r.n > 0
	}
	return T(r.m2 / float64(r.n-1)), r.n > 0
}

func (r *VarJoinReducer[T]) Time() time.Time {
	return r.t
}

func (r *VarJoinReducer[T]) Type() ReducerFunc {
	return ReducerFuncVarJoin
}

// STD JOIN
type StdJoinReducer[T Number] struct {
	t    time.Time
	j    T
	n    int
	mean float64 // mean aggregator
	m2   float64 // variance aggregator
}

func (r *StdJoinReducer[T]) Reduce(v T, t time.Time, join bool) {
	if r.t.IsZero() {
		r.t = t
		r.j = v
		return
	}
	if join {
		r.j += v
	} else {
		r.n++
		delta := float64(r.j) - r.mean
		r.mean += delta / float64(r.n)
		r.m2 += delta * (float64(r.j) - r.mean)
		r.j = v
	}
}

func (r *StdJoinReducer[T]) Reset() {
	r.t = time.Time{}
	r.j = 0
	r.n = 0
	r.mean = 0
	r.m2 = 0
}

func (r *StdJoinReducer[T]) Value() (T, bool) {
	r.Reduce(0, r.t, false)
	if r.n < 2 {
		return T(math.NaN()), r.n > 0
	}
	return T(math.Sqrt(r.m2 / float64(r.n-1))), r.n > 0
}

func (r *StdJoinReducer[T]) Time() time.Time {
	return r.t
}

func (r *StdJoinReducer[T]) Type() ReducerFunc {
	return ReducerFuncStdJoin
}

// HIST
// type HistReducer[T Number] struct {
// 	t time.Time
// 	v Histogram[T]
// }

// func (r *HistReducer[T]) Reduce(v T, t time.Time) {
// 	if r.t.IsZero() {
// 		r.t = t
// 	}
// 	// TODO: need `v` and `fn(t)` as histogram inputs
// 	r.v.Add(v)
// }

// func (r *HistReducer[T]) Reset() {
// 	r.t = time.Time{}
// 	r.v.Reset()
// }

// func (r *HistReducer[T]) Value() (T, bool) {
// 	return r.v, !r.t.IsZero()
// }

// func (r *HistReducer[T]) Time() time.Time {
// 	return r.t
// }

// func (r *HistReducer[T]) Type() ReducerFunc {
// 	return ReducerFuncHist
// }
