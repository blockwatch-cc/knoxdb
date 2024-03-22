// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package series

import (
	"fmt"
	"strings"

	"blockwatch.cc/knoxdb/pack"
	"blockwatch.cc/knoxdb/util"
)

type TypeMap map[string]Aggregatable

type LimitableRequest interface {
	ApplyLimits(int, int)
}

type Request struct {
	Select   ExprList  `form:"select"`
	Range    TimeRange `form:"range,default=M"`
	Interval TimeUnit  `form:"interval,default=d"`
	Fill     FillMode  `form:"fill,default=none"`
	Limit    int       `form:"limit,default=100"`
	GroupBy  string    `form:"group_by"`
	Table    string    `form:"table"`
	TypeMap  TypeMap
}

func (r *Request) ApplyLimits(def, max int) {
	if r.Limit == 0 {
		r.Limit = def
	}
	r.Limit = min(r.Limit, max)
}

func (r *Request) Sanitize() error {
	// add time column
	r.Select.AddUniqueFront("time", ReducerFuncFirst)

	// truncate time ranges to multiples of interval
	r.Range.From = r.Interval.Truncate(r.Range.From)

	// round up time range end so that `to` arg becomes inclusive
	r.Range.To = r.Interval.Next(r.Range.To, 1)

	// adjust limit to range
	if num := r.Range.NumSteps(r.Interval); num < r.Limit {
		r.Limit = num
	}

	return nil
}

func (r Request) MakeBucket(expr Expr, tinfo pack.FieldList) (Bucket, error) {
	// handle special count(*) expression
	if expr.Field == "count" || (expr.Reduce == ReducerFuncCount && expr.Field == "*") {
		return NewCountBucket().
			WithDimensions(r.Range, r.Interval).
			WithLimit(r.Limit).
			WithFill(r.Fill), nil
	}
	f := tinfo.Find(expr.Field)
	if f.Index < 0 {
		return nil, fmt.Errorf("unknown column %q", expr.Field)
	}
	b := NewBucket(f.Type)
	if b == nil {
		return nil, fmt.Errorf("unsupported column type %q", f.Type)
	}
	if v, ok := r.TypeMap[expr.Field]; ok {
		b = b.WithTypeOf(v)
	}
	return b.WithName(expr.Field).
		WithIndex(f.Index).
		WithReducer(expr.Reduce).
		WithDimensions(r.Range, r.Interval).
		WithLimit(r.Limit).
		WithFill(r.Fill), nil
}

type Expr struct {
	Field  string
	Reduce ReducerFunc
}

type ExprList []Expr

func (l ExprList) Cols() (cols util.StringList) {
	for _, v := range l {
		cols = append(cols, v.Field)
	}
	return
}

func (l ExprList) QueryFields() (cols util.StringList) {
	for _, v := range l {
		if v.Field == "count" || (v.Reduce == ReducerFuncCount && v.Field == "*") {
			continue
		}
		cols = append(cols, v.Field)
	}
	return
}

func (l *ExprList) AddUniqueFront(name string, fn ReducerFunc) {
	for _, v := range *l {
		if v.Field == name {
			return
		}
	}
	*l = append(*l, Expr{})
	copy((*l)[1:], *l)
	(*l)[0] = Expr{name, fn}
}

func (l *ExprList) UnmarshalText(src []byte) error {
	s := string(src)
	for _, v := range strings.Split(s, ",") {
		reducer := ReducerFuncSum
		name := v
		if fn, n, ok := strings.Cut(name, "("); ok {
			if !strings.HasSuffix(n, ")") {
				return fmt.Errorf("missing closing bracket")
			}
			if parsed := ParseReducerFunc(fn); !parsed.IsValid() {
				return fmt.Errorf("unknown reducer %q", fn)
			} else {
				reducer = parsed
				name = strings.TrimSuffix(n, ")")
			}
		}
		*l = append(*l, Expr{name, reducer})
	}
	return nil
}

func (s ExprList) Get(i int) (e Expr) {
	l := len(s)
	if l == 0 {
		return
	}
	if i < 0 {
		i = l + i%l - 1
	} else if l <= i {
		i = l - 1
	}
	e = s[i]
	return
}
