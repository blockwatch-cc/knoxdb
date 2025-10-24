// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package series

import (
	"fmt"
	"strings"
	"time"

	"blockwatch.cc/knoxdb/internal/engine"
	"blockwatch.cc/knoxdb/internal/reducer"
	"blockwatch.cc/knoxdb/pkg/schema"
	"blockwatch.cc/knoxdb/pkg/util"
	"github.com/echa/log"
)

type TypeMap map[string]reducer.Aggregatable

type LimitableRequest interface {
	ApplyLimits(int, int)
}

type Request struct {
	Select   ExprList         `form:"select"`
	Range    util.TimeRange   `form:"range,default=M"`
	Interval util.TimeUnit    `form:"interval,default=d"`
	Fill     reducer.FillMode `form:"fill,default=none"`
	Limit    int              `form:"limit,default=100"`
	GroupBy  string           `form:"group_by"`
	Table    string           `form:"table"`
	TypeMap  TypeMap
	table    engine.TableEngine
	log      log.Logger
}

func NewRequest() *Request {
	now := time.Now().UTC()
	unit := util.TimeUnit{Value: 1, Unit: 'M'}
	return &Request{
		Select: make(ExprList, 0),
		Range: util.TimeRange{
			From: unit.Sub(now).UTC(),
			To:   now,
		},
		Interval: unit,
		Fill:     reducer.FillModeNone,
		Limit:    100,
		TypeMap:  make(TypeMap),
		log:      log.Disabled,
	}
}

func (r *Request) WithLogger(l log.Logger) *Request {
	r.log = l
	return r
}

func (r *Request) WithTable(t engine.TableEngine) *Request {
	r.table = t
	return r
}

func (r *Request) WithExpr(field string, fn reducer.ReducerFunc) *Request {
	r.Select.AddUnique(field, fn)
	return r
}

func (r *Request) WithRange(rng util.TimeRange) *Request {
	r.Range = rng
	return r
}

func (r *Request) WithInterval(u util.TimeUnit) *Request {
	r.Interval = u
	return r
}

func (r *Request) WithFill(m reducer.FillMode) *Request {
	r.Fill = m
	return r
}

func (r *Request) WithLimit(l int) *Request {
	r.Limit = l
	return r
}

func (r *Request) WithGroupBy(g string) *Request {
	r.GroupBy = g
	return r
}

func (r *Request) WithType(name string, agg reducer.Aggregatable) *Request {
	r.TypeMap[name] = agg
	return r
}

func (r *Request) ApplyLimits(def, max int) *Request {
	if r.Limit == 0 {
		r.Limit = def
	}
	r.Limit = min(r.Limit, max)
	return r
}

func (r *Request) Sanitize() *Request {
	// add time column
	r.Select.AddUniqueFront("time", reducer.ReducerFuncFirst)

	// truncate time ranges to multiples of interval
	r.Range.From = r.Interval.Truncate(r.Range.From)

	// round up time range end so that `to` arg becomes inclusive
	r.Range.To = r.Interval.Next(r.Range.To, 1)

	// adjust limit to range
	if num := r.Range.NumSteps(r.Interval); num < r.Limit {
		r.Limit = num
	}

	return r
}

func (r *Request) MakeBucket(expr Expr, s *schema.Schema) (reducer.Bucket, error) {
	// handle special count(*) expression
	if expr.Field == "count" || (expr.Reduce == reducer.ReducerFuncCount && expr.Field == "*") {
		return reducer.NewCountBucket().
			WithDimensions(r.Range, r.Interval).
			WithLimit(r.Limit).
			WithFill(r.Fill), nil
	}
	f, ok := s.FieldByName(expr.Field)
	if !ok {
		return nil, fmt.Errorf("unknown column %q", expr.Field)
	}
	b := reducer.NewBucket(f.Type)
	if b == nil {
		return nil, fmt.Errorf("unsupported column type %q", f.Type)
	}
	if v, ok := r.TypeMap[expr.Field]; ok {
		b = b.WithTypeOf(v)
	}
	return b.WithName(expr.Field).
		WithIndex(int(f.Id)).
		WithReducer(expr.Reduce).
		WithDimensions(r.Range, r.Interval).
		WithLimit(r.Limit).
		WithFill(r.Fill), nil
}

type Expr struct {
	Field  string
	Reduce reducer.ReducerFunc
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
		if v.Field == "count" || (v.Reduce == reducer.ReducerFuncCount && v.Field == "*") {
			continue
		}
		cols = append(cols, v.Field)
	}
	return
}

func (l *ExprList) AddUnique(name string, fn reducer.ReducerFunc) {
	for _, v := range *l {
		if v.Field == name {
			return
		}
	}
	*l = append(*l, Expr{name, fn})
}

func (l *ExprList) AddUniqueFront(name string, fn reducer.ReducerFunc) {
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
		r := reducer.ReducerFuncSum
		name := v
		if fn, n, ok := strings.Cut(name, "("); ok {
			if !strings.HasSuffix(n, ")") {
				return fmt.Errorf("missing closing bracket")
			}
			if parsed := reducer.ParseReducerFunc(fn); !parsed.IsValid() {
				return fmt.Errorf("unknown reducer %q", fn)
			} else {
				r = parsed
				name = strings.TrimSuffix(n, ")")
			}
		}
		*l = append(*l, Expr{name, r})
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
