// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package knox

import (
	"bytes"
	"context"
	"fmt"
	"reflect"
	"time"

	"blockwatch.cc/knoxdb/internal/engine"
	"blockwatch.cc/knoxdb/internal/query"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/schema"
	"blockwatch.cc/knoxdb/pkg/util"
	"github.com/echa/log"
)

type (
	Condition  = query.Condition
	OrderType  = types.OrderType
	FilterMode = types.FilterMode
	QueryFlags = query.QueryFlags
	RangeValue = query.RangeValue
)

// condition builder functions
var (
	And      = query.And      // func (conds ...Condition) Condition
	Or       = query.Or       // func (conds ...Condition) Condition
	Equal    = query.Equal    // func (col string, val any) Condition
	NotEqual = query.NotEqual // func (col string, val any) Condition
	In       = query.In       // func (col string, val any) Condition
	NotIn    = query.NotIn    // func (col string, val any) Condition
	Lt       = query.Lt       // func (col string, val any) Condition
	Le       = query.Le       // func (col string, val any) Condition
	Gt       = query.Gt       // func (col string, val any) Condition
	Ge       = query.Ge       // func (col string, val any) Condition
	Regexp   = query.Regexp   // func (col string, val any) Condition
	Range    = query.Range    // func (col string, from, to any) Condition
)

const (
	FilterModeInvalid  = types.FilterModeInvalid
	FilterModeEqual    = types.FilterModeEqual
	FilterModeNotEqual = types.FilterModeNotEqual
	FilterModeGt       = types.FilterModeGt
	FilterModeGe       = types.FilterModeGe
	FilterModeLt       = types.FilterModeLt
	FilterModeLe       = types.FilterModeLe
	FilterModeIn       = types.FilterModeIn
	FilterModeNotIn    = types.FilterModeNotIn
	FilterModeRange    = types.FilterModeRange
	FilterModeRegexp   = types.FilterModeRegexp
)

const (
	QueryFlagNoCache = query.QueryFlagNoCache
	QueryFlagNoIndex = query.QueryFlagNoIndex
	QueryFlagDebug   = query.QueryFlagDebug
	QueryFlagStats   = query.QueryFlagStats
)

const (
	OrderAsc                 = types.OrderAsc
	OrderDesc                = types.OrderDesc
	OrderAscCaseInsensitive  = types.OrderAscCaseInsensitive
	OrderDescCaseInsensitive = types.OrderDescCaseInsensitive
)

// as seen from the sdk
type QueryStats struct {
	Name          string
	ExecutionTime time.Duration
	DecodeTime    time.Duration
	TotalTime     time.Duration
	RowsDecoded   int
}

func (s QueryStats) String() string {
	return fmt.Sprintf("query: %s execute=%s decode=%s total=%s returned=%d rows",
		s.Name,
		s.ExecutionTime,
		s.DecodeTime,
		s.TotalTime,
		s.RowsDecoded,
	)
}

// implements QueryResult
type BytesBufferCloser struct {
	*bytes.Buffer
}

func (b *BytesBufferCloser) Close() error { return nil }

type Query struct {
	schema *schema.Schema // SELECT
	table  Table          // FROM
	cond   Condition      // WHERE
	limit  int            // LIMIT
	tag    string
	order  OrderType
	flags  QueryFlags
	log    log.Logger
	stats  QueryStats
}

func NewQuery() Query {
	return Query{
		table: newErrorTable("query", ErrNoTable),
		order: OrderAsc,
		limit: 0,
		log:   log.New(nil).SetLevel(log.LevelInfo),
	}
}

func (q Query) Stats() QueryStats {
	return q.stats
}

func (q Query) WithSchema(s *schema.Schema) Query {
	q.schema = s
	return q
}

func (q Query) WithTag(tag string) Query {
	q.tag = tag
	return q
}

func (q Query) WithLogger(l log.Logger) Query {
	q.log = l
	return q
}

func (q Query) WithTable(t Table) Query {
	q.table = t
	return q
}

func (q Query) WithCache(enable bool) Query {
	if enable {
		q.flags &^= QueryFlagNoCache
	} else {
		q.flags |= QueryFlagNoCache
	}
	return q
}

func (q Query) WithIndex(enable bool) Query {
	if enable {
		q.flags &^= QueryFlagNoIndex
	} else {
		q.flags |= QueryFlagNoIndex
	}
	return q
}

func (q Query) WithDebug(enable bool) Query {
	if enable {
		q.flags |= QueryFlagDebug
		if q.tag == "" {
			q.tag = util.RandString(8)
		}
	} else {
		q.flags &^= QueryFlagDebug
	}
	return q
}

func (q Query) WithStats(enable bool) Query {
	if enable {
		q.flags |= QueryFlagStats
		if q.tag == "" {
			q.tag = util.RandString(8)
		}
	} else {
		q.flags &^= QueryFlagStats
	}
	return q
}

func (q Query) WithOrder(o OrderType) Query {
	q.order = o
	return q
}

func (q Query) WithDesc() Query {
	q.order = OrderDesc
	return q
}

func (q Query) WithAsc() Query {
	q.order = OrderAsc
	return q
}

func (q Query) WithLimit(l int) Query {
	q.limit = l
	return q
}

func (q Query) AndCondition(conds ...Condition) Query {
	if len(conds) == 0 {
		return q
	}
	q.cond.Add(query.And(conds...))
	return q
}

func (q Query) OrCondition(conds ...Condition) Query {
	if len(conds) == 0 {
		return q
	}
	q.cond.Add(query.Or(conds...))
	return q
}

func (q Query) And(field string, mode FilterMode, value any) Query {
	q.cond.And(field, mode, value)
	return q
}

func (q Query) Or(field string, mode FilterMode, value any) Query {
	q.cond.Or(field, mode, value)
	return q
}

func (q Query) AndEqual(field string, value any) Query {
	return q.And(field, FilterModeEqual, value)
}

func (q Query) AndNotEqual(field string, value any) Query {
	return q.And(field, FilterModeNotEqual, value)
}

func (q Query) AndIn(field string, value any) Query {
	return q.And(field, FilterModeIn, value)
}

func (q Query) AndNotIn(field string, value any) Query {
	return q.And(field, FilterModeNotIn, value)
}

func (q Query) AndLt(field string, value any) Query {
	return q.And(field, FilterModeLt, value)
}

func (q Query) AndLte(field string, value any) Query {
	return q.And(field, FilterModeLe, value)
}

func (q Query) AndGt(field string, value any) Query {
	return q.And(field, FilterModeGt, value)
}

func (q Query) AndGte(field string, value any) Query {
	return q.And(field, FilterModeGe, value)
}

func (q Query) AndRegexp(field string, value any) Query {
	return q.And(field, FilterModeRegexp, value)
}

func (q Query) AndRange(field string, from, to any) Query {
	return q.And(field, FilterModeRange, RangeValue{from, to})
}

func (q Query) Execute(ctx context.Context, val any) (err error) {
	// analyze result schema
	var s *schema.Schema
	s, err = schema.SchemaOf(val)
	if err != nil {
		return
	}

	// use schema from data if not set
	if q.schema == nil {
		q = q.WithSchema(s)
	}

	rval := reflect.ValueOf(val)
	if rval.Kind() != reflect.Ptr {
		return ErrNoPointer
	}
	rval = reflect.Indirect(rval)

	switch rval.Kind() {
	case reflect.Slice:
		// get slice element type
		elem := rval.Type().Elem()

		// take limit from slice or user defined value
		if q.limit == 0 {
			q.limit = rval.Len()
		}

		// reuse existing slice elements
		if rval.Len() > 0 {
			n := -1
			err = q.table.Stream(ctx, q, func(r QueryRow) error {
				n++
				return r.Decode(rval.Index(n).Interface())
			})
		} else {
			// allocate new slice elements
			err = q.table.Stream(ctx, q, func(r QueryRow) error {
				// create new slice element (may be a pointer to struct)
				e := reflect.New(elem)
				ev := e

				// if element is ptr to struct, allocate the underlying struct
				if e.Elem().Kind() == reflect.Ptr {
					ev.Elem().Set(reflect.New(e.Elem().Type().Elem()))
					ev = reflect.Indirect(e)
				}

				// decode the struct element (re-use our interface-based methods)
				if err := r.Decode(ev.Interface()); err != nil {
					return err
				}

				// append slice element
				rval.Set(reflect.Append(rval, e.Elem()))
				return nil
			})
		}

	case reflect.Struct:
		err = q.table.Stream(ctx, q.WithLimit(1), func(r QueryRow) error {
			return r.Decode(val)
		})
	default:
		err = fmt.Errorf("query %s: %T: %w", q.tag, val, schema.ErrInvalidResultType)
	}
	return
}

func (q Query) Stream(ctx context.Context, fn func(QueryRow) error) error {
	return q.table.Stream(ctx, q, fn)
}

func (q Query) Delete(ctx context.Context) (uint64, error) {
	n, err := q.table.Delete(ctx, q)
	if err != nil {
		return 0, fmt.Errorf("query %s: %v", q.tag, err)
	}
	return n, nil
}

func (q Query) Count(ctx context.Context) (uint64, error) {
	n, err := q.table.Count(ctx, q)
	if err != nil {
		return 0, fmt.Errorf("query %s: %v", q.tag, err)
	}
	return n, nil
}

func (q Query) Run(ctx context.Context) (QueryResult, error) {
	return q.table.Query(ctx, q)
}

func (q Query) Encode() ([]byte, error) {
	// // table must exist
	// if q.Query.table == nil {
	// 	return nil, engine.ErrNoTable
	// }

	// // validate T against table schema
	// if tableSchema := q.Query.table.Schema(); tableSchema != nil {
	// 	if err := tableSchema.CanSelect(q.Query.schema); err != nil {
	// 		return nil, err
	// 	}
	// }

	// // encode query to wire format
	// cmd := wire.QueryCommand{
	// 	Fields: q.schema.FieldIDs(),
	// 	Cond:   q.cond,
	// 	Limit:  uint32(q.limit),
	// 	Order:  q.order,
	// 	Flags:  q.flags,
	// 	Tag:    q.tag,
	// }

	// // write header and return full command buffer
	// return cmd.Encode(q.table.Schema())
	return nil, ErrNotImplemented
}

func (q Query) MakePlan() (engine.QueryPlan, error) {
	plan := query.NewQueryPlan().
		WithTag(q.tag).
		WithLimit(uint32(q.limit)).
		WithOrder(q.order).
		WithFlags(q.flags).
		WithTable(q.table.Engine()).
		WithLogger(q.log)

	// compile filters from conditions
	filters, err := q.cond.Compile(q.table.Schema(), q.table.DB().Enums())
	if err != nil {
		return nil, err
	}
	plan.Filters = filters

	// build request (filter fields) schema
	ts := q.table.Schema()
	rs, err := ts.SelectFields(q.cond.Fields()...)
	if err != nil {
		return nil, err
	}
	plan.RequestSchema = rs.Sort()

	// validate output schema
	if q.schema == nil {
		q.schema = ts
	} else {
		if err := ts.CanSelect(q.schema); err != nil {
			return nil, err
		}
	}
	plan.ResultSchema = q.schema

	return plan, nil
}

// Generic KnoxDB query specialized for result type T
type GenericQuery[T any] struct {
	Query
}

func NewGenericQuery[T any]() GenericQuery[T] {
	schema, err := schema.GenericSchema[T]()
	if err != nil {
		panic(err)
	}
	return GenericQuery[T]{
		NewQuery().WithSchema(schema),
	}
}

func (q GenericQuery[T]) WithTag(tag string) GenericQuery[T] {
	q.Query = q.Query.WithTag(tag)
	return q
}

func (q GenericQuery[T]) WithLogger(l log.Logger) GenericQuery[T] {
	q.Query = q.Query.WithLogger(l)
	return q
}

func (q GenericQuery[T]) WithTable(t Table) GenericQuery[T] {
	q.Query = q.Query.WithTable(t)
	return q
}

func (q GenericQuery[T]) WithCache(enable bool) GenericQuery[T] {
	q.Query = q.Query.WithCache(enable)
	return q
}

func (q GenericQuery[T]) WithIndex(enable bool) GenericQuery[T] {
	q.Query = q.Query.WithIndex(enable)
	return q
}

func (q GenericQuery[T]) WithDebug(enable bool) GenericQuery[T] {
	q.Query = q.Query.WithDebug(enable)
	return q
}

func (q GenericQuery[T]) WithStats(enable bool) GenericQuery[T] {
	q.Query = q.Query.WithStats(enable)
	return q
}

func (q GenericQuery[T]) WithOrder(o OrderType) GenericQuery[T] {
	q.Query = q.Query.WithOrder(o)
	return q
}

func (q GenericQuery[T]) WithDesc() GenericQuery[T] {
	q.Query = q.Query.WithDesc()
	return q
}

func (q GenericQuery[T]) WithAsc() GenericQuery[T] {
	q.Query = q.Query.WithAsc()
	return q
}

func (q GenericQuery[T]) WithLimit(l int) GenericQuery[T] {
	q.Query = q.Query.WithLimit(l)
	return q
}

func (q GenericQuery[T]) AndCondition(conds ...Condition) GenericQuery[T] {
	q.Query = q.Query.AndCondition(conds...)
	return q
}

func (q GenericQuery[T]) OrCondition(conds ...Condition) GenericQuery[T] {
	q.Query = q.Query.OrCondition(conds...)
	return q
}

func (q GenericQuery[T]) And(field string, mode FilterMode, value any) GenericQuery[T] {
	q.Query = q.Query.And(field, mode, value)
	return q
}

func (q GenericQuery[T]) Or(field string, mode FilterMode, value any) GenericQuery[T] {
	q.Query = q.Query.Or(field, mode, value)
	return q
}

func (q GenericQuery[T]) AndEqual(field string, value any) GenericQuery[T] {
	return q.And(field, FilterModeEqual, value)
}

func (q GenericQuery[T]) AndNotEqual(field string, value any) GenericQuery[T] {
	return q.And(field, FilterModeNotEqual, value)
}

func (q GenericQuery[T]) AndIn(field string, value any) GenericQuery[T] {
	return q.And(field, FilterModeIn, value)
}

func (q GenericQuery[T]) AndNotIn(field string, value any) GenericQuery[T] {
	return q.And(field, FilterModeNotIn, value)
}

func (q GenericQuery[T]) AndLt(field string, value any) GenericQuery[T] {
	return q.And(field, FilterModeLt, value)
}

func (q GenericQuery[T]) AndLte(field string, value any) GenericQuery[T] {
	return q.And(field, FilterModeLe, value)
}

func (q GenericQuery[T]) AndGt(field string, value any) GenericQuery[T] {
	return q.And(field, FilterModeGt, value)
}

func (q GenericQuery[T]) AndGte(field string, value any) GenericQuery[T] {
	return q.And(field, FilterModeGe, value)
}

func (q GenericQuery[T]) AndRegexp(field string, value any) GenericQuery[T] {
	return q.And(field, FilterModeRegexp, value)
}

func (q GenericQuery[T]) AndRange(field string, from, to any) GenericQuery[T] {
	q.Query = q.Query.AndRange(field, from, to)
	return q
}

func (q GenericQuery[T]) Execute(ctx context.Context, val any) (err error) {
	// validate val is any of *T, []T or []*T
	switch res := val.(type) {
	case *T:
		err = q.WithLimit(1).Stream(ctx, func(v *T) error {
			*res = *v
			return nil
		})
	case *[]T:
		if len(*res) > 0 {
			q = q.WithLimit(len(*res))
		} else if res == nil {
			*res = make([]T, 0, q.limit)
		}
		err = q.Stream(ctx, func(v *T) error {
			*res = append(*res, *v)
			return nil
		})
	case *[]*T:
		if len(*res) > 0 {
			q = q.WithLimit(len(*res))
		} else if *res == nil {
			*res = make([]*T, 0, q.limit)
		}
		err = q.Stream(ctx, func(v *T) error {
			*res = append(*res, v)
			return nil
		})
	default:
		err = fmt.Errorf("query %s: %T: %w", q.tag, val, schema.ErrInvalidResultType)
	}

	return
}

func (q GenericQuery[T]) Stream(ctx context.Context, fn func(*T) error) error {
	return q.Query.Stream(ctx, func(r QueryRow) error {
		var t T
		if err := r.Decode(&t); err != nil {
			return err
		}
		return fn(&t)
	})
}

func (q GenericQuery[T]) Delete(ctx context.Context) (uint64, error) {
	return q.Query.Delete(ctx)
}

func (q GenericQuery[T]) Count(ctx context.Context) (uint64, error) {
	return q.Query.Count(ctx)
}

func (q GenericQuery[T]) Run(ctx context.Context) ([]T, error) {
	res, err := q.Query.Run(ctx)
	if err != nil {
		return nil, fmt.Errorf("query %s: %v", q.tag, err)
	}
	defer res.Close()

	vals := make([]T, res.Len())
	i := -1
	err = res.ForEach(func(r QueryRow) error {
		i++
		return r.Decode(&vals[i])
	})
	if err != nil {
		return nil, fmt.Errorf("query %s: %v", q.tag, err)
	}

	return vals, nil
}
