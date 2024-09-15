// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package knox

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"blockwatch.cc/knoxdb/internal/engine"
	"blockwatch.cc/knoxdb/internal/query"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/schema"
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

// Generic KnoxDB query specialized for result type T
type Query[T any] struct {
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

func NewQuery[T any]() Query[T] {
	schema, err := schema.GenericSchema[T]()
	if err != nil {
		panic(err)
	}
	return Query[T]{
		schema: schema,
		table:  newErrorTable("query", fmt.Errorf("missing table, use WithTable()")),
		order:  OrderAsc,
		limit:  0,
		log:    log.New(nil).SetLevel(log.LevelInfo),
	}
}

func (q Query[T]) Stats() QueryStats {
	return q.stats
}

func (q Query[T]) WithTag(tag string) Query[T] {
	q.tag = tag
	return q
}

func (q Query[T]) WithLogger(l log.Logger) Query[T] {
	q.log = l
	return q
}

func (q Query[T]) WithTable(t Table) Query[T] {
	q.table = t
	return q
}

func (q Query[T]) WithCache(enable bool) Query[T] {
	if enable {
		q.flags &^= QueryFlagNoCache
	} else {
		q.flags |= QueryFlagNoCache
	}
	return q
}

func (q Query[T]) WithIndex(enable bool) Query[T] {
	if enable {
		q.flags &^= QueryFlagNoIndex
	} else {
		q.flags |= QueryFlagNoIndex
	}
	return q
}

func (q Query[T]) WithDebug(enable bool) Query[T] {
	if enable {
		q.flags |= QueryFlagDebug
	} else {
		q.flags &^= QueryFlagDebug
	}
	return q
}

func (q Query[T]) WithStats(enable bool) Query[T] {
	if enable {
		q.flags |= QueryFlagStats
	} else {
		q.flags &^= QueryFlagStats
	}
	return q
}

func (q Query[T]) WithOrder(o OrderType) Query[T] {
	q.order = o
	return q
}

func (q Query[T]) WithDesc() Query[T] {
	q.order = OrderDesc
	return q
}

func (q Query[T]) WithAsc() Query[T] {
	q.order = OrderAsc
	return q
}

func (q Query[T]) WithLimit(l int) Query[T] {
	q.limit = l
	return q
}

func (q Query[T]) AndCondition(conds ...Condition) Query[T] {
	if len(conds) == 0 {
		return q
	}
	q.cond.Add(query.And(conds...))
	return q
}

func (q Query[T]) OrCondition(conds ...Condition) Query[T] {
	if len(conds) == 0 {
		return q
	}
	q.cond.Add(query.Or(conds...))
	return q
}

func (q Query[T]) And(field string, mode FilterMode, value any) Query[T] {
	q.cond.And(field, mode, value)
	return q
}

func (q Query[T]) Or(field string, mode FilterMode, value any) Query[T] {
	q.cond.Or(field, mode, value)
	return q
}

func (q Query[T]) AndEqual(field string, value any) Query[T] {
	return q.And(field, FilterModeEqual, value)
}

func (q Query[T]) AndNotEqual(field string, value any) Query[T] {
	return q.And(field, FilterModeNotEqual, value)
}

func (q Query[T]) AndIn(field string, value any) Query[T] {
	return q.And(field, FilterModeIn, value)
}

func (q Query[T]) AndNotIn(field string, value any) Query[T] {
	return q.And(field, FilterModeNotIn, value)
}

func (q Query[T]) AndLt(field string, value any) Query[T] {
	return q.And(field, FilterModeLt, value)
}

func (q Query[T]) AndLte(field string, value any) Query[T] {
	return q.And(field, FilterModeLe, value)
}

func (q Query[T]) AndGt(field string, value any) Query[T] {
	return q.And(field, FilterModeGt, value)
}

func (q Query[T]) AndGte(field string, value any) Query[T] {
	return q.And(field, FilterModeGe, value)
}

func (q Query[T]) AndRegexp(field string, value any) Query[T] {
	return q.And(field, FilterModeRegexp, value)
}

func (q Query[T]) AndRange(field string, from, to any) Query[T] {
	q.cond.AndRange(field, from, to)
	return q
}

func (q Query[T]) Execute(ctx context.Context, val any) (err error) {
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

func (q Query[T]) Stream(ctx context.Context, fn func(*T) error) error {
	err := q.table.Stream(ctx, q, func(r QueryRow) error {
		var t T
		if err := r.Decode(&t); err != nil {
			return err
		}
		return fn(&t)
	})
	if err != nil {
		return fmt.Errorf("query %s: %v", q.tag, err)
	}

	return nil
}

func (q Query[T]) Delete(ctx context.Context) (uint64, error) {
	n, err := q.table.Delete(ctx, q)
	if err != nil {
		return 0, fmt.Errorf("query %s: %v", q.tag, err)
	}
	return n, nil
}

func (q Query[T]) Count(ctx context.Context) (uint64, error) {
	n, err := q.table.Count(ctx, q)
	if err != nil {
		return 0, fmt.Errorf("query %s: %v", q.tag, err)
	}
	return n, nil
}

func (q Query[T]) Run(ctx context.Context) ([]T, error) {
	res, err := q.table.Query(ctx, q)
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

func (q Query[T]) Encode() ([]byte, error) {
	// // table must exist
	// if q.table == nil {
	// 	return nil, engine.ErrNoTable
	// }

	// // validate T against table schema
	// if tableSchema := q.table.Schema(); tableSchema != nil {
	// 	if err := tableSchema.CanSelect(q.schema); err != nil {
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

func (q Query[T]) MakePlan() (engine.QueryPlan, error) {
	plan := query.NewQueryPlan().
		WithTag(q.tag).
		WithLimit(uint32(q.limit)).
		WithOrder(q.order).
		WithFlags(q.flags).
		WithTable(q.table.Engine()).
		WithLogger(q.log)

	// compile filters from conditions
	filters, err := q.cond.Compile(q.table.Schema())
	if err != nil {
		return nil, err
	}
	plan.Filters = filters

	// build request (filter fields) schema
	rs, err := q.table.Schema().SelectNames("", true, q.cond.Fields()...)
	if err != nil {
		return nil, err
	}
	plan.RequestSchema = rs

	// build result (output) schema from full struct T
	plan.ResultSchema = q.schema

	return plan, nil
}
