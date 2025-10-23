// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package knox

import (
	"context"
	"fmt"
	"reflect"

	"blockwatch.cc/knoxdb/internal/operator/join"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/schema"
	"github.com/echa/log"
)

type (
	JoinType = types.JoinType
)

const (
	// Note: this is WIP and more joins are coming
	InnerJoin = types.InnerJoin
	leftJoin  = types.LeftJoin
	CrossJoin = types.CrossJoin
)

type Join struct {
	tag   string
	typ   JoinType
	mode  FilterMode
	left  JoinTable
	right JoinTable
	limit uint32
	log   log.Logger
	flags QueryFlags
	// stats QueryStats
}

type JoinTable struct {
	Table  Table
	Where  Condition
	On     string
	Select []string // use all fields when empty
	As     []string
	Limit  uint32
}

func NewJoin() Join {
	return Join{
		left: JoinTable{
			Table: newErrorTable("join", ErrNoTable),
		},
		right: JoinTable{
			Table: newErrorTable("join", ErrNoTable),
		},
		log: log.New(nil).SetLevel(log.LevelInfo),
	}
}

func (j Join) WithTag(tag string) Join {
	j.tag = tag
	return j
}

func (j Join) WithFlags(f QueryFlags) Join {
	j.flags = f
	return j
}

func (j Join) WithLimit(n uint32) Join {
	j.limit = n
	return j
}

func (j Join) WithLimits(l, r uint32) Join {
	j.left.Limit = l
	j.right.Limit = r
	return j
}

func (j Join) WithLogger(l log.Logger) Join {
	j.log = l.Clone()
	return j
}

func (j Join) WithType(typ JoinType) Join {
	j.typ = typ
	return j
}

func (j Join) WithTables(l, r Table) Join {
	j.left.Table = l
	j.right.Table = r
	return j
}

func (j Join) WithConditions(l, r Condition) Join {
	j.left.Where = l
	j.right.Where = r
	return j
}

func (j Join) WithSelects(l, r []string) Join {
	j.left.Select = l
	j.right.Select = r
	return j
}

func (j Join) WithAliases(l, r []string) Join {
	j.left.As = l
	j.right.As = r
	return j
}

func (j Join) WithOn(f1, f2 string, mode FilterMode) Join {
	j.left.On = f1
	j.right.On = f2
	j.mode = mode
	return j
}

func (j Join) WithOnEqual(f1, f2 string) Join {
	j.left.On = f1
	j.right.On = f2
	j.mode = FilterModeEqual
	return j
}

func (j Join) WithDebug(enable bool) Join {
	if enable {
		j.flags |= QueryFlagDebug
	} else {
		j.flags &^= QueryFlagDebug
	}
	return j
}

func (j Join) WithStats(enable bool) Join {
	if enable {
		j.flags |= QueryFlagStats
	} else {
		j.flags &^= QueryFlagStats
	}
	return j
}

func (j Join) Execute(ctx context.Context, val any) error {
	rval := reflect.ValueOf(val)
	if rval.Kind() != reflect.Ptr {
		return ErrNoPointer
	}
	rval = reflect.Indirect(rval)

	// build join plan
	plan, err := j.MakePlan()
	if err != nil {
		return err
	}

	// use or open tx
	ctx, _, abort, err := j.left.Table.DB().Begin(ctx)
	if err != nil {
		return err
	}
	defer abort()

	// may query indexes
	err = plan.Compile(ctx)
	if err != nil {
		return err
	}

	// analyze result schema
	var s *schema.Schema
	s, err = schema.SchemaOf(val)
	if err != nil {
		return err
	}

	// check compatibility with join plan result
	err = plan.Schema().CanSelect(s)
	if err != nil {
		return err
	}

	switch rval.Kind() {
	case reflect.Slice:
		// get slice element type
		elem := rval.Type().Elem()

		// take limit from slice or user defined value
		if plan.Limit == 0 {
			// reuse existing slice elements
			plan.Limit = uint32(rval.Len())

			n := -1
			err = plan.Stream(ctx, func(r QueryRow) error {
				n++
				return r.Decode(rval.Index(n).Interface())
			})

		} else {
			// allocate new slice elements
			err = plan.Stream(ctx, func(r QueryRow) error {
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
		err = plan.WithLimit(1).Stream(ctx, func(r QueryRow) error {
			return r.Decode(val)
		})
	default:
		err = fmt.Errorf("join %s: %T: %w", j.tag, val, schema.ErrInvalidResultType)
	}
	return err
}

func (j Join) Stream(ctx context.Context, fn func(QueryRow) error) error {
	plan, err := j.MakePlan()
	if err != nil {
		return err
	}

	// use or open tx
	ctx, _, abort, err := j.left.Table.DB().Begin(ctx)
	if err != nil {
		return err
	}
	defer abort()

	err = plan.Compile(ctx)
	if err != nil {
		return err
	}

	return plan.Stream(ctx, fn)
}

func (j Join) Run(ctx context.Context) (QueryResult, error) {
	plan, err := j.MakePlan()
	if err != nil {
		return nil, err
	}

	// use or open tx
	ctx, _, abort, err := j.left.Table.DB().Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer abort()

	err = plan.Compile(ctx)
	if err != nil {
		return nil, err
	}

	return plan.Query(ctx)
}

func (j Join) MakePlan() (*join.JoinPlan, error) {
	plan := join.NewJoinPlan().
		WithTag(j.tag).
		WithFlags(j.flags).
		WithLimit(j.limit).
		WithLogger(j.log).
		WithType(j.typ).
		WithTables(j.left.Table.Engine(), j.right.Table.Engine()).
		WithAliases(j.left.As, j.right.As).
		WithLimits(j.left.Limit, j.right.Limit)

	// compile conditions
	ls, rs := j.left.Table.Schema(), j.right.Table.Schema()
	ltree, err := j.left.Where.Compile(ls)
	if err != nil {
		return nil, err
	}
	rtree, err := j.right.Where.Compile(rs)
	if err != nil {
		return nil, err
	}
	plan.WithFilters(ltree, rtree)

	// lookup select fields
	lfields, err := ls.SelectFields(j.left.Select...)
	if err != nil {
		return nil, err
	}
	rfields, err := rs.SelectFields(j.right.Select...)
	if err != nil {
		return nil, err
	}
	plan.WithSelects(lfields, rfields)

	// predicates
	lpred, ok := ls.FieldByName(j.left.On)
	if !ok {
		return nil, fmt.Errorf("join %s: invalid ON field %q", j.tag, j.left.On)
	}
	rpred, ok := rs.FieldByName(j.right.On)
	if !ok {
		return nil, fmt.Errorf("join %s: invalid ON field %q", j.tag, j.right.On)
	}
	plan.WithOn(lpred, rpred, j.mode)

	return plan, nil
}
