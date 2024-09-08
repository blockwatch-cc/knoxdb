// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package series

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"time"

	"blockwatch.cc/knoxdb/internal/engine"
	"blockwatch.cc/knoxdb/internal/query"
	"blockwatch.cc/knoxdb/pkg/util"
)

type Result struct {
	buckets  map[string][]Bucket // by group
	groups   []string
	table    string
	groupBy  string
	cols     util.StringList
	interval TimeRange
	window   TimeUnit
	fill     FillMode
}

func (r Result) Groups() []string {
	return r.groups
}

func (r Result) Columns() []string {
	return r.cols
}

// TODO: allow programmatic access to time-series data
// func GetResultTimes() []time.Time {
// }

// func GetResultVector[T any](group, column string) []T {
// }

// output series from all buckets
//
//	{"series": [{
//	   "name": "",
//	   "tags":{"entity":"quipu"} // <-- group
//	   "columns": [""],
//	   "values": [[...],[...]]
//	}]
func (r Result) MarshalJSON() ([]byte, error) {
	// alloc output buffer
	buf := bytes.NewBuffer(make([]byte, 0, len(r.buckets)*4096))
	buf.WriteString(`{"series":[`)
	first := true
	for _, group := range r.groups {
		buckets := r.buckets[group]
		if !first {
			buf.WriteRune(',')
		}
		// name
		buf.WriteString(`{"name":"` + r.table + `",`)
		// tags
		if group != "" {
			buf.WriteString(`"tags":{` + strconv.Quote(r.groupBy) + `:` + strconv.Quote(group) + `},`)
		}
		// columns
		buf.WriteString(`"columns":[`)
		buf.WriteString(strconv.Quote(r.cols[0]))
		for _, col := range r.cols[1:] {
			buf.WriteRune(',')
			buf.WriteString(strconv.Quote(col))
		}
		// values
		buf.WriteString(`],"values":[`)
		buckets[0].Emit(buf)
		for _, bucket := range buckets[1:] {
			buf.WriteRune(',')
			bucket.Emit(buf)
		}
		// close value and group
		buf.WriteString(`]}`)
		first = false
	}
	// close series list
	buf.WriteString(`]}`)

	return buf.Bytes(), nil
}

func (r Request) Query(key string) (*query.QueryPlan, error) {
	// round custom time ranges
	r.Sanitize()

	cols := r.Select.QueryFields()
	if r.GroupBy != "" {
		cols.AddUnique(r.GroupBy)
	}

	// derive query schema from table schema
	s, err := r.table.Schema().SelectNames(key, false, cols...)
	if err != nil {
		return nil, err
	}

	filters, err := query.Range("time", r.Range.From, r.Range.To).
		Compile(r.table.Schema())
	if err != nil {
		return nil, err
	}

	// build stream query
	plan := query.NewQueryPlan().
		WithSchema(s).
		WithTable(r.table).
		WithFilters(filters)

	return plan, nil
}

func (r Request) Run(ctx context.Context, key string) (*Result, error) {
	plan, err := r.Query(key)
	if err != nil {
		return nil, err
	}
	if err := plan.Compile(ctx); err != nil {
		return nil, err
	}
	return r.RunQuery(ctx, plan)
}

func (req Request) RunQuery(ctx context.Context, plan *query.QueryPlan) (*Result, error) {
	// load table type
	timeIndex, ok := plan.ResultSchema.FieldIndexByName("time")
	if !ok {
		return nil, fmt.Errorf("missing time field in result schema")
	}
	defer plan.Close()

	// create stream manager
	res := &Result{
		buckets:  make(map[string][]Bucket),
		table:    req.Table,
		cols:     req.Select.Cols(),
		groupBy:  req.GroupBy,
		interval: req.Range,
		window:   req.Interval,
		fill:     req.Fill,
	}
	res.buckets[""] = make([]Bucket, len(req.Select))

	// create buckets from type info
	for i, expr := range req.Select {
		b, err := req.MakeBucket(expr, plan.ResultSchema)
		if err != nil {
			return nil, err
		}
		res.buckets[""][i] = b
		req.log.Tracef("NEW bucket fn=%s field=%s typ=%T", expr.Reduce, expr.Field, b)
	}

	// identify groupBy column
	var groupByIndex int = -1
	if req.GroupBy != "" {
		groupByIndex, ok = plan.ResultSchema.FieldIndexByName(req.GroupBy)
		if !ok {
			return nil, fmt.Errorf("unknown group_by field %q", req.GroupBy)
		}
	} else {
		res.groups = append(res.groups, "")
	}

	req.log.Debugf("Query from=%s to=%s unit=%s limit=%d", req.Range.From, req.Range.To, req.Interval, req.Limit)

	// execute stream query
	var last time.Time
	err := plan.Table.Stream(ctx, plan, func(r engine.QueryRow) error {
		// read time
		val, err := r.Index(timeIndex)
		if err != nil {
			return err
		}
		t, ok := val.(time.Time)
		if !ok {
			return fmt.Errorf("invalid value type %T for time field", val)
		}

		// join same timestamp records
		next := t.UTC()
		join := last.Equal(next)
		last = next

		// match interval start (we do this here once for all reducers)
		t = req.Interval.TruncateRelative(next, req.Range.From)
		// ctx.Log.Infof("Hit time=%s join=%t bucket=%s", next, join, t)

		// identify bucket group
		buckets := res.buckets[""]
		if groupByIndex >= 0 {
			// we don't enforce groupBy field type, so we read any type
			// and try convert it to string
			group, err := r.Index(groupByIndex)
			if err != nil {
				return err
			}
			var groupName string
			// TODO: support enum type int->string conversion here
			// use tinfo or field type name
			groupName = util.ToString(group)

			if groupBuckets, ok := res.buckets[groupName]; ok {
				buckets = groupBuckets
			} else {
				// create new bucket group
				buckets = make([]Bucket, len(req.Select))
				for i, expr := range req.Select {
					buckets[i], _ = req.MakeBucket(expr, plan.ResultSchema)
				}
				res.buckets[groupName] = buckets
				res.groups = append(res.groups, groupName)
				req.log.Tracef("NEW bucket group for %s with %d buckets", groupName, len(req.Select))
			}
		}

		// process row
		for _, v := range buckets {
			if err := v.Push(t, r, join); err != nil {
				req.log.Error(err)
			}
		}

		// stop when limit is reached, make sure we process/aggregate all data rows
		// for the last window (stop at the first data point exceeding the limit)
		if req.Limit > 0 && buckets[0].Len() > req.Limit {
			return engine.EndStream
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return res, nil
}
