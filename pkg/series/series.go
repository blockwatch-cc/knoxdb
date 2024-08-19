// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package series

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"time"

	"blockwatch.cc/knoxdb/internal/query"
	"blockwatch.cc/knoxdb/pkg/util"
	"github.com/echa/log"
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

func (r Request) Query(key string) query.Query {
	// round custom time ranges
	r.Sanitize()

	cols := r.Select.QueryFields()
	if r.GroupBy != "" {
		cols.AddUnique(r.GroupBy)
	}

	// build stream query
	q := query.NewQuery().
		WithName(key).
		WithTable(r.table).
		WithFields(cols...).
		AndRange("time", r.Range.From, r.Range.To)

	return q
}

func (r Request) Run(ctx context.Context, key string) (*Result, error) {
	return r.RunQuery(ctx, r.Query(key))
}

func (req Request) RunQuery(ctx context.Context, q query.Query) (*Result, error) {
	// load table type
	table := q.Table()
	tinfo := table.Fields()
	timeIndex := tinfo.Find("time").Index

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
		b, err := req.MakeBucket(expr, tinfo)
		if err != nil {
			return nil, err
		}
		res.buckets[""][i] = b
		log.Tracef("NEW bucket fn=%s field=%s typ=%T", expr.Reduce, expr.Field, b)
	}

	// identify groupBy column
	var groupByIndex int = -1
	if req.GroupBy != "" {
		f := tinfo.Find(req.GroupBy)
		if f == nil {
			return nil, fmt.Errorf("unknown column %q", req.GroupBy)
		}
		groupByIndex = f.Index
	} else {
		res.groups = append(res.groups, "")
	}

	log.Debugf("Query from=%s to=%s unit=%s limit=%d", req.Range.From, req.Range.To, req.Interval, req.Limit)

	// execute stream query
	var last time.Time
	err := q.WithTable(table).Stream(ctx, func(r query.Row) error {
		// read time
		t, err := r.Time(timeIndex)
		if err != nil {
			return err
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
					buckets[i], _ = req.MakeBucket(expr, tinfo)
				}
				res.buckets[groupName] = buckets
				res.groups = append(res.groups, groupName)
				log.Tracef("NEW bucket group for %s with %d buckets", groupName, len(req.Select))
			}
		}

		// process row
		for _, v := range buckets {
			if err := v.Push(t, r, join); err != nil {
				log.Error(err)
			}
		}

		// stop when limit is reached, make sure we process/aggregate all data rows
		// for the last window (stop at the first data point exceeding the limit)
		if req.Limit > 0 && buckets[0].Len() > req.Limit {
			return query.EndStream
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return res, nil
}
