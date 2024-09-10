// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package reducer

import (
	"bytes"
	"reflect"
	"sort"
	"time"

	"blockwatch.cc/knoxdb/internal/engine"
	"blockwatch.cc/knoxdb/pkg/util"
)

type CountBucket struct {
	name     string               // name of result column
	index    int                  // block index in result
	reducers []*CountReducer[int] // combine source stream values into one result per window
	last     time.Time            // last window start time
	next     time.Time            // next window start time
	window   util.TimeUnit        // aggregation window
	trange   util.TimeRange       // series time range
	limit    int                  // value limit
	fill     FillMode             // fill missing data
}

func NewCountBucket() *CountBucket {
	return &CountBucket{
		reducers: make([]*CountReducer[int], 0),
	}
}

func (b *CountBucket) WithDimensions(r util.TimeRange, w util.TimeUnit) Bucket {
	b.trange = r
	b.window = w
	steps := r.NumSteps(w)
	if cap(b.reducers) < steps {
		b.reducers = make([]*CountReducer[int], 0, steps)
	}
	b.last = b.trange.From
	b.next = b.window.Next(b.last, 1)
	return b
}

func (b *CountBucket) WithLimit(limit int) Bucket {
	b.limit = limit
	return b
}

func (b *CountBucket) WithReducer(_ ReducerFunc) Bucket {
	return b
}

func (b *CountBucket) WithName(name string) Bucket {
	b.name = name
	return b
}

func (b *CountBucket) WithIndex(index int) Bucket {
	b.index = index
	return b
}

func (b *CountBucket) WithFill(mode FillMode) Bucket {
	b.fill = mode
	return b
}

func (b *CountBucket) WithType(_ reflect.Type) Bucket {
	return b
}

func (b *CountBucket) WithTypeOf(_ Aggregatable) Bucket {
	return b
}

func (b *CountBucket) WithInit(_ Aggregatable) Bucket {
	return b
}

func (b *CountBucket) Len() int {
	return len(b.reducers)
}

func (b *CountBucket) grow() *CountReducer[int] {
	r := &CountReducer[int]{}
	b.reducers = append(b.reducers, r)
	return r
}

func (b *CountBucket) Push(t time.Time, _ engine.QueryRow, join bool) error {
	target := len(b.reducers) - 1

	switch {
	case !t.Before(b.next):
		// Typical case: detect window boundary crossing

		// add new reducer
		b.grow()

		// update window
		b.last, b.next = t, b.window.Next(t, 1)
		target++

	case t.Before(b.last):
		// out of order case

		// find the first reducer after t (this is the best way using Go's search algo)
		// our target reducer for insertion is one before
		idx := sort.Search(len(b.reducers), func(i int) bool {
			return b.reducers[i].Time().After(t) // t < reducer time
		})

		// detect gaps in reducer time sequence and insert missing reducer
		if idx == 0 || b.window.Add(b.reducers[idx-1].Time()).Before(t) {
			// add new reducer
			r := b.grow()

			// insert in-place
			copy(b.reducers[idx+1:], b.reducers[idx:])
			b.reducers[idx] = r
			target = idx
		} else {
			target = idx - 1
		}

	case target < 0:
		// init case, add first reducer
		b.grow()
		target++

		// consider edge case where t >> b.next
		if t.After(b.next) {
			b.last, b.next = t, b.window.Next(t, 1)
		}
	}

	b.reducers[target].Reduce(0, t, join)
	return nil
}

func (b *CountBucket) Emit(buf *bytes.Buffer) error {
	buf.WriteByte('[')
	defer buf.WriteByte(']')
	if len(b.reducers) == 0 {
		return nil
	}
	var (
		last  = b.reducers[0]
		idx   int
		count int
	)

	// UTC and truncated to window
	start, end := b.trange.From, b.trange.To

	for step := start; !step.After(end) && count < b.limit; step = b.window.Next(step, 1) {
		var next *CountReducer[int]
		if idx < len(b.reducers) {
			next = b.reducers[idx]
		} else {
			next = last
		}

		// fill gap (start, middle, end)
		if !next.Time().Equal(step) {
			if b.fill != FillModeNone {
				nextVal, _ := next.Value()
				lastVal, _ := last.Value()
				if fillVal, ok, isNull := Fill(b.fill, step, last.Time(), next.Time(), lastVal, nextVal); ok {
					if count > 0 {
						buf.WriteByte(',')
					}
					if isNull {
						buf.Write(null)
					} else {
						buf.WriteString(emitIntegers(fillVal))
					}
					count++
				}
			}
			continue
		}

		// output value
		val, ok := next.Value()
		if ok {
			if count > 0 {
				buf.WriteByte(',')
			}
			buf.WriteString(emitIntegers(val))
			count++
		}
		idx++
		last = next
	}

	return nil
}
