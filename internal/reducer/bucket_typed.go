// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package reducer

import (
	"bytes"
	"fmt"
	"reflect"
	"sort"
	"time"

	"blockwatch.cc/knoxdb/internal/engine"
	"blockwatch.cc/knoxdb/pkg/num"
	"blockwatch.cc/knoxdb/pkg/util"
)

type TypedBucket struct {
	name     string         // name of result column
	index    int            // block index in result
	typ      reflect.Type   // the actual Go type
	template TypedReducer   // template reducer to store config
	reducers []TypedReducer // combine source stream values into one result per window
	last     time.Time      // last window start time
	next     time.Time      // next window start time
	window   util.TimeUnit  // aggregation window
	trange   util.TimeRange // series time range
	limit    int            // value limit
	fill     FillMode       // fill missing data
	read     func(engine.QueryRow) (Aggregatable, error)
}

func NewTypedBucket() *TypedBucket {
	t := &TypedBucket{
		reducers: make([]TypedReducer, 0),
	}
	t.read = t.readBytes
	return t
}

func (b *TypedBucket) WithDimensions(r util.TimeRange, w util.TimeUnit) Bucket {
	b.trange = r
	b.window = w
	steps := r.NumSteps(w)
	if cap(b.reducers) < steps {
		b.reducers = make([]TypedReducer, 0, steps)
	}
	b.last = b.trange.From
	b.next = b.window.Next(b.last, 1)
	return b
}

func (b *TypedBucket) WithLimit(limit int) Bucket {
	b.limit = limit
	return b
}

func (b *TypedBucket) WithReducer(fn ReducerFunc) Bucket {
	r := NewTypedReducer(b.typ, fn)
	if b.template != nil {
		r.Init(b.template.Config())
	}
	b.template = r
	return b
}

func (b *TypedBucket) WithName(name string) Bucket {
	b.name = name
	return b
}

func (b *TypedBucket) WithIndex(index int) Bucket {
	b.index = index
	return b
}

func (b *TypedBucket) WithFill(mode FillMode) Bucket {
	b.fill = mode
	return b
}

func (b *TypedBucket) WithType(typ reflect.Type) Bucket {
	if typ.Kind() == reflect.Pointer {
		typ = typ.Elem()
	}
	var (
		cfg Aggregatable
		fn  = ReducerFuncSum
	)
	if b.template != nil {
		fn = b.template.Type()
		cfg = b.template.Config()
	}
	b.template = NewTypedReducer(typ, fn)
	if cfg != nil {
		b.template.Init(cfg)
	}
	b.typ = typ
	return b
}

func (b *TypedBucket) WithTypeOf(val Aggregatable) Bucket {
	return b.WithType(reflect.TypeOf(val)).WithInit(val)
}

func (b *TypedBucket) WithInit(val Aggregatable) Bucket {
	if b.template == nil {
		b.template = NewTypedReducer(b.typ, ReducerFuncSum)
	}
	b.template.Init(val)
	return b
}

func (b *TypedBucket) Len() int {
	return len(b.reducers)
}

func (b *TypedBucket) grow() TypedReducer {
	r := NewTypedReducer(b.typ, b.template.Type())
	r.Init(b.template.Config())
	b.reducers = append(b.reducers, r)
	return r
}

func (b *TypedBucket) Push(t time.Time, r engine.QueryRow, join bool) error {
	// read next value from database stream
	nextVal, err := b.read(r)
	if err != nil {
		return err
	}
	target := len(b.reducers) - 1

	switch {
	case !t.Before(b.next):
		// Typical case: detect window boundary crossing

		// add new reducer
		b.grow()

		// update window (consider edge case where t >> b.next)
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

	b.reducers[target].Reduce(nextVal, t, join)
	return nil
}

func (b *TypedBucket) Emit(buf *bytes.Buffer) error {
	buf.WriteByte('[')
	defer buf.WriteByte(']')
	if len(b.reducers) == 0 {
		return nil
	}
	var (
		last   = b.reducers[0]
		filler Aggregatable
		idx    int
		count  int
	)

	if b.fill != FillModeNone {
		filler = reflect.New(b.typ).Interface().(Aggregatable)
		filler.Init(b.template.Config())
	}

	// UTC and truncated to window
	start, end := b.trange.From, b.trange.To

	for step := start; !step.After(end) && count < b.limit; step = b.window.Next(step, 1) {
		var next TypedReducer
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
				if fillVal, ok, isNull := Fill(b.fill, step, last.Time(), next.Time(), lastVal.Float64(), nextVal.Float64()); ok {
					if count > 0 {
						buf.WriteByte(',')
					}
					if isNull {
						buf.Write(null)
					} else {
						filler.SetFloat64(fillVal)
						filler.Emit(buf)
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
			val.Emit(buf)
			count++
		}
		idx++
		last = next
	}

	return nil
}

func (b *TypedBucket) readBytes(r engine.QueryRow) (Aggregatable, error) {
	val, err := r.Index(b.index)
	if err != nil {
		return nil, err
	}
	buf, ok := val.([]byte)
	if !ok {
		return nil, fmt.Errorf("invalid value type %T for []byte", val)
	}
	elem := reflect.New(b.typ).Interface().(Aggregatable)
	elem.Init(b.template.Config())
	err = elem.UnmarshalBinary(buf)
	return elem, err
}

func (b *TypedBucket) readInt128(r engine.QueryRow) (Aggregatable, error) {
	val, err := r.Index(b.index)
	if err != nil {
		return nil, err
	}
	i128, ok := val.(num.Int128)
	if !ok {
		return nil, fmt.Errorf("invalid value type %T for num.Int128", val)
	}
	elem := &Int128Aggregator{i128, 0}
	elem.Init(b.template.Config())
	return elem, err
}

func (b *TypedBucket) readInt256(r engine.QueryRow) (Aggregatable, error) {
	val, err := r.Index(b.index)
	if err != nil {
		return nil, err
	}
	i256, ok := val.(num.Int256)
	if !ok {
		return nil, fmt.Errorf("invalid value type %T for num.Int256", val)
	}
	elem := &Int256Aggregator{i256, 0}
	elem.Init(b.template.Config())
	return elem, err
}
