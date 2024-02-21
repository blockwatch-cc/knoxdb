// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package series

import (
	"bytes"
	"sort"

	// "fmt"
	"reflect"
	"strconv"
	"time"

	"blockwatch.cc/knoxdb/pack"
)

type NativeBucket[T Number] struct {
	name     string       // name of result column
	index    int          // block index in result
	template Reducer[T]   // template reducer to store config
	reducers []Reducer[T] // combine source stream values into one result per window
	locked   bool         // disallow reducer and fill changes (used for time buckets)
	last     time.Time    // last window start time
	next     time.Time    // next window start time
	window   TimeUnit     // aggregation window
	trange   TimeRange    // series time range
	limit    int          // value limit
	fill     FillMode     // fill missing data
	emit     func(T) string
	read     func(pack.Row) (T, error)
}

func NewNativeBucket[T Number]() *NativeBucket[T] {
	return &NativeBucket[T]{
		template: NewReducer[T](ReducerFuncSum),
		reducers: make([]Reducer[T], 0),
	}
}

func (b *NativeBucket[T]) WithDimensions(r TimeRange, w TimeUnit) Bucket {
	b.trange = r
	b.window = w
	steps := r.NumSteps(w)
	if cap(b.reducers) < steps {
		b.reducers = make([]Reducer[T], 0, steps)
	}
	b.last = b.trange.From
	b.next = b.window.Next(b.last, 1)
	return b
}

func (b *NativeBucket[T]) WithLimit(limit int) Bucket {
	b.limit = limit
	return b
}

func (b *NativeBucket[T]) WithReducer(fn ReducerFunc) Bucket {
	if !b.locked {
		b.template = NewReducer[T](fn)
	}
	return b
}

func (b *NativeBucket[T]) WithName(name string) Bucket {
	b.name = name
	return b
}

func (b *NativeBucket[T]) WithIndex(index int) Bucket {
	b.index = index
	return b
}

func (b *NativeBucket[T]) WithFill(mode FillMode) Bucket {
	if !b.locked || mode == FillModeNone {
		b.fill = mode
	}
	return b
}

func (b *NativeBucket[T]) WithType(_ reflect.Type) Bucket {
	return b
}

func (b *NativeBucket[T]) WithTypeOf(_ Aggregatable) Bucket {
	return b
}

func (b *NativeBucket[T]) WithInit(_ Aggregatable) Bucket {
	return b
}

func (b *NativeBucket[T]) Len() int {
	return len(b.reducers)
}

func (b *NativeBucket[T]) grow() Reducer[T] {
	r := NewReducer[T](b.template.Type())
	b.reducers = append(b.reducers, r)
	return r
}

func (b *NativeBucket[T]) Push(t time.Time, r pack.Row, join bool) error {
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

	b.reducers[target].Reduce(nextVal, t, join)
	return nil
}

func (b *NativeBucket[T]) Emit(buf *bytes.Buffer) error {
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
		var next Reducer[T]
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
						buf.WriteString(b.emit(fillVal))
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
			buf.WriteString(b.emit(val))
			count++
		}
		idx++
		last = next
	}

	return nil
}

func emitIntegers[T Signed](num T) string {
	return strconv.FormatInt(int64(num), 10)
}

func emitUnsigneds[T Unsigned](num T) string {
	return strconv.FormatUint(uint64(num), 10)
}

func emitFloats[T Float](num T) string {
	return strconv.FormatFloat(float64(num), 'f', -1, 64)
}

func (b *NativeBucket[T]) readInt64(r pack.Row) (int64, error) {
	return r.Int64(b.index)
}

func (b *NativeBucket[T]) readInt32(r pack.Row) (int32, error) {
	return r.Int32(b.index)
}

func (b *NativeBucket[T]) readInt16(r pack.Row) (int16, error) {
	return r.Int16(b.index)
}

func (b *NativeBucket[T]) readInt8(r pack.Row) (int8, error) {
	return r.Int8(b.index)
}

func (b *NativeBucket[T]) readUint64(r pack.Row) (uint64, error) {
	return r.Uint64(b.index)
}

func (b *NativeBucket[T]) readUint32(r pack.Row) (uint32, error) {
	return r.Uint32(b.index)
}

func (b *NativeBucket[T]) readUint16(r pack.Row) (uint16, error) {
	return r.Uint16(b.index)
}

func (b *NativeBucket[T]) readUint8(r pack.Row) (uint8, error) {
	return r.Uint8(b.index)
}

func (b *NativeBucket[T]) readFloat64(r pack.Row) (float64, error) {
	return r.Float64(b.index)
}

func (b *NativeBucket[T]) readFloat32(r pack.Row) (float32, error) {
	return r.Float32(b.index)
}

func (b *NativeBucket[T]) readDecimal256(r pack.Row) (float64, error) {
	val, err := r.Decimal256(b.index)
	return val.Float64(), err
}

func (b *NativeBucket[T]) readDecimal128(r pack.Row) (float64, error) {
	val, err := r.Decimal128(b.index)
	return val.Float64(), err
}

func (b *NativeBucket[T]) readDecimal64(r pack.Row) (float64, error) {
	val, err := r.Decimal64(b.index)
	return val.Float64(), err
}

func (b *NativeBucket[T]) readDecimal32(r pack.Row) (float64, error) {
	val, err := r.Decimal32(b.index)
	return val.Float64(), err
}
