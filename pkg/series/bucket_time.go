// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package series

import (
	"strconv"
	"time"

	"blockwatch.cc/knoxdb/internal/query"
)

type TimeBucket struct {
	NativeBucket[int64]
}

func NewTimeBucket() *TimeBucket {
	t := &TimeBucket{
		*NewNativeBucket[int64](),
	}
	t.template = NewReducer[int64](ReducerFuncFirst)
	t.fill = FillModeNow
	t.locked = true
	t.read = t.readTime
	t.emit = t.emitTime
	return t
}

func (b *TimeBucket) readTime(r query.Row) (int64, error) {
	t, err := r.Time(b.index)
	if err != nil {
		return 0, err
	}
	return t.UnixNano(), nil
}

func (b *TimeBucket) emitTime(t int64) string {
	val := b.window.Truncate(time.Unix(0, int64(t)).UTC())
	return strconv.Quote(val.Format(time.RFC3339))
}
