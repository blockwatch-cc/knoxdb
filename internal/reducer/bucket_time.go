// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package reducer

import (
	"strconv"
	"time"
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
	t.emit = t.emitTime
	return t
}

// func (b *TimeBucket) read(r engine.QueryRow) (int64, error) {
// 	val, err := r.Index(b.index)
// 	if err != nil {
// 		return 0, err
// 	}
// 	t, ok := val.(time.Time)
// 	if !ok {
// 		return 0, fmt.Errorf("invalid value type %T for time.Time", val)
// 	}
// 	return t.UnixNano(), nil
// }

func (b *TimeBucket) emitTime(t int64) string {
	val := b.window.Truncate(time.Unix(0, t).UTC())
	return strconv.Quote(val.Format(time.RFC3339))
}
