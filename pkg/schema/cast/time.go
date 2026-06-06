// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package cast

import (
	"time"

	"blockwatch.cc/knoxdb/pkg/schema"
)

// time caster
type TimeCaster struct {
	scale schema.TimeScale
}

func (c TimeCaster) CastValue(val any) (res any, err error) {
	v, ok := val.(time.Time)
	if !ok {
		err = CastError(val, "time")
	} else {
		res = c.scale.ToUnix(v)
	}
	return
}

func (c TimeCaster) CastSlice(val any) (res any, err error) {
	v, ok := val.([]time.Time)
	if !ok {
		err = CastError(val, "time")
	} else {
		r := make([]int64, len(v))
		for i := range v {
			r[i] = c.scale.ToUnix(v[i])
		}
		res = r
	}
	return
}

// date caster
type DateCaster struct{}

func (c DateCaster) CastValue(val any) (res any, err error) {
	v, ok := val.(time.Time)
	if !ok {
		err = CastError(val, "date")
	} else {
		res = schema.UnixDays(v)
	}
	return
}

func (c DateCaster) CastSlice(val any) (res any, err error) {
	v, ok := val.([]time.Time)
	if !ok {
		err = CastError(val, "date")
	} else {
		r := make([]int64, len(v))
		for i := range v {
			r[i] = schema.UnixDays(v[i])
		}
		res = r
	}
	return
}
