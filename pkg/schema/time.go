// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package schema

import "time"

type TimeScale byte

const (
	TIME_SCALE_NANO TimeScale = iota
	TIME_SCALE_MICRO
	TIME_SCALE_MILLI
	TIME_SCALE_SECOND
)

var timeScaleFactor = [...]int64{
	1,          // nanosecond
	1000,       // microsecond
	1000000,    // millisecond
	1000000000, // second
}

func ParseTimeScale(s string) (TimeScale, bool) {
	switch s {
	case "ns", "nano", "nanosecond":
		return TIME_SCALE_NANO, true
	case "us", "micro", "microsecond":
		return TIME_SCALE_MICRO, true
	case "ms", "milli", "millisecond":
		return TIME_SCALE_MILLI, true
	case "s", "sec", "second":
		return TIME_SCALE_SECOND, true
	default:
		return 0, false
	}
}

func (s TimeScale) ToUnix(t time.Time) int64 {
	switch s {
	case TIME_SCALE_MICRO:
		return t.UnixMicro()
	case TIME_SCALE_MILLI:
		return t.UnixMilli()
	case TIME_SCALE_SECOND:
		return t.Unix()
	default:
		return t.UnixNano()
	}
}

func (s TimeScale) FromUnix(v int64) time.Time {
	switch s {
	case TIME_SCALE_MICRO:
		return time.Unix(0, v*1000).UTC()
	case TIME_SCALE_MILLI:
		return time.Unix(0, v*1000000).UTC()
	case TIME_SCALE_SECOND:
		return time.Unix(v, 0).UTC()
	default:
		return time.Unix(0, v).UTC()
	}
}

func (s TimeScale) AsUint() uint8 {
	return uint8(s)
}
