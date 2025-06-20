// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package schema

import (
	"time"

	"blockwatch.cc/knoxdb/pkg/util"
)

type TimeScale byte

const (
	TIME_SCALE_NANO   TimeScale = iota // 0
	TIME_SCALE_MICRO                   // 1
	TIME_SCALE_MILLI                   // 2
	TIME_SCALE_SECOND                  // 3
	TIME_SCALE_DAY                     // 4
)

var (
	timeScaleFactor = [...]int64{
		1,              // nanosecond
		1000,           // microsecond
		1000000,        // millisecond
		1000000000,     // second
		86400000000000, // days
	}
	timeScaleFormats = [...]string{
		"2006-01-02 15:04:05.000000000 UTC",
		"2006-01-02 15:04:05.000000 UTC",
		"2006-01-02 15:04:05.000 UTC",
		"2006-01-02 15:04:05 UTC",
		"2006-01-02",
	}
	timeOnlyFormats = [...]string{
		"15:04:05.000000000",
		"15:04:05.000000",
		"15:04:05.000",
		"15:04:05",
		"",
	}
)

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
	case "d", "day":
		return TIME_SCALE_DAY, true
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
	case TIME_SCALE_DAY:
		return UnixDays(t)
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
	case TIME_SCALE_DAY:
		return FromUnixDays(v)
	default:
		return time.Unix(0, v).UTC()
	}
}

func (s TimeScale) AsUint() uint8 {
	return uint8(s)
}

func (s TimeScale) DateTimeFormat() string {
	return timeScaleFormats[s]
}

func (s TimeScale) TimeOnlyFormat() string {
	return timeOnlyFormats[s]
}

func (s TimeScale) Format(t time.Time) string {
	return t.Format(timeScaleFormats[s])
}

func (s TimeScale) Parse(v string, isTimeOnly bool) (int64, error) {
	if isTimeOnly {
		tm, err := time.Parse(timeOnlyFormats[s], v)
		if err != nil {
			return 0, err
		}
		// adjust date to UNIX epoch, Go parses as Jan 1, 0000
		return s.ToUnix(tm.AddDate(1970, 0, 0)), nil
	} else {
		tm, err := time.Parse(timeScaleFormats[s], v)
		if err != nil {
			return 0, err
		}
		return s.ToUnix(tm), nil
	}
}

// returns format string, scale factor, time only flag and ok flag
func DetectTimeFormat(s string) (string, TimeScale, bool, bool) {
	for i, f := range timeScaleFormats {
		if _, err := time.Parse(f, s); err == nil {
			return f, TimeScale(i), false, true
		}
	}
	for i, f := range timeOnlyFormats {
		if _, err := time.Parse(f, s); err == nil {
			return f, TimeScale(i), true, true
		}
	}
	if f, err := util.DetectTimeFormat(s); err == nil {
		return f, TIME_SCALE_NANO, false, true
	}
	return "", 0, false, false
}

func UnixDays(t time.Time) int64 {
	return int64(t.Sub(time.Unix(0, 0)) / (24 * time.Hour))
}

func FromUnixDays(d int64) time.Time {
	return time.Unix(0, d*timeScaleFactor[TIME_SCALE_DAY]).UTC()
}
