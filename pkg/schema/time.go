// Copyright (c) 2025-2026 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package schema

import (
	"time"
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
		"2006-01-02T15:04:05.000000000Z07:00",
		"2006-01-02T15:04:05.000000Z07:00",
		"2006-01-02T15:04:05.000Z07:00",
		"2006-01-02T15:04:05Z07:00",
		"2006-01-02",
	}
	timeOnlyFormats = [...]string{
		"15:04:05.000000000",
		"15:04:05.000000",
		"15:04:05.000",
		"15:04:05",
		"15:04",
		"",
	}
	extraTimeFormats = [...]string{
		time.RFC3339,
		time.ANSIC,
		time.UnixDate,
		time.RFC822,
		time.RFC822Z,
		time.RFC1123,
		time.RFC1123Z,
		"2006-01-02T15:04:05Z07:00",
		"2006-01-02T15:04:05Z",
		"2006-01-02T15:04:05 MST",
		"2006-01-02T15:04:05",
		"2006-01-02T15:04",
		"2006-01-02 15:04:05Z07:00",
		"2006-01-02 15:04:05Z",
		"2006-01-02 15:04:05 MST",
		"2006-01-02 15:04:05",
		"2006-01-02 15:04",
		"02.01.2006T15:04:05Z07:00",
		"02.01.2006T15:04:05Z",
		"02.01.2006T15:04:05 MST",
		"02.01.2006T15:04:05",
		"02.01.2006T15:04",
		"02.01.2006 15:04:05Z07:00",
		"02.01.2006 15:04:05Z",
		"02.01.2006 15:04:05 MST",
		"02.01.2006 15:04:05",
		"02.01.2006 15:04",
		"2006:01:02:15:04:05-07:00",
		"2006:01:02:15:04:05-07",
		"2006:01:02:15:04:05 MST",
		"2006:01:02:15:04:05",
		"2006:01:02 15:04:05-07:00",
		"2006:01:02 15:04:05-07",
		"2006:01:02 15:04:05 MST",
		"2006:01:02 15:04:05",
	}
	extraMsTimeFormats = [...]string{
		"2006-01-02T15:04:05.000Z07:00",
		"2006-01-02T15:04:05.000Z",
		"2006-01-02T15:04:05.000 MST",
		"2006-01-02T15:04:05.000",
		"2006-01-02 15:04:05.000Z07:00",
		"2006-01-02 15:04:05.000Z",
		"2006-01-02 15:04:05.000 MST",
		"2006-01-02 15:04:05.000",
		"02.01.2006T15:04:05.000Z07:00",
		"02.01.2006T15:04:05.000Z",
		"02.01.2006T15:04:05.000 MST",
		"02.01.2006T15:04:05.000",
		"02.01.2006 15:04:05.000Z07:00",
		"02.01.2006 15:04:05.000Z",
		"02.01.2006 15:04:05.000 MST",
		"02.01.2006 15:04:05.000",
		"2006:01:02 15:04:05.000-07:00",
		"2006:01:02 15:04:05.000Z",
		"2006:01:02 15:04:05.000 MST",
		"2006:01:02 15:04:05.000",
	}
	extraNsTimeFormats = [...]string{
		"2006-01-02T15:04:05.000000000Z07:00",
		"2006-01-02T15:04:05.000000000Z",
		"2006-01-02T15:04:05.000000000 MST",
		"2006-01-02T15:04:05.000000000",
		"2006-01-02 15:04:05.000000000Z07:00",
		"2006-01-02 15:04:05.000000000Z",
		"2006-01-02 15:04:05.000000000 MST",
		"2006-01-02 15:04:05.000000000",
		"02.01.2006T15:04:05.000000000Z07:00",
		"02.01.2006T15:04:05.000000000Z",
		"02.01.2006T15:04:05.000000000 MST",
		"02.01.2006T15:04:05.000000000",
		"02.01.2006 15:04:05.000000000Z07:00",
		"02.01.2006 15:04:05.000000000Z",
		"02.01.2006 15:04:05.000000000 MST",
		"02.01.2006 15:04:05.000000000",
		"2006:01:02 15:04:05.000000000-07:00",
		"2006:01:02 15:04:05.000000000Z",
		"2006:01:02 15:04:05.000000000 MST",
		"2006:01:02 15:04:05.000000000",
	}
	extraDateFormats = [...]string{
		"02.01.2006",
		"2006-01",
		"01.2006",
		"2006",
	}
)

func ParseTimeScale(s string) (TimeScale, bool) {
	switch s {
	case "0", "ns", "nano", "nanosecond":
		return TIME_SCALE_NANO, true
	case "1", "us", "micro", "microsecond":
		return TIME_SCALE_MICRO, true
	case "2", "ms", "milli", "millisecond":
		return TIME_SCALE_MILLI, true
	case "3", "s", "sec", "second":
		return TIME_SCALE_SECOND, true
	case "4", "d", "day":
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

func (s TimeScale) Int64(d time.Duration) int64 {
	return int64(d) / timeScaleFactor[s]
}

func (s TimeScale) Duration(v int64) time.Duration {
	return time.Duration(v * timeScaleFactor[s])
}

func (s TimeScale) ShortName() string {
	switch s {
	case TIME_SCALE_NANO:
		return "ns"
	case TIME_SCALE_MICRO:
		return "us"
	case TIME_SCALE_MILLI:
		return "ms"
	case TIME_SCALE_SECOND:
		return "s"
	case TIME_SCALE_DAY:
		return "d"
	default:
		return ""
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

func (s TimeScale) FormatTime(t time.Time) string {
	return t.Format(timeOnlyFormats[s])
}

func (s TimeScale) Parse(v string, isTimeOnly bool) (int64, error) {
	tm, err := s.ParseTime(v, isTimeOnly)
	if err != nil {
		return 0, err
	}
	return s.ToUnix(tm), nil
}

func (s TimeScale) ParseTime(v string, isTimeOnly bool) (time.Time, error) {
	if isTimeOnly {
		tm, err := time.Parse(timeOnlyFormats[s], v)
		if err != nil {
			return time.Time{}, err
		}
		// adjust date to UNIX epoch, Go parses as Jan 1, 0000
		return tm.AddDate(1970, 0, 0), nil
	} else {
		tm, err := time.Parse(timeScaleFormats[s], v)
		if err != nil {
			return time.Time{}, err
		}
		return tm, nil
	}
}

func (s TimeScale) ParseDuration(v string) (time.Duration, error) {
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, err
	}
	return d.Truncate(time.Duration(timeScaleFactor[s])), nil
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
	for _, f := range extraNsTimeFormats {
		if _, err := time.Parse(f, s); err == nil {
			return f, TIME_SCALE_NANO, false, true
		}
	}
	for _, f := range extraMsTimeFormats {
		if _, err := time.Parse(f, s); err == nil {
			return f, TIME_SCALE_MILLI, false, true
		}
	}
	for _, f := range extraTimeFormats {
		if _, err := time.Parse(f, s); err == nil {
			return f, TIME_SCALE_SECOND, false, true
		}
	}
	for _, f := range extraDateFormats {
		if _, err := time.Parse(f, s); err == nil {
			return f, TIME_SCALE_DAY, false, true
		}
	}
	return "", 0, false, false
}

func UnixDays(t time.Time) int64 {
	return int64(t.Sub(time.Unix(0, 0)) / (24 * time.Hour))
}

func FromUnixDays(d int64) time.Time {
	return time.Unix(0, d*timeScaleFactor[TIME_SCALE_DAY]).UTC()
}
