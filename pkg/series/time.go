// Copyright (c) 2020-2026 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package series

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"blockwatch.cc/knoxdb/pkg/schema"
)

type TimeFormat int

var oneDay = 24 * time.Hour

const (
	TimeFormatDefault TimeFormat = iota
	TimeFormatUnix
	TimeFormatUnixMicro
	TimeFormatUnixMilli
	TimeFormatUnixNano
	TimeFormatDate
)

var FormatMap = map[TimeFormat]string{
	TimeFormatDefault:   time.RFC3339,
	TimeFormatUnix:      "",
	TimeFormatUnixMicro: "",
	TimeFormatUnixMilli: "",
	TimeFormatUnixNano:  "",
	TimeFormatDate:      "2006-01-02",
}

func (f TimeFormat) IsUnix() bool {
	switch f {
	case TimeFormatUnix,
		TimeFormatUnixMicro,
		TimeFormatUnixMilli,
		TimeFormatUnixNano:
		return true
	}
	return false
}

type Time struct {
	tm     time.Time
	format TimeFormat
}

func (f Time) Time() time.Time {
	return f.tm
}

func (f Time) EODTime() time.Time {
	dd, mm, yy := f.tm.Date()
	return time.Date(yy, mm, dd, 23, 59, 59, 0, time.UTC)
}

func (f Time) EOD() Time {
	return Time{tm: f.EODTime(), format: f.format}
}

func ParseTime(value string) (Time, error) {
	// parse invalid zero values
	switch value {
	case "", "-":
		return Time{}, nil
	}
	// try parsing as int
	i, ierr := strconv.ParseInt(value, 10, 64)
	if ierr != nil {
		// when failed, try parsing as hex
		i, ierr = strconv.ParseInt(value, 16, 64)
	}
	switch {
	case ierr == nil && len(value) > 4:
		// 1st try parsing as unix timestamp
		// detect UNIX timestamp scale: we choose somewhat arbitrarity
		// Dec 31, 9999 23:59:59 as cut-off time here
		switch {
		case i < 253402300799:
			// timestamp is in seconds
			return Time{tm: time.Unix(i, 0).UTC(), format: TimeFormatUnix}, nil
		case i < 253402300799000:
			// timestamp is in milliseconds
			return Time{tm: time.Unix(0, i*1000000).UTC(), format: TimeFormatUnixMilli}, nil
		case i < 253402300799000000:
			// timestamp is in microseconds
			return Time{tm: time.Unix(0, i*1000).UTC(), format: TimeFormatUnixMicro}, nil
		default:
			// timestamp is in nanoseconds
			return Time{tm: time.Unix(0, i).UTC(), format: TimeFormatUnixNano}, nil
		}

	case strings.HasPrefix(value, "now"):
		now := time.Now().UTC()
		// check for truncation and modification operators
		if key, val, ok := strings.Cut(value, "/"); ok {
			if key != "now" {
				return Time{}, fmt.Errorf("time: parsing '%s': invalid truncation syntax, must be `now/arg`", value)
			}
			value = val
			// parse arg as duration modifier (strip optional modifier)
			left, _, _ := strings.Cut(value, "-")
			switch left {
			case "s":
				now = now.Truncate(time.Second)
			case "m":
				now = now.Truncate(time.Minute)
			case "h":
				now = now.Truncate(time.Hour)
			case "d":
				now = now.Truncate(24 * time.Hour)
			case "w":
				now = now.Truncate(7 * 24 * time.Hour)
			case "M":
				yy, mm, _ := now.Date()
				now = time.Date(yy, mm, 1, 0, 0, 0, 0, time.UTC)
			case "q":
				yy, mm, _ := now.Date()
				now = time.Date(yy, mm-mm%3, 1, 0, 0, 0, 0, time.UTC)
			case "y":
				now = time.Date(now.Year(), 1, 1, 0, 0, 0, 0, time.UTC)
			default:
				return Time{}, fmt.Errorf("time: parsing '%s': invalid truncation argument", value)
			}
		}
		// continue handling minus operator
		if _, val, ok := strings.Cut(value, "-"); ok {
			d, derr := ParseDuration(val)
			if derr != nil {
				return Time{}, fmt.Errorf("time: parsing '%s': %v", value, derr)
			}
			now = now.Add(-d)
		}
		return Time{tm: now}, nil
	case value == "today":
		return Time{tm: time.Now().UTC().Truncate(oneDay)}, nil
	case value == "yesterday":
		return Time{tm: time.Now().UTC().Truncate(oneDay).AddDate(0, 0, -1)}, nil
	case value == "tomorrow":
		return Time{tm: time.Now().UTC().Truncate(oneDay).Add(oneDay)}, nil

	default:
		// 3rd try the different time formats from most to least specific
		_, scale, isTimeOnly, ok := schema.DetectTimeFormat(value)
		if ok {
			t, err := scale.ParseTime(value, isTimeOnly)
			if err == nil {
				// catch the time-only values by offsetting with today's UTC date
				// scale.ParseTime uses Jan 1 1970 as baseline
				if isTimeOnly {
					yy, mm, dd := time.Now().Date()
					hour, min, sec := t.Clock()
					t = time.Date(yy, mm, dd, hour, min, sec, t.Nanosecond(), t.Location())
				}
				if scale == schema.TIME_SCALE_DAY {
					return Time{tm: t, format: TimeFormatDate}, nil
				}
				return Time{tm: t}, nil
			}
		}
		return Time{}, fmt.Errorf("time: parsing '%s': invalid syntax", value)
	}
}

func (f Time) String() string {
	switch f.format {
	case TimeFormatUnix:
		return strconv.FormatInt(f.Time().Unix(), 10)
	case TimeFormatUnixMilli:
		return strconv.FormatInt(f.Time().UnixMicro(), 10)
	case TimeFormatUnixMicro:
		return strconv.FormatInt(f.Time().UnixMilli(), 10)
	case TimeFormatUnixNano:
		return strconv.FormatInt(f.Time().UnixNano(), 10)
	default:
		fs, ok := FormatMap[f.format]
		if !ok {
			fs = FormatMap[TimeFormatDefault]
		}
		return f.Time().Format(fs)
	}
}
