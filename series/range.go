// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package series

import (
	"fmt"
	"strings"
	"time"

	"blockwatch.cc/knoxdb/util"
)

// Accepts time ranges of form
//
// date          - custom from date/time, to now (or calculated from limit and interval unit)
// date,date     - custom from,to date/time
// all           - all time
// ytd           - year to date
// qtd           - quarter to date
// mtd           - month to date
// wtd           - week to date (first weekday Sunday as per Go convention)
// 3m            - time unit offset from now
// last-year     - Jan 1 -- Dec 31 previous year
// last-quarter  - first day of last quarter -- last day of last quarter
// last-month    - first day of last month -- last day of last month
// last-week     - first day of last week -- last day of last week

const rangeLayout = "2006-01-02T15:04:05"

type TimeRange struct {
	Key        string // custom, ytd, 3m
	From       time.Time
	To         time.Time
	IsRelative bool
}

func NewTimeRangeSince(u TimeUnit) TimeRange {
	now := time.Now().UTC()
	return TimeRange{
		Key:        "since",
		From:       u.Sub(now).UTC(),
		To:         now,
		IsRelative: true,
	}
}

func (r TimeRange) String() string {
	return fmt.Sprintf("%s,%s", r.From.Format(rangeLayout), r.To.Format(rangeLayout))
}

func MustParseTimeRange(s string) TimeRange {
	r, err := ParseTimeRange(s)
	if err != nil {
		panic(err)
	}
	return r
}

func ParseTimeRange(s string) (TimeRange, error) {
	var r TimeRange
	if a, b, ok := strings.Cut(s, ","); ok {
		from, err := util.ParseTime(a)
		if err != nil {
			return r, err
		}
		to, err := util.ParseTime(b)
		if err != nil {
			return r, err
		}
		r.From = from.Time().UTC()
		r.To = to.Time().UTC()
		r.Key = "custom"
		return r, nil
	}

	now := time.Now().UTC()
	r.Key = s
	r.To = now
	switch s {
	case "all":
		r.From = time.Unix(0, 0)
	case "ytd":
		r.From = time.Date(now.Year(), 1, 1, 0, 0, 0, 0, time.UTC)
	case "qtd":
		yy, mm, _ := now.AddDate(0, -int((now.Month()-1)%3), 0).Date()
		r.From = time.Date(yy, mm, 1, 0, 0, 0, 0, time.UTC)
	case "mtd":
		yy, mm, _ := now.Date()
		r.From = time.Date(yy, mm, 1, 0, 0, 0, 0, time.UTC)
	case "wtd":
		yy, mm, dd := now.AddDate(0, 0, -int(now.Weekday())).Date()
		r.From = time.Date(yy, mm, dd, 0, 0, 0, 0, time.UTC)
	case "last-year":
		r.From = time.Date(now.Year()-1, 1, 1, 0, 0, 0, 0, time.UTC)
		r.To = time.Date(now.Year(), 1, 1, 0, 0, 0, 0, time.UTC).AddDate(0, 0, -1)
	case "last-quarter":
		yy, mm, _ := now.AddDate(0, -int((now.Month()-1)%3), 0).Date()
		r.From = time.Date(yy, mm, 1, 0, 0, 0, 0, time.UTC).AddDate(0, -3, 0)
		r.To = time.Date(yy, mm, 1, 0, 0, 0, 0, time.UTC).AddDate(0, 0, -1)
	case "last-month":
		yy, mm, _ := now.Date()
		r.From = time.Date(yy, mm, 1, 0, 0, 0, 0, time.UTC).AddDate(0, -1, 0)
		r.To = time.Date(yy, mm, 1, 0, 0, 0, 0, time.UTC).AddDate(0, 0, -1)
	case "last-week":
		yy, mm, dd := now.AddDate(0, 0, -int(now.Weekday())).Date()
		r.From = time.Date(yy, mm, dd, 0, 0, 0, 0, time.UTC).AddDate(0, 0, -7)
		r.To = time.Date(yy, mm, dd, 0, 0, 0, 0, time.UTC).AddDate(0, 0, -1)
	default:
		// try parse as unit
		u, err := ParseTimeUnit(s)
		if err == nil {
			r.From = u.Sub(now).UTC()
			r.IsRelative = true
		} else {
			// try parse as time
			t, err := util.ParseTime(s)
			if err != nil {
				return r, err
			}
			r.From = t.Time().UTC()
		}
	}
	return r, nil
}

func (r TimeRange) MarshalText() ([]byte, error) {
	return []byte(r.String()), nil
}

func (r *TimeRange) UnmarshalText(data []byte) error {
	p, err := ParseTimeRange(string(data))
	if err != nil {
		return err
	}
	*r = p
	return nil
}

func (r TimeRange) Duration() time.Duration {
	return r.To.Sub(r.From)
}

func (r TimeRange) Sanitize(now time.Time) TimeRange {
	if r.To.IsZero() || r.To.After(now) {
		r.To = now
	}
	if r.From.After(r.To) {
		r.From, r.To = r.To, r.From
	}
	return r
}

func (r TimeRange) Truncate(d time.Duration) TimeRange {
	r.From.Truncate(d)
	r.To.Truncate(d)
	return r
}

func (r TimeRange) NumSteps(u TimeUnit) (steps int) {
	switch u.Unit {
	case 'm', 'h':
		d := r.To.Sub(r.From)
		interval := u.Duration()
		steps = int(d / interval)
		if d%interval > 0 {
			steps++
		}
	default:
		for t := r.From; t.Before(r.To); t, steps = u.Add(t), steps+1 {
		}
	}
	return
}

func (r TimeRange) Epochs(interval TimeUnit, limit int) (epochs [][2]time.Time) {
	if interval.Duration() == 0 {
		epochs = [][2]time.Time{{r.From, r.To}}
		return
	}
	for t := r.From; t.Before(r.To) && len(epochs) <= limit; t = interval.Add(t) {
		epochs = append(epochs, [2]time.Time{
			t,
			interval.Add(t).Add(-time.Nanosecond),
		})
	}
	return
}

// Set implements the flags.Value interface for use in command line argument parsing.
func (r *TimeRange) Set(s string) (err error) {
	*r, err = ParseTimeRange(s)
	return
}
