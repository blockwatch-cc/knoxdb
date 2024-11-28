// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package util

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

const (
	units string = "mhdwMqy"

	UnitMinute  = 'm'
	UnitHour    = 'h'
	UnitDay     = 'd'
	UnitWeek    = 'w'
	UnitMonth   = 'M'
	UnitQuarter = 'q'
	UnitYear    = 'y'
)

var titles = []string{"Minute", "Hour", "Day", "Week", "Month", "Quarter", "Year"}

var (
	TimeUnitMinute  = MustParseTimeUnit("m")
	TimeUnitHour    = MustParseTimeUnit("h")
	TimeUnitDay     = MustParseTimeUnit("d")
	TimeUnitWeek    = MustParseTimeUnit("w")
	TimeUnitMonth   = MustParseTimeUnit("M")
	TimeUnitQuarter = MustParseTimeUnit("q")
	TimeUnitYear    = MustParseTimeUnit("y")
)

type TimeUnit struct {
	Value int
	Unit  rune
}

func (c TimeUnit) String() string {
	if c.Value == 1 {
		return string(c.Unit)
	}
	return strconv.Itoa(c.Value) + string(c.Unit)
}

func (c TimeUnit) Format(t time.Time) string {
	switch c.Unit {
	default:
		return t.Format("2006-01-02 15:04:05")
	case UnitDay, UnitWeek:
		return t.Format("2006-01-02")
	case UnitMonth, UnitQuarter:
		return t.Format("2006-01")
	case UnitYear:
		return t.Format("2006")
	}
}

func (c TimeUnit) Title() string {
	return titles[strings.Index(units, string(c.Unit))]
}

func MustParseTimeUnit(s string) TimeUnit {
	u, err := ParseTimeUnit(s)
	if err != nil {
		panic(err)
	}
	return u
}

func ParseTimeUnit(s string) (TimeUnit, error) {
	var c TimeUnit
	if len(s) < 1 {
		return c, fmt.Errorf("unit: invalid value %q", s)
	}
	if u := s[len(s)-1]; !strings.Contains(units, string(u)) {
		return c, fmt.Errorf("unit: invalid unit %q", u)
	} else {
		c.Unit = rune(u)
	}
	if sval := s[:len(s)-1]; len(sval) > 0 {
		if val, err := strconv.Atoi(sval); err != nil {
			return c, fmt.Errorf("unit: %v", err)
		} else {
			c.Value = val
		}
	}
	if c.Value < 0 {
		c.Value = -c.Value
	}
	if c.Value == 0 {
		c.Value = 1
	}
	return c, nil
}

// Set implements the flags.Value interface for use in command line argument parsing.
func (u *TimeUnit) Set(s string) (err error) {
	*u, err = ParseTimeUnit(s)
	return
}

func (c TimeUnit) MarshalText() ([]byte, error) {
	return []byte(c.String()), nil
}

func (c *TimeUnit) UnmarshalText(data []byte) error {
	cc, err := ParseTimeUnit(string(data))
	if err != nil {
		return err
	}
	*c = cc
	return nil
}

func (c TimeUnit) Add(t time.Time) time.Time {
	switch c.Unit {
	default:
		// add n*m units
		return t.Add(c.Duration())
	case UnitWeek:
		// add n*m weeks
		return t.AddDate(0, 0, c.Value*7)
	case UnitMonth:
		// add n*m months
		return t.AddDate(0, c.Value, 0)
	case UnitQuarter:
		// add n*3m months
		return t.AddDate(0, 3*c.Value, 0)
	case UnitYear:
		// add n*m years
		return t.AddDate(c.Value, 0, 0)
	}
}

func (c TimeUnit) Sub(t time.Time) time.Time {
	switch c.Unit {
	default:
		// add n*m units
		return t.Add(-c.Duration())
	case UnitWeek:
		// add n*m weeks
		return t.AddDate(0, 0, -c.Value*7)
	case UnitMonth:
		// add n*m months
		return t.AddDate(0, -c.Value, 0)
	case UnitQuarter:
		// add n*3m months
		return t.AddDate(0, -3*c.Value, 0)
	case UnitYear:
		// add n*m years
		return t.AddDate(-c.Value, 0, 0)
	}
}

func (c TimeUnit) Base() time.Duration {
	base := time.Minute
	switch c.Unit {
	case UnitMinute:
		base = time.Minute
	case UnitHour:
		base = time.Hour
	case UnitDay:
		base = 24 * time.Hour
	case UnitWeek:
		base = 24 * 7 * time.Hour
	case UnitMonth:
		base = 30*24*time.Hour + 629*time.Minute + 28*time.Second // 30.437 days
	case UnitQuarter:
		base = 91*24*time.Hour + 6*time.Hour // 91.25 days
	case UnitYear:
		base = 365 * 24 * time.Hour
	}
	return base
}

func (c TimeUnit) Duration() time.Duration {
	return time.Duration(c.Value) * c.Base()
}

// Truncate truncates t to time unit ignoring its value, e.g.
// - minutes: full minute
// - hours: full hour
// - days: midnight UTC
// - weeks: midnight UTC on first day of week (Sunday)
// - months: midnight UTC on first day of month
// - quarters: midnight UTC on first day of quarter
// - years: midnight UTC on first day of year
func (c TimeUnit) Truncate(t time.Time) time.Time {
	if c.Value == 0 {
		return t
	}
	switch c.Unit {
	default:
		// anything below a day is fine for go's time library
		return t.Truncate(c.Duration())
	case UnitDay:
		// truncate to day start,
		yy, mm, dd := t.Date()
		// truncate again to multiple days
		return time.Date(yy, mm, dd-((dd-1)%c.Value), 0, 0, 0, 0, time.UTC)
	case UnitWeek:
		// truncate to midnight on first day of week (weekdays are zero-based)
		_, ww := t.ISOWeek()
		yy, mm, dd := t.AddDate(0, 0, -int(t.Weekday())).Date()
		return time.Date(yy, mm, dd-(ww%c.Value)*7, 0, 0, 0, 0, time.UTC)

	case UnitMonth:
		// truncate to midnight on first day of month
		yy, mm, _ := t.Date()
		return time.Date(yy, mm-time.Month((int(mm)-1)%c.Value), 1, 0, 0, 0, 0, time.UTC)

	case UnitQuarter:
		// truncate to midnight on first day of quarter
		yy, mm, _ := t.Date()
		val := yy*12 + int(mm) - 1
		val -= val % (3 * c.Value)
		yy = val / 12
		mm = time.Month(val%12 + 1)
		return time.Date(yy, mm, 1, 0, 0, 0, 0, time.UTC)

	case UnitYear:
		// truncate to midnight on first day of year
		yy := t.Year()
		return time.Date(yy-((yy)%c.Value), time.January, 1, 0, 0, 0, 0, time.UTC)
	}
}

// TruncateRelative takes the unit's value into account and truncates t relative
// to base.
func (c TimeUnit) TruncateRelative(t time.Time, base time.Time) time.Time {
	last, next := base, c.Next(base, 1)
	for {
		if !t.Before(next) {
			last, next = next, c.Next(next, 1)
			continue
		}
		return last
	}
}

func (c TimeUnit) Next(t time.Time, n int) time.Time {
	switch c.Unit {
	default:
		// add n*m units
		return c.Truncate(t).Add(time.Duration(n) * c.Duration())
	case UnitWeek:
		// add n*m weeks
		return c.Truncate(t).AddDate(0, 0, n*c.Value*7)
	case UnitMonth:
		// add n*m months
		return c.Truncate(t).AddDate(0, n*c.Value, 0)
	case UnitQuarter:
		// add n*3m months
		return c.Truncate(t).AddDate(0, 3*n*c.Value, 0)
	case UnitYear:
		// add n*m years
		return c.Truncate(t).AddDate(n*c.Value, 0, 0)
	}
}

func (c TimeUnit) Steps(from, to time.Time, limit int) []time.Time {
	steps := make([]time.Time, 0)
	if from.After(to) {
		from, to = to, from
	}
	for {
		from = c.Next(from, 1)
		if from.After(to) {
			break
		}
		steps = append(steps, from)
		if len(steps) == limit {
			break
		}
	}
	return steps
}

func (c TimeUnit) Between(from, to time.Time, limit int) []time.Time {
	steps := make([]time.Time, 0)
	if from.After(to) {
		from, to = to, from
	}
	for {
		from = c.Next(from, 1)
		if !from.Before(to) { // >= (!)
			break
		}
		steps = append(steps, from)
		if len(steps) == limit {
			break
		}
	}
	return steps
}
