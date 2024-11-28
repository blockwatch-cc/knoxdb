// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package util

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"time"
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

func NewTime(t time.Time) Time {
	return Time{tm: t}
}

func Date(year int, month time.Month, day, hour, min, sec, nsec int, loc *time.Location) Time {
	return Time{tm: time.Date(year, month, day, hour, min, sec, nsec, loc)}
}

func Now() Time {
	return NewTime(time.Now())
}

var TimeFormats []string = []string{
	time.RFC3339,
	"02.01.2006T15:04:05.999999999Z07:00",
	"02.01.2006T15:04:05Z07:00",
	"02.01.2006 15:04:05.999999999Z07:00",
	"02.01.2006 15:04:05Z07:00",
	"2006:01:02 15:04:05.999999999-07:00",
	"2006:01:02 15:04:05-07:00",
	"2006:01:02:15:04:05-07:00",
	"2006:01:02:15:04:05-07",
	"2006-01-02T15:04:05.999999999Z",
	"2006-01-02T15:04:05Z",
	"2006-01-02 15:04:05.999999999Z",
	"2006-01-02 15:04:05Z",
	"02.01.2006T15:04:05.999999999Z",
	"02.01.2006T15:04:05Z",
	"02.01.2006 15:04:05.999999999Z",
	"02.01.2006 15:04:05Z",
	"2006-01-02T15:04:05.999999999",
	"2006-01-02T15:04:05",
	"2006-01-02 15:04:05.999999999",
	"2006-01-02 15:04:05",
	"02.01.2006T15:04:05.999999999",
	"02.01.2006T15:04:05",
	"02.01.2006 15:04:05.999999999",
	"02.01.2006 15:04:05",
	"2006-01-02T15:04",
	"2006-01-02 15:04",
	"02.01.2006T15:04",
	"02.01.2006 15:04",
	"2006-01-02",
	"02.01.2006",
	"2006-01",
	"01.2006",
	"15:04:05",
	"15:04",
	"2006",
}

var dateOnly = StringList{
	"2006-01-02",
	"02.01.2006",
	"2006-01",
	"01.2006",
	"2006",
}

func (f Time) Time() time.Time {
	return f.tm
}

func (f Time) IsDate() bool {
	return f.format == TimeFormatDate
}

func (f Time) EODTime() time.Time {
	dd, mm, yy := f.tm.Date()
	return time.Date(yy, mm, dd, 23, 59, 59, 0, time.UTC)
}

func (f Time) EOD() Time {
	return Time{tm: f.EODTime(), format: f.format}
}

func (f Time) GetFormat() TimeFormat {
	return f.format
}

func (t *Time) SetFormat(f TimeFormat) *Time {
	t.format = f
	return t
}

func NewTimeFrom(ts int64, res time.Duration) Time {
	return Time{tm: time.Unix(0, ts*int64(res))}
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
			u, derr := ParseTimeUnit(val)
			if derr != nil {
				return Time{}, fmt.Errorf("time: parsing '%s': %v", value, derr)
			}
			now = u.Sub(now)
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
		for _, f := range TimeFormats {
			if t, err := time.Parse(f, value); err == nil {
				// catch the time-only values by offsetting with today's UTC date
				if t.Year() == 0 {
					t = time.Now().UTC().Truncate(oneDay).Add(t.Sub(time.Time{}))
				}
				if dateOnly.Contains(f) {
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
		return strconv.FormatInt(f.UnixMicro(), 10)
	case TimeFormatUnixMicro:
		return strconv.FormatInt(f.UnixMilli(), 10)
	case TimeFormatUnixNano:
		return strconv.FormatInt(f.UnixNano(), 10)
	default:
		fs, ok := FormatMap[f.format]
		if !ok {
			fs = FormatMap[TimeFormatDefault]
		}
		return f.Time().Format(fs)
	}
}

func (f Time) MarshalText() ([]byte, error) {
	if f.IsZero() {
		return nil, nil
	}
	return []byte(f.String()), nil
}

func (f *Time) UnmarshalText(data []byte) error {
	t, err := ParseTime(string(data))
	if err != nil {
		return err
	}
	*f = t
	return nil
}

func (f *Time) UnmarshalJSON(data []byte) error {
	return f.UnmarshalText(bytes.Trim(data, "\""))
}

func (f Time) MarshalJSON() ([]byte, error) {
	if f.IsZero() {
		return nil, nil
	}
	s := f.String()
	if f.format.IsUnix() {
		return []byte(s), nil
	}

	return []byte(strconv.Quote(s)), nil
}

func (t Time) IsZero() bool {
	return t.Time().IsZero()
}

func (t Time) Before(a Time) bool {
	return t.Time().Before(a.Time())
}

func (t Time) After(a Time) bool {
	return t.Time().After(a.Time())
}

func (t Time) Unix() int64 {
	return t.Time().Unix()
}

func (t Time) Date() (int, time.Month, int) {
	return t.Time().Date()
}

func (t Time) Year() int {
	return t.Time().Year()
}

func (t Time) Truncate(d time.Duration) Time {
	return Time{
		tm:     t.Time().Truncate(d),
		format: t.format,
	}
}

func (t Time) Add(d time.Duration) Time {
	return Time{
		tm:     t.Time().Add(d),
		format: t.format,
	}
}

func (t Time) AddDate(years int, months int, days int) Time {
	return Time{
		tm:     t.Time().AddDate(years, months, days),
		format: t.format,
	}
}

func (t Time) Equal(t2 Time) bool {
	return t.Time().Equal(t2.Time())
}

func (t Time) UnixMilli() int64 {
	return t.tm.UnixNano() / 1000000
}

func (t Time) UnixMicro() int64 {
	return t.tm.UnixNano() / 1000
}

func (t Time) UnixNano() int64 {
	return t.tm.UnixNano()
}

func UnixNonZero(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.Unix()
}

func UnixMilli(t time.Time) int64 {
	return t.UnixNano() / 1000000
}

func UnixMilliNonZero(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixNano() / 1000000
}

func UnixMicro(t time.Time) int64 {
	return t.UnixNano() / 1000
}

func UnixMicroNonZero(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixNano() / 1000
}

func UnixNano(t time.Time) int64 {
	return t.UnixNano()
}

func UnixNanoNonZero(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixNano()
}

func (t Time) Format(layout string) string {
	return t.Time().Format(layout)
}

func (t Time) AppendFormat(b []byte, layout string) []byte {
	return t.Time().AppendFormat(b, layout)
}

func (t Time) DaysSince(a Time) int {
	return int(t.Time().Sub(a.Time()) / (24 * time.Hour))
}

func NonZeroSince(start time.Time) time.Duration {
	if start.IsZero() {
		return 0
	}
	return time.Since(start)
}

// DaysIn returns the number of days in a month for a given year.
func DaysIn(m time.Month, year int) int {
	// This is equivalent to unexported time.daysIn(m, year).
	return time.Date(year, m+1, 0, 0, 0, 0, 0, time.UTC).Day()
}

func DaysBetween(a, b time.Time) int {
	if a.Before(b) {
		a, b = b, a
	}
	return int(a.Sub(b) / (24 * time.Hour))
}

func StepsBetween(t1, t2 time.Time, d time.Duration) []time.Time {
	steps := make([]time.Time, 0)
	if t2.After(t1) && d > 0 {
		for {
			t1 = t1.Add(d)
			if !t1.Before(t2) {
				break
			}
			steps = append(steps, t1)
		}
	}
	return steps
}

func MaxTime(x, y time.Time) time.Time {
	if x.After(y) {
		return x
	}
	return y
}

func MinTime(x, y time.Time) time.Time {
	if x.Before(y) {
		return x
	}
	return y
}

func MaxTimeN(t ...time.Time) time.Time {
	switch len(t) {
	case 0:
		return time.Time{}
	case 1:
		return t[0]
	default:
		n := t[0]
		for _, v := range t[1:] {
			if v.After(n) {
				n = v
			}
		}
		return n
	}
}

func MinTimeN(t ...time.Time) time.Time {
	switch len(t) {
	case 0:
		return time.Time{}
	case 1:
		return t[0]
	default:
		n := t[0]
		for _, v := range t[1:] {
			if v.Before(n) {
				n = v
			}
		}
		return n
	}
}

func ClampTime(val, min, max time.Time) time.Time {
	return MinTime(MaxTime(val, min), max)
}

func FirstNonZeroTime(val time.Time, others ...time.Time) time.Time {
	if !val.IsZero() {
		return val
	}
	for _, v := range others {
		if !v.IsZero() {
			return v
		}
	}
	return time.Time{}
}

const (
	units string = "mhdwMqy"
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

func (c TimeUnit) Sub(t time.Time) time.Time {
	switch c.Unit {
	default:
		// add n*m units
		return t.Add(-c.Duration())
	case 'w':
		// add n*m weeks
		return t.AddDate(0, 0, -c.Value*7)
	case 'M':
		// add n*m months
		return t.AddDate(0, -c.Value, 0)
	case 'q':
		// add n*3m months
		return t.AddDate(0, -3*c.Value, 0)
	case 'y':
		// add n*m years
		return t.AddDate(-c.Value, 0, 0)
	}
}

func (c TimeUnit) Add(t time.Time) time.Time {
	switch c.Unit {
	default:
		// add n*m units
		return t.Add(c.Duration())
	case 'w':
		// add n*m weeks
		return t.AddDate(0, 0, c.Value*7)
	case 'M':
		// add n*m months
		return t.AddDate(0, c.Value, 0)
	case 'q':
		// add n*3m months
		return t.AddDate(0, 3*c.Value, 0)
	case 'y':
		// add n*m years
		return t.AddDate(c.Value, 0, 0)
	}
}

func (c TimeUnit) Base() time.Duration {
	base := time.Minute
	switch c.Unit {
	case 'm':
		base = time.Minute
	case 'h':
		base = time.Hour
	case 'd':
		base = 24 * time.Hour
	case 'w':
		base = 24 * 7 * time.Hour
	case 'M':
		base = 30*24*time.Hour + 629*time.Minute + 28*time.Second // 30.437 days
	case 'q':
		base = 91*24*time.Hour + 6*time.Hour // 91.25 days
	case 'y':
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
	switch c.Unit {
	default:
		// anything below a day is fine for go's time library
		return t.Truncate(c.Base())
	case 'd':
		// truncate to day start,
		yy, mm, dd := t.Date()
		return time.Date(yy, mm, dd, 0, 0, 0, 0, time.UTC)
	case 'w':
		// truncate to midnight on first day of week (weekdays are zero-based)
		yy, mm, dd := t.AddDate(0, 0, -int(t.Weekday())).Date()
		return time.Date(yy, mm, dd, 0, 0, 0, 0, time.UTC)

	case 'M':
		// truncate to midnight on first day of month
		yy, mm, _ := t.Date()
		return time.Date(yy, mm, 1, 0, 0, 0, 0, time.UTC)

	case 'q':
		// truncate to midnight on first day of quarter
		yy, mm, _ := t.Date()
		val := yy*12 + int(mm) - 1
		val -= val % 3
		yy = val / 12
		mm = time.Month(val%12 + 1)
		return time.Date(yy, mm, 1, 0, 0, 0, 0, time.UTC)

	case 'y':
		// truncate to midnight on first day of year
		yy := t.Year()
		return time.Date(yy, time.January, 1, 0, 0, 0, 0, time.UTC)
	}
}

func (c TimeUnit) Next(t time.Time, n int) time.Time {
	switch c.Unit {
	default:
		// add n*m units
		return c.Truncate(t).Add(time.Duration(n) * c.Duration())
	case 'w':
		// add n*m weeks
		return c.Truncate(t).AddDate(0, 0, n*c.Value*7)
	case 'M':
		// add n*m months
		return c.Truncate(t).AddDate(0, n*c.Value, 0)
	case 'q':
		// add n*3m months
		return c.Truncate(t).AddDate(0, 3*n*c.Value, 0)
	case 'y':
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
