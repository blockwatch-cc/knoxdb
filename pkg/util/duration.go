// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package util

import (
	"bytes"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
	"unicode"
)

type Duration time.Duration

func (d Duration) Duration() time.Duration {
	return time.Duration(d)
}

func (d Duration) String() string {
	return time.Duration(d).String()
}

func ParseDuration(d string) (Duration, error) {
	d = strings.ToLower(d)
	multiplier := time.Second
	switch {
	case strings.HasSuffix(d, "d"):
		multiplier = 24 * time.Hour
		d = d[:len(d)-1]
	case strings.HasSuffix(d, "w"):
		multiplier = 7 * 24 * time.Hour
		d = d[:len(d)-1]
	}
	// parse integer values as seconds
	if i, err := strconv.ParseInt(d, 10, 64); err == nil {
		return Duration(time.Duration(i) * multiplier), nil
	}
	// parse as duration string (note: no whitespace allowed)
	if i, err := time.ParseDuration(d); err == nil {
		return Duration(i), nil
	}
	// parse as duration string with whitespace removed
	d = strings.Map(func(r rune) rune {
		if unicode.IsSpace(r) {
			return -1
		}
		return r
	}, d)
	if i, err := time.ParseDuration(d); err == nil {
		return Duration(i), nil
	}
	return 0, fmt.Errorf("duration: parsing '%s': invalid syntax", d)
}

func (d Duration) MarshalText() ([]byte, error) {
	return []byte(time.Duration(d).String()), nil
}

func (d *Duration) UnmarshalText(data []byte) error {
	i, err := ParseDuration(string(data))
	if err != nil {
		return err
	}
	*d = i
	return nil
}

func (d *Duration) UnmarshalJSON(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	if data[0] == '"' {
		return d.UnmarshalText(bytes.Trim(data, "\""))
	}
	if i, err := strconv.ParseInt(string(data), 10, 64); err == nil {
		*d = Duration(time.Duration(i) * time.Second)
		return nil
	}
	return fmt.Errorf("duration: parsing '%s': invalid syntax", string(data))
}

func (d Duration) Truncate(r time.Duration) Duration {
	if d > 0 {
		return Duration(math.Ceil(float64(d)/float64(r))) * Duration(r)
	} else {
		return Duration(math.Floor(float64(d)/float64(r))) * Duration(r)
	}
}

func (d Duration) RoundToDays() int {
	return int(d.Truncate(time.Hour*24) / Duration(time.Hour*24))
}

func (d Duration) RoundToHours() int64 {
	return int64(d.Truncate(time.Hour) / Duration(time.Hour))
}

func (d Duration) RoundToMinutes() int64 {
	return int64(d.Truncate(time.Minute) / Duration(time.Minute))
}

func (d Duration) RoundToSeconds() int64 {
	return int64(d.Truncate(time.Second) / Duration(time.Second))
}

func (d Duration) RoundToMillisecond() int64 {
	return int64(d.Truncate(time.Millisecond) / Duration(time.Millisecond))
}

func TruncateDuration(d, r time.Duration) time.Duration {
	return Duration(d).Truncate(r).Duration()
}

func RoundToDays(d time.Duration) int {
	return int(Duration(d).Truncate(time.Hour*24) / Duration(time.Hour*24))
}

func RoundToHours(d time.Duration) int64 {
	return int64(Duration(d).Truncate(time.Hour) / Duration(time.Hour))
}

func RoundToMinutes(d time.Duration) int64 {
	return int64(Duration(d).Truncate(time.Minute) / Duration(time.Minute))
}

func RoundToSeconds(d time.Duration) int64 {
	return int64(Duration(d).Truncate(time.Second) / Duration(time.Second))
}

func RoundToMillisecond(d time.Duration) int64 {
	return int64(Duration(d).Truncate(time.Millisecond) / Duration(time.Millisecond))
}

func MaxDuration(a, b time.Duration) time.Duration {
	if int64(a) < int64(b) {
		return b
	}
	return a
}

func MinDuration(a, b time.Duration) time.Duration {
	if int64(a) > int64(b) {
		return b
	}
	return a
}
