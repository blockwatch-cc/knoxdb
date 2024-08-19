// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package series

import (
	"fmt"
	"strings"
	"time"
)

var null = []byte(`null`)

type FillMode string

const (
	FillModeInvalid FillMode = ""
	FillModeNone    FillMode = "none"
	FillModeNull    FillMode = "null"
	FillModeLast    FillMode = "last"
	FillModeLinear  FillMode = "linear"
	FillModeZero    FillMode = "zero"
	FillModeNow     FillMode = "now"
)

func ParseFillMode(s string) FillMode {
	switch m := FillMode(strings.ToLower(s)); m {
	case FillModeNone, FillModeNull, FillModeLast, FillModeLinear, FillModeZero:
		return m
	case "":
		return FillModeNone
	default:
		return FillModeInvalid
	}
}

func (m FillMode) IsValid() bool {
	return m != FillModeInvalid
}

func (m FillMode) String() string {
	return string(m)
}

func (m FillMode) MarshalText() ([]byte, error) {
	return []byte(m.String()), nil
}

func (m *FillMode) UnmarshalText(data []byte) error {
	mode := ParseFillMode(string(data))
	if !mode.IsValid() {
		return fmt.Errorf("invalid fill mode '%s'", string(data))
	}
	*m = mode
	return nil
}

// linearFill computes the slope of the line between the points (previousTime, previousValue)
// and (nextTime, nextValue) and returns the value of the point on the line with time
// windowTime where y = mx + b
func linearFill[T Number](windowTime, previousTime, nextTime int64, previousValue, nextValue T) T {
	m := float64(nextValue-previousValue) / float64(nextTime-previousTime) // the slope of the line
	x := float64(windowTime - previousTime)                                // how far into the interval we are
	b := float64(previousValue)
	return T(m*x + b)
}

func Fill[T Number](mode FillMode, now, prev, next time.Time, preval, nextval T) (T, bool, bool) {
	switch mode {
	case FillModeNone:
		return 0, false, false
	case FillModeNull:
		return 0, true, true
	case FillModeLast:
		return preval, true, false
	case FillModeLinear:
		return linearFill(now.Unix(), prev.Unix(), next.Unix(), preval, nextval), true, false
	case FillModeZero:
		return 0, true, false
	case FillModeNow:
		// used for time column only!
		return T(now.UnixNano()), true, false
	default:
		// none
		return 0, false, false
	}
}
