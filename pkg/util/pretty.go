// Copyright (c) 2018-2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package util

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

func PrettyInt(i int) string {
	return PrettyString(strconv.FormatInt(int64(i), 10))
}

func PrettyInt64(i int64) string {
	return PrettyString(strconv.FormatInt(i, 10))
}

func PrettyUint64(i uint64) string {
	return PrettyString(strconv.FormatUint(i, 10))
}

func PrettyFloat64(f float64) string {
	return PrettyString(strconv.FormatFloat(f, 'f', -1, 64))
}

func PrettyFloat64N(f float64, decimals int) string {
	return PrettyString(strconv.FormatFloat(f, 'f', decimals, 64))
}

// not pretty, but works: 1000000.123 -> 1,000,000.123
func PrettyString(s string) string {
	if l, i := len(s), strings.IndexByte(s, '.'); i == -1 && l > 3 || i > 3 {
		var rem string
		if i > -1 {
			rem = s[i:]
			s = s[:i]
		} else {
			i = 0
		}
		l = len(s)
		p := s[:l%3]
		if len(p) > 0 {
			p += ","
		}
		for j := 0; j <= l/3; j++ {
			start := l%3 + j*3
			end := start + 3
			if end > len(s) {
				end = len(s)
			}
			p += s[start:end] + ","
		}
		s = p[:len(p)-2] + rem
	}
	return s
}

func Pretty(val interface{}) string {
	switch v := val.(type) {
	case int:
		return PrettyInt64(int64(v))
	case int32:
		return PrettyInt64(int64(v))
	case int64:
		return PrettyInt64(int64(v))
	case uint:
		return PrettyUint64(uint64(v))
	case uint32:
		return PrettyUint64(uint64(v))
	case uint64:
		return PrettyUint64(uint64(v))
	case float32:
		return PrettyFloat64(float64(v))
	case float64:
		return PrettyFloat64(v)
	case string:
		return PrettyString(v)
	case time.Duration:
		return PrettyInt64(int64(v))
	default:
		return fmt.Sprintf("%v", val)
	}
}
