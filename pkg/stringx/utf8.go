// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package stringx

import (
	"unicode"
	"unicode/utf8"
)

func CmpCaseInsensitive(s, t string) int {
	for {
		if len(t) == 0 {
			if len(s) == 0 {
				return 0 // equal
			}
			return -1
		}
		if len(s) == 0 {
			return 1
		}
		c, sizec := utf8.DecodeRuneInString(s)
		d, sized := utf8.DecodeRuneInString(t)

		lowerc := unicode.ToLower(c)
		lowerd := unicode.ToLower(d)

		if lowerc < lowerd {
			return -1
		}
		if lowerc > lowerd {
			return 1
		}

		s = s[sizec:]
		t = t[sized:]
	}
}
