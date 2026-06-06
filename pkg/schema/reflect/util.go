// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package reflect

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

var (
	rx  = regexp.MustCompile("[^a-zA-Z0-9]+")
	sep = "_"
)

func sanitize(s string) string {
	if len(s) == 0 {
		return s
	}

	// Prefix internal field names
	if s[0] == '$' {
		s = "X" + s[1:]
	}

	// Replace invalid characters
	s = rx.ReplaceAllString(s, "_")

	// Replace multiple __ with single _
	s = strings.ReplaceAll(s, "__", "_")

	// Trim leading and trailing _
	s = strings.TrimPrefix(s, "_")
	s = strings.TrimSuffix(s, "_")

	return s
}

func toTitle(src string) string {
	if len(src) == 0 {
		return src
	}
	return strings.ToUpper(src[:1]) + src[1:]
}

func fromCamelCase(src, sep string) string {
	var b strings.Builder
	for idx := 0; idx < len(src); {
		offs := strings.IndexFunc(src[idx+1:], func(r rune) bool {
			return r >= 'A' && r <= 'Z'
		}) + 1
		if offs <= 0 {
			offs = len(src) - idx
		}
		if b.Len() > 0 {
			b.WriteString(sep)
		}
		b.WriteString(strings.ToLower(src[idx : idx+offs]))
		idx += offs
	}
	return b.String()
}

func toCamelCase(src, sep string) string {
	var (
		b    strings.Builder
		part string
	)
	for len(src) > 0 {
		part, src, _ = strings.Cut(src, sep)
		b.WriteString(toTitle(part))
	}
	return b.String()
}

func parseInt(val, name string, minVal, maxVal int) (int, error) {
	n, err := strconv.Atoi(val)
	if err != nil {
		return 0, fmt.Errorf("invalid %s value: %v", name, err)
	}
	return validateInt(name, n, minVal, maxVal)
}

func validateInt(name string, n, minVal, maxVal int) (int, error) {
	if n < minVal || (maxVal > 0 && n > maxVal) {
		return 0, fmt.Errorf("%s %d out of bounds [%d..%d]", name, n, minVal, maxVal)
	}
	return n, nil
}
