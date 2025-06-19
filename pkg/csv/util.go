// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package csv

import (
	"iter"
	"regexp"
	"strconv"
	"strings"
	"unicode"

	"blockwatch.cc/knoxdb/pkg/schema"
)

// SchemaOf detects struct schema from Go type. Only use struct types
// here, other functions will then take this type as `*T` and `[]*T`.
//
// CSV field names can be controlled using the `csv` struct tag.
//
//	// CSV field "name" will be assigned to struct field "Field".
//	Field int64 `csv:"name"`
func SchemaOf(m any) (*schema.Schema, error) {
	return schema.SchemaOfTag(m, "csv")
}

// Parse unquoted and regular quoted fields. This works with
// RFC 4180 compliant streams (`"1,"",1",2`), but fails when quotes
// are broken, e.g. `"a"a"`, `"a"a"a"`, `"a"""a"`, `""a"`,`""a"a""`,
// `"""`, `"""""`.
func ParseAndCut(buf []byte, sep byte) ([]byte, []byte, bool) {
	var (
		l      = len(buf)
		pos    int
		quoted bool
	)
loop:
	for pos < l {
		switch buf[pos] {
		case sep:
			if !quoted {
				break loop
			}
		case '"':
			if !quoted {
				quoted = pos == 0 // quoted line must start with a quote
			} else {
				if pos+1 < l && buf[pos+1] == '"' {
					pos++
				} else {
					quoted = false
				}
			}
		}
		pos++
	}
	return buf[0:pos], buf[min(l, pos+1):], pos < l
}

// Split returns a Go iterator for looping over the fields in
// a CSV line with a `for i, s := range Split(line) {}` loop.
// Works only with RFC 4180 compliant streams.
func Split(line []byte, sep byte) iter.Seq2[int, []byte] {
	return func(fn func(int, []byte) bool) {
		var i int
		for {
			tok, buf, ok := ParseAndCut(line, sep)
			if !fn(i, tok) {
				break
			}
			if !ok {
				break
			}
			line = buf
			i++
		}
	}
}

var rx = regexp.MustCompile("[^a-zA-Z0-9]+")

// SanitizeFieldName converts a parsed CSV header field to a valid
// schema field name without quotes, whitespace, invalid characters.
// Ensures the field name starts with a character, optionally prefixing
// with `f_`.
func SanitizeFieldName(name string, i int) string {
	// remove spaces, quotes and spaces inside quotes
	name = strings.TrimSpace(name)
	name = strings.Trim(name, `"`)
	name = strings.TrimSpace(name)

	// Replace invalid characters
	name = rx.ReplaceAllString(name, "_")

	// Replace multiple __ with single _
	name = strings.ReplaceAll(name, "__", "_")

	// Trim leading and trailing _
	name = strings.Trim(name, "_")

	// Handle empty result
	if name == "" {
		return "f_" + strconv.Itoa(i)
	}

	// Ensure name starts with a letter
	if !unicode.IsLetter(rune(name[0])) {
		name = "f_" + name
	}

	return name
}
