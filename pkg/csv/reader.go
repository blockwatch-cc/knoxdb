// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package csv

import (
	"bufio"
	"bytes"
	"fmt"
	"io"

	"blockwatch.cc/knoxdb/pkg/util"
)

const (
	Separator rune = ','
	Comment   rune = '#'
	Quote     rune = '"'
	Escape    rune = '"'
	Eol       rune = '\n'
)

var (
	SingleQuote = []byte{byte(Quote)}
	DoubleQuote = []byte{byte(Quote), byte(Quote)}
)

type ReadFlags byte

const (
	ReadFlagTrim ReadFlags = 1 << iota
	ReadFlagStrictQuotes
	ReadFlagQuiet
)

type Reader struct {
	scan      *bufio.Scanner
	record    []string
	lineNo    int
	numFields int
	numBytes  int
	sep       rune
	comment   rune
	flags     ReadFlags
}

func NewReader(r io.Reader, n int) *Reader {
	return &Reader{
		scan:      bufio.NewScanner(r),
		sep:       Separator,
		comment:   Comment,
		flags:     ReadFlagTrim | ReadFlagStrictQuotes,
		lineNo:    0,
		numFields: n,
		record:    make([]string, n),
	}
}

func (r *Reader) Reset(rd io.Reader) *Reader {
	r.scan = bufio.NewScanner(rd)
	return r
}

func (r *Reader) BytesProcessed() int {
	return r.numBytes
}

func (r *Reader) LinesProcessed() int {
	return r.lineNo
}

// WithTrim controls if the Decoder will trim whitespace surrounding header fields
// and records before processing them.
func (r *Reader) WithTrim(t bool) *Reader {
	if t {
		r.flags |= ReadFlagTrim
	} else {
		r.flags &^= ReadFlagTrim
	}
	return r
}

// Return error when encountering unclosed quotes or mixed quoted and
// unquoted text. When disabled, text fields will be eagerly parsed into
// strings.
func (r *Reader) WithStrictQuotes(t bool) *Reader {
	if t {
		r.flags |= ReadFlagStrictQuotes
	} else {
		r.flags &^= ReadFlagStrictQuotes
	}
	return r
}

// WithQuiet disables warning messages in non-strict mode.
func (r *Reader) WithQuiet(t bool) *Reader {
	if t {
		r.flags |= ReadFlagQuiet
	} else {
		r.flags &^= ReadFlagQuiet
	}
	return r
}

// WithSeparator sets rune s as record field separator that will be used for parsing.
func (r *Reader) WithSeparator(s rune) *Reader {
	r.sep = s
	return r
}

// WithComment sets rune c as comment line identifier. Comments must start with rune c
// as first character to be skipped.
func (r *Reader) WithComment(c rune) *Reader {
	r.comment = c
	return r
}

// WithBuffer sets a buffer buf to be used by the underlying bufio.Scanner
// for reading from io.Reader r.
func (r *Reader) WithBuffer(buf []byte) *Reader {
	r.scan.Buffer(buf, cap(buf))
	return r
}

// Read returns the next non-empty and non-commented line of input split
// into fields. Read returns an error when the underlying io.Reader fails
// and nil with io.EOF on EOF. The underlying memory for returned strings
// is only valid until the next call to read. Users must copy strings if
// contents is supposed to be preserved across calls.
func (r *Reader) Read() ([]string, error) {
	var line []byte

	// everything happens driven by a bufio.Scanner
	for r.scan.Scan() {
		// get next line of text
		buf := r.scan.Bytes()
		r.lineNo++
		r.numBytes += len(buf)

		// skip empty lines
		if len(buf) == 0 {
			continue
		}

		// skip comments
		if bytes.HasPrefix(buf, []byte{byte(r.comment)}) {
			continue
		}

		// handle the line
		line = buf
		break
	}

	// process error
	if err := r.scan.Err(); err != nil {
		return nil, fmt.Errorf("csv: read failed: %v", err)
	}

	// stop when no more lines are read
	if line == nil {
		return nil, io.EOF
	}

	// process line
	return r.SplitLine(line)
}

// Scans for the next closing quote `",`.
func nextClose(b []byte, sep rune, n, max int) (i int, ok bool, eol bool) {
	i = bytes.Index(b, []byte{byte(Quote), byte(sep)})
	if i < 0 {
		// if not found check if we're in the last field (no more separator follows)
		if j := bytes.Index(b, []byte{byte(sep)}); j < 0 {
			i = len(b)
			eol = true
		}
		return
	}
	// only accept the token extension when enough separator characters
	// exist fill all remaining fields
	ok = bytes.Count(b[i+1:], []byte{byte(sep)})+n+1 >= max
	return
}

// Splits line at separator into fields. Optionally trims whitespace
// around fields when configured and handles quoted fields. Unquotes
// following these rules:
//
// "xy"     -> `xy`     quoted fields are unquoted
// "x,y"    -> `x,y`    separators inside quoted text are copied as is
// "x""y""" -> `x"y"`   escaped quotes inside quotes become single quotes
// "x,y"a   -> `"x,y"a` half-quoted fields with extra text are copied as is (including all quotes)
func (r *Reader) SplitLine(line []byte) ([]string, error) {
	var (
		n   int // output token
		sep = []byte{byte(r.sep)}
		// src = line
	)
	clear(r.record)

	// try consume the entire line of text, stop when numFields are found
	for len(line) > 0 && n < r.numFields {
		token, next, hadSep := bytes.Cut(line, sep)

		// trim whitespace before quotes
		if r.flags&ReadFlagTrim > 0 {
			token = bytes.TrimSpace(token)
		}
		l := len(token)

		// fmt.Printf("cut l=%d token=%q next=%q\n", l, token, next)
		var checkDouble bool

		switch {
		case l >= 2 && token[0] == byte(Quote) && token[l-1] == byte(Quote):
			// full quoted text, remove quotes and use
			// fmt.Printf("F%d drop normal quotes\n", n)
			token = token[1 : l-1]
			checkDouble = l > 3

		case l > 0 && token[0] == byte(Quote):
			// Field starts with an open quote, but does not close. We need
			// to find a matching closing quote followed by separator. The heuristic
			// implemented here works as follows:
			//
			// We first assume there is no broken quoted field and we just hit
			// a separator character as part the quoted text. In this case
			// there should be a closing quote followed by separator. Any
			// remainder following this extension must contain enough separators
			// to reach the desired number of fields.
			//
			// If we do not find quote+separator combination we treat the current
			// field as broken and use the original token as is.
			//
			// If this happens inside the final field of a line we must handle
			// buffer offsets differently (there is no following separator).
			i, ok, eol := nextClose(next, r.sep, n, r.numFields)
			if eol {
				if i > 0 && next[i-1] == byte(Quote) {
					// strip quotes
					token = token[1 : len(token)+i]
					next = nil
					// fmt.Printf("F%d EOL1 token=%q\n", n, token)
				} else {
					token = token[:len(token)+i]
					next = nil
					// fmt.Printf("F%d EOL2 token=%q\n", n, token)
				}
				checkDouble = true
			} else if ok {
				// extend token, strip quotes
				token = token[1 : len(token)+i+1]
				next = next[min(len(next), i+2):]
				checkDouble = true
				// fmt.Printf("F%d found end quote token=%q, rest=%q\n", n, string(token), string(next))
			}

		default:
			// empty or unquoted text, use as is
			// fmt.Printf("F%d use unquoted %q\n", n, token)
		}

		// check for and replace double quotes
		if checkDouble {
			if bytes.Contains(token, DoubleQuote) {
				token = bytes.ReplaceAll(token, DoubleQuote, []byte{byte(Quote)})
			}
			checkDouble = false
		}

		// use token
		r.record[n] = util.UnsafeGetString(token)
		line = next
		n++

		// empty last field after a separator
		if hadSep && len(next) == 0 && n == r.numFields-1 {
			r.record[n] = ""
			n++
		}
	}

	// check if we consumed all content
	if len(line) > 0 {
		// log.Warn(string(src))
		return nil, &DecodeError{
			r.lineNo, 0, "",
			fmt.Sprintf("line contains more fields than expected (%d/%d)",
				n+bytes.Count(line, sep)+1, r.numFields),
			ErrParse,
		}
	}

	// check if we missed fields by erroneously merging broken fields
	if n < r.numFields {
		// log.Warn(string(src))
		return nil, &DecodeError{
			r.lineNo, 0, "",
			fmt.Sprintf("line contains less fields than expected (%d/%d)", n, r.numFields),
			ErrParse,
		}
	}

	return r.record, nil
}
