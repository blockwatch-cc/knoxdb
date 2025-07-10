// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package csv

import (
	"errors"
	"fmt"
)

type DecodeError struct {
	lineNo    int
	fieldNo   int
	fieldName string
	hint      string
	reason    error
}

func (e *DecodeError) Error() string {
	if e.fieldNo != 0 {
		return fmt.Sprintf("csv: line %d field %d (%s) %q: %v", e.lineNo, e.fieldNo, e.fieldName, e.hint, e.reason)
	} else if e.reason == nil {
		return fmt.Sprintf("csv: line %d: %s", e.lineNo, e.hint)
	}
	return fmt.Sprintf("csv: line %d: %v: %s", e.lineNo, e.reason, e.hint)
}

var (
	ErrEmptySlice        = errors.New("empty destination slice")
	ErrUnterminatedQuote = errors.New("unterminated quote")
	ErrInvalidQuotes     = errors.New("invalid quotes")
	ErrParse             = errors.New("parsing error")
)
