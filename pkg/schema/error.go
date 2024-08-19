// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package schema

import (
	"errors"
	"io"
)

var (
	EndStream            = errors.New("end stream")
	ErrNoTable           = errors.New("unknown table")
	ErrNoStore           = errors.New("unknown store")
	ErrNilValue          = errors.New("nil value")
	ErrInvalidValueType  = errors.New("invalid value type")
	ErrInvalidResultType = errors.New("invalid result type")
	ErrInvalidField      = errors.New("invalid field")
	ErrShortValue        = errors.New("value too short")
	ErrShortBuffer       = io.ErrShortBuffer
	ErrNotImplemented    = errors.New("not implemented")
)
