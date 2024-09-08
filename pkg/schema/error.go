// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package schema

import (
	"errors"
	"io"
)

var (
	ErrNilValue          = errors.New("nil value")
	ErrNameTooLong       = errors.New("name too long")
	ErrEnumFull          = errors.New("enum capacity exhausted")
	ErrDuplicateName     = errors.New("duplicate name")
	ErrInvalidValue      = errors.New("invalid value")
	ErrInvalidValueType  = errors.New("invalid value type")
	ErrInvalidResultType = errors.New("invalid result type")
	ErrInvalidField      = errors.New("invalid field")
	ErrShortValue        = errors.New("value too short")
	ErrShortBuffer       = io.ErrShortBuffer
	ErrNotImplemented    = errors.New("not implemented")
	ErrSchemaMismatch    = errors.New("schema mismatch")
)
