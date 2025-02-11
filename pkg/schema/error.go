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
	ErrEnumUndefined     = errors.New("enum translation not registered")
	ErrDuplicateName     = errors.New("duplicate name")
	ErrInvalidValue      = errors.New("invalid value")
	ErrInvalidValueType  = errors.New("invalid value type")
	ErrInvalidResultType = errors.New("invalid result type")
	ErrInvalidField      = errors.New("invalid field")
	ErrOverflow          = errors.New("integer overflow")
	ErrShortValue        = errors.New("value too short")
	ErrShortBuffer       = io.ErrShortBuffer
	ErrSchemaMismatch    = errors.New("schema mismatch")
	ErrDeletePrimary     = errors.New("cannot delete primary key field")
	ErrDeleteIndexed     = errors.New("cannot delete indexed field")
	ErrRenameEnum        = errors.New("cannot rename enum field")
)
