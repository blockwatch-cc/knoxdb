// Copyright (c) 2024-2026 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package schema

import (
	"errors"
)

var (
	ErrNilValue         = errors.New("schema: nil value")
	ErrLongValue        = errors.New("schema: value too long")
	ErrShortValue       = errors.New("schema: value too short")
	ErrUnsupportedType  = errors.New("schema: unsupported type")
	ErrDuplicateName    = errors.New("schema: duplicate field name")
	ErrDuplicateId      = errors.New("schema: duplicate field id")
	ErrInvalidField     = errors.New("schema: invalid field")
	ErrInvalidValueType = errors.New("schema: invalid value type")
	ErrInvalidResult    = errors.New("schema: invalid result type")
	ErrInvalidParent    = errors.New("schema: invalid parent field")
	ErrInvalidEnum      = errors.New("schema: invalid enum value")
	ErrShortBuffer      = errors.New("schema: short buffer")
	ErrSchemaMismatch   = errors.New("schema: mismatch")
	ErrDeletePrimary    = errors.New("schema: cannot delete primary key field")
	ErrRenameEnum       = errors.New("schema: cannot rename enum field")
	ErrEnumUndefined    = errors.New("schema: missing enum dictionary")
)
