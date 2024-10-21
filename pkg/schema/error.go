// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package schema

import (
	"errors"
	"fmt"
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
	ErrShortValue        = errors.New("value too short")
	ErrShortBuffer       = io.ErrShortBuffer
	ErrNotImplemented    = errors.New("not implemented")
	ErrSchemaMismatch    = errors.New("schema mismatch")
	ErrOverflow          = errors.New("value overflow")
	ErrFixedSizeMismatch = errors.New("fixed size mismatch")
	ErrUnsupportedArray  = errors.New("unsupported array type")
)

// FieldError wraps field-specific errors
type FieldError struct {
	FieldName string
	FieldType string
	Err       error
}

func (e *FieldError) Error() string {
	return fmt.Sprintf("field error (%s, %s): %v", e.FieldName, e.FieldType, e.Err)
}

func (e *FieldError) Unwrap() error {
	return e.Err
}

// NewFieldError creates a new FieldError
func NewFieldError(fieldName, fieldType string, err error) error {
	return &FieldError{
		FieldName: fieldName,
		FieldType: fieldType,
		Err:       err,
	}
}
