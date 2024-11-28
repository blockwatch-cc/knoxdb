// Copyright (c) 2018-2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package store_test

import (
	"errors"
	"testing"

	"blockwatch.cc/knoxdb/store"
)

// TestErrorCodeStringer tests the stringized output for the ErrorCode type.
func TestErrorCodeStringer(t *testing.T) {
	tests := []struct {
		in   store.ErrorCode
		want string
	}{
		{store.ErrDbTypeRegistered, "ErrDbTypeRegistered"},
		{store.ErrDbUnknownType, "ErrDbUnknownType"},
		{store.ErrDbDoesNotExist, "ErrDbDoesNotExist"},
		{store.ErrDbExists, "ErrDbExists"},
		{store.ErrDbNotOpen, "ErrDbNotOpen"},
		{store.ErrDbAlreadyOpen, "ErrDbAlreadyOpen"},
		{store.ErrInvalid, "ErrInvalid"},
		{store.ErrCorruption, "ErrCorruption"},
		{store.ErrTxConflict, "ErrTxConflict"},
		{store.ErrTxClosed, "ErrTxClosed"},
		{store.ErrTxNotWritable, "ErrTxNotWritable"},
		{store.ErrBucketNotFound, "ErrBucketNotFound"},
		{store.ErrBucketExists, "ErrBucketExists"},
		{store.ErrBucketNameRequired, "ErrBucketNameRequired"},
		{store.ErrKeyRequired, "ErrKeyRequired"},
		{store.ErrKeyTooLarge, "ErrKeyTooLarge"},
		{store.ErrValueTooLarge, "ErrValueTooLarge"},
		{store.ErrIncompatibleValue, "ErrIncompatibleValue"},
		{store.ErrRetry, "ErrRetry"},
		// {store.ErrBlockNotFound, "ErrBlockNotFound"},
		// {store.ErrBlockExists, "ErrBlockExists"},
		// {store.ErrBlockRegionInvalid, "ErrBlockRegionInvalid"},
		{store.ErrDriverSpecific, "ErrDriverSpecific"},
		{store.ErrNotImplemented, "ErrNotImplemented"},

		{0xffff, "Unknown ErrorCode (65535)"},
	}

	// Detect additional error codes that don't have the stringer added.
	if len(tests)-1 != int(store.TstNumErrorCodes) {
		t.Errorf("It appears an error code was added without adding " +
			"an associated stringer test")
	}

	t.Logf("Running %d tests", len(tests))
	for i, test := range tests {
		result := test.in.String()
		if result != test.want {
			t.Errorf("String #%d\ngot: %s\nwant: %s", i, result,
				test.want)
			continue
		}
	}
}

// TestError tests the error output for the Error type.
func TestError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		in   store.Error
		want string
	}{
		{
			store.Error{Description: "some error"},
			"some error",
		},
		{
			store.Error{Description: "human-readable error"},
			"human-readable error",
		},
		{
			store.Error{
				ErrorCode:   store.ErrDriverSpecific,
				Description: "some error",
				Err:         errors.New("driver-specific error"),
			},
			"some error: driver-specific error",
		},
	}

	t.Logf("Running %d tests", len(tests))
	for i, test := range tests {
		result := test.in.Error()
		if result != test.want {
			t.Errorf("Error #%d\n got: %s want: %s", i, result,
				test.want)
			continue
		}
	}
}
