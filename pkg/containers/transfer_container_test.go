// Copyright (c) 2025 Blockwatch Data Inc.
// Author: oliver@blockwatch.cc

package containers

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func makeID(lo, hi uint64) [2]uint64 {
	return [2]uint64{lo, hi}
}

func TestValidateTransferFlags(t *testing.T) {
	tcs := []struct {
		name   string
		input  Transfer
		hasErr bool
	}{
		{
			"valid pending",
			Transfer{ID: makeID(1, 1), Flags: FlagPending},
			false,
		},
		{
			"invalid: pending with pending_id",
			Transfer{ID: makeID(1, 1), Flags: FlagPending, PendingID: makeID(99, 0)},
			hasErr: true,
		},
		{
			"valid post",
			Transfer{ID: makeID(2, 2), Flags: FlagPost, PendingID: makeID(1, 1)},
			false,
		},
		{
			"invalid: post with no pending_id",
			Transfer{ID: makeID(3, 3), Flags: FlagPost},
			true,
		},
		{
			"invalid: post and void",
			Transfer{ID: makeID(4, 4), Flags: FlagPost | FlagVoid, PendingID: makeID(1, 1)},
			true,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateTransferFlags(tc.input)
			if tc.hasErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestCheckUniqueTransferIDs(t *testing.T) {
	transfers := []Transfer{
		{ID: makeID(1, 1)},
		{ID: makeID(2, 2)},
		{ID: makeID(1, 1)}, // duplicate
	}
	err := CheckUniqueTransferIDs(transfers)
	assert.Error(t, err)
}

func TestValidatePendingReferences(t *testing.T) {
	pending := Transfer{ID: makeID(1, 1), Flags: FlagPending}
	post := Transfer{ID: makeID(2, 2), Flags: FlagPost, PendingID: makeID(1, 1)}
	void := Transfer{ID: makeID(3, 3), Flags: FlagVoid, PendingID: makeID(1, 1)}
	bad := Transfer{ID: makeID(4, 4), Flags: FlagPost, PendingID: makeID(9, 9)}

	t.Run("valid post/void", func(t *testing.T) {
		err := ValidatePendingReferences([]Transfer{pending, post, void})
		assert.NoError(t, err)
	})

	t.Run("invalid missing pending_id", func(t *testing.T) {
		err := ValidatePendingReferences([]Transfer{post, bad})
		assert.Error(t, err)
	})
}
