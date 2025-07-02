// Copyright (c) 2025 Blockwatch Data Inc.
// Author: oliver@blockwatch.cc

package containers

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func u128(a, b uint64) [2]uint64 {
	return [2]uint64{a, b}
}

func TestValidateTransferFlags(t *testing.T) {
	tests := []struct {
		name      string
		transfer  Transfer
		expectErr bool
	}{
		{
			name: "valid pending transfer",
			transfer: Transfer{
				ID:              u128(1, 1),
				DebitAccountID:  u128(10, 10),
				CreditAccountID: u128(20, 20),
				Amount:          100,
				Flags:           FlagPending,
			},
		},
		{
			name: "valid post transfer",
			transfer: Transfer{
				ID:              u128(2, 2),
				DebitAccountID:  u128(10, 10),
				CreditAccountID: u128(20, 20),
				Amount:          100,
				PendingID:       u128(1, 1),
				Flags:           FlagPost,
			},
		},
		{
			name: "invalid: post + void",
			transfer: Transfer{
				ID:              u128(3, 3),
				DebitAccountID:  u128(10, 10),
				CreditAccountID: u128(20, 20),
				Amount:          100,
				Flags:           FlagPost | FlagVoid,
			},
			expectErr: true,
		},
		{
			name: "invalid: zero ID",
			transfer: Transfer{
				DebitAccountID:  u128(10, 10),
				CreditAccountID: u128(20, 20),
				Amount:          100,
				Flags:           FlagPending,
			},
			expectErr: true,
		},
		{
			name: "invalid: zero amount",
			transfer: Transfer{
				ID:              u128(4, 4),
				DebitAccountID:  u128(10, 10),
				CreditAccountID: u128(20, 20),
				Amount:          0,
				Flags:           FlagPending,
			},
			expectErr: true,
		},
		{
			name: "invalid: same debit and credit",
			transfer: Transfer{
				ID:              u128(5, 5),
				DebitAccountID:  u128(10, 10),
				CreditAccountID: u128(10, 10),
				Amount:          50,
				Flags:           FlagPending,
			},
			expectErr: true,
		},
		{
			name: "invalid: post without pending_id",
			transfer: Transfer{
				ID:              u128(6, 6),
				DebitAccountID:  u128(10, 10),
				CreditAccountID: u128(20, 20),
				Amount:          50,
				Flags:           FlagPost,
			},
			expectErr: true,
		},
		{
			name: "invalid: pending with pending_id",
			transfer: Transfer{
				ID:              u128(7, 7),
				DebitAccountID:  u128(10, 10),
				CreditAccountID: u128(20, 20),
				Amount:          50,
				Flags:           FlagPending,
				PendingID:       u128(99, 99),
			},
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateTransferFlags(tt.transfer)
			if tt.expectErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestValidateAllTransferConstraints(t *testing.T) {
	validPending := Transfer{
		ID:              u128(1, 1),
		DebitAccountID:  u128(10, 10),
		CreditAccountID: u128(20, 20),
		Amount:          100,
		Flags:           FlagPending,
	}
	validPost := Transfer{
		ID:              u128(2, 2),
		DebitAccountID:  u128(10, 10),
		CreditAccountID: u128(20, 20),
		Amount:          100,
		PendingID:       validPending.ID,
		Flags:           FlagPost,
	}
	invalidDuplicate := Transfer{
		ID:              validPending.ID,
		DebitAccountID:  u128(11, 11),
		CreditAccountID: u128(21, 21),
		Amount:          200,
		Flags:           FlagPending,
	}

	t.Run("valid batch", func(t *testing.T) {
		err := ValidateAllTransferConstraints([]Transfer{validPending, validPost})
		require.NoError(t, err)
	})

	t.Run("duplicate ID", func(t *testing.T) {
		err := ValidateAllTransferConstraints([]Transfer{validPending, invalidDuplicate})
		require.Error(t, err)
		require.Contains(t, err.Error(), "duplicate transfer id")
	})

	t.Run("missing pending reference", func(t *testing.T) {
		missing := validPost
		missing.PendingID = u128(99, 99)
		err := ValidateAllTransferConstraints([]Transfer{validPost, missing})
		require.Error(t, err)
		require.Contains(t, err.Error(), "missing referenced pending_id")
	})

	t.Run("duplicate pending_id references", func(t *testing.T) {
		void := validPost
		void.ID = u128(3, 3)
		void.Flags = FlagVoid
		err := ValidateAllTransferConstraints([]Transfer{validPending, validPost, void})
		require.Error(t, err)
		require.Contains(t, err.Error(), "referenced multiple times")
	})
}
