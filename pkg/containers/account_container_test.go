// Copyright (c) 2025 Blockwatch Data Inc.
// Author: oliver@blockwatch.cc

package containers

import (
	"testing"

	"blockwatch.cc/knoxdb/internal/pack"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func makeAccount(lo uint64) AccountContainer {
	return AccountContainer{
		ID:             [2]uint64{lo, lo + 1},
		DebitsPending:  lo * 10,
		CreditsPending: lo * 5,
		DebitsPosted:   lo * 20,
		CreditsPosted:  lo * 25,
		Ledger:         uint32(lo % 3),
		Code:           uint16(lo % 7),
		Flags:          uint8(lo % 4),
	}
}

func TestAccountContainer_Roundtrip(t *testing.T) {
	var rows []AccountContainer
	for i := uint64(1); i <= 100; i++ {
		rows = append(rows, makeAccount(i))
	}

	enc, err := pack.EncodeContainer(rows)
	require.NoError(t, err)
	t.Logf("Encoded size: %d bytes", len(enc))

	var decoded []AccountContainer
	err = pack.DecodeContainer(enc, &decoded)
	require.NoError(t, err)

	require.Equal(t, len(rows), len(decoded))
	for i := range rows {
		assert.Equal(t, rows[i], decoded[i])
	}
}

func makeTransfer(lo uint64) Transfer {
	return Transfer{
		ID:              [2]uint64{lo, lo + 1},
		DebitAccountID:  [2]uint64{100 + lo, 100 + lo + 1},
		CreditAccountID: [2]uint64{200 + lo, 200 + lo + 1},
		Amount:          lo * 1000,
		PendingID:       [2]uint64{0, 0},
		Ledger:          uint32(lo % 3),
		Code:            uint16(lo % 7),
		Flags:           uint8(lo % 4),
		Timestamp:       1680000000 + lo,
	}
}

func TestTransferContainer_Roundtrip(t *testing.T) {
	var rows []Transfer
	for i := uint64(1); i <= 100; i++ {
		rows = append(rows, makeTransfer(i))
	}

	enc, err := pack.EncodeContainer(rows)
	require.NoError(t, err)
	t.Logf("Encoded size: %d bytes", len(enc))

	var decoded []Transfer
	err = pack.DecodeContainer(enc, &decoded)
	require.NoError(t, err)

	require.Equal(t, len(rows), len(decoded))
	for i := range rows {
		assert.Equal(t, rows[i], decoded[i])
	}
}
