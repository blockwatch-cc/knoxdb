// Copyright (c) 2025 Blockwatch Data Inc.
// Author: oliver@blockwatch.cc

package containers

import (
	"blockwatch.cc/knoxdb/internal/pack"
)

// TransferContainer defines the layout of a TigerBeetle Transfer record in KnoxDB.
type TransferContainer struct {
	ID              [2]uint64 `knox:"id,bitpacked"`
	DebitAccountID  [2]uint64 `knox:"debit_account_id,bitpacked"`
	CreditAccountID [2]uint64 `knox:"credit_account_id,bitpacked"`
	Amount          uint64    `knox:"amount,alp_rd"`
	PendingID       [2]uint64 `knox:"pending_id,dict"`
	Ledger          uint32    `knox:"ledger,dict"`
	Code            uint16    `knox:"code,dict"`
	Flags           uint8     `knox:"flags,raw"`
	Timestamp       uint64    `knox:"timestamp,delta"`
}

func init() {
	pack.RegisterContainer[TransferContainer]("transfer")
}
