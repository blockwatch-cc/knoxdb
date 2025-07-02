// Copyright (c) 2025 Blockwatch Data Inc.
// Author: oliver@blockwatch.cc

package containers

import (
	"blockwatch.cc/knoxdb/internal/pack"
)

// AccountContainer defines the layout of a TigerBeetle Account record in KnoxDB.
type AccountContainer struct {
	ID             [2]uint64 `knox:"id,bitpacked"`
	DebitsPending  uint64    `knox:"debits_pending,alp_rd"`
	CreditsPending uint64    `knox:"credits_pending,alp_rd"`
	DebitsPosted   uint64    `knox:"debits_posted,alp_rd"`
	CreditsPosted  uint64    `knox:"credits_posted,alp_rd"`
	Ledger         uint32    `knox:"ledger,dict"`
	Code           uint16    `knox:"code,dict"`
	Flags          uint8     `knox:"flags,raw"`
}

func init() {
	pack.RegisterContainer[AccountContainer]("account")
}
