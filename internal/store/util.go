// Copyright (c) 2018-2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package store

// Range is a key range.
type Range struct {
	// Start of the key range, include in the range.
	Start []byte

	// Limit of the key range, not include in the range.
	Limit []byte
}

// BytesPrefix returns key range that satisfy the given prefix.
// This only applicable for the standard 'bytes comparer'.
func BytesPrefix(prefix []byte) *Range {
	var limit []byte
	for i := len(prefix) - 1; i >= 0; i-- {
		c := prefix[i]
		if c < 0xff {
			limit = make([]byte, i+1)
			copy(limit, prefix)
			limit[i] = c + 1
			break
		}
	}
	return &Range{prefix, limit}
}

// CommitAndContinue commits the current transaction and
// opens a new transaction of the same type. This is useful
// to batch commit large quantities of data in a loop.
func CommitAndContinue(tx Tx) (Tx, error) {
	db := tx.DB()
	iswrite := tx.IsWriteable()
	err := tx.Commit()
	if err != nil {
		return nil, err
	}
	return db.Begin(iswrite)
}
