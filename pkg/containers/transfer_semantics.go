// Copyright (c) 2025 Blockwatch Data Inc.
// Author: oliver@blockwatch.cc

package containers

import (
	"errors"
	"fmt"
)

const (
	FlagPending byte = 1 << 0
	FlagPost    byte = 1 << 1
	FlagVoid    byte = 1 << 2
	FlagLinked  byte = 1 << 3
)

type Transfer struct {
	ID              [2]uint64
	DebitAccountID  [2]uint64
	CreditAccountID [2]uint64
	Amount          uint64
	PendingID       [2]uint64
	Ledger          uint32
	Code            uint16
	Flags           byte
	Timestamp       uint64
}

// ValidateTransferFlags checks if the flag combination on a single transfer is logically valid.
func ValidateTransferFlags(t Transfer) error {
	post := t.Flags&FlagPost != 0
	void := t.Flags&FlagVoid != 0
	pending := t.Flags&FlagPending != 0

	if IsZeroID(t.ID) {
		return errors.New("transfer id cannot be zero")
	}
	if t.DebitAccountID == t.CreditAccountID {
		return errors.New("transfer debit and credit account cannot be the same")
	}
	if t.Amount == 0 {
		return errors.New("transfer amount cannot be zero")
	}
	if post && void {
		return errors.New("transfer cannot be both post and void")
	}
	if (post || void) && !HasPendingID(t) {
		return errors.New("post or void transfer must include a pending_id")
	}
	if pending && HasPendingID(t) {
		return errors.New("pending transfer must not have a pending_id")
	}
	return nil
}

// HasPendingID returns true if the transfer contains a non-zero pending_id.
func HasPendingID(t Transfer) bool {
	return t.PendingID[0] != 0 || t.PendingID[1] != 0
}

// IsZeroID returns true if the provided 128-bit ID is all zero.
func IsZeroID(id [2]uint64) bool {
	return id[0] == 0 && id[1] == 0
}

// transferIDKey formats a 128-bit ID as a string for use in maps.
func transferIDKey(id [2]uint64) string {
	return fmt.Sprintf("%016x%016x", id[0], id[1])
}

// CheckUniqueTransferIDs ensures no duplicate transfer IDs exist.
func CheckUniqueTransferIDs(transfers []Transfer) error {
	seen := make(map[string]struct{})
	for _, t := range transfers {
		k := transferIDKey(t.ID)
		if _, ok := seen[k]; ok {
			return fmt.Errorf("duplicate transfer id: %s", k)
		}
		seen[k] = struct{}{}
	}
	return nil
}

// ValidatePendingReferences ensures all post/void transfers reference existing pending transfers.
func ValidatePendingReferences(transfers []Transfer) error {
	pendingIDs := make(map[string]struct{})
	for _, t := range transfers {
		if t.Flags&FlagPending != 0 {
			k := transferIDKey(t.ID)
			pendingIDs[k] = struct{}{}
		}
	}
	for _, t := range transfers {
		if t.Flags&(FlagPost|FlagVoid) != 0 {
			pk := transferIDKey(t.PendingID)
			if _, ok := pendingIDs[pk]; !ok {
				return fmt.Errorf("missing referenced pending_id: %s", pk)
			}
		}
	}
	return nil
}

// CheckPendingIDUniqueness ensures each pending_id is only referenced once by post/void.
func CheckPendingIDUniqueness(transfers []Transfer) error {
	refCount := make(map[string]int)
	for _, t := range transfers {
		if t.Flags&(FlagPost|FlagVoid) != 0 {
			pk := transferIDKey(t.PendingID)
			refCount[pk]++
			if refCount[pk] > 1 {
				return fmt.Errorf("pending_id %s referenced multiple times", pk)
			}
		}
	}
	return nil
}

// ValidateAllTransferConstraints runs all validation checks for a batch of transfers.
func ValidateAllTransferConstraints(transfers []Transfer) error {
	for _, t := range transfers {
		if err := ValidateTransferFlags(t); err != nil {
			return fmt.Errorf("transfer %s: %w", transferIDKey(t.ID), err)
		}
	}
	if err := CheckUniqueTransferIDs(transfers); err != nil {
		return err
	}
	if err := ValidatePendingReferences(transfers); err != nil {
		return err
	}
	if err := CheckPendingIDUniqueness(transfers); err != nil {
		return err
	}
	return nil
}
