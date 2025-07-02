// Copyright (c) 2025 Blockwatch Data Inc.
// Author: oliver@blockwatch.cc

package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"

	"blockwatch.cc/knoxdb/internal/pack"
	"blockwatch.cc/knoxdb/pkg/containers"
	"blockwatch.cc/knoxdb/pkg/knox"
	"blockwatch.cc/knoxdb/pkg/util"
)

type Account struct {
	ID             [2]uint64 `json:"id"`
	DebitsPending  uint64    `json:"debits_pending"`
	CreditsPending uint64    `json:"credits_pending"`
	DebitsPosted   uint64    `json:"debits_posted"`
	CreditsPosted  uint64    `json:"credits_posted"`
	Ledger         uint32    `json:"ledger"`
	Code           uint16    `json:"code"`
	Flags          uint8     `json:"flags"`
}

type Transfer struct {
	ID              [2]uint64 `json:"id"`
	DebitAccountID  [2]uint64 `json:"debit_account_id"`
	CreditAccountID [2]uint64 `json:"credit_account_id"`
	Amount          uint64    `json:"amount"`
	PendingID       [2]uint64 `json:"pending_id"`
	Ledger          uint32    `json:"ledger"`
	Code            uint16    `json:"code"`
	Flags           uint8     `json:"flags"`
	Timestamp       uint64    `json:"timestamp"`
}

func main() {
	input := flag.String("input", "", "Path to JSON file containing account or transfer records")
	output := flag.String("output", "", "Output path for .knox file")
	mode := flag.String("mode", "transfer", "Either 'account' or 'transfer'")
	flag.Parse()

	if *input == "" || *output == "" {
		fmt.Fprintln(os.Stderr, "--input and --output are required")
		os.Exit(1)
	}

	ctx := context.Background()
	if err := runIngest(ctx, *input, *output, *mode); err != nil {
		fmt.Fprintln(os.Stderr, "ERROR:", err)
		os.Exit(1)
	}
}

func runIngest(ctx context.Context, input, output, mode string) error {
	data, err := os.ReadFile(input)
	if err != nil {
		return fmt.Errorf("reading input: %w", err)
	}

	switch strings.ToLower(mode) {
	case "account":
		var rows []Account
		if err := json.Unmarshal(data, &rows); err != nil {
			return fmt.Errorf("decoding account JSON: %w", err)
		}
		var cont []containers.AccountContainer
		for _, row := range rows {
			cont = append(cont, containers.AccountContainer(row))
		}
		return encodePack(output, cont)

	case "transfer":
		var rows []Transfer
		if err := json.Unmarshal(data, &rows); err != nil {
			return fmt.Errorf("decoding transfer JSON: %w", err)
		}
		var tlist []containers.Transfer
		for _, row := range rows {
			tlist = append(tlist, containers.Transfer(row))
		}
		if err := containers.ValidateAllTransferConstraints(tlist); err != nil {
			return fmt.Errorf("transfer constraint check failed: %w", err)
		}
		var cont []containers.TransferContainer
		for _, row := range tlist {
			cont = append(cont, containers.TransferContainer(row))
		}
		return encodePack(output, cont)

	default:
		return fmt.Errorf("unknown mode: %s", mode)
	}
}

func encodePack[T any](out string, slice []T) error {
	f, err := os.Create(out)
	if err != nil {
		return fmt.Errorf("creating output file: %w", err)
	}
	defer f.Close()

	w, err := stream.NewWriter(f, &pack.WriterOptions{})
	if err != nil {
		return fmt.Errorf("initializing stream writer: %w", err)
	}
	defer w.Close()

	return w.Append(slice)
}
