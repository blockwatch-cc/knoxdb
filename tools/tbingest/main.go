// Copyright (c) 2025 Blockwatch Data Inc.
// Author: oliver@blockwatch.cc

package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"blockwatch.cc/knoxdb/internal/pack"
	"blockwatch.cc/knoxdb/pkg/containers"
	"blockwatch.cc/knoxdb/pkg/knox"
	"blockwatch.cc/knoxdb/pkg/util"

	"github.com/parquet-go/parquet-go"
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
	input := flag.String("input", "", "Path to input file (.json or .parquet)")
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
	switch ext := strings.ToLower(filepath.Ext(input)); ext {
	case ".json":
		return ingestJSON(ctx, input, output, mode)
	case ".parquet":
		return ingestParquet(ctx, input, output, mode)
	default:
		return fmt.Errorf("unsupported input file extension: %s", ext)
	}
}

func ingestJSON(ctx context.Context, input, output, mode string) error {
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

func ingestParquet(ctx context.Context, input, output, mode string) error {
	f, err := os.Open(input)
	if err != nil {
		return fmt.Errorf("opening parquet file: %w", err)
	}
	defer f.Close()

	reader, err := parquet.NewReader(f)
	if err != nil {
		return fmt.Errorf("initializing parquet reader: %w", err)
	}
	defer reader.Close()

	switch strings.ToLower(mode) {
	case "account":
		var cont []containers.AccountContainer
		for {
			var row Account
			if err := reader.Read(&row); errors.Is(err, parquet.ErrEOF) {
				break
			} else if err != nil {
				return fmt.Errorf("reading account row: %w", err)
			}
			cont = append(cont, containers.AccountContainer(row))
		}
		return encodePack(output, cont)

	case "transfer":
		var tlist []containers.Transfer
		for {
			var row Transfer
			if err := reader.Read(&row); errors.Is(err, parquet.ErrEOF) {
				break
			} else if err != nil {
				return fmt.Errorf("reading transfer row: %w", err)
			}
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
