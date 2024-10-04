// Copyright (c) 2024 Blockwatch Data Inc.
// Author: abdul@blockwatch.cc

package wal

import (
	"errors"
	"io"
	"math/rand"
	"testing"

	"blockwatch.cc/knoxdb/internal/types"
)

type TestCase struct {
	Name     string
	DataSize uint64
	Fn       func(*testing.T, *bufferedReader)
}

func generateRecord(data []byte) *Record {
	return &Record{
		Type:   RecordTypeCommit,
		Tag:    types.ObjectTagDatabase,
		Entity: 100,
		Data:   data,
		TxID:   rand.Uint64(),
	}
}

var bufferReadTestCases = []TestCase{
	{
		Name:     "can read a segment",
		DataSize: 50,
	},
	{
		Name:     "can read across two segments",
		DataSize: 100,
	},
	{
		Name:     "can read across 10 segments",
		DataSize: 1000,
	},
	{
		Name:     "cannot read after reading to end of segment",
		DataSize: 70,
		Fn: func(t *testing.T, b *bufferedReader) {
			_, err := b.Read(100)
			if err != nil {
				t.Errorf("failed to read: %v", err)
			}
			_, err = b.Read(HeaderSize)
			if !errors.Is(err, io.EOF) {
				t.Errorf("reading end of segment should return EOF: %v", err)
			}
		},
	},
	{
		Name:     "cannot read after buffered reader is closed",
		DataSize: 50,
		Fn: func(t *testing.T, b *bufferedReader) {
			err := b.Close()
			if err != nil {
				t.Errorf("failed to read: %v", err)
			}
			_, err = b.Read(HeaderSize)
			if !errors.Is(err, ErrClosed) {
				t.Errorf("reading closed buffered reader was successful: %v", err)
			}
		},
	},
}
