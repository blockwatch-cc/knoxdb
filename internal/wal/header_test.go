// Copyright (c) 2024 Blockwatch Data Inc.
// Author: oliver@blockwatch.cc

package wal

import (
	"fmt"
	"math"
	"testing"

	"blockwatch.cc/knoxdb/internal/types"
	"github.com/stretchr/testify/require"
)

func genHeader(typ RecordType, tag types.ObjectTag, entity, xid uint64, bodysz int) (head RecordHeader) {
	head.SetType(typ)
	head.SetTag(tag)
	head.SetTxId(xid)
	head.SetEntity(entity)
	head.SetBodySize(bodysz)
	return
}

var headerTests = []struct {
	name        string
	header      RecordHeader
	lastTxID    uint64
	currentLSN  LSN
	maxWalLSN   LSN
	expectError bool
}{
	{
		name:        "Valid header",
		header:      genHeader(RecordTypeInsert, types.ObjectTagDatabase, 1, 100, 1000),
		lastTxID:    50,
		currentLSN:  1000,
		maxWalLSN:   1024 * 1024,
		expectError: false,
	},
	{
		name:        "Invalid record type",
		header:      genHeader(255, types.ObjectTagDatabase, 1, 100, 1000),
		lastTxID:    50,
		currentLSN:  1000,
		maxWalLSN:   1024 * 1024,
		expectError: true,
	},
	{
		name:        "Invalid object tag",
		header:      genHeader(RecordTypeInsert, 255, 1, 100, 1000),
		lastTxID:    50,
		currentLSN:  1000,
		maxWalLSN:   1024 * 1024,
		expectError: true,
	},
	{
		name:        "Zero TxID for non-checkpoint",
		header:      genHeader(RecordTypeInsert, types.ObjectTagDatabase, 1, 0, 1000),
		lastTxID:    50,
		currentLSN:  1000,
		maxWalLSN:   1024 * 1024,
		expectError: true,
	},
	{
		name:        "Valid zero TxID for checkpoint",
		header:      genHeader(RecordTypeCheckpoint, types.ObjectTagDatabase, 1, 0, 0),
		lastTxID:    50,
		currentLSN:  1000,
		maxWalLSN:   1024 * 1024,
		expectError: false,
	},
	{
		name:        "Non zero TxID for checkpoint",
		header:      genHeader(RecordTypeCheckpoint, types.ObjectTagDatabase, 1, 2, 0),
		lastTxID:    50,
		currentLSN:  1000,
		maxWalLSN:   1024 * 1024,
		expectError: true,
	},
	{
		name:        "Invalid body size for checkpoint",
		header:      genHeader(RecordTypeCheckpoint, types.ObjectTagDatabase, 1, 0, 10),
		lastTxID:    50,
		currentLSN:  1000,
		maxWalLSN:   1024 * 1024,
		expectError: true,
	},
	{
		name:        "Maximum allowed record size",
		header:      genHeader(RecordTypeInsert, types.ObjectTagDatabase, 1, 100, 1024*1024-HeaderSize),
		lastTxID:    50,
		currentLSN:  0,
		maxWalLSN:   1024 * 1024,
		expectError: false,
	},
	{
		name:        "Record exactly fills remaining WAL space",
		header:      genHeader(RecordTypeInsert, types.ObjectTagDatabase, 1, 100, 1024*1024-HeaderSize),
		lastTxID:    50,
		currentLSN:  0,
		maxWalLSN:   1024 * 1024,
		expectError: false,
	},
	{
		name:        "Maximum LSN value",
		header:      genHeader(RecordTypeInsert, types.ObjectTagDatabase, 1, 100, 100),
		lastTxID:    50,
		currentLSN:  LSN(math.MaxInt64 - HeaderSize - 100),
		maxWalLSN:   math.MaxInt64,
		expectError: false,
	},
}

func TestHeader(t *testing.T) {
	for _, tt := range headerTests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.header.Validate(tt.lastTxID, tt.currentLSN, tt.maxWalLSN)
			if tt.expectError {
				require.Error(t, err, fmt.Sprintf("%#v", tt.header))
			} else {
				require.NoError(t, err, fmt.Sprintf("%#v", tt.header))
			}
		})
	}
}

// go test ./internal/wal -fuzz=HeaderCheck -fuzztime 5s
func FuzzHeaderCheck(f *testing.F) {
	// Add seed inputs
	for _, tt := range headerTests {
		f.Add(tt.header[:])
	}

	f.Fuzz(func(t *testing.T, buf []byte) {
		var head RecordHeader
		copy(head[:], buf)

		// Perform the check
		maxWal := LSN(1 << 31)
		err := head.Validate(head.TxId(), 0, maxWal)
		if err != nil {
			t.Skip()
		}

		// catch unexpected cases
		if LSN(0).Add(head.BodySize()) > maxWal {
			t.Logf("Input: RecordType=%d, Tag=%c, EntityID=%d, TxID=%d, BodyLen=%d, MaxWalSize=%d",
				head.Type(), head.Tag(), head.Entity(), head.TxId(), head.BodySize(), maxWal)
			t.Error("body too large")
		}
	})
}
