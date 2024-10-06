// Copyright (c) 2024 Blockwatch Data Inc.
// Author: oliver@blockwatch.cc

package wal

import (
	"fmt"
	"io"
	"sync"
	"testing"

	"blockwatch.cc/knoxdb/internal/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestReaderFilteredReading tests the WAL's ability to read records using various filters,
// ensuring that only records matching the specified criteria are returned.
func TestReaderFilter(t *testing.T) {
	opts := createWalOptions(t)
	w := createWal(t, opts)
	defer w.Close()

	// Write test records
	records := []*Record{
		{Type: RecordTypeInsert, Tag: types.ObjectTagDatabase, Entity: 1, TxID: 100, Data: []byte("data1")},
		{Type: RecordTypeUpdate, Tag: types.ObjectTagStore, Entity: 2, TxID: 200, Data: []byte("data2")},
		{Type: RecordTypeDelete, Tag: types.ObjectTagStream, Entity: 3, TxID: 300, Data: []byte("data3")},
	}

	for _, rec := range records {
		_, err := w.Write(rec)
		require.NoError(t, err)
	}

	tests := []struct {
		name   string
		filter func(WalReader) WalReader
		expect *Record
	}{
		{"FilterByType", func(r WalReader) WalReader { return r.WithType(RecordTypeInsert) }, records[0]},
		{"FilterByTag", func(r WalReader) WalReader { return r.WithTag(types.ObjectTagDatabase) }, records[0]},
		{"FilterByEntity", func(r WalReader) WalReader { return r.WithEntity(2) }, records[1]},
		{"FilterByTxID", func(r WalReader) WalReader { return r.WithTxID(300) }, records[2]},
		{"CombinedFilters", func(r WalReader) WalReader { return r.WithType(RecordTypeUpdate).WithTag(types.ObjectTagStore) }, records[1]},
		{"NoMatchFilter", func(r WalReader) WalReader { return r.WithType(RecordTypeInsert).WithTag(types.ObjectTagStore) }, nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := tt.filter(w.NewReader())
			rec, err := reader.Next()
			if tt.expect == nil {
				assert.Equal(t, io.EOF, err, "Expected EOF when no records match the filter")
			} else {
				if assert.NoError(t, err) {
					t.Logf("Read record: %+v", rec)
					assert.Equal(t, tt.expect.Type, rec.Type, "Record type mismatch")
					assert.Equal(t, tt.expect.Tag, rec.Tag, "Record tag mismatch")
					assert.Equal(t, tt.expect.Entity, rec.Entity, "Record entity mismatch")
					assert.Equal(t, tt.expect.TxID, rec.TxID, "Record TxID mismatch")
					assert.Equal(t, tt.expect.Data, rec.Data, "Record data mismatch")
				}
			}
		})
	}
}

// TestReaderSeek tests the WAL reader's ability to seek to specific positions within the log,
// verifying that it can accurately locate and read records from different LSNs
func TestReaderSeek(t *testing.T) {
	opts := createWalOptions(t)
	w := createWal(t, opts)
	defer w.Close()

	records := []*Record{
		{Type: RecordTypeInsert, Tag: types.ObjectTagDatabase, Entity: 1, TxID: 100, Data: []byte("data1")},
		{Type: RecordTypeCheckpoint, Tag: types.ObjectTagDatabase},
		{Type: RecordTypeUpdate, Tag: types.ObjectTagDatabase, Entity: 2, TxID: 200, Data: []byte("data2")},
		{Type: RecordTypeCheckpoint, Tag: types.ObjectTagDatabase},
		{Type: RecordTypeDelete, Tag: types.ObjectTagDatabase, Entity: 3, TxID: 300, Data: []byte("data3")},
		{Type: RecordTypeCheckpoint, Tag: types.ObjectTagDatabase},
		{Type: RecordTypeInsert, Tag: types.ObjectTagDatabase, Entity: 4, TxID: 400, Data: []byte("data4")},
		{Type: RecordTypeCheckpoint, Tag: types.ObjectTagDatabase},
		{Type: RecordTypeInsert, Tag: types.ObjectTagDatabase, Entity: 4, TxID: 400, Data: []byte("data4")},
	}

	lsns := make([]LSN, len(records))
	for i, rec := range records {
		lsn, err := w.Write(rec)
		require.NoError(t, err)
		lsns[i] = lsn
	}

	reader := w.NewReader()
	defer reader.Close()

	for i, lsn := range lsns {
		if i%2 == 0 {
			continue
		}
		t.Logf("Seeking to LSN: %v", lsn)
		err := reader.Seek(lsn)
		require.NoError(t, err)

		rec, err := reader.Next()
		require.NoError(t, err)
		t.Logf("Read record: %+v", rec)
		rec.Lsn = 0
		assert.Equal(t, records[i+1], rec)
	}

	// Test seeking beyond the end
	invalidLSN := LSN(uint64(lsns[len(lsns)-1]) + SEG_FILE_MINSIZE)
	err := reader.Seek(invalidLSN)
	assert.Error(t, err, "Expected error when seeking to invalid LSN")
}

// TestReaderSeekInvalidLSN tests the WAL's behavior when attempting to seek to or read
// from invalid LSNs, ensuring proper error handling and system stability.
func TestReaderSeekInvalidLSN(t *testing.T) {
	opts := WalOptions{
		Path:           t.TempDir(),
		MaxSegmentSize: SEG_FILE_MINSIZE,
		Seed:           12345,
	}

	w, err := Create(opts)
	require.NoError(t, err, "Failed to create WAL")
	defer w.Close()

	// Write a valid record to ensure the WAL is initialized
	validRec := &Record{
		Type: RecordTypeCheckpoint,
		Tag:  types.ObjectTagDatabase,
		TxID: 0,
		// Data: bytes.Repeat([]byte("valid data"), SEG_FILE_MINSIZE),
	}
	validLSN, err := w.Write(validRec)
	require.NoError(t, err, "Failed to write valid record")
	t.Logf("Valid LSN: %v", validLSN)

	// Define invalid LSN scenarios
	invalidLSNs := []struct {
		name string
		lsn  LSN
	}{
		{"OutOfBoundsSegment", LSN(100 * opts.MaxSegmentSize)},
		{"OutOfBoundsOffset", LSN(opts.MaxSegmentSize + 100)},
	}

	for _, tc := range invalidLSNs {
		t.Run(tc.name, func(t *testing.T) {
			reader := w.NewReader()
			defer reader.Close()

			// Seek to invalid LSN
			err := reader.Seek(tc.lsn)
			require.Error(t, err, "Expected error when reading after seeking to invalid LSN %v", tc.lsn)
		})
	}

	// Test seeking to a valid LSN after invalid attempts
	t.Run("SeekToValidLSNAfterInvalid", func(t *testing.T) {
		reader := w.NewReader()
		defer reader.Close()

		// First, try an invalid seek
		err := reader.Seek(LSN(1<<64 - 1))
		require.ErrorIs(t, io.EOF, err, "Seek to out-of-bound LSN should return an error")

		// Now, seek to the valid LSN
		err = reader.Seek(validLSN)
		require.NoError(t, err, "Failed to seek to valid LSN after invalid attempt")

		// Try to read the valid record
		readRec, err := reader.Next()
		require.NoError(t, err, "Failed to read valid record after invalid LSN attempts")
		require.Equal(t, []byte(nil), readRec.Data, "Read record data doesn't match written data")
	})
}

// TestReaderNext tests the WAL reader's Next function, ensuring it can correctly
// iterate through records in the log and handle reaching the end of the log.
func TestReaderNext(t *testing.T) {
	opts := createWalOptions(t)
	w := createWal(t, opts)
	defer w.Close()

	// Write some test records
	records := []*Record{
		{Type: RecordTypeInsert, Tag: types.ObjectTagDatabase, Entity: 1, TxID: 100, Data: []byte("data1")},
		{Type: RecordTypeUpdate, Tag: types.ObjectTagDatabase, Entity: 2, TxID: 200, Data: []byte("data2")},
		{Type: RecordTypeDelete, Tag: types.ObjectTagDatabase, Entity: 3, TxID: 300, Data: []byte("data3")},
	}

	for _, rec := range records {
		_, err := w.Write(rec)
		require.NoError(t, err)
	}

	reader := w.NewReader()
	defer reader.Close()

	// Test reading all records
	for i, expected := range records {
		rec, err := reader.Next()
		require.NoError(t, err)
		rec.Lsn = 0
		assert.Equal(t, expected, rec, "Record %d mismatch", i)
	}

	// Test reading beyond the end
	_, err := reader.Next()
	assert.Equal(t, io.EOF, err)
}

// TestTwoSimultaneousReaders verifies that the WAL can handle multiple readers
// simultaneously, ensuring that they can read records independently and correctly.
func TestTwoSimultaneousReaders(t *testing.T) {
	opts := createWalOptions(t)
	w := createWal(t, opts)
	defer w.Close()

	// Write some records
	numRecords := 100
	for i := 1; i <= numRecords; i++ {
		rec := &Record{
			Type:   RecordTypeInsert,
			Tag:    types.ObjectTagDatabase,
			Entity: uint64(i),
			TxID:   uint64(i * 100),
			Data:   []byte(fmt.Sprintf("data%d", i)),
		}
		_, err := w.Write(rec)
		require.NoError(t, err)
	}

	// Create two readers
	reader1 := w.NewReader()
	defer reader1.Close()
	reader2 := w.NewReader()
	defer reader2.Close()

	wg := sync.WaitGroup{}
	// Read alternately from both readers
	readWal := func(r WalReader) {
		defer wg.Done()
		for i := 1; i <= numRecords; i++ {
			var rec *Record
			var err error
			rec, err = r.Next()
			require.NoError(t, err)
			assert.Equal(t, RecordTypeInsert, rec.Type)
			assert.Equal(t, types.ObjectTagDatabase, rec.Tag)
			assert.Equal(t, uint64(i), rec.Entity)
			assert.Equal(t, uint64(i*100), rec.TxID)
			assert.Equal(t, []byte(fmt.Sprintf("data%d", i)), rec.Data)
		}
	}

	wg.Add(1)
	go readWal(reader1)
	wg.Add(1)
	go readWal(reader2)

	wg.Wait()
	// Both readers should be at EOF now
	_, err := reader1.Next()
	assert.Equal(t, io.EOF, err)
	_, err = reader2.Next()
	assert.Equal(t, io.EOF, err)
}

// TestConcurrentReadersLargeDataset tests the WAL's performance and correctness
// when multiple readers are concurrently accessing a large dataset.
func TestConcurrentReadersLargeDataset(t *testing.T) {
	opts := createWalOptions(t)
	w := createWal(t, opts)
	defer w.Close()

	// Write a large number of records
	numRecords := 100000
	for i := 1; i <= numRecords; i++ {
		rec := &Record{
			Type:   RecordTypeInsert,
			Tag:    types.ObjectTagDatabase,
			Entity: uint64(i),
			TxID:   uint64(i * 100),
			Data:   []byte(fmt.Sprintf("data%d", i)),
		}
		_, err := w.Write(rec)
		require.NoError(t, err)
	}

	// Create multiple concurrent readers
	numReaders := 1
	var wg sync.WaitGroup
	errors := make(chan error, numReaders)

	for r := 0; r < numReaders; r++ {
		wg.Add(1)
		go func(readerID int) {
			defer wg.Done()
			reader := w.NewReader()
			defer reader.Close()

			count := 0
			for {
				rec, err := reader.Next()
				if err == io.EOF {
					break
				}
				if err != nil {
					errors <- fmt.Errorf("reader %d error: %v", readerID, err)
					return
				}

				expectedI := count + 1
				assert.Equal(t, RecordTypeInsert, rec.Type)
				assert.Equal(t, types.ObjectTagDatabase, rec.Tag)
				assert.Equal(t, uint64(expectedI), rec.Entity)
				assert.Equal(t, uint64(expectedI*100), rec.TxID)
				assert.Equal(t, []byte(fmt.Sprintf("data%d", expectedI)), rec.Data)

				count++
			}

			if count != numRecords {
				errors <- fmt.Errorf("reader %d read %d records, expected %d", readerID, count, numRecords)
			}
		}(r)
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		t.Error(err)
	}
}

// BenchmarkWalRead tests reading records from the WAL.
func BenchmarkWalRead(b *testing.B) {
	opts := createWalOptions(b)
	w := createWal(b, opts)
	defer w.Close()

	// Write records
	numRecords := 10000
	recordSize := 1024
	lsns := make([]LSN, numRecords)
	data := make([]byte, recordSize)

	for i := 1; i < numRecords; i++ {
		rec := &Record{
			Type:   RecordTypeInsert,
			Tag:    types.ObjectTagDatabase,
			Entity: uint64(i),
			TxID:   uint64(i),
			Data:   data,
		}
		lsn, err := w.Write(rec)
		require.NoError(b, err)
		lsns[i] = lsn
	}

	reader := w.NewReader()
	defer reader.Close()

	b.SetBytes(int64(recordSize))

	patterns := []struct {
		name   string
		random bool
	}{
		{"Sequential", false},
		{"Random", true},
	}

	for _, pattern := range patterns {
		b.Run(pattern.name, func(b *testing.B) {
			b.ResetTimer()
			for i := 1; i < b.N; i++ {
				var lsn LSN
				if pattern.random {
					lsn = lsns[i%numRecords]
				} else {
					lsn = lsns[(i/recordSize)%numRecords]
				}
				err := reader.Seek(lsn)
				require.NoError(b, err)
				_, err = reader.Next()
				require.NoError(b, err)
			}
		})
	}
}
