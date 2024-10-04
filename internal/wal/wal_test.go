// Copyright (c) 2024 Blockwatch Data Inc.
// Author: oliver@blockwatch.cc, abdul@blockwatch.cc

package wal

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"blockwatch.cc/knoxdb/internal/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type TestingCommon interface {
	Helper()
	TempDir() string
	Errorf(format string, args ...interface{})
	FailNow()
}

// createWal creates a new WAL instance with specified options and returns it.
func createWal(t TestingCommon, opts WalOptions) *Wal {
	t.Helper()
	w, err := Create(opts)
	require.NoError(t, err)
	require.NotNil(t, w)
	return w
}

// createWalOptions create a new WAL options
func createWalOptions(t TestingCommon) WalOptions {
	t.Helper()
	return WalOptions{
		Path:           t.TempDir(),
		MaxSegmentSize: MinSegmentSize,
	}
}

// openWal creates a WAL instance with specified options and returns it.
func openWal(t *testing.T, lsn LSN, opts WalOptions) *Wal {
	t.Helper()
	w, err := Open(lsn, opts)
	require.NoError(t, err)
	require.NotNil(t, w)
	return w
}

// TestWalCreation tests the creation of a new WAL to ensure it initializes correctly.
func TestWalCreation(t *testing.T) {
	opts := createWalOptions(t)
	w := createWal(t, opts)
	defer w.Close()

	assert.NotNil(t, w)
	// Check for the existence of the first segment file
	files, err := os.ReadDir(opts.Path)
	require.NoError(t, err)
	assert.Equal(t, 1, len(files), "Expected one segment file")
	assert.True(t, strings.HasSuffix(files[0].Name(), ".SEG"), "Expected segment file with .SEG extension")
}

// TestWalWrite tests writing multiple records to the WAL to ensure data is written correctly.
func TestWalWrite(t *testing.T) {
	t.Log("Starting TestWalWrite")
	opts := createWalOptions(t)
	w := createWal(t, opts)
	defer w.Close()

	testCases := []struct {
		recordType RecordType
		tag        types.ObjectTag
		entity     uint64
		txID       uint64
		data       string
	}{
		{RecordTypeInsert, types.ObjectTagDatabase, 1, 100, "d1"},
		{RecordTypeUpdate, types.ObjectTagDatabase, 2, 200, "d2"},
		{RecordTypeDelete, types.ObjectTagDatabase, 3, 300, "d3"},
	}

	for _, tc := range testCases {
		rec := &Record{
			Type:   tc.recordType,
			Tag:    tc.tag,
			Entity: tc.entity,
			TxID:   tc.txID,
			Data:   []byte(tc.data),
		}
		_, err := w.Write(rec)
		require.NoError(t, err)
	}

	// Verify that the records were written
	reader := w.NewReader()
	defer reader.Close()

	err := reader.Seek(0)
	require.NoError(t, err)

	for _, expected := range testCases {
		readRec, err := reader.Next()
		require.NoError(t, err)
		assert.Equal(t, expected.recordType, readRec.Type)
		assert.Equal(t, expected.tag, readRec.Tag)
		assert.Equal(t, expected.entity, readRec.Entity)
		assert.Equal(t, expected.txID, readRec.TxID)
		assert.Equal(t, []byte(expected.data), readRec.Data)
	}

	_, err = reader.Next()
	assert.Equal(t, io.EOF, err)
	t.Log("Finished TestWalWrite")
}

// TestWalWriteErrors tests the WAL's error handling when writing records under various error conditions.
func TestWalWriteErrors(t *testing.T) {
	testDir := t.TempDir()

	t.Run("WriteToReadOnlyDir", func(t *testing.T) {
		readOnlyDir := filepath.Join(testDir, "readonly")
		require.NoError(t, os.MkdirAll(readOnlyDir, 0755))
		defer os.RemoveAll(readOnlyDir)

		// Change permissions before WAL creation
		require.NoError(t, os.Chmod(readOnlyDir, 0500))

		_, err := Create(WalOptions{
			Path:           readOnlyDir,
			MaxSegmentSize: 1024 * 1024,
			Seed:           12345,
		})
		assert.Error(t, err, "Expected an error when creating WAL in a read-only directory")
		assert.Contains(t, err.Error(), "permission denied", "Expected a permission denied error")
	})
}

// TestWalRead tests reading records from the WAL to ensure data is read correctly and matches what was written.
func TestWalRead(t *testing.T) {
	opts := createWalOptions(t)
	w := createWal(t, opts)
	defer w.Close()

	testData := []struct {
		recordType RecordType
		tag        types.ObjectTag
		entity     uint64
		txID       uint64
		data       string
	}{
		{RecordTypeInsert, types.ObjectTagDatabase, 1, 100, "data1"},
		{RecordTypeUpdate, types.ObjectTagDatabase, 2, 200, "data2"},
		{RecordTypeDelete, types.ObjectTagDatabase, 3, 300, "data3"},
	}

	for _, td := range testData {
		rec := &Record{
			Type:   td.recordType,
			Tag:    td.tag,
			Entity: td.entity,
			TxID:   td.txID,
			Data:   []byte(td.data),
		}
		_, err := w.Write(rec)
		require.NoError(t, err)
	}

	reader := w.NewReader()
	defer reader.Close()

	err := reader.Seek(0)
	require.NoError(t, err)

	for _, expected := range testData {
		readRec, err := reader.Next()
		assert.NoError(t, err)
		assert.Equal(t, expected.recordType, readRec.Type)
		assert.Equal(t, expected.tag, readRec.Tag)
		assert.Equal(t, expected.entity, readRec.Entity)
		assert.Equal(t, expected.txID, readRec.TxID)
		assert.Equal(t, []byte(expected.data), readRec.Data)
	}

	_, err = reader.Next()
	assert.Equal(t, io.EOF, err)
}

// / TestWalFilteredReading tests the WAL's ability to read records using various filters,
// ensuring that only records matching the specified criteria are returned.
func TestWalFilteredReading(t *testing.T) {
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

// TestWalSeek tests the WAL reader's ability to seek to specific positions within the log,
// verifying that it can accurately locate and read records from different LSNs
func TestWalSeek(t *testing.T) {
	opts := createWalOptions(t)
	w := createWal(t, opts)
	defer w.Close()

	records := []*Record{
		{Type: RecordTypeInsert, Tag: types.ObjectTagDatabase, Entity: 1, TxID: 100, Data: []byte("data1")},
		{Type: RecordTypeUpdate, Tag: types.ObjectTagDatabase, Entity: 2, TxID: 200, Data: []byte("data2")},
		{Type: RecordTypeDelete, Tag: types.ObjectTagDatabase, Entity: 3, TxID: 300, Data: []byte("data3")},
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
		t.Logf("Seeking to LSN: %v", lsn)
		err := reader.Seek(lsn)
		require.NoError(t, err)

		rec, err := reader.Next()
		require.NoError(t, err)
		t.Logf("Read record: %+v", rec)
		assert.Equal(t, records[i].Type, rec.Type)
		assert.Equal(t, records[i].Tag, rec.Tag)
		assert.Equal(t, records[i].Entity, rec.Entity)
		assert.Equal(t, records[i].TxID, rec.TxID)
		assert.Equal(t, records[i].Data, rec.Data)
	}

	// Test seeking beyond the end
	invalidLSN := LSN(uint64(lsns[len(lsns)-1]) + MinSegmentSize)
	err := reader.Seek(invalidLSN)
	assert.Error(t, err, "Expected error when seeking to invalid LSN")
}

// TestWalNext tests the WAL reader's Next function, ensuring it can correctly
// iterate through records in the log and handle reaching the end of the log.
func TestWalNext(t *testing.T) {
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
		assert.Equal(t, expected, rec, "Record %d mismatch", i)
	}

	// Test reading beyond the end
	_, err := reader.Next()
	assert.Equal(t, io.EOF, err)
}

// TestWalClose tests the WAL's Close function, verifying that it properly closes
// the log and prevents further operations after closing.
func TestWalClose(t *testing.T) {
	opts := createWalOptions(t)
	w := createWal(t, opts)

	// Write a test record
	rec := &Record{Type: RecordTypeInsert, Tag: types.ObjectTagDatabase, Entity: 1, TxID: 100, Data: []byte("data1")}
	_, err := w.Write(rec)
	require.NoError(t, err)

	reader := w.NewReader()
	err = reader.Close()
	require.NoError(t, err)

	// Attempt to read after closing
	_, err = reader.Next()
	assert.Error(t, err, "Expected error when reading from closed reader")
	assert.Contains(t, err.Error(), "closed", "Expected 'closed' in error message")

	// Attempt to seek after closing
	err = reader.Seek(LSN(0))
	assert.Error(t, err, "Expected error when seeking with closed reader")
	assert.Contains(t, err.Error(), "closed", "Expected 'closed' in error message")

	// Close the WAL
	err = w.Close()
	require.NoError(t, err)

	// Attempt to write after closing
	_, err = w.Write(rec)
	assert.Error(t, err, "Expected error when writing to closed WAL")

	// Create a new WAL and reader to ensure the WAL is still functional
	w = openWal(t, LSN(0), opts)
	defer w.Close()
	newReader := w.NewReader()
	defer newReader.Close()
	readRec, err := newReader.Next()
	assert.NoError(t, err, "Should be able to read from a new reader after closing the previous one")
	assert.Equal(t, rec.Type, readRec.Type)
	assert.Equal(t, rec.Tag, readRec.Tag)
	assert.Equal(t, rec.Entity, readRec.Entity)
	assert.Equal(t, rec.TxID, readRec.TxID)
	assert.Equal(t, rec.Data, readRec.Data)
}

// TestWalCrashRecovery simulates a crash scenario and tests the WAL's ability
// to recover and maintain data integrity after an unexpected shutdown.
func TestWalCrashRecovery(t *testing.T) {
	opts := createWalOptions(t)
	w := createWal(t, opts)

	// Write some records
	records := []*Record{
		{Type: RecordTypeInsert, Tag: types.ObjectTagDatabase, Entity: 1, TxID: 100, Data: []byte("data1")},
		{Type: RecordTypeUpdate, Tag: types.ObjectTagDatabase, Entity: 2, TxID: 200, Data: []byte("data2")},
		{Type: RecordTypeDelete, Tag: types.ObjectTagDatabase, Entity: 3, TxID: 300, Data: []byte("data3")},
	}

	for _, rec := range records {
		_, err := w.Write(rec)
		require.NoError(t, err)
	}

	// Simulate crash by forcefully closing without proper shutdown
	w.active.fd.Close()

	// Attempt to recover
	recoveredWal, err := Open(0, w.opts)
	require.NoError(t, err)
	defer recoveredWal.Close()

	// Verify recovered data
	reader := recoveredWal.NewReader()
	defer reader.Close()

	for i, expected := range records {
		rec, err := reader.Next()
		require.NoError(t, err)
		assert.Equal(t, expected, rec, "Record %d mismatch after recovery", i)
	}

	// Ensure we've read all records
	_, err = reader.Next()
	assert.Equal(t, io.EOF, err)
}

// TestWalConfiguration tests various configuration options of the WAL,
// ensuring that the WAL behaves correctly with different settings.
func TestWalConfiguration(t *testing.T) {
	t.Run("DefaultConfiguration", func(t *testing.T) {
		t.Skip()
		testDir := t.TempDir()
		w, err := Create(WalOptions{Path: testDir})
		require.NoError(t, err)
		defer w.Close()

		// Verify default values
		assert.NotZero(t, w.opts.Seed, "Seed should have a non-zero default value")
		assert.NotZero(t, w.opts.MaxSegmentSize, "MaxSegmentSize should have a non-zero default value")
	})

	t.Run("CustomConfiguration", func(t *testing.T) {
		testDir := t.TempDir()
		customOpts := WalOptions{
			Path:           testDir,
			Seed:           12345,
			MaxSegmentSize: 1024 * 1024, // 1MB
		}
		w, err := Create(customOpts)
		require.NoError(t, err)
		defer w.Close()

		assert.Equal(t, customOpts.Seed, w.opts.Seed, "Custom Seed not set correctly")
		assert.Equal(t, customOpts.MaxSegmentSize, w.opts.MaxSegmentSize, "Custom MaxSegmentSize not set correctly")
	})

	t.Run("ExtremeValues", func(t *testing.T) {
		t.Skip()
		testDir := t.TempDir()
		extremeOpts := WalOptions{
			Path:           testDir,
			Seed:           0,
			MaxSegmentSize: 1, // Extremely small segment size
		}
		_, err := Create(extremeOpts)
		assert.Error(t, err, "Should error with extremely small MaxSegmentSize")

		extremeOpts.MaxSegmentSize = 1024 * 1024 * 1024 * 10 // 10GB
		w, err := Create(extremeOpts)
		require.NoError(t, err)
		defer w.Close()

		// Write a record larger than normal segment size
		largeRec := &Record{
			Type: RecordTypeInsert,
			Data: bytes.Repeat([]byte("a"), 1024*1024*2), // 2MB data
		}
		_, err = w.Write(largeRec)
		require.NoError(t, err, "Should handle writing large records with large MaxSegmentSize")
	})

	t.Run("InvalidConfiguration", func(t *testing.T) {
		testDir := t.TempDir()
		invalidOpts := []struct {
			name string
			opts WalOptions
		}{
			{"EmptyPath", WalOptions{Path: "", MaxSegmentSize: 1024}},
			{"NegativeSegmentSize", WalOptions{Path: testDir, MaxSegmentSize: -1}},
			{"ZeroSegmentSize", WalOptions{Path: testDir, MaxSegmentSize: 0}},
		}

		for _, tc := range invalidOpts {
			t.Run(tc.name, func(t *testing.T) {
				_, err := Create(tc.opts)
				assert.Error(t, err, "Should error with invalid configuration")
			})
		}
	})

	t.Run("ConfigurationImpact", func(t *testing.T) {
		testDir := t.TempDir()
		smallSegmentOpts := WalOptions{
			Path:           testDir,
			MaxSegmentSize: MinSegmentSize,
		}
		w, err := Create(smallSegmentOpts)
		require.NoError(t, err)
		defer w.Close()

		// Write records to force multiple segment creation
		for i := 0; i < 100; i++ {
			rec := &Record{
				TxID: uint64(i + 1),
				Tag:  types.ObjectTagDatabase,
				Type: RecordTypeInsert,
				Data: bytes.Repeat([]byte("a"), 1000), // 100 byte records
			}
			_, err = w.Write(rec)
			require.NoError(t, err)
		}

		// Verify multiple segments were created
		files, err := os.ReadDir(testDir)
		require.NoError(t, err)
		assert.Greater(t, len(files), 1, "Multiple segments should have been created with small MaxSegmentSize")
	})
}

// TestWalSegmentRollover tests the behavior when the WAL rolls over to a new segment due to reaching the maximum segment size.
func TestWalSegmentRollover(t *testing.T) {
	testDir := t.TempDir()
	opts := WalOptions{
		Path:           testDir,
		MaxSegmentSize: MinSegmentSize,
	}
	w, err := Create(opts)
	require.NoError(t, err)
	defer w.Close()

	recordsWritten := 0
	bytesWritten := 0
	for i := 0; i < 100; i++ {
		rec := &Record{
			Type:   RecordTypeInsert,
			Tag:    types.ObjectTagDatabase,
			Entity: uint64(i),
			TxID:   uint64(i + 1),
			Data:   []byte(strings.Repeat("a", 100)),
		}
		lsn, err := w.Write(rec)
		require.NoError(t, err)
		t.Logf("Wrote record %d, LSN: %v", i, lsn)
		recordsWritten++
		bytesWritten += HeaderSize + len(rec.Data)

		// Force sync after each write
		err = w.Sync()
		require.NoError(t, err)
	}
	t.Logf("Wrote %d records, total bytes: %d", recordsWritten, bytesWritten)

	expectedSegments := (bytesWritten + int(opts.MaxSegmentSize) - 1) / int(opts.MaxSegmentSize)
	t.Logf("Expected segments: %d", expectedSegments)

	// Check for multiple segment files
	files, err := os.ReadDir(testDir)
	require.NoError(t, err)
	segmentCount := 0
	for _, file := range files {
		if strings.HasSuffix(file.Name(), ".SEG") {
			segmentCount++
			fileInfo, err := os.Stat(filepath.Join(testDir, file.Name()))
			if err != nil {
				t.Logf("Error getting file info for %s: %v", file.Name(), err)
				continue
			}
			t.Logf("Segment file: %s, size: %d", file.Name(), fileInfo.Size())
		}
	}
	t.Logf("Found %d segment files", segmentCount)
	assert.Equal(t, expectedSegments, segmentCount, "Unexpected number of segment files")
}

func TestWalRecovery(t *testing.T) {
	testDir := t.TempDir()
	opts := WalOptions{
		Path:           testDir,
		MaxSegmentSize: 1024 * 1024, // 1MB segments
		Seed:           12345,
	}

	t.Run("NormalRecovery", func(t *testing.T) {
		// Create and populate WAL
		w, err := Create(opts)
		require.NoError(t, err)

		records := []*Record{
			{Type: RecordTypeInsert, Tag: types.ObjectTagDatabase, Entity: 1, TxID: 100, Data: []byte("data1")},
			{Type: RecordTypeUpdate, Tag: types.ObjectTagDatabase, Entity: 2, TxID: 200, Data: []byte("data2")},
			{Type: RecordTypeDelete, Tag: types.ObjectTagDatabase, Entity: 3, TxID: 300, Data: []byte("data3")},
		}

		for _, rec := range records {
			_, err := w.Write(rec)
			require.NoError(t, err)
		}

		err = w.Close()
		require.NoError(t, err)

		// Recover WAL
		recoveredWal, err := Open(0, opts)
		require.NoError(t, err)
		defer recoveredWal.Close()

		// Verify recovered data
		reader := recoveredWal.NewReader()
		defer reader.Close()

		for i, expected := range records {
			rec, err := reader.Next()
			require.NoError(t, err)
			assert.Equal(t, expected, rec, "Record %d mismatch after recovery", i)
		}

		_, err = reader.Next()
		assert.Equal(t, io.EOF, err)
	})

	t.Run("PartialWriteRecovery", func(t *testing.T) {
		w, err := Create(opts)
		require.NoError(t, err)

		// Write some records
		for i := 0; i < 5; i++ {
			rec := &Record{
				Type:   RecordTypeInsert,
				Tag:    types.ObjectTagDatabase,
				Entity: uint64(i),
				TxID:   uint64(i + 1*100),
				Data:   []byte(fmt.Sprintf("data%d", i)),
			}
			_, err := w.Write(rec)
			require.NoError(t, err)
		}

		// Simulate a crash by forcefully closing the file
		w.active.fd.Close()

		// Attempt to recover
		recoveredWal, err := Open(0, opts)
		require.NoError(t, err)
		defer recoveredWal.Close()

		// Verify recovered data
		reader := recoveredWal.NewReader()
		defer reader.Close()

		recoveredCount := 0
		for {
			_, err := reader.Next()
			if err == io.EOF {
				break
			}
			require.NoError(t, err)
			recoveredCount++
		}

		assert.True(t, recoveredCount > 0 && recoveredCount <= 5, "Should recover some but possibly not all records")
	})

	t.Run("CorruptedSegmentRecovery", func(t *testing.T) {
		w, err := Create(opts)
		require.NoError(t, err)

		// Write some records
		for i := 0; i < 10; i++ {
			rec := &Record{
				Type:   RecordTypeInsert,
				Tag:    types.ObjectTagDatabase,
				Entity: uint64(i),
				TxID:   uint64(i + 1*100),
				Data:   []byte(fmt.Sprintf("data%d", i)),
			}
			_, err := w.Write(rec)
			require.NoError(t, err)
		}

		w.Close()

		// Corrupt the last segment
		files, err := os.ReadDir(testDir)
		require.NoError(t, err)
		var lastSegment string
		for _, file := range files {
			if strings.HasSuffix(file.Name(), ".SEG") {
				lastSegment = filepath.Join(testDir, file.Name())
			}
		}
		require.NotEmpty(t, lastSegment, "No segment file found")

		f, err := os.OpenFile(lastSegment, os.O_RDWR, 0644)
		require.NoError(t, err)
		_, err = f.WriteAt([]byte("CORRUPT"), 100)
		require.NoError(t, err)
		defer f.Close()

		// Attempt to recover
		recoveredWal, err := Open(0, opts)
		require.NoError(t, err)
		defer recoveredWal.Close()

		// Verify recovered data
		reader := recoveredWal.NewReader()
		defer reader.Close()

		recoveredCount := 0
		var lastError error
		for {
			_, err := reader.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				lastError = err
				break
			}
			recoveredCount++
		}

		assert.True(t, recoveredCount > 0, "Should recover some records")
		assert.Error(t, lastError, "Should encounter an error due to corruption")
	})
}

func TestWalRecoveryWithPartialRecords(t *testing.T) {
	opts := WalOptions{
		Path:           t.TempDir(),
		MaxSegmentSize: MinSegmentSize, // Small segment size to force multiple segments
		Seed:           12345,
	}

	// Create and populate the WAL
	w, err := Create(opts)
	require.NoError(t, err)

	// Write some complete records
	completeRecords := 10
	for i := 1; i <= completeRecords; i++ {
		rec := &Record{
			Type:   RecordTypeInsert,
			Tag:    types.ObjectTagDatabase,
			Entity: uint64(i),
			TxID:   uint64(i * 100),
			Data:   []byte(fmt.Sprintf("complete data %d", i)),
		}
		_, err := w.Write(rec)
		require.NoError(t, err)
	}

	// Write a partial record
	partialRec := &Record{
		Type:   RecordTypeUpdate,
		Tag:    types.ObjectTagDatabase,
		Entity: uint64(completeRecords),
		TxID:   uint64(completeRecords * 100),
		Data:   []byte("partial data "),
	}

	// Start writing the partial record
	lsn, err := w.Write(partialRec)
	require.NoError(t, err)

	// Simulate a crash by forcefully closing the file
	w.active.fd.Close()

	// Corrupt the last part of the file to simulate incomplete write
	segmentFile := filepath.Join(opts.Path, fmt.Sprintf("%016d.SEG", lsn.calculateFilename(w.opts.MaxSegmentSize)))
	f, err := os.OpenFile(segmentFile, os.O_RDWR, 0644)
	require.NoError(t, err)

	info, err := f.Stat()
	require.NoError(t, err)

	// Truncate the file to simulate partial write
	err = f.Truncate(info.Size() - int64(len(partialRec.Data))) // Remove last 100 bytes
	require.NoError(t, err)
	err = f.Close()
	require.NoError(t, err)

	// // Attempt to recover
	recoveredWal, err := Open(0, opts)
	require.NoError(t, err)
	defer recoveredWal.Close()

	// // Read and verify recovered records
	reader := recoveredWal.NewReader()
	defer reader.Close()

	j := 1
	recoveredCounter := 0
	for {
		rec, err := reader.Next()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		assert.Equal(t, RecordTypeInsert, rec.Type)
		assert.Equal(t, types.ObjectTagDatabase, rec.Tag)
		assert.Equal(t, uint64(j), rec.Entity)
		assert.Equal(t, uint64(j*100), rec.TxID)
		assert.Equal(t, []byte(fmt.Sprintf("complete data %d", j)), rec.Data)
		recoveredCounter++
		j++
	}

	// Verify that only the complete records were recovered
	assert.Equal(t, completeRecords, recoveredCounter, "Should recover only complete records")

	// Verify that writing new records after recovery works
	newRec := &Record{
		Type:   RecordTypeInsert,
		Tag:    types.ObjectTagDatabase,
		Entity: uint64(completeRecords + 1),
		TxID:   uint64((completeRecords + 1) * 100),
		Data:   []byte("new record after recovery"),
	}
	_, err = recoveredWal.Write(newRec)
	require.NoError(t, err)

	// // Verify the new record
	// err = reader.Seek(0) // Reset reader to beginning
	// require.NoError(t, err)

	// recoveredCounter = 0
	// var rec *Record
	// for i := 0; i <= completeRecords; i++ {
	// 	rec, err = reader.Next()
	// 	require.NoError(t, err)
	// 	recoveredCounter++
	// }
	// if recoveredCounter == completeRecords {
	// 	assert.Equal(t, newRec.Data, rec.Data, "New record should be readable after recovery")
	// }
}

// TestWalSyncAndClose tests the WAL's behavior when sync and close operations are performed to ensure data integrity and consistency.
func TestWalSyncAndClose(t *testing.T) {
	opts := createWalOptions(t)
	w := createWal(t, opts)

	// Write some records
	for i := 0; i < 10; i++ {
		rec := &Record{
			Type:   RecordTypeInsert,
			Tag:    types.ObjectTagDatabase,
			Entity: uint64(i),
			TxID:   uint64(i + 1*100),
			Data:   []byte(fmt.Sprintf("data%d", i)),
		}
		_, err := w.Write(rec)
		require.NoError(t, err)
	}
	err := w.Sync()
	require.NoError(t, err)
	err = w.Close()
	require.NoError(t, err)

	// Attempt to write after close (this should fail)
	_, err = w.Write(&Record{
		Type: RecordTypeInsert,
		Data: []byte("test"),
	})
	assert.Error(t, err, "Write after close should fail")
}

// TestWalInvalidRecords tests the WAL's behavior when attempting to write records with invalid types or tags.
func TestWalInvalidRecords(t *testing.T) {
	opts := createWalOptions(t)
	w := createWal(t, opts)
	defer w.Close()

	// Try to write a record with an invalid type
	invalidRec := &Record{
		Type:   RecordType(255), // Assuming 255 is an invalid type
		Entity: 1,
		TxID:   100,
		Data:   []byte("invalid record"),
	}
	_, err := w.Write(invalidRec)
	assert.Error(t, err, "Expected an error when writing an invalid record type")

	// Try to write a record with an invalid tag
	invalidTagRec := &Record{
		Type:   RecordTypeInsert,
		Tag:    types.ObjectTag(255), // Assuming 255 is an invalid tag
		Entity: 1,
		TxID:   100,
		Data:   []byte("invalid tag record"),
	}
	_, err = w.Write(invalidTagRec)
	assert.Error(t, err, "Expected an error when writing an invalid record tag")
}

// TestWalEmptyRecords tests the WAL's ability to handle writing and reading empty or minimal-sized records.
func TestWalEmptyRecords(t *testing.T) {
	opts := createWalOptions(t)
	w := createWal(t, opts)
	defer w.Close()

	// Write an empty record
	emptyRec := &Record{
		Type:   RecordTypeInsert,
		Tag:    types.ObjectTagDatabase,
		Entity: 1,
		TxID:   100,
		Data:   []byte{},
	}
	lsn, err := w.Write(emptyRec)
	require.NoError(t, err, "Failed to write empty record")

	// Read the empty record back
	reader := w.NewReader()
	err = reader.Seek(lsn)
	require.NoError(t, err, "Failed to seek to empty record")

	readRec, err := reader.Next()
	require.NoError(t, err, "Failed to read empty record")
	assert.Empty(t, readRec.Data, "Expected empty data in read record")

	// Write a record with minimal data (1 byte)
	minimalRec := &Record{
		Type:   RecordTypeUpdate,
		Tag:    types.ObjectTagDatabase,
		Entity: 2,
		TxID:   101,
		Data:   []byte{0},
	}
	lsn, err = w.Write(minimalRec)
	require.NoError(t, err, "Failed to write minimal record")

	// Read the minimal record back
	err = reader.Seek(lsn)
	require.NoError(t, err, "Failed to seek to minimal record")

	readRec, err = reader.Next()
	require.NoError(t, err, "Failed to read minimal record")
	assert.Equal(t, minimalRec.Data, readRec.Data, "Minimal record data mismatch")
}

// TestWalTruncateOnPartialWrite tests the WAL's behavior when encountering a partially written (truncated) record.
func TestWalTruncateOnPartialWrite(t *testing.T) {
	opts := createWalOptions(t)
	w := createWal(t, opts)
	defer w.Close()

	// Write several records
	numRecords := 10
	var lastLSN LSN
	for i := 0; i < numRecords; i++ {
		rec := &Record{
			Type:   RecordTypeInsert,
			Tag:    types.ObjectTagDatabase,
			Entity: uint64(i),
			TxID:   uint64(100 + i),
			Data:   []byte(fmt.Sprintf("test data %d", i)),
		}
		lsn, err := w.Write(rec)
		require.NoError(t, err)
		lastLSN = lsn
		t.Logf("Wrote record %d, LSN: %v", i, lsn)
	}

	// Simulate a partial write by truncating the last record
	segmentFile := filepath.Join(opts.Path, fmt.Sprintf("%016d.SEG", lastLSN.calculateFilename(w.opts.MaxSegmentSize)))
	file, err := os.OpenFile(segmentFile, os.O_RDWR, 0644)
	require.NoError(t, err)

	fileInfo, err := file.Stat()
	require.NoError(t, err)

	truncateSize := fileInfo.Size() - 10
	err = file.Truncate(truncateSize)
	require.NoError(t, err)
	t.Logf("Truncated file from %d to %d bytes", fileInfo.Size(), truncateSize)
	file.Close()

	// Try to write a new record after truncation
	newRec := &Record{
		Type:   RecordTypeInsert,
		Tag:    types.ObjectTagDatabase,
		Entity: uint64(numRecords),
		TxID:   uint64(100 + numRecords),
		Data:   []byte(fmt.Sprintf("test data %d", numRecords)),
	}
	newLSN, err := w.Write(newRec)
	require.NoError(t, err)
	t.Logf("Attempted to write new record after truncation, LSN: %v", newLSN)

	// Read all records and verify
	reader := w.NewReader()
	defer reader.Close()

	var readRecords int
	var lastReadRecord *Record
	for {
		rec, err := reader.Next()
		if err != nil {
			if err != io.EOF {
				t.Logf("Error reading record: %v", err)
			}
			break
		}
		t.Logf("Read record %d: Entity=%d, TxID=%d", readRecords, rec.Entity, rec.TxID)
		lastReadRecord = rec
		readRecords++
	}

	t.Logf("Read %d records after truncation", readRecords)
	expectedRecords := numRecords - 1 // We expect to read one less than we wrote due to truncation
	assert.Equal(t, expectedRecords, readRecords, "Unexpected number of records after truncation")

	// Check if the last read record is the second-to-last one we originally wrote
	assert.NotNil(t, lastReadRecord, "Last read record should not be nil")
	if lastReadRecord != nil {
		assert.Equal(t, uint64(numRecords-2), lastReadRecord.Entity, "Last record Entity mismatch")
		assert.Equal(t, uint64(100+numRecords-2), lastReadRecord.TxID, "Last record TxID mismatch")
	}

	// Attempt to seek to the new LSN and read the record, expecting an error
	err = reader.Seek(newLSN)
	assert.Error(t, err, "Expected an error when reading the record written after truncation")
	assert.Contains(t, err.Error(), "checksum mismatch", "Error should indicate checksum mismatch")
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

// TestWalInvalidLSN tests the WAL's behavior when attempting to seek to or read
// from invalid LSNs, ensuring proper error handling and system stability.
func TestWalInvalidLSN(t *testing.T) {
	opts := WalOptions{
		Path:           t.TempDir(),
		MaxSegmentSize: MinSegmentSize,
		Seed:           12345,
	}

	w, err := Create(opts)
	require.NoError(t, err, "Failed to create WAL")
	defer w.Close()

	// Write a valid record to ensure the WAL is initialized
	validRec := &Record{
		Type: RecordTypeInsert,
		Tag:  types.ObjectTagDatabase,
		TxID: 1,
		Data: bytes.Repeat([]byte("valid data"), MinSegmentSize),
	}
	validLSN, err := w.Write(validRec)
	require.NoError(t, err, "Failed to write valid record")
	t.Logf("Valid LSN: %v", validLSN)

	// Define invalid LSN scenarios
	invalidLSNs := []struct {
		name string
		lsn  LSN
	}{
		{"NegativeSegment", NewLSN(-1, int64(opts.MaxSegmentSize), 0)},
		{"NegativeOffset", NewLSN(0, int64(opts.MaxSegmentSize), -1)},
		{"OutOfBoundsSegment", NewLSN(100, int64(opts.MaxSegmentSize), 0)},
		{"OutOfBoundsOffset", NewLSN(0, int64(opts.MaxSegmentSize), int64(opts.MaxSegmentSize+100))},
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
		err := reader.Seek(NewLSN(-1, 0, 0))
		require.NoError(t, err, "Seek to invalid LSN should not return an error")

		// Now, seek to the valid LSN
		err = reader.Seek(validLSN)
		require.NoError(t, err, "Failed to seek to valid LSN after invalid attempt")

		// Try to read the valid record
		readRec, err := reader.Next()
		require.NoError(t, err, "Failed to read valid record after invalid LSN attempts")
		require.Equal(t, validRec.Data, readRec.Data, "Read record data doesn't match written data")
	})
}

func verifySegmentExists(t *testing.T, dir string, lsn LSN, maxSegmentSize int) {
	segmentFile := filepath.Join(dir, fmt.Sprintf("%016d.SEG", lsn.calculateFilename(maxSegmentSize)))
	_, err := os.Stat(segmentFile)
	require.NoError(t, err, "Segment file should exist: %s", segmentFile)
}

// TestWalFaultInjection simulates various fault scenarios to test the WAL's
// resilience and error handling capabilities under adverse conditions.
func TestWalFaultInjection(t *testing.T) {
	testDir := t.TempDir()
	opts := WalOptions{
		Path:           testDir,
		MaxSegmentSize: 1024 * 1024, // 1MB segments
		Seed:           12345,
	}

	t.Run("WriteFailure", func(t *testing.T) {
		w, err := Create(opts)
		require.NoError(t, err)
		defer w.Close()

		// Simulate a write failure by making the directory read-only
		require.NoError(t, os.Chmod(testDir, 0555))
		defer os.Chmod(testDir, 0755)

		// Try to create a new segment file
		rec := &Record{
			Type: RecordTypeInsert,
			TxID: 1,
			Tag:  types.ObjectTagDatabase,
			Data: bytes.Repeat([]byte("a"), int(opts.MaxSegmentSize)+1), // Force new segment creation
		}
		_, err = w.Write(rec)
		assert.Error(t, err, "Expected an error when writing to a read-only directory")
	})

	t.Run("CorruptChecksum", func(t *testing.T) {
		w, err := Create(opts)
		require.NoError(t, err)
		defer w.Close()

		rec := &Record{
			Type: RecordTypeInsert,
			Tag:  types.ObjectTagDatabase,
			Data: []byte("test data"),
			TxID: 2,
		}
		lsn, err := w.Write(rec)
		require.NoError(t, err)

		// Corrupt the checksum
		segmentFile := filepath.Join(testDir, fmt.Sprintf("%016d.SEG", lsn.calculateFilename(w.opts.MaxSegmentSize)))
		f, err := os.OpenFile(segmentFile, os.O_RDWR, 0644)
		require.NoError(t, err)
		defer f.Close()

		_, err = f.Seek(int64(lsn.calculateOffset(w.opts.MaxSegmentSize))+HeaderSize-8, io.SeekStart)
		require.NoError(t, err)

		_, err = f.Write([]byte{0, 0, 0, 0, 0, 0, 0, 0}) // Overwrite checksum with zeros
		require.NoError(t, err)

		// Try to read the corrupted record
		reader := w.NewReader()
		defer reader.Close()
		err = reader.Seek(lsn)
		require.NoError(t, err)

		_, err = reader.Next()
		assert.Error(t, err, "Expected an error due to checksum mismatch")
	})

	t.Run("PartialWrite", func(t *testing.T) {
		testDir := t.TempDir()
		w, err := Create(WalOptions{
			Path:           testDir,
			MaxSegmentSize: 1024 * 1024, // 1MB segments
			Seed:           12345,
		})
		require.NoError(t, err)
		defer w.Close()

		rec := &Record{
			Type: RecordTypeInsert,
			Tag:  types.ObjectTagDatabase,
			Data: bytes.Repeat([]byte("a"), 1000),
			TxID: 1,
		}
		lsn, err := w.Write(rec)
		require.NoError(t, err)

		// Simulate a partial write by truncating the file
		segmentFile := filepath.Join(testDir, fmt.Sprintf("%016d.SEG", lsn.calculateFilename(w.opts.MaxSegmentSize)))
		f, err := os.OpenFile(segmentFile, os.O_RDWR, 0644)
		require.NoError(t, err)
		defer f.Close()

		info, err := f.Stat()
		require.NoError(t, err)

		err = f.Truncate(info.Size() - 500) // Remove last 500 bytes
		require.NoError(t, err)

		// Try to read the partially written record
		reader := w.NewReader()
		defer reader.Close()
		err = reader.Seek(lsn)
		require.NoError(t, err)

		_, err = reader.Next()
		assert.Error(t, err, "Expected an error when reading a partially written record")
	})

	t.Run("RecoveryAfterCrash", func(t *testing.T) {
		opts := createWalOptions(t)
		w, err := Create(opts)
		require.NoError(t, err)

		// Write some records
		var lastLSN LSN
		for i := 1; i <= 10; i++ {
			rec := &Record{
				Type: RecordTypeInsert,
				Tag:  types.ObjectTagDatabase,
				TxID: uint64(i),
				Data: []byte(fmt.Sprintf("data %d", i)),
			}
			lastLSN, err = w.Write(rec)
			require.NoError(t, err)
		}

		// Verify segment file exists
		verifySegmentExists(t, opts.Path, lastLSN, opts.MaxSegmentSize)

		// Simulate a crash by forcefully closing without proper shutdown
		w.active.Close()

		// Reopen the WAL
		reopenedWal, err := Open(NewLSN(0, int64(opts.MaxSegmentSize), 0), opts)
		require.NoError(t, err)
		defer reopenedWal.Close()

		// Verify that we can read all the records
		reader := reopenedWal.NewReader()
		defer reader.Close()

		count := 0
		for {
			_, err := reader.Next()
			if err == io.EOF {
				break
			}
			require.NoError(t, err)
			count++
		}

		assert.Equal(t, 10, count, "Expected to read all 10 records after recovery")
	})

	t.Run("CorruptHeader", func(t *testing.T) {
		w, err := Create(opts)
		require.NoError(t, err)
		defer w.Close()

		rec := &Record{
			Type: RecordTypeInsert,
			Tag:  types.ObjectTagDatabase,
			TxID: 2,
			Data: []byte("test data"),
		}
		lsn, err := w.Write(rec)
		require.NoError(t, err)

		// Corrupt the header
		segmentFile := filepath.Join(testDir, fmt.Sprintf("%016d.SEG", lsn.calculateFilename(w.opts.MaxSegmentSize)))
		f, err := os.OpenFile(segmentFile, os.O_RDWR, 0644)
		require.NoError(t, err)
		defer f.Close()

		_, err = f.Seek(int64(lsn.calculateOffset(w.opts.MaxSegmentSize)), io.SeekStart)
		require.NoError(t, err)

		corruptHeader := make([]byte, HeaderSize)
		_, err = f.Write(corruptHeader) // Overwrite header with zeros
		require.NoError(t, err)

		// Try to read the corrupted record
		reader := w.NewReader()
		defer reader.Close()
		err = reader.Seek(lsn)
		require.NoError(t, err)

		_, err = reader.Next()
		assert.Error(t, err, "Reading record with corrupted header should fail")
	})

	t.Run("IncompleteRecord", func(t *testing.T) {
		testDir := t.TempDir()
		w, err := Create(WalOptions{
			Path:           testDir,
			MaxSegmentSize: 1024 * 1024, // 1MB segments
			Seed:           12345,
		})
		require.NoError(t, err)
		defer w.Close()

		rec := &Record{
			Type: RecordTypeInsert,
			Tag:  types.ObjectTagDatabase,
			TxID: 1,
			Data: bytes.Repeat([]byte("a"), 1000),
		}
		lsn, err := w.Write(rec)
		require.NoError(t, err)

		// Truncate the file to create an incomplete record
		segmentFile := filepath.Join(testDir, fmt.Sprintf("%016d.SEG", lsn.calculateFilename(w.opts.MaxSegmentSize)))
		f, err := os.OpenFile(segmentFile, os.O_RDWR, 0644)
		require.NoError(t, err)
		defer f.Close()

		info, err := f.Stat()
		require.NoError(t, err)

		err = f.Truncate(info.Size() - 100) // Remove last 100 bytes
		require.NoError(t, err)

		// Try to read the incomplete record
		reader := w.NewReader()
		defer reader.Close()
		err = reader.Seek(lsn)
		require.NoError(t, err)

		_, err = reader.Next()
		assert.Error(t, err, "Reading incomplete record should fail")
	})

	t.Run("CorruptedRecordType", func(t *testing.T) {
		w, err := Create(opts)
		require.NoError(t, err)
		defer w.Close()

		rec := &Record{
			Type: RecordTypeInsert,
			Tag:  types.ObjectTagDatabase,
			TxID: 1,
			Data: []byte("test data"),
		}
		lsn, err := w.Write(rec)
		require.NoError(t, err)

		// Corrupt the record type
		segmentFile := filepath.Join(testDir, fmt.Sprintf("%016d.SEG", lsn.calculateFilename(w.opts.MaxSegmentSize)))
		f, err := os.OpenFile(segmentFile, os.O_RDWR, 0644)
		require.NoError(t, err)
		defer f.Close()

		_, err = f.Seek(int64(lsn.calculateOffset(w.opts.MaxSegmentSize))+1, io.SeekStart) // Seek to record type position
		require.NoError(t, err)

		_, err = f.Write([]byte{255}) // Write invalid record type
		require.NoError(t, err)

		// Try to read the record with corrupted type
		reader := w.NewReader()
		defer reader.Close()
		err = reader.Seek(lsn)
		require.NoError(t, err)

		_, err = reader.Next()
		assert.Error(t, err, "Reading record with corrupted type should fail")
	})

	t.Run("CorruptedSegmentBoundary", func(t *testing.T) {
		w, err := Create(opts)
		require.NoError(t, err)
		defer w.Close()

		// Write records until close to segment boundary
		var lastLSN LSN
		for i := 0; i < 1000; i++ {
			rec := &Record{
				Type: RecordTypeInsert,
				Tag:  types.ObjectTagDatabase,
				Data: bytes.Repeat([]byte("a"), 900),
				TxID: uint64(i + 1),
			}
			lsn, err := w.Write(rec)
			require.NoError(t, err)
			lastLSN = lsn
		}

		// Corrupt the segment boundary
		segmentFile := filepath.Join(testDir, fmt.Sprintf("%016d.SEG", lastLSN.calculateFilename(w.opts.MaxSegmentSize)))
		f, err := os.OpenFile(segmentFile, os.O_RDWR, 0644)
		require.NoError(t, err)
		defer f.Close()

		info, err := f.Stat()
		require.NoError(t, err)

		_, err = f.Seek(info.Size()-10, io.SeekStart)
		require.NoError(t, err)

		_, err = f.Write(bytes.Repeat([]byte{0}, 20)) // Overwrite segment boundary
		require.NoError(t, err)

		// Try to read past the corrupted segment boundary
		reader := w.NewReader()
		defer reader.Close()
		err = reader.Seek(lastLSN)
		require.NoError(t, err)

		for i := 0; i < 5; i++ {
			_, err = reader.Next()
			if err != nil {
				break
			}
		}
		assert.Error(t, err, "Reading past corrupted segment boundary should fail")
	})
}

// Benchmarks
//
// `go test -v blockwatch.cc/knoxdb/internal/wal -run=^$ -bench .`
//
// BenchmarkWalWrite tests writing records of various sizes to the WAL
func BenchmarkWalWrite(b *testing.B) {
	sizes := []int{256, 512, 1024, 2 * 1024, 4 * 1024, 8 * 1024, 16 * 1024, 32 * 1024, 64 * 1024, 128 * 1024, 256 * 1024, 512 * 1024, 1024 * 1024}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("Size-%d", size), func(b *testing.B) {
			opts := createWalOptions(b)
			w := createWal(b, opts) // 1 MB segments
			defer w.Close()

			data := make([]byte, size)
			b.SetBytes(int64(size))
			b.ResetTimer()

			for i := 1; i <= b.N; i++ {
				rec := &Record{
					Type:   RecordTypeInsert,
					Tag:    types.ObjectTagDatabase,
					Entity: uint64(i),
					TxID:   uint64(i),
					Data:   data,
				}
				_, err := w.Write(rec)
				require.NoError(b, err)
			}
		})
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

// BenchmarkWalWriteSync tests writing records with and without sync
func BenchmarkWalWriteSync(b *testing.B) {
	syncOptions := []bool{false, true}
	for _, withSync := range syncOptions {
		name := "WithoutSync"
		if withSync {
			name = "WithSync"
		}
		b.Run(name, func(b *testing.B) {
			opts := createWalOptions(b)
			w := createWal(b, opts)
			defer w.Close()

			size := 1024
			data := make([]byte, size)
			b.SetBytes(int64(size))
			b.ResetTimer()

			for i := 1; i < b.N; i++ {
				rec := &Record{
					Type:   RecordTypeInsert,
					Tag:    types.ObjectTagDatabase,
					Entity: uint64(i),
					TxID:   uint64(i),
					Data:   data,
				}
				_, err := w.Write(rec)
				require.NoError(b, err)
				if withSync {
					err = w.Sync()
					require.NoError(b, err)
				}
			}
		})
	}
}

// BenchmarkWalWriteVaryingSegmentSize tests writing with different segment sizes
func BenchmarkWalWriteVaryingSegmentSize(b *testing.B) {
	segmentSizes := []int{1024 * 10, 4096 * 3, 16384, 65536, 262144, 1048576}
	for _, segmentSize := range segmentSizes {
		b.Run(fmt.Sprintf("SegmentSize-%d", segmentSize), func(b *testing.B) {
			opts := createWalOptions(b)
			opts.MaxSegmentSize = segmentSize
			w := createWal(b, opts)
			defer w.Close()

			recordSize := 1024
			data := make([]byte, recordSize)
			b.SetBytes(int64(recordSize))
			b.ResetTimer()

			for i := 1; i < b.N; i++ {
				rec := &Record{
					Type:   RecordTypeInsert,
					Tag:    types.ObjectTagDatabase,
					Entity: uint64(i),
					TxID:   uint64(i),
					Data:   data,
				}
				_, err := w.Write(rec)
				require.NoError(b, err)
			}
		})
	}
}
