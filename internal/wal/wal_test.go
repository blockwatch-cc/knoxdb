// Copyright (c) 2024 Blockwatch Data Inc.
// Author: oliver@blockwatch.cc

package wal

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"os/user"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"blockwatch.cc/knoxdb/internal/types"
	"github.com/echa/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func createWalOptions(t testing.TB) WalOptions {
	t.Helper()
	if testing.Verbose() {
		log.Log.SetLevel(log.LevelDebug)
	}
	return WalOptions{
		Path:           t.TempDir(),
		SyncDelay:      time.Second,
		MaxSegmentSize: 100 << 20, // 100MB
		RecoveryMode:   RecoveryModeTruncate,
		Logger:         log.Log,
	}
}

func createWal(t testing.TB, opts WalOptions) *Wal {
	t.Helper()
	w, err := Create(opts)
	require.NoError(t, err)
	require.NotNil(t, w)
	return w
}

func openWal(t testing.TB, lsn LSN, opts WalOptions) *Wal {
	t.Helper()
	w, err := Open(lsn, opts)
	require.NoError(t, err)
	require.NotNil(t, w)
	return w
}

func verifySegmentExists(t *testing.T, dir string, lsn LSN, maxSegmentSize int) {
	name := filepath.Join(dir, fmt.Sprintf(SEG_FILE_PATTERN, lsn.Segment(maxSegmentSize)))
	_, err := os.Stat(name)
	require.NoError(t, err, "Segment file should exist: %s", name)
}

// TestWalOptions tests various configuration options of the WAL,
// ensuring that the WAL behaves correctly with different settings.
func TestWalOptions(t *testing.T) {
	t.Run("DefaultConfiguration", func(t *testing.T) {
		testDir := t.TempDir()
		w, err := Create(WalOptions{Path: testDir})
		require.NoError(t, err)
		defer w.Close()

		// Verify default values
		assert.Zero(t, w.opts.Seed, "Seed should be a zero value")
		assert.Equalf(t, w.opts.Path, testDir, "Wal Path should the test path provided: %s", testDir)
		assert.Equalf(t, w.opts.RecoveryMode, RecoveryModeFail, "Default RecoveryMode should be, %s", DefaultOptions.RecoveryMode)
		assert.NotNil(t, w.opts.Logger, "Default logger is not nil")
		assert.NotZero(t, w.opts.MaxSegmentSize, "MaxSegmentSize should have a non-zero default value")
		assert.Equalf(t, w.opts.MaxSegmentSize, DefaultOptions.MaxSegmentSize, "Default MaxSegmentSize should be: %v", DefaultOptions.MaxSegmentSize)
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
		err = w.Close()
		require.NoError(t, err, "")
	})

	t.Run("InvalidConfiguration", func(t *testing.T) {
		testDir := t.TempDir()
		invalidOpts := []struct {
			name string
			opts WalOptions
		}{
			{"EmptyPath", WalOptions{Path: "", MaxSegmentSize: 1024}},
			{"NegativeSegmentSize", WalOptions{Path: testDir, MaxSegmentSize: -1}},
			{"OverflowSegmentSize", WalOptions{Path: testDir, MaxSegmentSize: SEG_FILE_MAXSIZE + 1}},
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
			MaxSegmentSize: SEG_FILE_MINSIZE,
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
				Data: [][]byte{bytes.Repeat([]byte("a"), 1000)}, // 100 byte records
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

// TestWalCreate tests the creation of a new WAL to ensure it initializes correctly.
func TestWalCreate(t *testing.T) {
	opts := createWalOptions(t)
	w := createWal(t, opts)
	defer w.Close()

	assert.NotNil(t, w)
	// Check for the existence of the first segment file
	files, err := os.ReadDir(opts.Path)
	require.NoError(t, err)
	assert.Equal(t, 1, len(files), "Expected one segment file")
	assert.True(t, strings.HasSuffix(files[0].Name(), SEG_FILE_SUFFIX), "Expected segment file with .seg extension")
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
			Data:   [][]byte{[]byte(tc.data)},
		}
		_, err := w.Write(rec)
		require.NoError(t, err)
	}
	require.NoError(t, w.Sync())

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
		assert.Equal(t, [][]byte{[]byte(expected.data)}, readRec.Data)
	}

	_, err = reader.Next()
	assert.Equal(t, io.EOF, err)
	t.Log("Finished TestWalWrite")
}

// TestWalWriteErrors tests the WAL's error handling when writing records under various error conditions.
func TestWalWriteErrors(t *testing.T) {
	t.Run("WriteToReadOnlyDir", func(t *testing.T) {
		if u, err := user.Current(); err != nil || u.Uid == "0" {
			t.Skip()
		}
		testDir := t.TempDir()
		readOnlyDir := filepath.Join(testDir, "readonly")
		require.NoError(t, os.MkdirAll(readOnlyDir, 0500)) // r-x------
		defer os.RemoveAll(readOnlyDir)

		_, err := Create(WalOptions{
			Path:           readOnlyDir,
			MaxSegmentSize: 1024 * 1024,
			Seed:           12345,
		})
		require.Error(t, err, "Expected an error when creating WAL in a read-only directory")
		assert.Contains(t, err.Error(), "permission denied", "Expected a permission denied error")
	})

	t.Run("WriteLargeRecord", func(t *testing.T) {
		w, err := Create(WalOptions{Path: t.TempDir()})
		require.NoError(t, err)
		defer w.Close()

		// Write a record larger than normal segment size
		largeRec := &Record{
			TxID: 1,
			Type: RecordTypeInsert,
			Tag:  types.ObjectTagDatabase,
			Data: [][]byte{bytes.Repeat([]byte("a"), 1024*1024*2)}, // 2MB data
		}
		_, err = w.Write(largeRec)
		require.NoError(t, err, "Should handle writing large records with large MaxSegmentSize")
	})
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
		Data:   [][]byte{[]byte("invalid record")},
	}
	_, err := w.Write(invalidRec)
	assert.Error(t, err, "Expected an error when writing an invalid record type")

	// Try to write a record with an invalid tag
	invalidTagRec := &Record{
		Type:   RecordTypeInsert,
		Tag:    types.ObjectTag(255), // Assuming 255 is an invalid tag
		Entity: 1,
		TxID:   100,
		Data:   [][]byte{[]byte("invalid tag record")},
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
		Data:   nil,
	}
	_, err := w.Write(emptyRec)
	require.Error(t, err, "Accepted empty record")

	// Write a correct empty record
	emptyRec = &Record{
		Type:   RecordTypeCommit,
		Tag:    types.ObjectTagDatabase,
		Entity: 1,
		TxID:   100,
		Data:   nil,
	}
	_, err = w.Write(emptyRec)
	require.NoError(t, err, "Failed commit record")
	require.NoError(t, w.Sync())

	// Read the empty record back (works because LSN is zero)
	reader := w.NewReader()
	err = reader.Seek(0)
	require.NoError(t, err, "Failed to seek to first record")

	readRec, err := reader.Next()
	require.NoError(t, err, "Failed to read first record")
	assert.Equal(t, emptyRec, readRec, "Expected data in read record")
	require.NoError(t, reader.Close())

	// Write a checkpoint record with no data
	checkpointRec := &Record{
		Type:   RecordTypeCheckpoint,
		Tag:    types.ObjectTagTable,
		Entity: 2,
		TxID:   0,
	}
	lsn, err := w.Write(checkpointRec)
	require.NoError(t, err, "Failed to write checkpoint record")
	require.NoError(t, w.Sync())

	// Write another test record with data
	dataRec := &Record{
		Type:   RecordTypeInsert,
		Tag:    types.ObjectTagTable,
		Entity: 2,
		TxID:   121,
		Data:   [][]byte{{1}},
	}
	_, err = w.Write(dataRec)
	require.NoError(t, err, "Failed to write data record")
	require.NoError(t, w.Sync())

	// Seek to the checkpoint record
	reader = w.NewReader()
	require.NoError(t, reader.Seek(lsn), "Failed to seek to checkpoint record")

	// Read the next data record back
	readRec, err = reader.Next()
	require.NoError(t, err, "Failed to read data record")
	assert.Equal(t, dataRec, readRec, "Data record mismatch")
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
			Data:   [][]byte{[]byte(td.data)},
		}
		_, err := w.Write(rec)
		require.NoError(t, err)
	}
	require.NoError(t, w.Sync())

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
		assert.Equal(t, [][]byte{[]byte(expected.data)}, readRec.Data)
	}

	_, err = reader.Next()
	assert.Equal(t, io.EOF, err)
}

// TestWalClose tests the WAL's Close function, verifying that it properly closes
// the log and prevents further operations after closing.
func TestWalClose(t *testing.T) {
	opts := createWalOptions(t)
	w := createWal(t, opts)

	// Write a test record
	rec := &Record{Type: RecordTypeInsert, Tag: types.ObjectTagDatabase, Entity: 1, TxID: 100, Data: [][]byte{[]byte("data1")}}
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
	w = openWal(t, 0, opts)
	defer w.Close()
	newReader := w.NewReader()
	defer newReader.Close()
	readRec, err := newReader.Next()
	assert.NoError(t, err, "Should be able to read from a new reader after closing the previous one")
	readRec.Lsn = 0
	assert.Equal(t, rec, readRec)
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
			Data:   [][]byte{[]byte(fmt.Sprintf("data%d", i))},
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
		Data: [][]byte{[]byte("test")},
	})
	assert.Error(t, err, "Write after close should fail")
}

// TestWalAsyncWait tests batched fsync mode where simulated tx wait for sync completion.
func TestWalAsyncWait(t *testing.T) {
	opts := createWalOptions(t)
	opts.SyncDelay = 10 * time.Millisecond
	w := createWal(t, opts)

	// Write concurrent records
	errg := &errgroup.Group{}
	errg.SetLimit(32)
	for th := 0; th < 32; th++ {
		errg.Go(func() error {
			for i := 0; i < 10; i++ {
				rec := &Record{
					Type:   RecordTypeInsert,
					Tag:    types.ObjectTagDatabase,
					Entity: uint64(i),
					TxID:   uint64(i + th*100 + 1),
					Data:   [][]byte{[]byte(fmt.Sprintf("data%d", i))},
				}
				_, fut, err := w.WriteAndSchedule(rec)
				if err != nil {
					return err
				}
				fut.Wait()
				if err := fut.Err(); err != nil {
					return err
				}
			}
			return nil
		})
	}

	require.NoError(t, errg.Wait())
	require.NoError(t, w.Close())
}

// TestWalAsyncNoWait tests batched fsync mode where simulated tx do not wait for sync completion.
func TestWalAsyncNoWait(t *testing.T) {
	opts := createWalOptions(t)
	opts.SyncDelay = 10 * time.Millisecond
	w := createWal(t, opts)

	// Write concurrent records
	errg := &errgroup.Group{}
	errg.SetLimit(32)
	for th := 0; th < 32; th++ {
		errg.Go(func() error {
			for i := 0; i < 100; i++ {
				rec := &Record{
					Type:   RecordTypeInsert,
					Tag:    types.ObjectTagDatabase,
					Entity: uint64(i),
					TxID:   uint64(i + th*100 + 1),
					Data:   [][]byte{[]byte(fmt.Sprintf("data%d", i))},
				}
				_, fut, err := w.WriteAndSchedule(rec)
				if err != nil {
					return err
				}
				fut.Close()
			}
			return nil
		})
	}

	require.NoError(t, errg.Wait())
	require.NoError(t, w.Close())
}

// TestWalAsyncClose tests batched fsync mode where tx waits for completion on close.
func TestWalAsyncClose(t *testing.T) {
	opts := createWalOptions(t)
	opts.SyncDelay = 10 * time.Millisecond
	w := createWal(t, opts)

	// Write records
	rec := &Record{
		Type:   RecordTypeInsert,
		Tag:    types.ObjectTagDatabase,
		Entity: uint64(1),
		TxID:   uint64(100 + 1),
		Data:   [][]byte{[]byte("data")},
	}
	_, fut, err := w.WriteAndSchedule(rec)
	require.NoError(t, err)
	require.NoError(t, w.Close())
	require.Eventually(t, func() bool {
		fut.Wait()
		return true
	}, time.Second, time.Millisecond)
	require.NoError(t, fut.Err())
}

// TestWalSegmentRollover tests the behavior when the WAL rolls over to a new segment due to reaching the maximum segment size.
func TestWalSegmentRollover(t *testing.T) {
	testDir := t.TempDir()
	opts := WalOptions{
		Path:           testDir,
		MaxSegmentSize: SEG_FILE_MINSIZE,
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
			Data:   [][]byte{bytes.Repeat([]byte("a"), 100)},
		}
		_, err := w.Write(rec)
		require.NoError(t, err)
		// t.Logf("Wrote record %d, LSN: %v", i, lsn)
		recordsWritten++
		bytesWritten += HeaderSize + rec.BodySize()

		// Force sync after each write
		err = w.Sync()
		require.NoError(t, err)
	}
	// t.Logf("Wrote %d records, total bytes: %d", recordsWritten, bytesWritten)

	expectedSegments := (bytesWritten + opts.MaxSegmentSize - 1) / opts.MaxSegmentSize
	// t.Logf("Expected segments: %d", expectedSegments)

	// Check for multiple segment files
	files, err := os.ReadDir(testDir)
	require.NoError(t, err)
	segmentCount := 0
	for _, file := range files {
		if strings.HasSuffix(file.Name(), SEG_FILE_SUFFIX) {
			segmentCount++
			_, err := os.Stat(filepath.Join(testDir, file.Name()))
			if err != nil {
				t.Logf("Error getting file info for %s: %v", file.Name(), err)
				continue
			}
			// t.Logf("Segment file: %s, size: %d", file.Name(), fileInfo.Size())
		}
	}
	// t.Logf("Found %d segment files", segmentCount)
	assert.Equal(t, expectedSegments, segmentCount, "Unexpected number of segment files")
}

func TestWalRecovery(t *testing.T) {
	t.Run("NormalRecovery", func(t *testing.T) {
		// Create and populate WAL
		opts := WalOptions{
			Path:           t.TempDir(),
			MaxSegmentSize: 1024 * 1024, // 1MB segments
			Seed:           12345,
		}
		w, err := Create(opts)
		require.NoError(t, err)

		records := []*Record{
			{Type: RecordTypeInsert, Tag: types.ObjectTagDatabase, Entity: 1, TxID: 100, Data: [][]byte{[]byte("data1")}},
			{Type: RecordTypeUpdate, Tag: types.ObjectTagDatabase, Entity: 2, TxID: 200, Data: [][]byte{[]byte("data2")}},
			{Type: RecordTypeDelete, Tag: types.ObjectTagDatabase, Entity: 3, TxID: 300, Data: [][]byte{[]byte("data3")}},
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
		opts := WalOptions{
			Path:           t.TempDir(),
			MaxSegmentSize: 1024 * 1024, // 1MB segments
			Seed:           12345,
		}
		w, err := Create(opts)
		require.NoError(t, err)

		// Write some records
		for i := 0; i < 5; i++ {
			rec := &Record{
				Type:   RecordTypeInsert,
				Tag:    types.ObjectTagDatabase,
				Entity: uint64(i),
				TxID:   uint64(i + 1*100),
				Data:   [][]byte{[]byte(fmt.Sprintf("data%d", i))},
			}
			_, err := w.Write(rec)
			require.NoError(t, err)
		}
		require.NoError(t, w.Sync())

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
		testDir := t.TempDir()
		opts := WalOptions{
			Path:           testDir,
			MaxSegmentSize: 1024 * 1024, // 1MB segments
			Seed:           12345,
			RecoveryMode:   RecoveryModeTruncate,
		}
		w, err := Create(opts)
		require.NoError(t, err)

		// Write some records
		for i := 0; i < 10; i++ {
			rec := &Record{
				Type:   RecordTypeInsert,
				Tag:    types.ObjectTagDatabase,
				Entity: uint64(i),
				TxID:   uint64(i + 1*100),
				Data:   [][]byte{[]byte(fmt.Sprintf("data%d", i))},
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
			if strings.HasSuffix(file.Name(), SEG_FILE_SUFFIX) {
				lastSegment = filepath.Join(testDir, file.Name())
			}
		}
		require.NotEmpty(t, lastSegment, "No segment file found")

		f, err := os.OpenFile(lastSegment, os.O_RDWR, 0644)
		require.NoError(t, err)
		_, err = f.WriteAt([]byte("CORRUPT"), 100)
		require.NoError(t, err)
		f.Close()

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
		assert.NoError(t, lastError, "Should encounter no more corruption")
	})
}

// TestWalCrashRecovery simulates a crash scenario and tests the WAL's ability
// to recover and maintain data integrity after an unexpected shutdown.
func TestWalCrashRecovery(t *testing.T) {
	opts := createWalOptions(t)
	w := createWal(t, opts)

	// Write some records
	records := []*Record{
		{Type: RecordTypeInsert, Tag: types.ObjectTagDatabase, Entity: 1, TxID: 100, Data: [][]byte{[]byte("data1")}},
		{Type: RecordTypeUpdate, Tag: types.ObjectTagDatabase, Entity: 2, TxID: 200, Data: [][]byte{[]byte("data2")}},
		{Type: RecordTypeDelete, Tag: types.ObjectTagDatabase, Entity: 3, TxID: 300, Data: [][]byte{[]byte("data3")}},
	}

	for _, rec := range records {
		_, err := w.Write(rec)
		require.NoError(t, err)
	}
	require.NoError(t, w.Sync())

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

func TestWalRecoveryWithPartialRecords(t *testing.T) {
	opts := WalOptions{
		Path:           t.TempDir(),
		MaxSegmentSize: SEG_FILE_MINSIZE, // Small segment size to force multiple segments
		Seed:           12345,
		RecoveryMode:   RecoveryModeTruncate,
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
			TxID:   uint64(i),
			Data:   [][]byte{[]byte(fmt.Sprintf("complete data %d", i))},
		}
		_, err := w.Write(rec)
		require.NoError(t, err)
	}

	// Write a partial record
	partialRec := &Record{
		Type:   RecordTypeUpdate,
		Tag:    types.ObjectTagDatabase,
		Entity: uint64(completeRecords),
		TxID:   uint64(completeRecords),
		Data:   [][]byte{[]byte("partial data")},
	}

	// Start writing the partial record
	lsn, err := w.Write(partialRec)
	require.NoError(t, err)

	// Close wal
	w.Close()

	// Simulate a crash by forcefully closing the file
	// Corrupt the last part of the file to simulate incomplete write
	segmentFile := w.segmentName(lsn.Segment(w.opts.MaxSegmentSize))
	f, err := os.OpenFile(segmentFile, os.O_RDWR, 0644)
	require.NoError(t, err)

	info, err := f.Stat()
	require.NoError(t, err)

	// Truncate the file to simulate partial write
	newSz := info.Size() - int64(len(partialRec.Data))
	// t.Logf("Truncate from %d to %d", info.Size(), newSz)
	err = f.Truncate(newSz)
	require.NoError(t, err)
	err = f.Close()
	require.NoError(t, err)

	// Attempt to recover
	recoveredWal, err := Open(0, opts)
	require.NoError(t, err)
	defer recoveredWal.Close()

	// Read and verify recovered records
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
		assert.Equal(t, uint64(j), rec.TxID)
		assert.Equal(t, [][]byte{[]byte(fmt.Sprintf("complete data %d", j))}, rec.Data)
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
		TxID:   uint64((completeRecords + 1)),
		Data:   [][]byte{[]byte("new record after recovery")},
	}
	_, err = recoveredWal.Write(newRec)
	require.NoError(t, err)
}

// TestWalFaultInjection simulates various fault scenarios to test the WAL's
// resilience and error handling capabilities under adverse conditions.
func TestWalFaultInjection(t *testing.T) {
	t.Run("WriteFailure", func(t *testing.T) {
		if u, err := user.Current(); err != nil || u.Uid == "0" {
			t.Skip()
		}
		testDir := t.TempDir()
		opts := WalOptions{
			Path:           testDir,
			MaxSegmentSize: 1024 * 1024, // 1MB segments
			Seed:           12345,
		}

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
			Data: [][]byte{bytes.Repeat([]byte("a"), opts.MaxSegmentSize+1)}, // Force new segment creation
		}
		_, err = w.Write(rec)
		assert.Error(t, err, "Expected an error when writing to a read-only directory")
	})

	t.Run("CorruptChecksum", func(t *testing.T) {
		testDir := t.TempDir()
		opts := WalOptions{
			Path:           testDir,
			MaxSegmentSize: 1024 * 1024, // 1MB segments
			Seed:           12345,
		}

		w, err := Create(opts)
		require.NoError(t, err)
		defer w.Close()

		rec := &Record{
			Type: RecordTypeInsert,
			Tag:  types.ObjectTagDatabase,
			Data: [][]byte{[]byte("test data")},
			TxID: 2,
		}
		lsn, err := w.Write(rec)
		require.NoError(t, err)

		// Corrupt the checksum
		segmentFile := w.segmentName(lsn.Segment(w.opts.MaxSegmentSize))
		f, err := os.OpenFile(segmentFile, os.O_RDWR, 0644)
		require.NoError(t, err)
		defer f.Close()

		_, err = f.Seek(lsn.Offset(w.opts.MaxSegmentSize)+HeaderSize-8, io.SeekStart)
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
			Data: [][]byte{bytes.Repeat([]byte("a"), 1000)},
			TxID: 1,
		}
		lsn, err := w.Write(rec)
		require.NoError(t, err)
		require.NoError(t, w.Sync())

		// Simulate a partial write by truncating the file
		segmentFile := w.segmentName(lsn.Segment(w.opts.MaxSegmentSize))
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
				Data: [][]byte{[]byte(fmt.Sprintf("data %d", i))},
			}
			lastLSN, err = w.Write(rec)
			require.NoError(t, err)
		}
		require.NoError(t, w.Sync())

		// Verify segment file exists
		verifySegmentExists(t, opts.Path, lastLSN, opts.MaxSegmentSize)

		// Simulate a crash by forcefully closing without proper shutdown
		w.active.Close()

		// Reopen the WAL
		reopenedWal, err := Open(0, opts)
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
		testDir := t.TempDir()
		opts := WalOptions{
			Path:           testDir,
			MaxSegmentSize: 1024 * 1024, // 1MB segments
			Seed:           12345,
		}
		w, err := Create(opts)
		require.NoError(t, err)
		defer w.Close()

		rec := &Record{
			Type: RecordTypeInsert,
			Tag:  types.ObjectTagDatabase,
			TxID: 2,
			Data: [][]byte{[]byte("test data")},
		}
		lsn, err := w.Write(rec)
		require.NoError(t, err)

		// Corrupt the header
		segmentFile := w.segmentName(lsn.Segment(w.opts.MaxSegmentSize))
		f, err := os.OpenFile(segmentFile, os.O_RDWR, 0644)
		require.NoError(t, err)
		defer f.Close()

		_, err = f.Seek(lsn.Offset(w.opts.MaxSegmentSize), io.SeekStart)
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
			Data: [][]byte{bytes.Repeat([]byte("a"), 1000)},
		}
		lsn, err := w.Write(rec)
		require.NoError(t, err)
		require.NoError(t, w.Sync())

		// Truncate the file to create an incomplete record
		segmentFile := w.segmentName(lsn.Segment(w.opts.MaxSegmentSize))
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
		testDir := t.TempDir()
		opts := WalOptions{
			Path:           testDir,
			MaxSegmentSize: 1024 * 1024, // 1MB segments
			Seed:           12345,
		}

		w, err := Create(opts)
		require.NoError(t, err)
		defer w.Close()

		rec := &Record{
			Type: RecordTypeInsert,
			Tag:  types.ObjectTagDatabase,
			TxID: 1,
			Data: [][]byte{[]byte("test data")},
		}
		lsn, err := w.Write(rec)
		require.NoError(t, err)

		// Corrupt the record type
		segmentFile := w.segmentName(lsn.Segment(w.opts.MaxSegmentSize))
		f, err := os.OpenFile(segmentFile, os.O_RDWR, 0644)
		require.NoError(t, err)
		defer f.Close()

		_, err = f.Seek(lsn.Offset(w.opts.MaxSegmentSize)+1, io.SeekStart) // Seek to record type position
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
		testDir := t.TempDir()
		opts := WalOptions{
			Path:           testDir,
			MaxSegmentSize: 1024 * 1024, // 1MB segments
			Seed:           12345,
		}

		w, err := Create(opts)
		require.NoError(t, err)
		defer w.Close()

		// Write records until close to segment boundary
		for i := 0; i < 1000; i++ {
			rec := &Record{
				Type: RecordTypeInsert,
				Tag:  types.ObjectTagDatabase,
				Data: [][]byte{bytes.Repeat([]byte("a"), 900)},
				TxID: uint64(i + 1),
			}
			_, err := w.Write(rec)
			require.NoError(t, err)
		}
		checkpoint := &Record{
			Type: RecordTypeCheckpoint,
			Tag:  types.ObjectTagTable,
		}
		lastLSN, err := w.Write(checkpoint)
		require.NoError(t, err)
		require.NoError(t, w.Sync())

		// Corrupt the segment boundary
		segmentFile := w.segmentName(lastLSN.Segment(w.opts.MaxSegmentSize))
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
	sizes := []int{256, 1 << 16, 1 << 20}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("RecordSize-%d", size), func(b *testing.B) {
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
					Data:   [][]byte{data},
				}
				_, err := w.Write(rec)
				require.NoError(b, err)
			}
		})
	}
}

// BenchmarkWalWriteSync tests writing records with and without sync
func BenchmarkWalWriteSync(b *testing.B) {
	opts := createWalOptions(b)
	w := createWal(b, opts)
	defer w.Close()

	size := 256
	data := make([]byte, size)
	b.SetBytes(int64(size))
	b.ResetTimer()

	for i := 1; i < b.N; i++ {
		rec := &Record{
			Type:   RecordTypeInsert,
			Tag:    types.ObjectTagDatabase,
			Entity: uint64(i),
			TxID:   uint64(i),
			Data:   [][]byte{data},
		}
		_, err := w.Write(rec)
		require.NoError(b, err)
		require.NoError(b, w.Sync())
	}
}

// BenchmarkWalWriteSchedule tests writing records with delayed batch sync
func BenchmarkWalWriteSchedule(b *testing.B) {
	opts := createWalOptions(b)
	w := createWal(b, opts)
	defer w.Close()

	size := 256
	data := make([]byte, size)
	b.SetBytes(int64(size))
	b.ResetTimer()

	for i := 1; i < b.N; i++ {
		rec := &Record{
			Type:   RecordTypeInsert,
			Tag:    types.ObjectTagDatabase,
			Entity: uint64(i),
			TxID:   uint64(i),
			Data:   [][]byte{data},
		}
		_, err := w.Write(rec)
		require.NoError(b, err)
		fut := w.Schedule()
		require.NoError(b, fut.Err())
		fut.Close()
	}
}

// BenchmarkWalSegmentSize tests writing with different segment sizes
func BenchmarkWalSegmentSize(b *testing.B) {
	segmentSizes := []int{1 << 16, 1 << 20, 1 << 26}
	for _, segmentSize := range segmentSizes {
		b.Run(fmt.Sprintf("sz-%d", segmentSize), func(b *testing.B) {
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
					Data:   [][]byte{data},
				}
				_, err := w.Write(rec)
				require.NoError(b, err)
			}
		})
	}
}
