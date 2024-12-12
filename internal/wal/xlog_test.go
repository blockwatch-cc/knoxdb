package wal

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/util"
	"github.com/stretchr/testify/require"
)

func xmin(id int) uint64 {
	return uint64(id) << CommitFrameShift
}

func xmax(id int) uint64 {
	return xmin(id) + 1<<CommitFrameShift - 1
}

func makeCommitFrameFile(dirPath string) (*os.File, error) {
	name := filepath.Join(dirPath, CommitLogName)
	fd, err := os.OpenFile(name, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	return fd, nil
}

func makeCommitLog(wal *Wal) *CommitLog {
	wal.createSegment(0)
	commitLog := NewCommitLog()
	return commitLog
}

func makeRecords(sz int) []*Record {
	recs := make([]*Record, 0, sz)
	for i := range sz {
		walTxId := i
		walBody := bytes.Repeat([]byte("data"), i)
		walTyp := RecordTypeInsert
		if i%10 == 0 {
			walTyp = RecordTypeCheckpoint
			walTxId = 0
			walBody = nil
		} else if i%15 == 0 {
			walTyp = RecordTypeCommit
		}
		recs = append(recs, &Record{
			Type:   walTyp,
			Tag:    types.ObjectTagDatabase,
			TxID:   uint64(walTxId),
			Entity: 10,
			Data:   [][]byte{walBody},
		})
	}
	return recs
}

func TestCommitFrameXmin(t *testing.T) {
	type testCase struct {
		Name         string
		Id           int64
		ExpectedXmin uint64
	}

	var testCases = []testCase{
		{
			Name:         "Xmin for frame 0",
			Id:           0,
			ExpectedXmin: xmin(0),
		},
		{
			Name:         "Xmin for frame 1",
			Id:           1,
			ExpectedXmin: xmin(1),
		},
		{
			Name:         "Xmin for frame 2",
			Id:           2,
			ExpectedXmin: xmin(2),
		},
		{
			Name:         "Xmin for frame -1",
			Id:           -1,
			ExpectedXmin: xmin(-1),
		},
		{
			Name:         "Xmin for frame -10",
			Id:           -10,
			ExpectedXmin: xmin(-10),
		},
	}
	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			commitFrame := NewCommitFrame(tc.Id)
			require.Equalf(t, tc.ExpectedXmin, commitFrame.Xmin(), "expected xmin to be %d", tc.ExpectedXmin)
		})
	}
}

func TestCommitFrameXmax(t *testing.T) {
	type testCase struct {
		Name         string
		Id           int64
		ExpectedXmax uint64
	}

	var testCases = []testCase{
		{
			Name:         "Xmax for frame 0",
			Id:           0,
			ExpectedXmax: xmax(0),
		},
		{
			Name:         "Xmax for frame 1",
			Id:           1,
			ExpectedXmax: xmax(1),
		},
		{
			Name:         "Xmax for frame 2",
			Id:           2,
			ExpectedXmax: xmax(2),
		},
		{
			Name:         "Xmin for frame -1",
			Id:           -1,
			ExpectedXmax: xmax(-1),
		},
		{
			Name:         "Xmin for frame -10",
			Id:           -10,
			ExpectedXmax: xmax(-10),
		},
	}
	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			commitFrame := NewCommitFrame(tc.Id)
			require.Equalf(t, tc.ExpectedXmax, commitFrame.Xmax(), "expected xmax to be %d", tc.ExpectedXmax)
		})
	}
}

func TestCommitFrameIsCommittedFrame(t *testing.T) {
	t.Run("checks if appended transaction id is committed", func(t *testing.T) {
		xid := uint64(100)
		commitFrame := NewCommitFrame(0)
		commitFrame.Append(xid, LSN(0))
		require.Truef(t, commitFrame.IsCommitted(xid), "xid %d was appended ", xid)
	})
	t.Run("checks if transaction id is not appended id, it is not committed", func(t *testing.T) {
		xid := uint64(100)
		commitFrame := NewCommitFrame(3)
		commitFrame.Append(xid, LSN(2))
		require.Falsef(t, commitFrame.IsCommitted(xid), "xid %d was not appended ", xid)
	})
}

func TestCommitFrameAppend(t *testing.T) {
	xid := uint64(100)
	commitFrame := NewCommitFrame(0)
	commitFrame.Append(xid, LSN(2))
	require.Truef(t, commitFrame.IsCommitted(xid), "xid %d was not appended ", xid)
	commitFrame.Close()
}

func TestCommitFrameContains(t *testing.T) {
	commitFrames := make([]CommitFrame, 10)
	for i := range commitFrames {
		commitFrames[i] = *NewCommitFrame(int64(i))
	}
	xids := make([]uint64, 10)
	for i := range xids {
		xids[i] = uint64(i << 19)
	}

	for j := range xids {
		for i, cm := range commitFrames {
			xidIsContained := cm.Contains(xids[j])
			if j == i {
				require.True(t, xidIsContained)
			} else {
				require.False(t, xidIsContained)
			}
		}
	}
}

func TestCommitFrameWriteTo(t *testing.T) {
	dir := t.TempDir()
	commitFrameFile, err := makeCommitFrameFile(dir)
	require.NoError(t, err)

	// get size of file
	statBefore, err := commitFrameFile.Stat()
	require.NoError(t, err, "failed to check file stat")

	cm := NewCommitFrame(0)
	cm.Append(0, LSN(0))
	cm.Append(2, LSN(10))
	cm.Append(4, LSN(20))
	err = cm.WriteTo(commitFrameFile)
	require.NoError(t, err)

	// check file exists
	f, err := os.Open(filepath.Join(dir, CommitLogName))
	require.NoError(t, err, "failed to open file")

	// check file size should be same after append
	statAfter, err := f.Stat()
	require.NoError(t, err, "fail to check file stats")
	require.Falsef(t, statAfter.Size() == statBefore.Size(), "file size should not be the same")
}

func TestCommitFrameReadFrom(t *testing.T) {
	t.Run("read data from frame", func(t *testing.T) {
		// test setup
		dir := t.TempDir()
		commitFrameFile, err := makeCommitFrameFile(dir)
		require.NoError(t, err)

		cm := NewCommitFrame(0)
		cm.Append(0, LSN(0))
		cm.Append(2, LSN(10))
		cm.Append(4, LSN(20))
		err = cm.WriteTo(commitFrameFile)
		require.NoError(t, err)
		cm.Close()

		// read from frame
		readCm := NewCommitFrame(0)
		err = readCm.ReadFrom(commitFrameFile)
		require.NoError(t, err, "failed to read frame")
	})

	t.Run("read corrupted data from frame", func(t *testing.T) {
		// test setup
		dir := t.TempDir()
		commitFrameFile, err := makeCommitFrameFile(dir)
		require.NoError(t, err)

		cm := NewCommitFrame(0)
		cm.Append(0, LSN(0))
		cm.Append(2, LSN(10))
		cm.Append(4, LSN(20))
		err = cm.WriteTo(commitFrameFile)
		require.NoError(t, err)
		cm.Close()

		// write data to head/body
		_, err = commitFrameFile.WriteAt([]byte("data"), 10)
		require.NoError(t, err, "write shouldnt fail")

		// read from file
		readCm := NewCommitFrame(0)
		err = readCm.ReadFrom(commitFrameFile)
		require.Error(t, err, "failed to read frame")
		require.ErrorIs(t, err, ErrChecksum, "error should be checksum error")
	})
}

func TestCommitLogOpen(t *testing.T) {
	dir := t.TempDir()
	wal := makeWal(t)
	commitLog := makeCommitLog(wal)
	err := commitLog.Open(dir, wal)
	require.NoError(t, err, "opening commit log should not return err")

	// check if file is created
	f, err := os.Stat(filepath.Join(dir, CommitLogName))
	require.NoError(t, err, "commit  should exist")

	// check if file is dir
	require.Falsef(t, f.IsDir(), "file should not be a directory")

	require.NoError(t, commitLog.Close(), "closing should not fail")
}

func TestCommitLogAppend(t *testing.T) {
	dir := t.TempDir()
	wal := makeWal(t)
	commitLog := makeCommitLog(wal)
	err := commitLog.Open(dir, wal)
	defer commitLog.Close()
	require.NoError(t, err, "opening commit log should not return err")

	recs := makeRecords(1 << 16)
	for _, rec := range recs {
		err := commitLog.Append(rec.TxID, rec.Lsn)
		require.NoError(t, err, "appending rec should not fail")
	}

	p := filepath.Join(dir, CommitLogName)
	f, err := os.OpenFile(p, os.O_APPEND|os.O_RDWR, 0600)
	require.NoError(t, err, "commit logger should exist")

	finfo, err := f.Stat()
	require.NoError(t, err, "error file information")

	_, err = f.Write([]byte("datanewdata"))
	require.NoError(t, err, "write extra data ")

	// try reopen commitlogger
	newCommitLogger := NewCommitLog()
	err = newCommitLogger.Open(dir, wal)
	require.NoError(t, err, "opening commit log should not return err")

	fdinfo, err := os.Stat(p)
	require.NoError(t, err, "error file information ")

	// check file size is the same as after the records were wrriten
	// corrupted data written was truncated
	require.Truef(t, finfo.Size() == fdinfo.Size(), "extra data written would be truncated")

	for i := 0; i < 100; i++ {
		commitLogAppendHelper(t)
	}
}

func commitLogAppendHelper(t *testing.T) {
	t.Helper()
	recs := []*Record{
		{
			Type:   RecordTypeCommit,
			Tag:    types.ObjectTagDatabase,
			TxID:   5,
			Lsn:    0,
			Entity: 10,
			Data:   [][]byte{[]byte("data")},
		},
		{
			Type:   RecordTypeCommit,
			Tag:    types.ObjectTagDatabase,
			TxID:   1 << 19,
			Lsn:    34 << 14,
			Entity: 10,
			Data:   [][]byte{[]byte("data")},
		},
		{
			Type:   RecordTypeCommit,
			Tag:    types.ObjectTagDatabase,
			TxID:   2 << 19,
			Lsn:    34 << 15,
			Entity: 10,
			Data:   [][]byte{[]byte("data")},
		},
		{
			Type:   RecordTypeCommit,
			Tag:    types.ObjectTagDatabase,
			TxID:   3 << 19,
			Lsn:    34 << 16,
			Entity: 10,
			Data:   [][]byte{[]byte("data")},
		},
	}

	dir := t.TempDir()
	wal := makeWal(t)

	commitLog := makeCommitLog(wal)
	err := commitLog.Open(dir, wal)
	require.NoError(t, err, "opening commit log should not return err")

	// write to each frame first
	for _, rec := range recs {
		err := commitLog.Append(rec.TxID+1, rec.Lsn+HeaderSize+4)
		require.NoError(t, err, "appending record should not fail")
	}

	util.RandShuffle(len(recs), func(i, j int) {
		recs[i], recs[j] = recs[j], recs[i]
	})

	for _, rec := range recs {
		err := commitLog.Append(rec.TxID, rec.Lsn)
		require.NoError(t, err, "appending record should not fail")
	}

	// check all records in frames are committed
	for _, rec := range recs {
		isCommitted, err := commitLog.IsCommitted(rec.TxID)
		require.NoError(t, err)
		require.True(t, isCommitted)
	}

	err = commitLog.Close()
	require.NoError(t, err, "closing logger should not fail")
}
