package wal

import (
	"os"
	"path/filepath"
	"testing"

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
	t.Run("can append transaction id to commit frame", func(t *testing.T) {
		xid := uint64(100)
		commitFrame := NewCommitFrame(0)
		commitFrame.Append(xid, LSN(2))
		require.Truef(t, commitFrame.IsCommitted(xid), "xid %d was not appended ", xid)
		commitFrame.Close()
	})
}

func TestCommitFrameContains(t *testing.T) {
	t.Run("check transaction id is contained in a frame", func(t *testing.T) {
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
	})
}

func TestCommitFrameWriteTo(t *testing.T) {
	t.Run("write data to frame", func(t *testing.T) {
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

	})
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

	t.Run("fails to read corrupted data from frame", func(t *testing.T) {
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

// TestCommitLogOpen
// TestCommitLogClose
// TestCommitLogSync
// TestCommitLogIsCommitted
// TestCommitLogAppend
// TestCommitLogLoadFrame
// TestCommitLogRecover
