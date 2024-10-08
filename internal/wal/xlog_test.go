package wal

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func xmin(id int) uint64 {
	return uint64(id) << CommitFrameShift
}

func xmax(id int) uint64 {
	return xmin(id) + 1<<CommitFrameShift - 1
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

// TestCommitFrameReadFrom
// TestCommitFrameWriteTo

// TestCommitLogOpen
// TestCommitLogClose
// TestCommitLogSync
// TestCommitLogIsCommitted
// TestCommitLogAppend
// TestCommitLogLoadFrame
// TestCommitLogRecover
