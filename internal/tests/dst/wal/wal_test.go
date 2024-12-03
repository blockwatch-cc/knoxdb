package wal

import (
	"bytes"
	"math/rand"
	"os"
	"strconv"
	"testing"

	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/internal/wal"
	"github.com/stretchr/testify/require"
)

const (
	randomSeedKey = "GORANDSEED"
)

var (
	random *rand.Rand
)

func TestWal(t *testing.T) {
	if os.Getenv(randomSeedKey) == "" {
		t.Fatalf("%s not set, skipping deterministic simulation tests", randomSeedKey)
	}
	t.Helper()
	t.Run(os.Getenv(randomSeedKey), runTestDSTWAL)
}

func runTestDSTWAL(t *testing.T) {
	seed, err := strconv.ParseUint(os.Getenv(randomSeedKey), 0, 64)
	require.NoError(t, err)
	random = rand.New(rand.NewSource(int64(seed)))

	opts := wal.WalOptions{
		Path:           t.TempDir(),
		MaxSegmentSize: 100 << 20, // 100MB
		RecoveryMode:   wal.RecoveryModeFail,
	}
	w, err := wal.Create(opts)
	require.NoError(t, err)

	// num := random.Int()
	records := GenerateRecords(100)

	for _, record := range records {
		_, err = w.Write(record)
		require.NoError(t, err)
	}

	// close wal
	err = w.Close()
	require.NoError(t, err)
}

func GenerateRecords(sz int) []*wal.Record {
	recs := make([]*wal.Record, 0, sz)
	for i := range sz {
		walTxId := i
		walBody := bytes.Repeat([]byte("data"), i)
		walTyp := wal.RecordTypeInsert
		if i%10 == 0 {
			walTyp = wal.RecordTypeCheckpoint
			walTxId = 0
			walBody = nil
		} else if i%15 == 0 {
			walTyp = wal.RecordTypeCommit
		}
		recs = append(recs, &wal.Record{
			Type:   walTyp,
			Tag:    types.ObjectTagDatabase,
			TxID:   uint64(walTxId),
			Entity: 10,
			Data:   [][]byte{walBody},
		})
	}
	return recs
}
