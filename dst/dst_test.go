// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc
//
// TestDST runs deterministic simulation tests against KnoxDB.
//
// For true determinism and reproducibility, this test needs to be
// compiled to WASM and run inside a (single-threaded) WASM runtime.
//
// To compile the test
// - compile against the modified go runtime found at github.com/polarsignals/go
// - compile target is GOOS=wasip1 GOARCH=wasm
// - use -faketime flag
// - enable compile -tags=with_assert
//
// export GOROOT=/path/to/polarsignals/go
// GOOS=wasip1 GOARCH=wasm $GOROOT/bin/go test -tags=faketime,with_assert ./dst/ -c
//
// To run the test
// - set GORANDSEED
// - load the WASM module into a runtime
//
// GORANDSEED=1 go run ./runtime -module dst.test [test-flags]

package dst

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"blockwatch.cc/knoxdb/pack"
	"blockwatch.cc/knoxdb/store"
	"blockwatch.cc/knoxdb/store/mem"

	"github.com/echa/log"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

const (
	randomSeedKey = "GORANDSEED"
	numCommands   = 2048
	dbName        = "test"
	tableName     = "test"
)

type command int

const (
	insert command = iota
	update
	delete
	query
	stream
	sync
	compact
	snapshot
	restart
)

func (c command) String() string {
	switch c {
	case insert:
		return "insert"
	case update:
		return "update"
	case delete:
		return "delete"
	case query:
		return "query"
	case stream:
		return "stream"
	case sync:
		return "compact"
	case compact:
		return "compact"
	case snapshot:
		return "snapshot"
	case restart:
		return "restart"
	default:
		return "<unknown>"
	}
}

var commands = []command{insert, update, delete, query, stream, compact, snapshot, restart}

// probabilities are command probabilities. It is not strictly necessary that
// these sum to 1.
var probabilities = map[command]float64{
	insert: 0.25,
	update: 0.05,
	delete: 0.05,
	query:  0.25,
	stream: 0.25,
	// compact:  0.01,
	// snapshot: 0.01,
	// restart:  0.01,
}

var (
	cumulativeProbabilities []float64
	random                  *rand.Rand
)

func init() {
	var sum float64
	for _, p := range probabilities {
		sum += p
		cumulativeProbabilities = append(cumulativeProbabilities, sum)
	}
	log.SetLevel(log.LevelInfo)
	store.UseLogger(log.Log)
}

func genCommand() command {
	f := random.Float64()
	// Normalize f so it falls within a range.
	f *= cumulativeProbabilities[len(cumulativeProbabilities)-1]
	for i, p := range cumulativeProbabilities {
		if f < p {
			return commands[i]
		}
	}
	// Should never reach here unless rounding error, but return an insert.
	return insert
}

type testType struct {
	Id    uint64 `knox:"id,pk"` // auto-increment serial
	Val   int64  `knox:"val"`   // some random value
	Count int64  `knox:"count"` // counting updates
}

var opts = &mem.Options{
	// GetCallback: func(k, v []byte) []byte {
	// 	log.Infof("GET %x (%s)", k, string(k))
	// 	return v
	// },
	// PutCallback: func(k, v []byte) ([]byte, []byte, error) {
	// 	log.Infof("PUT %x (%s)", k, string(k))
	// 	return k, v, nil
	// },
	// DeleteCallback: func(k []byte) ([]byte, error) {
	// 	log.Infof("DEL %x (%s)", k, string(k))
	// 	return k, nil
	// },
}

func newPackTable(path string) (pack.Table, error) {
	fields, err := pack.Fields(testType{})
	if err != nil {
		return nil, err
	}
	db, err := pack.CreateDatabaseIfNotExists("mem", path, dbName, "*", opts)
	if err != nil {
		return nil, fmt.Errorf("creating database %q: %v", dbName, err)
	}

	table, err := db.CreateTableIfNotExists(
		pack.TableEnginePack,
		tableName,
		fields,
		pack.Options{
			PackSizeLog2:    8, // 256 entries
			JournalSizeLog2: 8, // 256 entries
			CacheSize:       1,
			FillLevel:       100,
		})
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("creating table %q: %v", tableName, err)
	}

	return table, nil
}

func openPackTable(path string) (pack.Table, error) {
	db, err := pack.OpenDatabase("mem", path, dbName, "*", opts)
	if err != nil {
		return nil, fmt.Errorf("open database %q: %v", dbName, err)
	}
	table, err := db.OpenTable(
		pack.TableEnginePack,
		tableName,
		pack.Options{
			JournalSizeLog2: 8,
			CacheSize:       1,
		},
	)
	if err != nil {
		return nil, fmt.Errorf("open table %q: %v", tableName, err)
	}
	return table, nil
}

type tableProvider struct {
	table atomic.Value
}

func (t *tableProvider) GetTable() pack.Table {
	return t.table.Load().(pack.Table)
}

func (t *tableProvider) Update(table pack.Table) {
	t.table.Store(table)
}

func canIgnoreError(err error) bool {
	switch {
	case errors.Is(err, context.Canceled):
		return true
	case errors.Is(err, pack.ErrDatabaseClosed):
		return true
	default:
		return false
	}
}

func TestDST(t *testing.T) {
	if os.Getenv(randomSeedKey) == "" {
		t.Fatalf("%s not set, skipping deterministic simulation tests", randomSeedKey)
	}
	t.Helper()
	t.Run(os.Getenv(randomSeedKey), runTestDST)
}

func runTestDST(t *testing.T) {
	seed, err := strconv.ParseUint(os.Getenv(randomSeedKey), 0, 64)
	require.NoError(t, err)
	random = rand.New(rand.NewSource(int64(seed)))

	// create new database and table
	// storageDir := t.TempDir()
	storageDir := "mem_test"
	tp := &tableProvider{}
	{
		// Separate scope to avoid table pointer misuse.
		table, err := newPackTable(storageDir)
		require.NoError(t, err)
		tp.Update(table)
	}

	t.Logf("DB initialized, running %d commands", numCommands)
	ctx := context.Background()
	errg := &errgroup.Group{}
	errg.SetLimit(32)
	commandDistribution := make(map[command]int)
	var numTuples int64

	for i := 0; i < numCommands; i++ {
		cmd := genCommand()
		commandDistribution[cmd]++
		switch cmd {
		case insert:
			errg.Go(func() error {
				// This is a hack to ensure some randomized goroutine scheduling.
				time.Sleep(1 * time.Millisecond)
				table := tp.GetTable()
				val := testType{
					Val: atomic.LoadInt64(&numTuples) + 1,
				}
				if err := table.Insert(ctx, &val); err != nil && !canIgnoreError(err) {
					return fmt.Errorf("insert error: %s", err)
				}
				atomic.AddInt64(&numTuples, 1)
				return nil
			})
		case update:
			id := random.Uint64()
			errg.Go(func() error {
				// This is a hack to ensure some randomized goroutine scheduling.
				time.Sleep(1 * time.Millisecond)
				table := tp.GetTable()
				id = id % uint64(max(atomic.LoadInt64(&numTuples), 1))
				var val testType
				err := pack.NewQuery("update").WithTable(table).AndGte("id", id).Execute(ctx, &val)
				if err != nil && !canIgnoreError(err) {
					return fmt.Errorf("update query error: %s", err)
				}
				if val.Id == 0 {
					// commandDistribution[cmd]--
					return nil
				}
				val.Count++
				if err := table.Update(ctx, &val); err != nil && !canIgnoreError(err) {
					return fmt.Errorf("update error: %s", err)
				}
				return nil
			})
		case delete:
			id := random.Uint64()
			errg.Go(func() error {
				// This is a hack to ensure some randomized goroutine scheduling.
				time.Sleep(1 * time.Millisecond)
				table := tp.GetTable()
				id = id % uint64(max(atomic.LoadInt64(&numTuples), 1))

				// find a value that actually exists
				var val testType
				err := pack.NewQuery("delete").WithTable(table).AndGte("id", id).Execute(ctx, &val)
				if err != nil && !canIgnoreError(err) {
					return fmt.Errorf("delete query error: %s", err)
				}
				if val.Id == 0 {
					// commandDistribution[cmd]--
					return nil
				}

				// delete it
				if n, err := table.DeletePks(ctx, []uint64{val.Id}); err != nil && !canIgnoreError(err) {
					return fmt.Errorf("delete error: %s", err)
				} else if n > 0 {
					atomic.AddInt64(&numTuples, -1)
				}
				return nil
			})
		case query:
			id := random.Uint64()
			errg.Go(func() error {
				// This is a hack to ensure some randomized goroutine scheduling.
				time.Sleep(1 * time.Millisecond)
				table := tp.GetTable()
				id = id % uint64(max(atomic.LoadInt64(&numTuples), 1))
				var val testType
				err := pack.NewQuery("query").WithTable(table).AndGte("id", id).Execute(ctx, &val)
				if err != nil && !canIgnoreError(err) {
					return fmt.Errorf("query error: %s", err)
				}
				return nil
			})
		case stream:
			action := random.Intn(3)
			after := random.Int63()
			errg.Go(func() error {
				// This is a hack to ensure some randomized goroutine scheduling.
				time.Sleep(1 * time.Millisecond)
				ctx2, cancel := context.WithCancel(ctx)
				table := tp.GetTable()
				after = after % int64(max(atomic.LoadInt64(&numTuples), 1))
				err := table.
					Stream(ctx2,
						pack.NewQuery("query").AndGt("id", 0).WithDesc(),
						func(r pack.Row) error {
							var val testType
							if err := r.Decode(&val); err != nil {
								return err
							}
							after--
							if after > 0 {
								return nil
							}
							switch action {
							case 0:
								// cancel context
								cancel()
								return nil
							case 1:
								// skip results
								return pack.EndStream
							default:
								// continue reading results
								return nil
							}
						})
				if err != nil && !canIgnoreError(err) {
					return fmt.Errorf("query error: %s", err)
				}
				return nil
			})
		case sync:
			errg.Go(func() error {
				err := tp.GetTable().Sync(ctx)
				if err != nil && !canIgnoreError(err) {
					return fmt.Errorf("sync error: %s", err)
				}
				return nil
			})
		case compact:
			errg.Go(func() error {
				err := tp.GetTable().Compact(ctx)
				if err != nil && !canIgnoreError(err) {
					return fmt.Errorf("compact error: %s", err)
				}
				return nil
			})
		case snapshot:
			errg.Go(func() error {
				err := tp.GetTable().DB().Dump(io.Discard)
				if err != nil && !canIgnoreError(err) {
					return fmt.Errorf("snaphsot error: %s", err)
				}
				return nil
			})
		case restart:
			errg.Go(func() error {
				// This is a hack to ensure some randomized goroutine scheduling.
				time.Sleep(1 * time.Millisecond)

				// Graceful shutdown.
				require.NoError(t, tp.GetTable().DB().Close())
				_ = errg.Wait()

				// open again
				table, err := openPackTable(storageDir)
				require.NoError(t, err)
				tp.Update(table)

				// try insert
				val := testType{Val: int64(atomic.LoadInt64(&numTuples) + 1)}
				require.NoError(t, table.Insert(ctx, &val))

				return nil
			})
		}
	}

	// Wait for all requests to complete.
	require.NoError(t, errg.Wait())

	t.Log("All commands completed.")
	t.Logf("Command distribution: %v", commandDistribution)
	t.Log("Sync/merge database.")

	// Merge-flush journal data into table, also updates tuple counter
	require.NoError(t, tp.GetTable().(*pack.PackTable).Flush(ctx))

	// Defer a close here. This is not done at the start of the test because
	// the test run itself may close the store.
	defer tp.GetTable().Close()

	t.Log("Verifying data integrity.")
	stats := tp.GetTable().Stats()
	require.Len(t, stats, 1)
	require.Equal(t, numTuples, stats[0].TupleCount)
	t.Log("OK.")
}
