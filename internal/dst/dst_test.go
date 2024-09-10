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
	"math/rand"
	"os"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"blockwatch.cc/knoxdb/internal/engine"
	"blockwatch.cc/knoxdb/internal/store"
	"blockwatch.cc/knoxdb/pkg/knox"
	"blockwatch.cc/knoxdb/pkg/num"
	"blockwatch.cc/knoxdb/pkg/schema"

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
	Id        uint64         `knox:"id,pk"` // auto-increment serial
	Val       int64          `knox:"val"`   // some random value
	Count     int64          `knox:"count"` // counting updates
	Timestamp time.Time      `knox:"time"`
	Hash      []byte         `knox:"hash,index=bloom:3"`
	String    string         `knox:"string"`
	Bool      bool           `knox:"bool"`
	MyEnum    schema.Enum    `knox:"my_enum,enum"`
	Int64     int64          `knox:"int64"`
	Int32     int32          `knox:"int32"`
	Int16     int16          `knox:"int16"`
	Int8      int8           `knox:"int8"`
	Int_64    int            `knox:"int_as_int64"`
	Uint64    uint64         `knox:"uint64,index=bloom:2"`
	Uint32    uint32         `knox:"uint32"`
	Uint16    uint16         `knox:"uint16"`
	Uint8     uint8          `knox:"uint8"`
	Uint_64   uint           `knox:"uint_as_uint64"`
	Float64   float64        `knox:"float64"`
	Float32   float32        `knox:"float32"`
	D32       num.Decimal32  `knox:"decimal32,scale=5"`
	D64       num.Decimal64  `knox:"decimal64,scale=15"`
	D128      num.Decimal128 `knox:"decimal128,scale=18"`
	D256      num.Decimal256 `knox:"decimal256,scale=24"`
	I128      num.Int128     `knox:"int128"`
	I256      num.Int256     `knox:"int256"`
}

func newDB(ctx context.Context, path string) (knox.Database, error) {
	log.Info("Creating DB")
	db, err := knox.CreateDatabase(ctx, "dst", knox.DatabaseOptions{
		Path:      path,
		Driver:    "mem",
		Namespace: "cx.bwd.knox.deterministic-simulation-test",
		CacheSize: 1 << 20 * 16,
		Logger:    log.Log,
	})
	if err != nil {
		return nil, err
	}

	log.Info("Creating Enum")
	var enum schema.EnumLUT
	enum, err = db.CreateEnum(ctx, "my_enum")
	if err != nil {
		db.Close(ctx)
		return nil, err
	}
	err = db.ExtendEnum(ctx, "my_enum", "one", "two", "three", "four")
	if err != nil {
		db.Close(ctx)
		return nil, err
	}
	schema.RegisterEnum(enum)

	s, err := schema.SchemaOf(&testType{})
	if err != nil {
		db.Close(ctx)
		return nil, err
	}

	log.Infof("Creating Table %s", s.Name())
	_, err = db.CreateTable(ctx, s, knox.TableOptions{
		Engine:      "pack",
		Driver:      "mem",
		PackSize:    1 << 16,
		JournalSize: 1 << 17,
		PageFill:    0.9,
	})
	if err != nil {
		db.Close(ctx)
		return nil, err
	}

	return db, nil
}

func openDB(ctx context.Context, path string) (knox.Database, error) {
	log.Info("Opening DB")
	return knox.OpenDatabase(ctx, "types", knox.DatabaseOptions{
		Path:      path,
		Driver:    "mem",
		Namespace: "cx.bwd.knox.deterministic-simulation-test",
		Logger:    log.Log,
	})
}

type dbProvider struct {
	db atomic.Value
}

func (p *dbProvider) GetDB() knox.Database {
	return p.db.Load().(knox.Database)
}

func (p *dbProvider) Update(db knox.Database) {
	p.db.Store(db)
}

func canIgnoreError(err error) bool {
	switch {
	case errors.Is(err, context.Canceled):
		return true
	case errors.Is(err, engine.ErrDatabaseClosed):
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

	ctx := context.Background()

	// create new database and table
	// storageDir := t.TempDir()
	storageDir := "mem_test"
	tp := &dbProvider{}
	{
		// Separate scope to avoid table pointer misuse.
		table, err := newDB(ctx, storageDir)
		require.NoError(t, err)
		tp.Update(table)
	}

	t.Logf("DB initialized, running %d commands", numCommands)
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
				table, err := knox.UseGenericTable[testType]("test_type", tp.GetDB())
				if err != nil {
					return err
				}
				val := testType{
					Val:    atomic.LoadInt64(&numTuples) + 1,
					MyEnum: "one",
				}
				if _, err := table.Insert(ctx, &val); err != nil && !canIgnoreError(err) {
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
				table, err := tp.GetDB().UseTable("test_type")
				if err != nil {
					return err
				}
				id = id % uint64(max(atomic.LoadInt64(&numTuples), 1))
				var val testType
				err = knox.NewQuery[testType]().
					WithKey("update").
					WithTable(table).
					AndGte("id", id).
					Execute(ctx, &val)
				if err != nil && !canIgnoreError(err) {
					return fmt.Errorf("update query error: %s", err)
				}
				if val.Id == 0 {
					// commandDistribution[cmd]--
					return nil
				}
				val.Count++
				val.MyEnum = "two"
				if _, err := table.Update(ctx, &val); err != nil && !canIgnoreError(err) {
					return fmt.Errorf("update error: %s", err)
				}
				return nil
			})
		case delete:
			id := random.Uint64()
			errg.Go(func() error {
				// This is a hack to ensure some randomized goroutine scheduling.
				time.Sleep(1 * time.Millisecond)
				table, err := knox.UseGenericTable[testType]("test_type", tp.GetDB())
				if err != nil {
					return err
				}
				id = id % uint64(max(atomic.LoadInt64(&numTuples), 1))

				// find a value that actually exists
				var val testType
				err = knox.NewQuery[testType]().
					WithKey("delete").
					WithTable(table.Table()).
					AndGte("id", id).
					Execute(ctx, &val)
				if err != nil && !canIgnoreError(err) {
					return fmt.Errorf("delete query error: %s", err)
				}
				if val.Id == 0 {
					// commandDistribution[cmd]--
					return nil
				}

				// delete it
				n, err := knox.NewQuery[testType]().
					WithKey("delete").
					WithTable(table.Table()).
					AndEqual("id", val.Id).
					Delete(ctx)

				if err != nil && !canIgnoreError(err) {
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
				table, err := tp.GetDB().UseTable("test_type")
				if err != nil {
					return err
				}
				id = id % uint64(max(atomic.LoadInt64(&numTuples), 1))
				var val testType
				err = knox.NewQuery[testType]().
					WithKey("query").
					WithTable(table).
					AndGte("id", id).
					Execute(ctx, &val)
				if err != nil && !canIgnoreError(err) {
					return fmt.Errorf("query error: %s", err)
				}
				return nil
			})
		case stream:
			action := random.Intn(3)
			order := knox.OrderType(random.Intn(1))
			after := random.Int63()
			errg.Go(func() error {
				// This is a hack to ensure some randomized goroutine scheduling.
				time.Sleep(1 * time.Millisecond)
				ctx, cancel := context.WithCancel(ctx)
				defer cancel()
				table, err := tp.GetDB().UseTable("test_type")
				if err != nil {
					return err
				}
				after = after % int64(max(atomic.LoadInt64(&numTuples), 1))
				err = knox.NewQuery[testType]().
					WithKey("query").
					WithTable(table).
					AndGt("id", 0).
					WithOrder(order).
					Stream(ctx, func(v *testType) error {
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
							return engine.EndStream
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
				err := tp.GetDB().Sync(ctx)
				if err != nil && !canIgnoreError(err) {
					return fmt.Errorf("sync error: %s", err)
				}
				return nil
			})
		case compact:
			errg.Go(func() error {
				err := tp.GetDB().CompactTable(ctx, "test_type")
				if err != nil && !canIgnoreError(err) {
					return fmt.Errorf("compact error: %s", err)
				}
				return nil
			})
		// case snapshot:
		// 	errg.Go(func() error {
		// 		err := tp.GetDB().Snapshot(ctx, io.Discard)
		// 		if err != nil && !canIgnoreError(err) {
		// 			return fmt.Errorf("snaphsot error: %s", err)
		// 		}
		// 		return nil
		// 	})
		case restart:
			errg.Go(func() error {
				// This is a hack to ensure some randomized goroutine scheduling.
				time.Sleep(1 * time.Millisecond)

				// Graceful shutdown.
				require.NoError(t, tp.GetDB().Close(ctx))
				_ = errg.Wait()

				// open again
				db, err := openDB(ctx, storageDir)
				require.NoError(t, err)
				tp.Update(db)

				// try insert
				table, err := knox.UseGenericTable[testType]("test_type", db)
				require.NoError(t, err)
				val := testType{Val: int64(atomic.LoadInt64(&numTuples) + 1), MyEnum: "one"}
				_, err = table.Insert(ctx, &val)
				require.NoError(t, err)

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
	require.NoError(t, tp.GetDB().CompactTable(ctx, "test_type"))

	// Defer a close here. This is not done at the start of the test because
	// the test run itself may close the store.
	defer tp.GetDB().Close(ctx)

	t.Log("Verifying data integrity.")
	table, err := tp.GetDB().UseTable("test_type")
	require.NoError(t, err)
	stats := table.Stats()
	t.Logf("%#v", stats)
	require.Equal(t, numTuples, stats.TupleCount)
	t.Log("OK.")
}
