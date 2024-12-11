// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc
//
// TestWorkload5 is a deterministic simulation test which executes
// database commands selected from a pseudo-random distribution.

package scenarios

import (
	"context"
	"errors"
	"fmt"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"blockwatch.cc/knoxdb/internal/engine"
	"blockwatch.cc/knoxdb/internal/tests"
	"blockwatch.cc/knoxdb/pkg/knox"
	"blockwatch.cc/knoxdb/pkg/util"
	"github.com/echa/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

type command int

const (
	insert command = iota
	update
	delete
	query
	stream
	fsync
	compact
	snapshot
	restart
	crash
)

func (c command) String() string {
	return cmdNames[cmdOfs[c] : cmdOfs[c+1]-1]
}

const (
	numCommands = 16 // 2048
	maxProcs    = 32
	tableName   = "all_types"
)

var (
	cmdNames = "insert_update_delete_query_stream_sync_compact_snapshot_restart_crash"
	cmdOfs   = []int{0, 7, 14, 21, 27, 34, 39, 47, 56, 64, 70}
	cumProbs []float64
	commands = []command{
		insert,
		update,
		delete,
		query,
		stream,
		fsync,
		compact,
		snapshot,
		restart,
		crash,
	}

	// the expected random occurance of commands, does not need to sum to 1.
	probs = map[command]float64{
		insert:   0.25,
		update:   0.05,
		delete:   0.05,
		query:    0.25,
		stream:   0.25,
		fsync:    0.02,
		compact:  0.001,
		snapshot: 0.0001,
		restart:  0.01,
		crash:    0.01,
	}

	NewTestValue = tests.NewAllTypes

	testRun int
)

type testType = tests.AllTypes

func init() {
	var sum float64
	for _, c := range commands {
		sum += probs[c]
		cumProbs = append(cumProbs, sum)
	}
}

func genCommand() command {
	f := util.RandFloat64()
	// Normalize f so it falls within a range.
	f *= cumProbs[len(cumProbs)-1]
	for i, p := range cumProbs {
		if f < p {
			return commands[i]
		}
	}
	// Should never reach here unless rounding error, but return an insert.
	return insert
}

type dbProvider struct {
	db atomic.Value
}

func (p *dbProvider) Get() *engine.Engine {
	return p.db.Load().(*engine.Engine)
}

func (p *dbProvider) Update(eng *engine.Engine) {
	p.db.Store(eng)
}

func canIgnoreError(err error) bool {
	if err == nil {
		return true
	}
	switch {
	case errors.Is(err, context.Canceled):
		return true
	case errors.Is(err, engine.ErrDatabaseClosed):
		return true
	case errors.Is(err, engine.ErrDatabaseShutdown):
		return true
	default:
		log.Infof("Don't ignore %T %v", err, err)
		return false
	}
}

func TestWorkload5(t *testing.T) {
	var (
		numTuples int64
		executed  = make(map[command]int)
		cmdCh     = make(chan command)
		errg      errgroup.Group
		ctx       = context.Background()
	)

	// manage random seeds to drive the determinism for this test
	seed := util.RandSeed()

	// create a new random seed for multiple runs unless a user-defined seed is used
	testRun++
	if testRun > 1 && os.Getenv(util.GORANDSEED) == "" {
		seed = util.RandUint64()
	}

	// re-init random number generator (resets pseudo-randomness so that
	// rand usage in other testcases does not impact the random selection here)
	t.Logf("Random seed 0x%016x", seed)
	util.RandInit(seed)

	if testing.Verbose() {
		log.SetLevel(log.LevelDebug)
	} else {
		log.SetLevel(log.LevelInfo)
	}

	// create new database and table
	db := &dbProvider{}
	{
		eng, _ := tests.NewDatabase(t, &tests.AllTypes{})
		db.Update(eng)
	}
	errg.SetLimit(maxProcs)

	// save database files on failure
	t.Cleanup(func() {
		tests.SaveDatabaseFiles(t, db.Get())
	})

	// set test failed when we detect a panic, this ensures cleanup above
	// actually runs
	defer func() {
		if e := recover(); e != nil {
			var msg string
			switch v := e.(type) {
			case string:
				msg = v
			case error:
				msg = v.Error()
			default:
				msg = fmt.Sprintf("%v", v)
			}
			t.Log("FAIL -- ", strings.SplitN(msg, "\n", 1)[0])
			t.Fail()
			panic(e)
		}
	}()

	// count number of commands for logging
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			c, ok := <-cmdCh
			if !ok {
				return
			}
			executed[c]++
		}
	}()

	// init: insert 1024 values (wrapped into sub-test to catch panics)
	t.Run("init", func(t *testing.T) {
		ins := make([]*testType, 1024)
		for i := range ins {
			ins[i] = NewTestValue(i + 1)
		}
		table, err := knox.UseGenericTable[testType](tableName, knox.WrapEngine(db.Get()))
		require.NoError(t, err)
		_, err = table.Insert(ctx, ins)
		require.NoError(t, err)
		numTuples = int64(len(ins))
		clear(ins)
	})

	// produce sequence of commands all at once so that even with non-deterministic
	// go runtime we get a somewhat reproducible behavior
	t.Logf("Running %d commands on %d goroutines", numCommands, maxProcs)
	schedule := make([]command, numCommands)
	for i := range schedule {
		schedule[i] = genCommand()
	}

	for i, cmd := range schedule {
		wrapErr := func(err error) error {
			if canIgnoreError(err) {
				return nil
			}
			return fmt.Errorf("%04d [%s]: %v", i, cmd, err)
		}
		switch cmd {
		case insert:
			errg.Go(func() error {
				runtime.Gosched()
				table, err := knox.UseGenericTable[testType](tableName, knox.WrapEngine(db.Get()))
				if err != nil {
					return wrapErr(err)
				}
				id := int(atomic.LoadInt64(&numTuples) + 1)
				pk, err := table.Insert(ctx, NewTestValue(id))
				if err != nil {
					return wrapErr(err)
				}
				atomic.AddInt64(&numTuples, 1)
				t.Logf("%04d [%s] %d", i, cmd, pk)
				cmdCh <- cmd
				return nil
			})
		case update:
			errg.Go(func() error {
				runtime.Gosched()
				table, err := knox.WrapEngine(db.Get()).UseTable(tableName)
				if err != nil {
					return wrapErr(err)
				}

				// pick a random id (may not exist due to delete)
				id := util.RandUint64n(uint64(atomic.LoadInt64(&numTuples)) + 1)
				t.Logf("%04d [%s] %d", i, cmd, id)

				// load record if exists
				var val testType
				err = knox.NewGenericQuery[testType]().
					WithTag("update").
					WithTable(table).
					AndEqual("id", id).
					Execute(ctx, &val)
				if err != nil {
					return wrapErr(err)
				}

				// ignore not found
				if val.Id == 0 {
					t.Logf("Update id %d[%d] not found", id, atomic.LoadInt64(&numTuples))
					return nil
				}

				// update
				val.Int64++
				_, err = table.Update(ctx, &val)
				if err != nil {
					return wrapErr(err)
				}
				cmdCh <- cmd
				return nil
			})
		case delete:
			errg.Go(func() error {
				runtime.Gosched()
				table, err := knox.UseGenericTable[testType](tableName, knox.WrapEngine(db.Get()))
				if err != nil {
					return wrapErr(err)
				}

				// pick a random id (may not exist post delete)
				id := util.RandUint64n(uint64(atomic.LoadInt64(&numTuples)) + 1)
				t.Logf("%04d [%s] %d", i, cmd, id)

				// load record if exists
				var val testType
				err = knox.NewGenericQuery[testType]().
					WithTag("delete").
					WithTable(table.Table()).
					AndEqual("id", id).
					Execute(ctx, &val)
				if err != nil {
					return wrapErr(err)
				}

				// ignore not found
				if val.Id == 0 {
					t.Logf("Delete id %d[%d] not found", id, atomic.LoadInt64(&numTuples))
					return nil
				}

				// delete by id
				n, err := knox.NewGenericQuery[testType]().
					WithTag("delete").
					WithTable(table.Table()).
					AndEqual("id", val.Id).
					Delete(ctx)

				if err != nil {
					return wrapErr(err)
				} else if n > 0 {
					atomic.AddInt64(&numTuples, -1)
					cmdCh <- cmd
				}
				return nil
			})
		case query:
			errg.Go(func() error {
				runtime.Gosched()
				table, err := knox.WrapEngine(db.Get()).UseTable(tableName)
				if err != nil {
					return wrapErr(err)
				}

				// pick a random id (may not exist post delete)
				id := util.RandUint64n(uint64(atomic.LoadInt64(&numTuples)) + 1)
				t.Logf("%04d [%s] %d", i, cmd, id)

				// point query
				var val testType
				err = knox.NewGenericQuery[testType]().
					WithTag("query").
					WithTable(table).
					AndGte("id", id).
					Execute(ctx, &val)
				if err != nil {
					return wrapErr(err)
				}
				cmdCh <- cmd
				return nil
			})
		case stream:
			errg.Go(func() error {
				runtime.Gosched()
				ctx, cancel := context.WithCancel(ctx)
				defer cancel()
				table, err := knox.WrapEngine(db.Get()).UseTable(tableName)
				if err != nil {
					return wrapErr(err)
				}

				// limit to N records (stop after, don't use query.Limit)
				after := util.RandUint64n(uint64(atomic.LoadInt64(&numTuples)) + 1)
				t.Logf("%04d [%s]", i, cmd)

				// pick an action randomly
				action := util.RandIntn(3)

				// pick an order randomly
				order := knox.OrderType(util.RandIntn(2))

				err = knox.NewGenericQuery[testType]().
					WithTag("stream").
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
				if err != nil {
					return wrapErr(err)
				}
				cmdCh <- cmd
				return nil
			})
		case fsync:
			errg.Go(func() error {
				t.Logf("%04d [%s]", i, cmd)
				err := db.Get().Sync(ctx)
				if err != nil {
					return wrapErr(err)
				}
				cmdCh <- cmd
				return nil
			})
		case compact:
			errg.Go(func() error {
				t.Logf("%04d [%s]", i, cmd)
				err := db.Get().CompactTable(ctx, tableName)
				if err != nil {
					return wrapErr(err)
				}
				cmdCh <- cmd
				return nil
			})
		case snapshot:
			errg.Go(func() error {
				t.Logf("%04d [%s]", i, cmd)
				// err := db.Get().Snapshot(ctx, io.Discard)
				// if err != nil {
				//     return wrapErr(err)
				// }
				cmdCh <- cmd
				return nil
			})

		case restart:
			t.Logf("%04d [%s]", i, cmd)
			// Graceful shutdown. Concurrent goroutines may fail.
			_ = errg.Wait()
			dir := db.Get().Options().Path
			require.NoError(t, db.Get().Close(ctx))

			// reopen
			t.Logf("Reopening DB at %s", dir)
			dbo := tests.NewTestDatabaseOptions(t, "").WithPath(dir)
			db.Update(tests.OpenTestEngine(t, dbo))
			cmdCh <- cmd

		case crash:
			t.Logf("%04d [%s]", i, cmd)
			_ = errg.Wait()
			eng := db.Get()
			dir := eng.Options().Path
			// Crash/unclean shutdown. Concurrent goroutines may fail.
			require.NoError(t, eng.ForceShutdown())
			eng = nil

			// reopen
			t.Logf("Reopening DB at %s", dir)
			dbo := tests.NewTestDatabaseOptions(t, "").WithPath(db.Get().Options().Path)
			db.Update(tests.OpenTestEngine(t, dbo))
			cmdCh <- cmd
		}
	}

	// Wait for all requests to complete.
	err := errg.Wait()
	if err != nil {
		t.Fail()
		require.NoError(t, err, "command error")
	}

	// close statistics channel
	close(cmdCh)
	wg.Wait()
	t.Logf("All commands completed: %v", executed)

	// sync (wrapped into sub-test to catch panics)
	t.Run("sync", func(t *testing.T) {
		t.Log("Sync/merge database.")
		require.NoError(t, db.Get().Sync(ctx))
	})

	// verify (wrapped into sub-test to catch panics)
	t.Run("verify", func(t *testing.T) {
		t.Log("Verifying data integrity.")
		table, err := knox.WrapEngine(db.Get()).UseTable(tableName)
		require.NoError(t, err, "use table")

		// log metrics
		m := table.Metrics()
		t.Logf("Tuple metrics -- total:%d inserted:%d updated:%d deleted:%d flushed:%d queried:%d streamed:%d",
			m.TupleCount,
			m.InsertedTuples,
			m.UpdatedTuples,
			m.DeletedTuples,
			m.FlushedTuples,
			m.QueriedTuples,
			m.StreamedTuples,
		)
		t.Logf("Call metrics -- inserts:%d updates:%d deletes:%d flushes:%d queries:%d streams:%d",
			m.InsertCalls,
			m.UpdateCalls,
			m.DeleteCalls,
			m.FlushCalls,
			m.QueryCalls,
			m.StreamCalls,
		)

		// TODO: improve integrity checks
		require.Equal(t, numTuples, m.TupleCount, "tuple count")

		// range scan
		var allTuples []*testType
		err = knox.NewQuery().
			WithTable(table).
			Execute(ctx, &allTuples)
		require.NoError(t, err, "range scan failed")
		require.Equal(t, len(allTuples), int(numTuples), "tuple count mismatch")

		// point queries
		for _, v := range allTuples {
			var oneTuple testType
			err = knox.NewQuery().
				WithTable(table).
				AndEqual("id", v.Id).
				Execute(ctx, &oneTuple)
			require.NoError(t, err, "range scan failed")
			require.Equal(t, v.Id, oneTuple.Id, "tuple id mismatch")
		}

		t.Log("OK.")
	})

	// close DB
	t.Log("Closing database.")
	tests.NoDeadlock(t, func() bool {
		assert.NoError(t, db.Get().Close(ctx))
		return true
	}, "deadlock on close")
	t.Log("Done.")
}
