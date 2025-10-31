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
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"blockwatch.cc/knoxdb/internal/engine"
	tests "blockwatch.cc/knoxdb/internal/tests/engine"
	"blockwatch.cc/knoxdb/internal/types"
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
	numCommands = 2048
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
		compact:  0.0001,
		snapshot: 0.0001,
		restart:  0.01,
		crash:    0.01,
	}

	NewTestValue = tests.NewAllTypes

	testRun int
)

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

var lastCrash atomic.Int64

type dbProvider struct {
	db atomic.Value
}

func (p *dbProvider) Get() *engine.Engine {
	return p.db.Load().(*engine.Engine)
}

func (p *dbProvider) Update(eng *engine.Engine) {
	p.db.Store(eng)
}

func canIgnoreError(err error, round int) bool {
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
		if round < int(lastCrash.Load()) {
			return true
		}
		// log.Error(err)
		return false
	}
}

func TestWorkload5(t *testing.T) {
	var (
		numTuples  int64
		numInserts int64
		executed   = make(map[command]int)
		cmdCh      = make(chan command)
		errg       errgroup.Group
		ctx        = context.Background()
	)

	// setup determinism
	SetupDeterministicRand(t)

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
		ins := make([]*tests.AllTypes, 1024)
		for i := range ins {
			ins[i] = NewTestValue(i + 1)
		}
		table, err := knox.FindGenericTable[tests.AllTypes](tableName, knox.WrapEngine(db.Get()))
		require.NoError(t, err)
		_, _, err = table.Insert(ctx, ins)
		require.NoError(t, err)
		numTuples = int64(len(ins))
		clear(ins)
	})

	if t.Failed() {
		return
	}

	// produce sequence of commands all at once so that even with non-deterministic
	// go runtime we get a somewhat reproducible behavior
	t.Logf("Running %d commands on %d goroutines", numCommands, maxProcs)
	schedule := make([]command, numCommands)
	for i := range schedule {
		schedule[i] = genCommand()
	}

	for i, cmd := range schedule {
		round := i
		wrapErr := func(err error) error {
			if canIgnoreError(err, round) {
				return nil
			}
			return fmt.Errorf("%04d [%s]: %v", round, cmd, err)
		}
		switch cmd {
		case insert:
			errg.Go(func() error {
				runtime.Gosched()
				if round < int(lastCrash.Load()) {
					return nil
				}
				table, err := knox.FindGenericTable[tests.AllTypes](tableName, knox.WrapEngine(db.Get()))
				if err != nil {
					return wrapErr(err)
				}
				pk, _, err := table.Insert(ctx, NewTestValue(int(atomic.AddInt64(&numInserts, 1))))
				if err != nil {
					return wrapErr(err)
				}
				t.Logf("%04d [%s] %d", round, cmd, pk)
				atomic.AddInt64(&numTuples, 1)

				cmdCh <- cmd
				return nil
			})
		case update:
			errg.Go(func() error {
				runtime.Gosched()
				if round < int(lastCrash.Load()) {
					return nil
				}
				table, err := knox.WrapEngine(db.Get()).FindTable(tableName)
				if err != nil {
					return wrapErr(err)
				}

				// pick a random id (may not exist due to delete)
				id := util.RandUint64n(uint64(atomic.LoadInt64(&numInserts)) + 1)
				t.Logf("%04d [%s] %d", round, cmd, id)

				// load record if exists
				var val tests.AllTypes
				n, err := knox.NewGenericQuery[tests.AllTypes]().
					WithTag("update-"+strconv.Itoa(round)).
					// WithDebug(true).
					WithTable(table).
					AndEqual("id", id).
					Execute(ctx, &val)
				if err != nil {
					return wrapErr(err)
				}

				// ignore not found
				if n == 0 {
					t.Logf("%04d [%s] id %d not found", round, cmd, id)
					return nil
				}

				// sanity check
				if id != val.Id {
					return fmt.Errorf("found invalid id=%d for query with id=%d", val.Id, id)
				}

				// update
				val.Int64++
				n, err = table.Update(ctx, &val)
				switch {
				case errors.Is(err, knox.ErrNoRecord):
					// race condition with delete
				case err != nil && n == 0:
					return wrapErr(err)
				case n == 0:
					// invalid zero update without error
					return fmt.Errorf("invalid zero update without error")
				case n > 1:
					// must not happen
					return fmt.Errorf("updated %d records with id=%d", n, val.Id)
				case n == 1:
					// success
					cmdCh <- cmd
				}
				return nil
			})
		case delete:
			errg.Go(func() error {
				runtime.Gosched()
				if round < int(lastCrash.Load()) {
					return nil
				}
				table, err := knox.FindGenericTable[tests.AllTypes](tableName, knox.WrapEngine(db.Get()))
				if err != nil {
					return wrapErr(err)
				}

				// pick a random id (may not exist post delete)
				id := util.RandUint64n(uint64(atomic.LoadInt64(&numInserts)) + 1)

				t.Logf("%04d [%s] %d", round, cmd, id)

				// load record if exists
				var val tests.AllTypes
				n, err := knox.NewGenericQuery[tests.AllTypes]().
					WithTag("delete-"+strconv.Itoa(round)).
					WithTable(table.Table()).
					AndEqual("id", id).
					Execute(ctx, &val)
				if err != nil {
					return wrapErr(err)
				}

				// ignore not found
				if n == 0 {
					t.Logf("%04d [%s] id %d not found", round, cmd, id)
					return nil
				}

				// sanity check
				if id != val.Id {
					return fmt.Errorf("found invalid id=%d for query with id=%d", val.Id, id)
				}

				// delete by id
				n, err = knox.NewGenericQuery[tests.AllTypes]().
					WithTag("delete-"+strconv.Itoa(round)).
					// WithDebug(true).
					WithTable(table.Table()).
					AndEqual("id", val.Id).
					Delete(ctx)

				switch {
				case err != nil:
					// must not happen
					return wrapErr(err)
				case n == 0:
					// may happen due to race with concurrent delete
				case n == 1:
					// expected success case
					atomic.AddInt64(&numTuples, -1)
					cmdCh <- cmd
				case n > 1:
					// must not happen
					return fmt.Errorf("deleted %d records with id=%d", n, val.Id)
				}
				return nil
			})
		case query:
			errg.Go(func() error {
				runtime.Gosched()
				if round < int(lastCrash.Load()) {
					return nil
				}
				table, err := knox.WrapEngine(db.Get()).FindTable(tableName)
				if err != nil {
					return wrapErr(err)
				}

				// pick a random id (may not exist post delete)
				id := util.RandUint64n(uint64(atomic.LoadInt64(&numTuples)) + 1)
				t.Logf("%04d [%s] %d", round, cmd, id)

				// point query
				var val tests.AllTypes
				_, err = knox.NewGenericQuery[tests.AllTypes]().
					WithTag("query-"+strconv.Itoa(round)).
					// WithDebug(testing.Verbose()).
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
				if round < int(lastCrash.Load()) {
					return nil
				}

				// limit to N records (stop after, don't use query.Limit)
				after := util.RandUint64n(uint64(atomic.LoadInt64(&numTuples)) + 1)
				t.Logf("%04d [%s]", round, cmd)

				// pick an action randomly
				action := util.RandIntn(3)

				// pick an order randomly
				order := knox.OrderType(util.RandIntn(2))

				ctx, cancel := context.WithCancel(ctx)
				defer cancel()
				eng := db.Get()
				table, err := knox.WrapEngine(eng).FindTable(tableName)
				if err != nil {
					return wrapErr(err)
				}

				err = knox.NewGenericQuery[tests.AllTypes]().
					WithTag("stream-"+strconv.Itoa(round)).
					// WithDebug(testing.Verbose()).
					WithTable(table).
					AndGt("id", 0).
					WithOrder(order).
					Stream(ctx, func(v *tests.AllTypes) error {
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
							return types.EndStream
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
				if round < int(lastCrash.Load()) {
					return nil
				}
				t.Logf("%04d [%s]", round, cmd)
				err := db.Get().Sync(ctx)
				if err != nil {
					return wrapErr(err)
				}
				cmdCh <- cmd
				return nil
			})
		case compact:
			errg.Go(func() error {
				if round < int(lastCrash.Load()) {
					return nil
				}
				t.Logf("%04d [%s]", round, cmd)
				err := db.Get().CompactTable(ctx, tableName)
				if err != nil {
					return wrapErr(err)
				}
				cmdCh <- cmd
				return nil
			})
		case snapshot:
			errg.Go(func() error {
				if round < int(lastCrash.Load()) {
					return nil
				}
				t.Logf("%04d [%s]", round, cmd)
				// err := db.Get().Snapshot(ctx, io.Discard)
				// if err != nil {
				//     return wrapErr(err)
				// }
				cmdCh <- cmd
				return nil
			})

		case restart:
			t.Logf("%04d [%s]", round, cmd)
			lastCrash.Store(int64(round))
			// Graceful shutdown. Concurrent goroutines may fail.
			_ = errg.Wait()
			dir := db.Get().Options().Path
			require.NoError(t, db.Get().Close(ctx))

			// reopen
			t.Logf("%04d [%s] reopening DB at %s", round, cmd, dir)
			dbo := tests.NewTestDatabaseOptions(t, "").WithPath(dir)
			eng, err := engine.Open(context.Background(), tests.TEST_DB_NAME, dbo)
			if err != nil {
				lastCrash.Store(int64(len(schedule)))
			}
			require.NoError(t, err, "Failed to open database at %s", dbo.Path)
			t.Logf("%04d [%s] set new engine %p", round, cmd, eng)
			db.Update(eng)
			cmdCh <- cmd

		case crash:
			t.Logf("%04d [%s]", round, cmd)
			lastCrash.Store(int64(round))
			_ = errg.Wait()
			eng := db.Get()
			dir := eng.Options().Path
			// Crash/unclean shutdown. Concurrent goroutines may fail.
			require.NoError(t, eng.ForceShutdown())
			eng = nil

			// reopen
			t.Logf("%04d [%s] reopening DB at %s", round, cmd, dir)
			dbo := tests.NewTestDatabaseOptions(t, "").WithPath(dir)
			eng, err := engine.Open(context.Background(), tests.TEST_DB_NAME, dbo)
			if err != nil {
				lastCrash.Store(int64(len(schedule)))
			}
			require.NoError(t, err, "Failed to open database at %s", dbo.Path)
			t.Logf("%04d [%s] set new engine %p", round, cmd, eng)
			db.Update(eng)
			cmdCh <- cmd
		}
	}

	// Wait for all requests to complete.
	err := errg.Wait()
	if err != nil {
		log.Error(err)
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
		table, err := knox.WrapEngine(db.Get()).FindTable(tableName)
		require.NoError(t, err, "use table")

		// log metrics
		m := table.Metrics()
		t.Logf("Tuple metrics -- total:%d inserted:%d updated:%d deleted:%d queried:%d streamed:%d",
			m.TupleCount,
			m.InsertedTuples,
			m.UpdatedTuples,
			m.DeletedTuples,
			m.QueriedTuples,
			m.StreamedTuples,
		)
		t.Logf("Call metrics -- inserts:%d updates:%d deletes:%d queries:%d streams:%d",
			m.InsertCalls,
			m.UpdateCalls,
			m.DeleteCalls,
			m.QueryCalls,
			m.StreamCalls,
		)

		// TODO: improve integrity checks
		require.Equal(t, numTuples, m.TupleCount, "tuple count")

		// range scan
		var allTuples []*tests.AllTypes
		_, err = knox.NewQuery().
			WithTable(table).
			Execute(ctx, &allTuples)
		require.NoError(t, err, "range scan failed")
		require.Equal(t, len(allTuples), int(numTuples), "tuple count mismatch")

		// point queries
		for _, v := range allTuples {
			var oneTuple tests.AllTypes
			_, err = knox.NewQuery().
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
