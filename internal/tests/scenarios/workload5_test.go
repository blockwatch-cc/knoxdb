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
	"runtime/debug"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	"blockwatch.cc/knoxdb/internal/engine"
	tests "blockwatch.cc/knoxdb/internal/tests/engine"
	"blockwatch.cc/knoxdb/internal/tests/testutil"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/knox"
	"github.com/echa/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	tableName   = "transfer"
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
		update:   0.10,
		delete:   0.10,
		query:    0.25,
		stream:   0.25,
		fsync:    0.02,
		compact:  0.0001,
		snapshot: 0.0001,
		restart:  0.001,
		crash:    0.001,
	}

	NewTestValue = tests.NewTransfer

	testRun int
)

type TestType = tests.Transfer

func init() {
	var sum float64
	for _, c := range commands {
		sum += probs[c]
		cumProbs = append(cumProbs, sum)
	}
}

func genCommand() command {
	f := testutil.RandFloat64()
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

var (
	// lastCrash atomic.Int64 // round of last restart or crash
	epoch atomic.Int64 // current restart/crash epoch
)

func canIgnoreError(ctx context.Context, err error, ep int64) bool {
	if err == nil {
		return true
	}
	if ctx.Err() != nil {
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
		if ep != epoch.Load() {
			return true
		}
		return false
	}
}

type Dispatcher struct {
	ctx    context.Context
	cancel context.CancelCauseFunc
	ch     chan func() error
	stop   chan struct{}
	wg     sync.WaitGroup
}

func NewDispatcher() *Dispatcher {
	return &Dispatcher{
		ch:   make(chan func() error),
		stop: make(chan struct{}),
	}
}

func (d *Dispatcher) Go(fn func() error) {
	select {
	case d.ch <- fn:
	case <-d.ctx.Done():
	case <-d.stop:
	}
}

func (d *Dispatcher) Run(ctx context.Context, n int) context.Context {
	d.ctx, d.cancel = context.WithCancelCause(ctx)
	for range n {
		// start n workers, stop on first error
		d.wg.Go(func() {
			defer func() {
				if e := recover(); e != nil {
					debug.PrintStack()
					switch err := e.(type) {
					case error:
						d.cancel(err)
					default:
						d.cancel(fmt.Errorf("%v", e))
					}
				}
			}()
			for {
				select {
				case <-d.ctx.Done():
					return
				case <-d.stop:
					return
				case fn := <-d.ch:
					if err := fn(); err != nil {
						d.cancel(err)
					}
				}
			}
		})
	}
	return d.ctx
}

func (d *Dispatcher) Stop() {
	close(d.stop)
}

func (d *Dispatcher) Wait() error {
	d.wg.Wait()
	return context.Cause(d.ctx)
}

func TestWorkload5(t *testing.T) {
	var (
		nTuples  atomic.Int64
		nInserts atomic.Int64
		executed = make(map[command]int)
		cmdCh    = make(chan command)
		wg       sync.WaitGroup
		liveIds  sync.Map
		db       atomic.Pointer[engine.Engine]
	)

	// setup determinism
	SetupDeterministicRand(t)

	// create new database and table
	eng, _ := tests.NewDatabase(t, &TestType{})
	dbo := eng.Options()
	db.Store(eng)

	// save database files on failure
	t.Cleanup(func() {
		tests.SaveDatabaseFiles(t, db.Load())

		// manual cleanup because we restart often (db may be closed at this point)
		t.Log("Cleaning up after test.")
		eng := db.Load()
		ctx := context.Background()
		if eng.IsShutdown() {
			// reopen
			dir := db.Load().Options().Path
			dbo := tests.NewTestDatabaseOptions(t, engine.WithPath(dir))
			eng, _ = engine.Open(ctx, tests.TEST_DB_NAME, dbo.DatabaseOptions()...)
		}
		if eng != nil {
			for _, name := range eng.TableNames() {
				for _, iname := range eng.IndexNames(name) {
					eng.DropIndex(ctx, iname)
				}
				eng.DropTable(ctx, name)
			}
			for _, name := range eng.EnumNames() {
				eng.DropEnum(ctx, name)
			}
			eng.Close(ctx)
		}
		require.NoError(t, engine.Drop(tests.TEST_DB_NAME, dbo.DatabaseOptions()...))
	})

	// count number of commands for logging
	wg.Go(func() {
		for {
			c, ok := <-cmdCh
			if !ok {
				return
			}
			executed[c]++
		}
	})

	// init: insert values (wrapped into sub-test to catch panics)
	t.Run("init", func(t *testing.T) {
		ins := make([]*TestType, 1024)
		for i := range ins {
			ins[i] = NewTestValue(int(nInserts.Add(1)))
		}
		table, err := knox.FindTableFor[TestType](knox.WrapEngine(db.Load()), tableName)
		require.NoError(t, err)
		pk, n, err := table.Insert(context.Background(), ins...)
		require.NoError(t, err)
		require.Equal(t, len(ins), n, "seed tuples")
		t.Logf("Inserted %d/%d seed tuples", n, len(ins))
		nTuples.Add(int64(n))
		for range ins {
			liveIds.Store(pk, nil)
			pk++
		}
		clear(ins)
	})

	if t.Failed() {
		return
	}

	randId := func() uint64 {
		// pick close-by values to trigger a lot of traffic on the same keys
		return testutil.RandUint64n(uint64(nInserts.Load())+1) + 1
	}

	t.Run("run", func(t *testing.T) {
		// produce sequence of commands all at once so that even with non-deterministic
		// go runtime we get a somewhat reproducible behavior
		t.Logf("Running %d commands on %d goroutines", numCommands, maxProcs)
		schedule := make([]command, numCommands)
		for i := range schedule {
			schedule[i] = genCommand()
		}

		// run dispatcher
		disp := NewDispatcher()
		ctx := disp.Run(context.Background(), maxProcs)
		epoch.Add(1)

		for i, cmd := range schedule {
			// stop scheduling commands after the first failure
			// canceled the context (due to async execution this
			// may be delayed)
			if ctx.Err() != nil || t.Failed() {
				break
			}

			// capture round and epoch for error logging
			// should a restart/crash update the epoch we
			// ignore errors from the earlier epoch
			round := i
			thisEpoch := epoch.Load()
			wrapErr := func(err error) error {
				if err == nil {
					return nil
				}
				if canIgnoreError(ctx, err, thisEpoch) {
					t.Logf("%04d [%s]: IGNORE %v", round, cmd, err)
					return nil
				}
				err = fmt.Errorf("%04d [%s]: %w", round, cmd, err)
				t.Log(err)
				return err
			}
			switch cmd {
			case insert:
				disp.Go(func() error {
					runtime.Gosched()
					if ctx.Err() != nil {
						t.Logf("%04d [%s] skip", round, cmd)
						return nil
					}
					db := knox.WrapEngine(db.Load())
					table, err := knox.FindTableFor[TestType](db, tableName)
					if err != nil {
						return wrapErr(err)
					}

					// open write tx
					ctx, commit, abort, err := db.Begin(ctx)
					if err != nil {
						return wrapErr(err)
					}
					defer abort()

					pk, _, err := table.Insert(ctx,
						NewTestValue(int(nInserts.Add(1))),
					)
					if err != nil {
						return wrapErr(err)
					}

					cmdCh <- cmd
					if err := commit(); err == nil {
						t.Logf("%04d [%s] pk=%d", round, cmd, pk)
						nTuples.Add(1)
						liveIds.Store(pk, nil)
						return nil
					} else {
						return wrapErr(fmt.Errorf("insert pk=%d: %w", pk, err))
					}
				})

			case update:
				disp.Go(func() error {
					runtime.Gosched()
					if ctx.Err() != nil {
						t.Logf("%04d [%s] skip", round, cmd)
						return nil
					}

					db := knox.WrapEngine(db.Load())
					table, err := knox.FindTableFor[TestType](db, tableName)
					if err != nil {
						return wrapErr(err)
					}

					// pick a random id (may not exist due to delete)
					id := randId()

					// open write tx
					ctx, commit, abort, err := db.Begin(ctx)
					if err != nil {
						return wrapErr(err)
					}
					defer abort()

					// load record if exists
					var val TestType
					n, err := table.NewQuery().
						WithTag("update-"+strconv.Itoa(round)).
						WithDebug(log.Log.Level() == log.LevelTrace).
						AndEqual("id", id).
						Execute(ctx, &val)
					if err != nil {
						return wrapErr(err)
					}

					// ignore not found
					if n == 0 {
						t.Logf("%04d [%s] pk=%d not found", round, cmd, id)
						return nil
					}

					// sanity check
					if id != val.ID {
						err := fmt.Errorf("%04d [%s] found invalid pk=%d for query with pk=%d",
							round, cmd, val.ID, id)
						t.Log(err)
						return err
					}

					// update in the same write tx
					val.DebitAccountID++
					n, err = table.Update(ctx, &val)
					switch {
					case errors.Is(err, knox.ErrNoRecord):
						if _, ok := liveIds.Load(id); ok {
							err := fmt.Errorf("notfound error for existing pk=%d", id)
							return wrapErr(err)
						} else {
							err := fmt.Errorf("pk=%d not found (race with delete?)", id)
							return wrapErr(err)
						}
					case err != nil && n == 0:
						return wrapErr(err)
					case n == 0:
						// invalid zero update without error
						err := fmt.Errorf("invalid zero update without error")
						return wrapErr(err)
					case n > 1:
						// must not happen
						err := fmt.Errorf("updated %d records with pk=%d", n, val.ID)
						return wrapErr(err)
					case n == 1:
						// success
						if err := commit(); err == nil {
							t.Logf("%04d [%s] pk=%d", round, cmd, id)
							cmdCh <- cmd
						} else {
							return wrapErr(err)
						}
					}
					return nil
				})
			case delete:
				disp.Go(func() error {
					runtime.Gosched()
					if ctx.Err() != nil {
						t.Logf("%04d [%s] skip", round, cmd)
						return nil
					}

					db := knox.WrapEngine(db.Load())
					table, err := knox.FindTableFor[TestType](db, tableName)
					if err != nil {
						return wrapErr(err)
					}

					// pick a random id (may not exist post delete)
					id := randId()

					// open write tx
					ctx, commit, abort, err := db.Begin(ctx)
					if err != nil {
						return wrapErr(err)
					}
					defer abort()

					// load record if exists
					var val TestType
					n, err := table.NewQuery().
						WithTag("delete-"+strconv.Itoa(round)).
						AndEqual("id", id).
						Execute(ctx, &val)
					if err != nil {
						return wrapErr(err)
					}

					// ignore not found
					if n == 0 {
						t.Logf("%04d [%s] pk=%d not found", round, cmd, id)
						return nil
					}

					// sanity check
					if id != val.ID {
						err := fmt.Errorf("%04d [%s] found invalid pk=%d for query with pk=%d", round, cmd, val.ID, id)
						t.Log(err)
						return err
					}

					// delete by id
					n, err = table.NewQuery().
						WithTag("delete-"+strconv.Itoa(round)).
						WithDebug(log.Log.Level() == log.LevelTrace).
						AndEqual("id", val.ID).
						Delete(ctx)

					switch {
					case err != nil:
						// may happen on shutdown
						return wrapErr(err)
					case n == 0:
						// race with concurrent delete must not happen
						err := fmt.Errorf("cannot delete existing record pk=%d", val.ID)
						return wrapErr(err)
					case n == 1:
						// expected success case
						if err := commit(); err == nil {
							t.Logf("%04d [%s] pk=%d", round, cmd, id)
							nTuples.Add(-1)
							liveIds.Delete(val.ID)
							cmdCh <- cmd
						} else {
							return wrapErr(err)
						}
					case n > 1:
						// must not happen
						err := fmt.Errorf("deleted %d records with pk=%d", n, val.ID)
						return wrapErr(err)
					}
					return nil
				})
			case query:
				disp.Go(func() error {
					runtime.Gosched()
					if ctx.Err() != nil {
						t.Logf("%04d [%s] skip", round, cmd)
						return nil
					}

					table, err := knox.FindTableFor[TestType](
						knox.WrapEngine(db.Load()),
						tableName,
					)
					if err != nil {
						return wrapErr(err)
					}

					// pick a random id (may not exist post delete)
					id := randId()

					// point query
					var val TestType
					_, err = table.NewQuery().
						WithTag("query-"+strconv.Itoa(round)).
						WithDebug(log.Log.Level() == log.LevelTrace).
						AndGte("id", id).
						Execute(ctx, &val)
					if err != nil {
						return wrapErr(err)
					}
					t.Logf("%04d [%s] %d", round, cmd, id)
					cmdCh <- cmd
					return nil
				})
			case stream:
				disp.Go(func() error {
					runtime.Gosched()
					if ctx.Err() != nil {
						t.Logf("%04d [%s] skip", round, cmd)
						return nil
					}

					// limit to N records (stop after, don't use query.Limit)
					after := randId()

					// pick an action randomly
					action := testutil.RandIntn(3)

					// pick an order randomly
					order := knox.OrderType(testutil.RandIntn(2))

					ctx, cancel := context.WithCancel(ctx)
					defer cancel()
					table, err := knox.FindTableFor[TestType](
						knox.WrapEngine(db.Load()),
						tableName,
					)
					if err != nil {
						return wrapErr(err)
					}

					var nrecords int
					err = table.NewQuery().
						WithTag("stream-"+strconv.Itoa(round)).
						WithDebug(log.Log.Level() == log.LevelTrace).
						AndGt("id", 0).
						WithOrder(order).
						Stream(ctx, func(v *TestType) error {
							nrecords++
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
					t.Logf("%04d [%s] act=%d recs=%d", round, cmd, action, nrecords)
					cmdCh <- cmd
					return nil
				})
			case fsync:
				disp.Go(func() error {
					if ctx.Err() != nil {
						t.Logf("%04d [%s] skip", round, cmd)
						return nil
					}
					err := db.Load().Sync(ctx)
					if err != nil {
						return wrapErr(err)
					}
					t.Logf("%04d [%s]", round, cmd)
					cmdCh <- cmd
					return nil
				})
			case compact:
				disp.Go(func() error {
					if ctx.Err() != nil {
						t.Logf("%04d [%s] skip", round, cmd)
						return nil
					}
					err := db.Load().CompactTable(ctx, tableName)
					if err != nil {
						return wrapErr(err)
					}
					t.Logf("%04d [%s]", round, cmd)
					cmdCh <- cmd
					return nil
				})
			case snapshot:
				disp.Go(func() error {
					if ctx.Err() != nil {
						t.Logf("%04d [%s] skip", round, cmd)
						return nil
					}
					t.Logf("%04d [%s] noop", round, cmd)
					// err := db.Load().Snapshot(ctx, io.Discard)
					// if err != nil {
					//     return wrapErr(err)
					// }
					cmdCh <- cmd
					return nil
				})

			case restart:
				// wait until restart is complete
				var wg sync.WaitGroup
				wg.Add(1)

				disp.Go(func() error {
					defer wg.Done()
					if ctx.Err() != nil {
						t.Logf("%04d [%s] skip", round, cmd)
						return nil
					}
					t.Logf("%04d [%s]", round, cmd)

					dir := db.Load().Options().Path
					// Graceful shutdown. Concurrent goroutines may fail.
					require.NoError(t, db.Load().Close(ctx), "shutdown during close")

					// reopen
					t.Logf("%04d [%s] reopening DB at %s", round, cmd, dir)
					dbo := tests.NewTestDatabaseOptions(t, engine.WithPath(dir))
					eng, err := engine.Open(ctx, tests.TEST_DB_NAME, dbo.DatabaseOptions()...)
					require.NoError(t, err, "Failed to open database at %s", dbo.Path)
					t.Logf("%04d [%s] set new engine %p", round, cmd, eng)
					db.Store(eng)
					cmdCh <- cmd
					return nil
				})

				// continue scheduling commands after db restart is complete
				wg.Wait()
				epoch.Add(1)

			case crash:
				// wait until restart is complete
				var wg sync.WaitGroup
				wg.Add(1)

				// update epoch earlier to silence crash-related errors
				epoch.Add(1)

				disp.Go(func() error {
					defer wg.Done()
					if ctx.Err() != nil {
						t.Logf("%04d [%s] skip", round, cmd)
						return nil
					}

					eng := db.Load()
					dir := eng.Options().Path
					t.Logf("%04d [%s] engine %p", round, cmd, eng)

					// Crash/unclean shutdown. Concurrent goroutines may fail.
					require.NoError(t, eng.ForceClose(ctx), "force shutdown during shutdown")
					eng = nil

					// reopen
					t.Logf("%04d [%s] reopening DB at %s", round, cmd, dir)
					dbo := tests.NewTestDatabaseOptions(t, engine.WithPath(dir))
					eng, err := engine.Open(ctx, tests.TEST_DB_NAME, dbo.DatabaseOptions()...)
					require.NoError(t, err, "Failed to open database at %s", dbo.Path)
					t.Logf("%04d [%s] set new engine %p", round, cmd, eng)
					db.Store(eng)
					cmdCh <- cmd
					return nil
				})

				// continue scheduling commands after db restart is complete
				wg.Wait()
			}
		}

		// Wait for all requests to complete.
		disp.Stop()
		require.NoError(t, disp.Wait(), "command error")
	})

	// close statistics channel
	close(cmdCh)
	wg.Wait()

	// don't run validation when an earlier test failed to prevent
	// polluting the tail of the debug log
	if t.Failed() {
		return
	}
	t.Logf("All commands completed: %v", executed)

	// sync (wrapped into sub-test to catch panics)
	t.Run("sync", func(t *testing.T) {
		t.Log("Sync/merge database.")
		require.NoError(t, db.Load().Sync(context.Background()))
	})

	// verify (wrapped into sub-test to catch panics)
	t.Run("verify", func(t *testing.T) {
		t.Log("Verifying data integrity.")
		table, err := knox.WrapEngine(db.Load()).FindTable(tableName)
		require.NoError(t, err, "use table")

		// count live records (ground truth)
		var nLive int
		liveIds.Range(func(key, _ any) bool {
			nLive++
			return true
		})

		// check metrics counters match
		m := table.Metrics()
		t.Logf("Tuple metrics (since last crash) -- total:%d inserted:%d updated:%d deleted:%d queried:%d streamed:%d",
			m.TupleCount,
			m.InsertedTuples,
			m.UpdatedTuples,
			m.DeletedTuples,
			m.QueriedTuples,
			m.StreamedTuples,
		)
		t.Logf("Call metrics (since last crash) -- inserts:%d updates:%d deletes:%d queries:%d streams:%d",
			m.InsertCalls,
			m.UpdateCalls,
			m.DeleteCalls,
			m.QueryCalls,
			m.StreamCalls,
		)

		assert.Equal(t, nLive, int(nTuples.Load()), "testcase bug: mismatched live map vs atomic counter")
		assert.Equal(t, nLive, int(m.TupleCount), "db bug: mismatched live map vs tuple metrics")

		// count scan all db records
		t.Log("Counting records.")
		n, err := knox.NewQuery().WithTable(table).Count(context.Background())
		require.NoError(t, err, "count scan failed")
		assert.Equal(t, nLive, n, "mismatched live map vs tuple count")

		// range scan for all db records
		t.Log("Scanning records.")
		var all []*TestType
		_, err = knox.NewQuery().
			WithTable(table).
			Execute(context.Background(), &all)
		require.NoError(t, err, "range scan failed")
		assert.Equal(t, nLive, len(all), "mismatched live map vs scan count")

		seenIds := make(map[uint64]bool)
		for _, v := range all {
			seenIds[v.ID] = false
		}

		// check all expected records exist in the db
		t.Log("Lookup records.")
		liveIds.Range(func(key, _ any) bool {
			if _, ok := seenIds[key.(uint64)]; !ok {
				t.Logf("Error: expected pk=%d is not in table", key)
				t.Fail()
				return true
			}

			// try point lookup
			var val TestType
			n, err := knox.NewQuery().
				WithTable(table).
				AndEqual("id", key.(uint64)).
				Execute(context.Background(), &val)
			switch {
			case err != nil:
				t.Logf("Error: query pk=%d: %v", key, err)
				t.Fail()
			case n == 0:
				t.Logf("Error: missing expected pk=%d", key)
				t.Fail()
			case val.ID != key.(uint64):
				t.Logf("Error: mismatched pk=%d, got %d => %#v", key, val.ID, val)
				t.Fail()
			default:
				seenIds[key.(uint64)] = true
			}
			return true
		})

		// cross-check we have no extra/unexpected DB records
		for pk, seen := range seenIds {
			if seen {
				continue
			}
			t.Logf("Error: unexpected pk=%d in table, should not exist", pk)
			t.Fail()
		}
		if nLive != len(seenIds) {
			t.Logf("Error: table scan & testcase seen pks mismatch: testcase=%d scan=%d", nLive, len(seenIds))
			t.Fail()
		}

		// done
		if !t.Failed() {
			t.Log("Verify OK.")
		} else {
			t.Log("Verify completed with errors.")
		}
	})

	// close DB
	t.Log("Closing database.")
	tests.NoDeadlock(t, func() bool {
		assert.NoError(t, db.Load().Close(context.Background()))
		return true
	}, "deadlock on close")
	t.Log("Done.")
}
