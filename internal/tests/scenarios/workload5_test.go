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
		nTuples  atomic.Int64
		nInserts atomic.Int64
		executed = make(map[command]int)
		cmdCh    = make(chan command)
		errg     errgroup.Group
		wg       sync.WaitGroup
		liveIds  sync.Map
		db       atomic.Pointer[engine.Engine]
	)

	// setup determinism
	SetupDeterministicRand(t)

	// create new database and table
	eng, _ := tests.NewDatabase(t, &tests.AllTypes{})
	dbo := eng.Options()
	db.Store(eng)

	errg.SetLimit(maxProcs)

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
			dbo := tests.NewTestDatabaseOptions(t, "").WithPath(dir)
			eng, _ = engine.Open(ctx, tests.TEST_DB_NAME, dbo)
		}
		if eng != nil {
			for _, name := range eng.TableNames() {
				for _, iname := range eng.IndexNames(name) {
					eng.DropIndex(ctx, iname)
				}
				eng.DropTable(ctx, name)
			}
			for _, name := range eng.StoreNames() {
				eng.DropStore(ctx, name)
			}
			for _, name := range eng.EnumNames() {
				eng.DropEnum(ctx, name)
			}
			eng.Close(ctx)
		}
		require.NoError(t, engine.Drop(tests.TEST_DB_NAME, dbo))
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
		table, err := knox.FindGenericTable[tests.AllTypes](knox.WrapEngine(db.Load()), tableName)
		require.NoError(t, err)
		pk, n, err := table.Insert(context.Background(), ins)
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
		// return util.RandUint64n(uint64(nTuples.Load())+1) + 1

		// pick close-by values to trigger a lot of traffic on the same keys
		return util.RandUint64n(uint64(nInserts.Load())+1) + 1
	}

	t.Run("run", func(t *testing.T) {
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
				err = fmt.Errorf("%04d [%s]: %v", round, cmd, err)
				t.Log(err)
				return err
			}
			switch cmd {
			case insert:
				errg.Go(func() error {
					runtime.Gosched()
					if round < int(lastCrash.Load()) {
						return nil
					}
					table, err := knox.FindGenericTable[tests.AllTypes](
						knox.WrapEngine(db.Load()),
						tableName,
					)
					if err != nil {
						return wrapErr(err)
					}
					pk, _, err := table.Insert(context.Background(), NewTestValue(int(nInserts.Add(1))))
					if err != nil {
						return wrapErr(err)
					}
					t.Logf("%04d [%s] pk=%d", round, cmd, pk)
					nTuples.Add(1)
					liveIds.Store(pk, nil)

					cmdCh <- cmd
					return nil
				})
			case update:
				errg.Go(func() error {
					runtime.Gosched()
					if round < int(lastCrash.Load()) {
						return nil
					}
					table, err := knox.WrapEngine(db.Load()).FindTable(tableName)
					if err != nil {
						return wrapErr(err)
					}

					// pick a random id (may not exist due to delete)
					id := randId()

					// load record if exists
					var val tests.AllTypes
					n, err := knox.NewGenericQuery[tests.AllTypes]().
						WithTag("update-"+strconv.Itoa(round)).
						// WithDebug(true).
						WithTable(table).
						AndEqual("id", id).
						Execute(context.Background(), &val)
					if err != nil {
						return wrapErr(err)
					}

					// ignore not found
					if n == 0 {
						t.Logf("%04d [%s] pk=%d not found", round, cmd, id)
						return nil
					}

					// sanity check
					if id != val.Id {
						err := fmt.Errorf("%04d [%s] found invalid pk=%d for query with pk=%d", round, cmd, val.Id, id)
						t.Log(err)
						return err
					}

					// update
					val.Int64++
					n, err = table.Update(context.Background(), &val)
					switch {
					case errors.Is(err, knox.ErrNoRecord):
						if _, ok := liveIds.Load(id); ok {
							err := fmt.Errorf("%04d [%s] wrong update error for existing pk=%d", round, cmd, id)
							t.Log(err)
							return err
						}
						// race condition with delete?
						t.Logf("%04d [%s] pk=%d not found (race with delete?)", round, cmd, id)
					case err != nil && n == 0:
						return wrapErr(err)
					case n == 0:
						// invalid zero update without error
						err := fmt.Errorf("%04d [%s] invalid zero update without error", round, cmd)
						t.Log(err)
						return err
					case n > 1:
						// must not happen
						err := fmt.Errorf("%04d [%s] updated %d records with pk=%d", round, cmd, n, val.Id)
						t.Log(err)
						return err
					case n == 1:
						// success
						t.Logf("%04d [%s] pk=%d", round, cmd, id)
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
					table, err := knox.FindGenericTable[tests.AllTypes](
						knox.WrapEngine(db.Load()),
						tableName,
					)
					if err != nil {
						return wrapErr(err)
					}

					// pick a random id (may not exist post delete)
					id := randId()

					// load record if exists
					var val tests.AllTypes
					n, err := knox.NewGenericQuery[tests.AllTypes]().
						WithTag("delete-"+strconv.Itoa(round)).
						WithTable(table.Table()).
						AndEqual("id", id).
						Execute(context.Background(), &val)
					if err != nil {
						return wrapErr(err)
					}

					// ignore not found
					if n == 0 {
						t.Logf("%04d [%s] pk=%d not found", round, cmd, id)
						return nil
					}

					// sanity check
					if id != val.Id {
						err := fmt.Errorf("%04d [%s] found invalid pk=%d for query with pk=%d", round, cmd, val.Id, id)
						t.Log(err)
						return err
					}

					// delete by id
					n, err = knox.NewGenericQuery[tests.AllTypes]().
						WithTag("delete-"+strconv.Itoa(round)).
						// WithDebug(true).
						WithTable(table.Table()).
						AndEqual("id", val.Id).
						Delete(context.Background())

					switch {
					case err != nil:
						// must not happen
						return wrapErr(err)
					case n == 0:
						// may happen due to race with concurrent delete
					case n == 1:
						// expected success case
						t.Logf("%04d [%s] pk=%d", round, cmd, id)
						nTuples.Add(-1)
						liveIds.Delete(val.Id)
						cmdCh <- cmd
					case n > 1:
						// must not happen
						err := fmt.Errorf("%04d [%s] deleted %d records with pk=%d", round, cmd, n, val.Id)
						t.Log(err)
						return err
					}
					return nil
				})
			case query:
				errg.Go(func() error {
					runtime.Gosched()
					if round < int(lastCrash.Load()) {
						return nil
					}
					table, err := knox.WrapEngine(db.Load()).FindTable(tableName)
					if err != nil {
						return wrapErr(err)
					}

					// pick a random id (may not exist post delete)
					id := randId()
					t.Logf("%04d [%s] %d", round, cmd, id)

					// point query
					var val tests.AllTypes
					_, err = knox.NewGenericQuery[tests.AllTypes]().
						WithTag("query-"+strconv.Itoa(round)).
						// WithDebug(testing.Verbose()).
						WithTable(table).
						AndGte("id", id).
						Execute(context.Background(), &val)
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
					after := randId()
					t.Logf("%04d [%s]", round, cmd)

					// pick an action randomly
					action := util.RandIntn(3)

					// pick an order randomly
					order := knox.OrderType(util.RandIntn(2))

					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()
					eng := db.Load()
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
					err := db.Load().Sync(context.Background())
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
					err := db.Load().CompactTable(context.Background(), tableName)
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
					// err := db.Load().Snapshot(context.Background(), io.Discard)
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
				dir := db.Load().Options().Path
				require.NoError(t, db.Load().Close(context.Background()))

				// reopen
				t.Logf("%04d [%s] reopening DB at %s", round, cmd, dir)
				dbo := tests.NewTestDatabaseOptions(t, "").WithPath(dir)
				eng, err := engine.Open(context.Background(), tests.TEST_DB_NAME, dbo)
				if err != nil {
					lastCrash.Store(int64(len(schedule)))
				}
				require.NoError(t, err, "Failed to open database at %s", dbo.Path)
				t.Logf("%04d [%s] set new engine %p", round, cmd, eng)
				db.Store(eng)
				cmdCh <- cmd

			case crash:
				lastCrash.Store(int64(round))
				_ = errg.Wait()
				eng := db.Load()
				dir := eng.Options().Path
				t.Logf("%04d [%s] engine %p", round, cmd, eng)
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
				db.Store(eng)
				cmdCh <- cmd
			}
		}
	})

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

		assert.Equal(t, nLive, int(nTuples.Load()), "mismatched live map vs atomic counter")
		assert.Equal(t, nLive, int(m.TupleCount), "mismatched live map vs tuple metrics")

		// count scan all db records
		t.Log("Counting records.")
		n, err := knox.NewQuery().WithTable(table).Count(context.Background())
		require.NoError(t, err, "count scan failed")
		assert.Equal(t, nLive, n, "mismatched live map vs tuple count")

		// range scan for all db records
		t.Log("Scanning records.")
		var all []*tests.AllTypes
		_, err = knox.NewQuery().
			WithTable(table).
			Execute(context.Background(), &all)
		require.NoError(t, err, "range scan failed")
		assert.Equal(t, nLive, len(all), "mismatched live map vs scan count")

		seenIds := make(map[uint64]bool)
		for _, v := range all {
			seenIds[v.Id] = false
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
			var val tests.AllTypes
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
			case val.Id != key.(uint64):
				t.Logf("Error: mismatched pk=%d, got %d => %#v", key, val.Id, val)
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
