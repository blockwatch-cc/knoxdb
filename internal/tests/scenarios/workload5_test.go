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
	"sync/atomic"
	"testing"

	"blockwatch.cc/knoxdb/internal/engine"
	"blockwatch.cc/knoxdb/internal/tests"
	"blockwatch.cc/knoxdb/pkg/knox"
	"blockwatch.cc/knoxdb/pkg/util"
	"github.com/echa/log"
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
	switch {
	case errors.Is(err, context.Canceled):
		return true
	case errors.Is(err, engine.ErrDatabaseClosed):
		return true
	default:
		return false
	}
}

func TestWorkload5(t *testing.T) {
	log.SetLevel(log.LevelInfo)
	ctx := context.Background()

	t.Logf("Random seed %s", util.RandSeed())

	// create new database and table
	db := &dbProvider{}
	{
		eng, _ := tests.NewDatabase(t, &tests.AllTypes{})
		db.Update(eng)
	}

	errg := &errgroup.Group{}
	errg.SetLimit(maxProcs)

	var (
		numTuples           int64
		commandDistribution = make(map[command]int)
		cmdCh               = make(chan command)
	)

	// count number of commands for logging
	go func() {
		for {
			c, ok := <-cmdCh
			if !ok {
				return
			}
			commandDistribution[c]++
		}
	}()

	t.Logf("Running %d commands", numCommands)
	for i := 0; i < numCommands; i++ {
		cmd := genCommand()
		t.Logf("%04d [%s]", i, cmd)
		wrapErr := func(err error) error {
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
				i := int(atomic.LoadInt64(&numTuples) + 1)
				_, err = table.Insert(ctx, NewTestValue(i))
				if err != nil && !canIgnoreError(err) {
					return wrapErr(err)
				}
				atomic.AddInt64(&numTuples, 1)
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

				// pick a random id (may not exist post delete)
				id := util.RandUint64n(uint64(atomic.LoadInt64(&numTuples)) + 1)

				// load record if exists
				var val testType
				err = knox.NewGenericQuery[testType]().
					WithTag("update").
					WithTable(table).
					AndGte("id", id).
					Execute(ctx, &val)
				if err != nil && !canIgnoreError(err) {
					return wrapErr(err)
				}

				// ignore not found
				if val.Id == 0 {
					return nil
				}

				// update
				val.Int64++
				_, err = table.Update(ctx, &val)
				if err != nil && !canIgnoreError(err) {
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

				// load record if exists
				var val testType
				err = knox.NewGenericQuery[testType]().
					WithTag("delete").
					WithTable(table.Table()).
					AndGte("id", id).
					Execute(ctx, &val)
				if err != nil && !canIgnoreError(err) {
					return wrapErr(err)
				}

				// ignore not found
				if val.Id == 0 {
					return nil
				}

				// delete by id
				n, err := knox.NewGenericQuery[testType]().
					WithTag("delete").
					WithTable(table.Table()).
					AndEqual("id", val.Id).
					Delete(ctx)

				if err != nil && !canIgnoreError(err) {
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

				// point query
				var val testType
				err = knox.NewGenericQuery[testType]().
					WithTag("query").
					WithTable(table).
					AndGte("id", id).
					Execute(ctx, &val)
				if err != nil && !canIgnoreError(err) {
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
				if err != nil && !canIgnoreError(err) {
					return wrapErr(err)
				}
				cmdCh <- cmd
				return nil
			})
		case fsync:
			errg.Go(func() error {
				err := db.Get().Sync(ctx)
				if err != nil && !canIgnoreError(err) {
					return wrapErr(err)
				}
				cmdCh <- cmd
				return nil
			})
		case compact:
			errg.Go(func() error {
				err := db.Get().CompactTable(ctx, tableName)
				if err != nil && !canIgnoreError(err) {
					return wrapErr(err)
				}
				cmdCh <- cmd
				return nil
			})
		case snapshot:
			errg.Go(func() error {
				// err := db.Get().Snapshot(ctx, io.Discard)
				// if err != nil && !canIgnoreError(err) {
				//     return wrapErr(err)
				// }
				cmdCh <- cmd
				return nil
			})

		case restart:
			// Graceful shutdown. Concurrent goroutines may fail.
			require.NoError(t, db.Get().Close(ctx))
			_ = errg.Wait()

			// reopen
			db.Update(tests.OpenTestEngine(t, tests.NewTestDatabaseOptions(t, "")))
			cmdCh <- cmd

		case crash:
			// Crash/unclean shutdown. Concurrent goroutines may fail.
			require.NoError(t, db.Get().ForceShutdown())
			_ = errg.Wait()

			// reopen
			db.Update(tests.OpenTestEngine(t, tests.NewTestDatabaseOptions(t, "")))
			cmdCh <- cmd
		}
	}

	// Wait for all requests to complete.
	require.NoError(t, errg.Wait(), "command error")

	// close statistics channel
	close(cmdCh)

	t.Log("All commands completed.")
	t.Logf("Command distribution: %v", commandDistribution)
	t.Log("Sync/merge database.")

	// Sync db
	require.NoError(t, db.Get().Sync(ctx))

	t.Log("Verifying data integrity.")
	table, err := db.Get().UseTable(tableName)
	require.NoError(t, err, "use table")
	m := table.Metrics()
	t.Logf("Metrics: %s", util.ToString(m))
	require.Equal(t, numTuples, m.TupleCount, "tuple count")
	t.Log("OK.")
	require.NoError(t, db.Get().Close(ctx))
}
