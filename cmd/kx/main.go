// Copyright (c) 2018-2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc
//go:build ignore
// +build ignore

// knoxdb cli

package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"blockwatch.cc/knoxdb/pkg/knox"
	"github.com/echa/log"
)

func main() {
	if err := run(); err != nil {
		log.Error(err)
	}
}

func run() error {
	args, err := parseArgs()
	if err != nil {
		return err
	}

	switch args.cmd {
	case "stats":
		return boltStats(args.db)
	case "compact":
		db, table, err := openTable(args)
		if err != nil {
			return err
		}
		defer db.Close()
		defer table.Close()
		return runAbortable(table, compact)
	case "reindex":
		db, table, err := openTable(args)
		if err != nil {
			return err
		}
		defer db.Close()
		defer table.Close()
		return runAbortable(table, reindex)
	case "rebuild":
		return rebuildStatistics(args)
	case "flush":
		db, table, err := openTable(args)
		if err != nil {
			return err
		}
		defer db.Close()
		defer table.Close()
		return runAbortable(table, flush)
	case "gc":
		db, err := openDatabase(args)
		if err != nil {
			return err
		}
		defer db.Close()
		return runAbortable(db, gc)
	default:
		return fmt.Errorf("unsupported command %s", args.cmd)
	}
}

func openTable(args Args) (knox.Table, error) {
	// db, err := openDatabase(args)
	// if err != nil {
	// 	return nil, nil, err
	// }
	// opts := pack.DefaultOptions.
	// 	WithEngine(pack.TableEnginePack).
	// 	WithDriver("bolt").
	// 	WithDriverOpts(args.bolt).
	// 	WithReadOnly(true)

	table, err := db.UseTable(args.table)
	if err != nil {
		return nil, nil, fmt.Errorf("opening table '%s': %v", args.table, err)
	}
	return table, nil
}

func openDatabase(args Args) (knox.Database, error) {
	opts := knox.ReadonlyDatabaseOptions
	db, err := knox.OpenDatabase(
		filepath.Dir(args.db),
		strings.TrimSuffix(filepath.Base(args.db), ".db"),
		opts,
	)
	if err != nil {
		return nil, fmt.Errorf("opening database: %v", err)
	}
	return db, nil
}

type Abortable func(context.Context, interface{}) error

func runAbortable(data interface{}, fn Abortable) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		// wait for Ctrl-C
		stop := make(chan os.Signal, 1)
		signal.Notify(stop,
			syscall.SIGHUP,
			syscall.SIGINT,
			syscall.SIGTERM,
			syscall.SIGQUIT,
		)
		select {
		case <-ctx.Done():
			return
		case <-stop:
			log.Info("Aborting...")
			cancel()
		}
	}()
	log.Info("Stop with Ctrl-C")
	start := time.Now()
	if err := fn(ctx, data); err != nil {
		return err
	}
	log.Infof("Done in %s", time.Since(start))
	return nil
}
