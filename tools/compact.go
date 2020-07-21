// Copyright (c) 2018-2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

// packed table compaction (like for utxo) & gc

package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	bolt "go.etcd.io/bbolt"

	"blockwatch.cc/packdb-pro/pack"
	"blockwatch.cc/packdb-pro/store"
	_ "blockwatch.cc/packdb-pro/store/bolt"
	"blockwatch.cc/packdb-pro/util"
	"github.com/echa/log"
)

var (
	flags   = flag.NewFlagSet("compact", flag.ContinueOnError)
	verbose bool
	debug   bool
	trace   bool
	dbname  string
)

var (
	p        = util.PrettyInt64
	pi       = util.PrettyInt
	boltopts = &bolt.Options{
		Timeout:      time.Second, // open timeout when file is locked
		NoGrowSync:   true,        // assuming Docker + XFS
		NoSync:       true,        // skip fsync (DANGEROUS on crashes)
		FreelistType: bolt.FreelistMapType,
	}
)

func b(n int) string {
	return util.ByteSize(n).String()
}

func init() {
	flags.Usage = func() {}
	flags.BoolVar(&verbose, "v", false, "be verbose")
	flags.BoolVar(&debug, "vv", false, "debug mode")
	flags.BoolVar(&trace, "vvv", false, "trace mode")
	flags.StringVar(&dbname, "db", "", "database")
}

func main() {
	if err := run(); err != nil {
		log.Error(err)
	}
}

func run() error {
	if err := flags.Parse(os.Args[1:]); err != nil {
		if err == flag.ErrHelp {
			fmt.Println("Pack database compaction")
			flags.PrintDefaults()
			return nil
		}
		return err
	}
	lvl := log.LevelInfo
	switch true {
	case trace:
		lvl = log.LevelTrace
	case debug:
		lvl = log.LevelDebug
	case verbose:
		lvl = log.LevelInfo
	}
	log.SetLevel(lvl)
	pack.UseLogger(log.Log)
	store.UseLogger(log.Log)

	if dbname == "" {
		dbname = flags.Arg(0)
	}

	name := strings.TrimSuffix(filepath.Base(dbname), ".db")
	db, err := pack.OpenDatabase(filepath.Dir(dbname), name, "*", boltopts)
	if err != nil {
		return fmt.Errorf("opening database: %v", err)
	}
	defer db.Close()

	table, err := db.Table(name)
	if err != nil {
		return err
	}
	defer table.Close()

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
	if err := table.Flush(ctx); err != nil {
		return err
	}
	if err := table.Compact(ctx); err != nil {
		return err
	}
	table.Close()
	if err := db.GC(ctx, 1.0); err != nil {
		return err
	}
	log.Info("Done.")
	return nil
}
