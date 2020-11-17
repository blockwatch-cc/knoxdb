// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

// drops and rebuilds table indexes

package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"path/filepath"
	"runtime/debug"
	"strings"
	"syscall"
	"time"

	bolt "go.etcd.io/bbolt"

	"blockwatch.cc/knoxdb/pack"
	_ "blockwatch.cc/knoxdb/store/bolt"
	"blockwatch.cc/knoxdb/util"
	"github.com/echa/log"
)

var (
	flags   = flag.NewFlagSet("rebuild", flag.ContinueOnError)
	verbose bool
	vdebug  bool
	vtrace  bool
	gogc    int
	dbname  string
	tname   string
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
	flags.BoolVar(&vdebug, "vv", false, "debug mode")
	flags.BoolVar(&vtrace, "vvv", false, "trace mode")
	flags.IntVar(&gogc, "gogc", 20, "gc `percentage`")
	flags.StringVar(&dbname, "db", "", "database `filename`")
	flags.StringVar(&tname, "table", "", "table `name`")
}

func main() {
	if err := run(); err != nil {
		log.Error(err)
	}
}

func run() error {
	if err := flags.Parse(os.Args[1:]); err != nil {
		if err == flag.ErrHelp {
			fmt.Println("Pack table index rebuild")
			flags.PrintDefaults()
			return nil
		}
		return err
	}
	lvl := log.LevelInfo
	switch true {
	case vtrace:
		lvl = log.LevelTrace
	case vdebug:
		lvl = log.LevelDebug
	case verbose:
		lvl = log.LevelInfo
	}
	log.SetLevel(lvl)
	pack.UseLogger(log.Log)

	// set GC trigger
	if gogc <= 0 {
		gogc = 20
	}
	// Block and transaction processing can cause bursty allocations. This
	// limits the garbage collector from excessively overallocating during
	// bursts. This value was arrived at with the help of profiling live
	// usage.
	debug.SetGCPercent(gogc)

	if dbname == "" {
		dbname = flags.Arg(0)
	}

	if dbname == "" {
		return fmt.Errorf("Missing database.")
	}

	name := strings.TrimSuffix(filepath.Base(dbname), ".db")
	if tname == "" {
		tname = name
	}
	db, err := pack.OpenDatabase(filepath.Dir(dbname), name, "*", boltopts)
	if err != nil {
		return fmt.Errorf("opening database: %v", err)
	}
	defer db.Close()

	table, err := db.Table(
		tname,
		pack.Options{CacheSize: 2}, // table opts
		pack.Options{JournalSizeLog2: 22, CacheSize: 8}, // index opts
	)
	if err != nil {
		return err
	}
	defer table.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		log.Error(http.ListenAndServe("0.0.0.0:6060", nil))
	}()

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

	// make sure source table journals are flushed
	log.Infof("Flushing source table %s", table.Database().Path())
	if err := table.Flush(ctx); err != nil {
		return err
	}

	// walk source table in packs and bulk-insert data into target
	stats := table.Stats()
	log.Infof("Rebuild indexes over %d rows / %d packs from table %s",
		stats.TupleCount, stats.PacksCount, table.Database().Path())

	// rebuild indexes
	for _, idx := range table.Indexes() {
		log.Infof("Rebuilding %s index on field %s (%s)", idx.Name, idx.Field.Name, idx.Field.Type)
		prog := make(chan float64, 100)
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case f := <-prog:
					log.Infof("Index build progress %.2f%%", f)
					if f == 100 {
						return
					}
				}
			}
		}()
		// flush every 1024 packs (i.e. 32M entries = 512 MB)
		err = idx.Reindex(ctx, 1024, prog)
		close(prog)
		if err != nil {
			return err
		}
	}

	log.Infof("Done in %s", time.Since(start))
	return nil
}
