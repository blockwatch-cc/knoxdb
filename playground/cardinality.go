// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime/pprof"
	"time"

	"github.com/echa/log"
	bolt "go.etcd.io/bbolt"

	"blockwatch.cc/knoxdb/pack"
	_ "blockwatch.cc/knoxdb/store/bolt"
	"blockwatch.cc/knoxdb/util"
	"blockwatch.cc/knoxdb/vec"
)

const (
	PackSizeLog2         = 15  // 32k packs ~4M
	JournalSizeLog2      = 16  // 64k - search for spending op, so keep small
	CacheSize            = 128 // 128=512MB
	FillLevel            = 100
	IndexPackSizeLog2    = 15   // 16k packs (32k split size) ~256k
	IndexJournalSizeLog2 = 16   // 64k
	IndexCacheSize       = 1024 // ~256M
	IndexFillLevel       = 90
)

var (
	verbose   bool
	debug     bool
	trace     bool
	cache     bool
	dbname    string
	prec      uint
	bloomOnly bool
	cpuprof   string
	flags     = flag.NewFlagSet("cardinality", flag.ContinueOnError)
	boltopts  = &bolt.Options{
		Timeout:      time.Second, // open timeout when file is locked
		NoGrowSync:   true,        // assuming Docker + XFS
		ReadOnly:     false,       // set true to disallow write transactions
		NoSync:       true,        // skip fsync (DANGEROUS on crashes)
		FreelistType: bolt.FreelistMapType,
	}
)

func init() {
	flags.Usage = func() {}
	flags.BoolVar(&verbose, "v", false, "be verbose")
	flags.BoolVar(&debug, "vv", false, "enable debug mode")
	flags.BoolVar(&trace, "vvv", false, "enable trace mode")
	flags.BoolVar(&cache, "cache", false, "enable db cache")
	flags.BoolVar(&bloomOnly, "bloom", false, "estimate only for fields with bloom flag")
	flags.UintVar(&prec, "precision", 12, "loglog-beta precision 1<<n")
	flags.StringVar(&dbname, "db", "", "database")
	flags.StringVar(&cpuprof, "profile", "", "write CPU profile to filename")
}

func printhelp() {
	fmt.Println("Usage:\n  cardinality [flags]")
	fmt.Println("Flags:")
	flags.PrintDefaults()
	fmt.Println()
}

func main() {
	if err := run(); err != nil {
		log.Error(err)
	}
}

func run() error {
	if err := flags.Parse(os.Args[1:]); err != nil {
		if err == flag.ErrHelp {
			printhelp()
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

	if cpuprof != "" {
		f, err := os.Create(cpuprof)
		if err != nil {
			return fmt.Errorf("cannot write cpu profile: %s", err)
		} else {
			defer f.Close()
			pprof.StartCPUProfile(f)
			defer pprof.StopCPUProfile()
		}
	}

	// open existing table
	table, err := Open(dbname, boltopts)
	if err != nil {
		return err
	}

	count := 0
	start := time.Now()
	stats := make([]vec.Uint64Reducer, len(table.Fields()))
	errors := make([]vec.Uint64Reducer, len(table.Fields()))
	bloomSizeErr := make([][2]int, len(table.Fields()))
	fields := table.Fields()
	u64 := make([]uint64, 0, 1<<PackSizeLog2)
	var totalSize uint64
	err = table.WalkPacks(func(pkg *pack.Package) error {
		count++
		for i, v := range pkg.Blocks() {
			// skip for non-bloom fields when requested
			if bloomOnly && !fields[i].Flags.Contains(pack.FlagBloom) {
				continue
			}

			est := fields[i].Type.EstimateCardinality(v, prec)

			// true values
			u64 = vec.Uint64.Unique(v.Hashes(u64))

			// add to stats and errors
			stats[i].Add(uint64(est))
			errors[i].Add(uint64(util.Abs(int64(est) - int64(len(u64)))))

			b1 := pow2(uint64(est*8)) / 8
			b2 := pow2(uint64(len(u64)*8)) / 8
			if b1 < b2 {
				bloomSizeErr[i][0]++
			} else if b1 > b2 {
				bloomSizeErr[i][1]++
			}

			u64 = u64[:0]

			// track total size of bloom filters
			totalSize += pow2(uint64(est*8)) / 8
		}
		fmt.Printf(".")
		return nil
	})
	if err != nil {
		return err
	}

	if err := Close(table); err != nil {
		return err
	}

	fmt.Printf("\nProcessed %d packs at loglog precision %d in %s\n", count, prec, time.Since(start))
	fmt.Printf("Total bloom size %d bytes\n", totalSize)
	fmt.Printf("%03s  %15s  %10s  %5s  %5s  %5s  %10s  %7s  %7s  %7s  %10s %7s %7s\n", "Col", "Name", "Type", "Min", "Max", "Avg", "Std", "Err-Min", "Err-Max", "Err-Avg", "Err-Std", "Under", "Over")
	for i, f := range table.Fields() {
		s := stats[i]
		e := errors[i]
		d := bloomSizeErr[i]
		fmt.Printf("%02d   %15s  %10s  %5d  %5d  %5d  %9.1f  %7d  %7d  %7d  %9.1f %7d %7d\n",
			i, f.Alias, f.Type, s.Min(), s.Max(), uint64(s.Mean()), s.Stddev(), e.Min(), e.Max(), uint64(e.Mean()), e.Stddev(), d[0], d[1])
	}

	return nil
}

// Open an existing database at `path` and looks for a table with the
// same name as the file's basename (without extension). Optional parameter `opts`
// allows to configure settings of the underlying boltdb engine.
//
// # Example
//
// ```
// // opens file `op.db` in path `./db` and looks for table `op`
// t, err := Open("./db/op.db")
// ```
func Open(path string, opts interface{}) (*pack.Table, error) {
	name := filepath.Base(path)
	name = name[:len(name)-len(filepath.Ext(name))]
	db, err := pack.OpenDatabase("bolt", filepath.Dir(path), name, "*", opts)
	if err != nil {
		return nil, err
	}
	return db.Table(
		name,
		pack.Options{
			JournalSizeLog2: JournalSizeLog2,
			CacheSize:       CacheSize,
		},
		pack.Options{
			JournalSizeLog2: IndexJournalSizeLog2,
			CacheSize:       IndexCacheSize,
		})
}

// Closes table and database. Must be called before shutdown to flush any state
// changes to disk.
func Close(table *pack.Table) error {
	if table == nil {
		return nil
	}
	if err := table.Close(); err != nil {
		return err
	}
	return table.Database().Close()
}

func pow2(v uint64) uint64 {
	for i := uint64(8); i < 1<<62; i *= 2 {
		if i >= v {
			return i
		}
	}
	return 0
}
