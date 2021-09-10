// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc
package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime/pprof"
	"time"

	"github.com/echa/log"
	bolt "go.etcd.io/bbolt"

	"blockwatch.cc/knoxdb/pack"
	_ "blockwatch.cc/knoxdb/store/bolt"
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
	startPack int
	endPack   int
	dbname    string
	fname     string
	cpuprof   string
	flags     = flag.NewFlagSet("dedup", flag.ContinueOnError)
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
	flags.IntVar(&startPack, "start", -1, "start pack")
	flags.IntVar(&endPack, "end", -1, "end pack")
	flags.StringVar(&dbname, "db", "", "database")
	flags.StringVar(&fname, "field", "", "field name (required)")
	flags.StringVar(&cpuprof, "profile", "", "write CPU profile to filename")
}

func printhelp() {
	fmt.Println("Usage:\n  dedup [flags]")
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

	// open existing table
	table, err := Open(dbname, boltopts)
	if err != nil {
		return err
	}

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

	count := 0
	start := time.Now()
	dupRows := make([]vec.Uint64Reducer, len(table.Fields()))
	dupBytes := make([]vec.Uint64Reducer, len(table.Fields()))
	fields := table.Fields()
	u64 := make([]uint64, 0, 1<<PackSizeLog2)
	err = table.WalkPacksRange(startPack, endPack, func(pkg *pack.Package) error {
		for i, v := range pkg.Blocks() {
			dup := make(map[uint64]struct{})
			var dr, db uint64
			for j, h := range v.Hashes(u64) {
				if _, ok := dup[h]; ok {
					dr++
					db += uint64(len(fields[i].Type.Bytes(v.At(j))))
				} else {
					dup[h] = struct{}{}
				}
			}
			dupRows[i].Add(dr)
			dupBytes[i].Add(db)
		}
		count++
		fmt.Printf(".")
		return nil
	})
	if err != nil && err != io.EOF {
		return err
	}

	if err := Close(table); err != nil {
		return err
	}

	fmt.Printf("\nProcessed %d packs in %s\n", count, time.Since(start))
	fmt.Printf("                                  Rows -------------------------------------------------------    Bytes ----------------------------------------------------\n")
	fmt.Printf("%03s  %15s  %10s  %9s  %9s  %9s  %9s  %15s  %9s  %9s  %9s  %9s  %15s\n", "Col", "Name", "Type", "Min", "Max", "Avg", "Sum", "Std", "Min", "Max", "Avg", "Sum", "Std")
	for i, f := range table.Fields() {
		a := dupRows[i]
		b := dupBytes[i]
		fmt.Printf("%02d   %15s  %10s  %9d  %9d  %9d  %9d  %15.4f  %9d  %9d  %9d  %9d  %15.4f\n",
			i, f.Alias, f.Type, a.Min(), a.Max(), uint64(a.Mean()), a.Sum(), a.Stddev(), b.Min(), b.Max(), uint64(b.Mean()), b.Sum(), b.Stddev())
	}

	return nil
}

// Open an existing database at `path` and looks for a table with the
// same name as the file's basename (without extension). Optional parameter `opts`
// allows to configure settings of the underlying boltdb engine.
//
// Example
//
// ```
// // opens file `op.db` in path `./db` and looks for table `op`
// t, err := Open("./db/op.db")
// ```
func Open(path string, opts interface{}) (*pack.Table, error) {
	name := filepath.Base(path)
	name = name[:len(name)-len(filepath.Ext(name))]
	db, err := pack.OpenDatabase(filepath.Dir(path), name, "*", opts)
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
