// Copyright (c) 2020 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc
package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"runtime/pprof"
	"time"

	"github.com/echa/log"
	bolt "go.etcd.io/bbolt"

	"blockwatch.cc/knoxdb/pack"
	_ "blockwatch.cc/knoxdb/store/bolt"
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
	flags.UintVar(&prec, "precision", 14, "loglog-beta precision 1<<n")
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
	pos := make([]int, len(table.Fields()))
	neg := make([]int, len(table.Fields()))
	fields := table.Fields()
	err = table.WalkPacks(func(pkg *pack.Package) error {
		for i, v := range pkg.Blocks() {
			// skip for non-bloom fields when requested
			if bloomOnly && !fields[i].Flags.Contains(pack.FlagBloom) {
				continue
			}
			switch fields[i].Type {
			case pack.FieldTypeUint64:
			case pack.FieldTypeInt64, pack.FieldTypeDatetime, pack.FieldTypeDecimal64:
			case pack.FieldTypeUint32, pack.FieldTypeInt32, pack.FieldTypeDecimal32:
			default:
				continue
			}

			est := fields[i].Type.EstimateCardinality(v, prec)
			if est == 0 {
				est++
			}

			flt := fields[i].Type.BuildBloomFilter(v, est, 1)
			if flt == nil {
				fmt.Printf("\nerror creating bloom filter pack %d Block %s card=%d\n", count, fields[i].Alias, est)
				continue
			}

			switch fields[i].Type {
			case pack.FieldTypeUint64, pack.FieldTypeInt64, pack.FieldTypeDecimal64, pack.FieldTypeDatetime:
				var buf [8]byte
				for j := 0; j < 65536; j++ {
					val := rand.Int63()
					binary.BigEndian.PutUint64(buf[:], uint64(val))

					if flt.Contains(buf[:]) {
						pos[i]++
					} else {
						neg[i]++
					}
				}

			case pack.FieldTypeUint32, pack.FieldTypeInt32, pack.FieldTypeDecimal32:
				var buf [4]byte
				for j := 0; j < 65536; j++ {
					val := rand.Int31()
					binary.BigEndian.PutUint32(buf[:], uint32(val))

					if flt.Contains(buf[:]) {
						pos[i]++
					} else {
						neg[i]++
					}
				}
			}
		}

		count++
		fmt.Printf(".")
		return nil
	})
	if err != nil {
		return err
	}

	if err := Close(table); err != nil {
		return err
	}

	fmt.Printf("\nProcessed %d packs in %s\n", count, time.Since(start))
	fmt.Printf("%03s  %15s  %10s  %10s  %10s %10s\n", "Col", "Name", "Type", "Pos", "Neg", "Ratio")
	for i, f := range table.Fields() {
		if pos[i]+neg[i] > 0 {
			fmt.Printf("%02d   %15s  %10s  %10d  %10d %10.1f%%\n",
				i, f.Alias, f.Type, pos[i], neg[i], 100*float64(pos[i])/float64(pos[i]+neg[i]))
		} else {
			fmt.Printf("%02d   %15s  %10s\n",
				i, f.Alias, f.Type)

		}
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
