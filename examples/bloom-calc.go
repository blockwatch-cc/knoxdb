// Copyright (c) 2022 Blockwatch Data Inc.
// Author: alex@blockwatch.cc
// Author: stefan@blockwatch.cc
package main

import (
	"flag"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime/pprof"
	"time"

	"github.com/echa/log"
	bolt "go.etcd.io/bbolt"

	"blockwatch.cc/knoxdb/filter/bloom"
	"blockwatch.cc/knoxdb/hash/xxhashVec"
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
	cache     bool
	dbname    string
	fieldname string
	prec      uint
	scale     int
	cpuprof   string
	flags     = flag.NewFlagSet("bloom", flag.ContinueOnError)
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
	flags.UintVar(&prec, "precision", 14, "loglog-beta precision 1<<n")
	flags.IntVar(&scale, "scale", 1, "bloom error rate 1 (high=2%) .. 4 (low=0.002%)")
	flags.StringVar(&fieldname, "field", "sender_id", "use DB field")
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
	dbname := flags.Arg(0)
	if dbname == "" {
		return fmt.Errorf("Missing database file")
	}
	table, err := Open(dbname, boltopts)
	if err != nil {
		return err
	}

	// keep real and bloom filter data for each pack
	reals := make([]*vec.Bitset, 0)
	blooms := make([]*bloom.Filter, 0)
	var (
		maxid     uint32
		bloomSize int
	)

	count := 0
	start := time.Now()
	field := table.Fields().Find(fieldname)
	if !field.IsValid() {
		return fmt.Errorf("Field %q not found", fieldname)
	}
	if !field.Flags.Contains(pack.FlagBloom) {
		return fmt.Errorf("Field %q has no bloom flag", fieldname)
	}

	fmt.Printf("Using database field %s flags=%s type=%s\n", field.Alias, field.Flags, field.Type)
	err = table.WalkPacks(func(pkg *pack.Package) error {
		block := pkg.Blocks()[field.Index]
		_, max := block.MinMax()
		maxVal := max.(uint32)
		maxid = max(maxid, maxVal)

		realBits := vec.NewBitset(int(maxVal))
		for _, v := range block.Uint32 {
			realBits.Set(int(v))
		}
		reals = append(reals, realBits)

		// dimension and build a bloom filter
		est := field.Type.EstimateCardinality(block, prec)
		if est == 0 {
			est++
		}

		flt := field.Type.BuildBloomFilter(block, est, scale)
		if flt == nil {
			return fmt.Errorf("creating bloom filter for pack=%d len=%d sz=%d scale=%d", count, pkg.Len(), est, scale)
		}
		bloomSize += int(flt.Len())
		blooms = append(blooms, flt)

		count++
		fmt.Printf(".")
		if count%100 == 0 {
			fmt.Printf("%d", count)
		}
		return nil
	})
	if err != nil {
		return err
	}

	if err := Close(table); err != nil {
		return err
	}
	fmt.Printf("\nRead %d packs in %s\n", count, time.Since(start))

	start = time.Now()
	fmt.Printf("Running stats for %d values\n", maxid)
	// now process all ids from 1..max and measure the overall impact of false positive
	// filter matches on simulated full table scans:
	// - number of extra packs read over optimum
	// - percent of extra packs vs optimal packs read
	var (
		pctStats       vec.Float64Reducer
		optimalMatches int64
		bloomMatches   int64
		noMatches      int64
	)
	absStats := vec.NewWindowInt64Reducer(int(maxid))
	for id := uint32(1); id <= maxid; id++ {
		var optimal, bloomed int64
		var h [2]uint32
		h[0] = xxhashVec.XXHash32Uint32(id, 1312) // same as in bloom lib
		h[1] = xxhashVec.XXHash32Uint32(id, 0)
		for packid := range reals {
			if reals[packid].IsSet(int(id)) {
				optimal++
				optimalMatches++
			}
			if blooms[packid].ContainsHash(h) {
				bloomed++
				bloomMatches++
			}
		}
		if optimal == 0 {
			noMatches++
			continue
		}
		if bloomed < optimal {
			fmt.Printf("False negative for id=%d bloom-matches=%d optimal-matches=%d\n",
				id, bloomed, optimal)
		} else {
			absStats.Add(int64(bloomed - optimal))
			pctStats.Add(float64(bloomed-optimal) / float64(optimal))
		}
		if id%10240 == 0 {
			fmt.Printf(".")
		}
	}
	fmt.Printf("\nProcessed in %s\n", time.Since(start))

	fmt.Println("Bloom Filter Accuracy Statistics")
	fmt.Println("--------------------------------")
	fmt.Printf("Total Ids          %d\n", maxid)
	fmt.Printf("LLB precision      %d\n", prec)
	fmt.Printf("Bloom Err Rate     %f (scale=%d)\n", 20.0/math.Pow10(scale), scale)
	fmt.Printf("Total Bloom Bytes  %d\n", bloomSize)
	fmt.Printf("No Matches         %d\n", noMatches)
	fmt.Printf("Optimal Matches    %d\n", optimalMatches)
	fmt.Printf("Bloom Matches      %d (%+.2f%%)\n", bloomMatches, float64(bloomMatches)/float64(optimalMatches)*100-100)
	fmt.Println("--------------------------------")
	fmt.Printf("Bloom Min Abs Err  %d\n", absStats.Min())
	fmt.Printf("Bloom Max Abs Err  %d\n", absStats.Max())
	fmt.Printf("Bloom Avg Abs Err  %.2f\n", absStats.Mean())
	fmt.Printf("Bloom Med Abs Err  %.2f\n", absStats.Median())
	fmt.Printf("Bloom Abs Err Std  %.2f\n", absStats.Stddev())
	fmt.Println("--------------------------------")
	fmt.Printf("Bloom Min Pct Err  %.2f\n", pctStats.Min())
	fmt.Printf("Bloom Max Pct Err  %.2f\n", pctStats.Max())
	fmt.Printf("Bloom Avg Pct Err  %.2f\n", pctStats.Mean())
	fmt.Printf("Bloom Pct Err Std  %.2f\n", pctStats.Stddev())

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
