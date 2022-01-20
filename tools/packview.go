// Copyright (c) 2018-2022 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

// KnoxDB database inspector

package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/echa/log"
	bolt "go.etcd.io/bbolt"

	"blockwatch.cc/knoxdb/encoding/block"
	"blockwatch.cc/knoxdb/encoding/csv"
	"blockwatch.cc/knoxdb/pack"
	_ "blockwatch.cc/knoxdb/store/bolt"
	"blockwatch.cc/knoxdb/util"
)

var (
	flags     = flag.NewFlagSet("packview", flag.ContinueOnError)
	verbose   bool
	debug     bool
	trace     bool
	ashex     bool
	sorted    bool
	csvfile   string
	dbname    string
	cmd       string
	id1, id2  int
	tablename string
)

var (
	p        = util.PrettyInt64
	pi       = util.PrettyInt
	boltopts = &bolt.Options{
		Timeout:      time.Second, // open timeout when file is locked
		NoGrowSync:   true,        // assuming Docker + XFS
		ReadOnly:     true,
		NoSync:       true, // skip fsync (DANGEROUS on crashes)
		FreelistType: bolt.FreelistMapType,
	}
)

var cmdinfo = `
Available Commands:
  table        list all table packs
  index        list all index packs
  blocks       show pack block headers
  type         show type info (from journal pack)
  dump-journal dump journal contents
  dump-block   dump raw block data after decode
  dump-table   dump full table contents
  dump-index   dump full index contents (add /:index-id, default 0)
  dump-tpack   dump pack contents (add /:pack-id to select a pack, default 0)
  dump-ipack   dump index pack contents (add /:index-id/:pack-id, default 0)
  validate     cross-check pack index lists for table and indexes
`

func b(n int) string {
	return util.ByteSize(n).String()
}

func init() {
	flags.Usage = func() {}
	flags.BoolVar(&verbose, "v", false, "be verbose")
	flags.BoolVar(&debug, "vv", false, "enable debug mode")
	flags.BoolVar(&trace, "vvv", false, "enable trace mode")
	flags.BoolVar(&ashex, "hex", false, "hex output mode")
	flags.BoolVar(&sorted, "sorted", false, "sort pack headers by min value")
	flags.StringVar(&csvfile, "csv", "", "csv output `filename`")
}

func main() {
	if err := run(); err != nil {
		log.Error(err)
	}
}

func printhelp() {
	fmt.Println("Usage:\n  packview [flags] [command] [database][/table][/index][/pack]")
	fmt.Println(cmdinfo)
	fmt.Println("Flags:")
	flags.PrintDefaults()
	fmt.Println()
}

func readNumber(s string) (int, bool) {
	var (
		p   int
		err error
	)
	if strings.HasPrefix(s, "0x") {
		p, err = strconv.Atoi(strings.TrimPrefix(s, "0x"))
	} else {
		p, err = strconv.Atoi(s)
	}
	if err == nil {
		return p, true
	}
	return 0, false
}

func run() error {
	err := flags.Parse(os.Args[1:])
	if err != nil {
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

	if debug {
		util.LogCPUFeatures(log.Log.Logger())
	}

	if flags.NArg() < 2 {
		return fmt.Errorf("Missing argument. Need command and database file!")
	}

	cmd = flags.Arg(0)
	dbname = strings.Split(flags.Arg(1), ".db")[0] + ".db"
	switch dbx := strings.Split(strings.TrimPrefix(strings.TrimPrefix(flags.Arg(1), dbname), "/"), "/"); len(dbx) {
	case 0:
		// none
	case 1:
		// 1: /:table-name
		// 2: /:pack-id
		// 3: /:index-id
		if p, ok := readNumber(dbx[0]); ok {
			id1 = p
		} else {
			tablename = dbx[0]
		}
	case 2:
		// 1: /:table-name/:pack-id
		// 2: /:table-name/:index-id
		// 3: /:index-id/:pack-id
		p1, ok1 := readNumber(dbx[0])
		p2, ok2 := readNumber(dbx[1])
		if !ok1 {
			tablename = dbx[0]
			id1 = p2
		} else if ok2 {
			id1 = p1
			id2 = p2
		} else {
			return fmt.Errorf("invalid id '%s': %v", dbx[1], err)
		}
	case 3:
		// 1: /:table-name/:index-id/:pack-id
		tablename = dbx[0]
		if p, ok := readNumber(dbx[1]); ok {
			id1 = p
		} else {
			return fmt.Errorf("invalid index id '%s': %v", dbx[1], err)
		}
		if p, ok := readNumber(dbx[2]); ok {
			id2 = p
		} else {
			return fmt.Errorf("invalid pack id '%s': %v", dbx[2], err)
		}
	default:
		return fmt.Errorf("invalid database locator")
	}

	// table name defaults to same name as db file basename
	if tablename == "" {
		tablename = strings.TrimSuffix(filepath.Base(dbname), ".db")
	}

	if debug {
		fmt.Printf("cmd=%s\n", cmd)
		fmt.Printf("db=%s\n", dbname)
		fmt.Printf("table=%s\n", tablename)
		fmt.Printf("id1=%d\n", id1)
		fmt.Printf("id2=%d\n", id2)
	}

	if cmd == "" {
		return fmt.Errorf("Missing command. See -h")
	}

	db, err := pack.OpenDatabase(
		filepath.Dir(dbname),
		strings.TrimSuffix(filepath.Base(dbname), ".db"),
		"*",
		boltopts,
	)
	if err != nil {
		return fmt.Errorf("opening database: %v", err)
	}
	defer db.Close()

	table, err := db.Table(tablename)
	if err != nil {
		return fmt.Errorf("opening table '%s': %v", tablename, err)
	}

	out := io.Writer(os.Stdout)
	mode := pack.DumpModeDec
	if ashex {
		mode = pack.DumpModeHex
	}
	if csvfile != "" {
		mode = pack.DumpModeCSV
		f, err := os.OpenFile(csvfile, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
		if err != nil {
			return err
		}
		defer f.Close()
		out = csv.NewEncoder(f)
	}

	switch cmd {
	case "type":
		return table.DumpType(out)
	case "blocks":
		return table.DumpPackBlocks(out, mode)
	case "table":
		return table.DumpPackInfo(out, mode, sorted)
	case "index":
		return table.DumpIndexPackInfo(out, id1, mode, sorted)
	case "dump-journal":
		return table.DumpJournal(out, mode)
	case "dump-table":
		return viewAllTablePacks(table, out, mode)
	case "dump-index":
		return viewAllIndexPacks(table, id1, out, mode)
	case "dump-tpack":
		return table.DumpPack(out, id1, mode)
	case "dump-ipack":
		return table.DumpIndexPack(out, id1, id2, mode)
	case "dump-block":
		return dumpByteBlock(table, id1, out)
	case "validate":
		table.ValidatePackIndex(out)
		table.ValidateIndexPackIndex(out)
		return nil
	default:
		return fmt.Errorf("unsupported command %s", cmd)
	}
}

func viewAllTablePacks(table *pack.Table, w io.Writer, mode pack.DumpMode) error {
	for i := 0; ; i++ {
		err := table.DumpPack(w, i, mode)
		if err != nil && err != pack.ErrPackNotFound {
			return err
		}
		if err == pack.ErrPackNotFound {
			break
		}
	}
	return nil
}

func viewAllIndexPacks(table *pack.Table, idx int, w io.Writer, mode pack.DumpMode) error {
	for i := 0; ; i++ {
		err := table.DumpIndexPack(w, idx, i, mode)
		if err != nil && err != pack.ErrPackNotFound {
			return err
		}
		if err == pack.ErrPackNotFound {
			break
		}
	}
	return nil
}

func dumpByteBlock(table *pack.Table, id int, w io.Writer) error {
	return table.WalkPacksRange(id, id, func(p *pack.Package) error {
		for i, v := range p.Blocks() {
			if v.Type() == block.BlockBytes {
				fmt.Printf("Dump raw data for pack=%x block=%d\n", p.Key(), i)
				w.Write([]byte(v.Bytes.Dump()))
			}
		}
		return nil
	})
}
