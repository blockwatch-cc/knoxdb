// Copyright (c) 2018-2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

// packed index generation test

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

	"blockwatch.cc/knoxdb/encoding/csv"
	"blockwatch.cc/knoxdb/pack"
	_ "blockwatch.cc/knoxdb/store/bolt"
	"blockwatch.cc/knoxdb/util"
	"blockwatch.cc/knoxdb/vec"
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
	packid    int
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
  table       list all table packs
  index       list all index packs
  blocks      show pack block headers
  type        show type info (from journal pack)
  journal     dump journal contents
  dump-all    dump full table contents
  dump-pack   dump pack contents (use -pack <id> to select a pack, default 0)
  dump-index  dump index pack contents (use -pack <id> to select a pack, default 0)
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
	flags.StringVar(&dbname, "db", "", "database")
	flags.StringVar(&cmd, "cmd", "", "run `command`")
	flags.IntVar(&packid, "pack", 0, "use pack `number`")
	flags.StringVar(&tablename, "table", "", "use table `name` (optional, for multi-table files)")
}

func main() {
	if err := run(); err != nil {
		log.Error(err)
	}
}

func printhelp() {
	fmt.Println("Usage:\n  packview [command] [database][/table][/pack] [flags]")
	fmt.Println(cmdinfo)
	fmt.Println("Flags:")
	flags.PrintDefaults()
	fmt.Println()
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
		vec.LogAVXFeatures(log.Log.Logger())
	}

	cmd = flags.Arg(0)
	dbname = strings.Split(flags.Arg(1), ".db")[0] + ".db"
	switch dbx := strings.Split(strings.TrimPrefix(strings.TrimPrefix(flags.Arg(1), dbname), "/"), "/"); len(dbx) {
	case 0:
		// none
	case 1:
		// table or pack
		var p int64
		if strings.HasPrefix(dbx[0], "0x") {
			p, err = strconv.ParseInt(strings.TrimPrefix(dbx[0], "0x"), 16, 64)
		} else {
			p, err = strconv.ParseInt(dbx[0], 10, 64)
		}
		if err == nil {
			packid = int(p)
		} else {
			tablename = dbx[0]
		}
	case 2:
		// table and pack
		var p int64
		tablename = dbx[0]
		if strings.HasPrefix(dbx[0], "0x") {
			p, err = strconv.ParseInt(strings.TrimPrefix(dbx[1], "0x"), 16, 64)
		} else {
			p, err = strconv.ParseInt(dbx[1], 10, 64)
		}
		if err == nil {
			packid = int(p)
		} else {
			return fmt.Errorf("invalid pack id '%s': %v", dbx[1], err)
		}
	default:
		return fmt.Errorf("invalid database locator")
	}

	if debug {
		fmt.Printf("db=%s\n", dbname)
		fmt.Printf("cmd=%s\n", cmd)
		fmt.Printf("pack=%d\n", packid)
	}

	if cmd == "" {
		return fmt.Errorf("Missing command. See -h")
	}

	name := strings.TrimSuffix(filepath.Base(dbname), ".db")
	db, err := pack.OpenDatabase(filepath.Dir(dbname), name, "*", boltopts)
	if err != nil {
		return fmt.Errorf("opening database: %v", err)
	}
	defer db.Close()

	if tablename == "" {
		tablename = name
	}
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
	case "journal":
		return table.DumpJournal(out, mode)
	case "type":
		return table.DumpType(out)
	case "blocks":
		return table.DumpPackBlocks(out, mode)
	case "table":
		return table.DumpPackHeaders(out, mode, sorted)
	case "index":
		return table.DumpIndexPackHeaders(out, mode, sorted)
	case "dump-all":
		return viewAllPacks(table, out, mode)
	case "dump-pack":
		return table.DumpPack(out, packid, mode)
	case "dump-index":
		return table.DumpIndexPack(out, 0, packid, mode)
	case "validate":
		table.ValidatePackHeaders(out)
		table.ValidateIndexPackHeaders(out)
		return nil
	default:
		return fmt.Errorf("unsupported command %s", cmd)
	}
}

func viewAllPacks(table *pack.Table, w io.Writer, mode pack.DumpMode) error {
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
