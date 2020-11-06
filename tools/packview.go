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
)

var (
	flags     = flag.NewFlagSet("packview", flag.ContinueOnError)
	verbose   bool
	debug     bool
	trace     bool
	ashex     bool
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
	fmt.Println("Usage:\n  packview [flags] [command]")
	fmt.Println(cmdinfo)
	fmt.Println("Flags:")
	flags.PrintDefaults()
	fmt.Println()
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

	switch flags.NArg() {
	case 0:
		if dbname == "" {
			return fmt.Errorf("Missing database.")
		}
	case 1:
		if dbname == "" {
			dbname = flags.Arg(0)
		} else {
			cmd = flags.Arg(0)
		}
	default:
		var i int
		if dbname == "" {
			dbname = flags.Arg(i)
			i++
		}
		if cmd == "" {
			cmd = flags.Arg(i)
			i++
		}
		if packid == 0 {
			id := flags.Arg(i)
			var (
				p   int64
				err error
			)
			if strings.HasPrefix(id, "0x") {
				p, err = strconv.ParseInt(strings.TrimPrefix(id, "0x"), 16, 64)
			} else {
				p, err = strconv.ParseInt(id, 10, 64)
			}
			if err == nil {
				packid = int(p)
			} else {
				return fmt.Errorf("invalid pack id '%s': %v", id, err)
			}
		}
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
		return table.DumpPackHeaders(out, mode)
	case "index":
		return table.DumpIndexPackHeaders(out, mode)
	case "dump-all":
		return viewAllPacks(table, out, mode)
	case "dump-pack":
		return table.DumpPack(out, packid, mode)
	case "dump-index":
		return table.DumpIndexPack(out, 0, packid, mode)
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
