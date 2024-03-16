// Copyright (c) 2018-2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc
//go:build ignore
// +build ignore

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
	flags   = flag.NewFlagSet("packview", flag.ContinueOnError)
	verbose bool
	debug   bool
	trace   bool
	//	ashex     bool
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
  compress-pack         compress pack (use -pack <id> to select a pack, default 0)
  compress-all          compress full table
  compress-index-pack   compress index pack (use -pack <id> to select a pack, default 0)
  compress-index-all    compress full index
  index-collisions      count hash collisions
  show                  show compression
  `

func b(n int) string {
	return util.ByteSize(n).String()
}

func init() {
	flags.Usage = func() {}
	flags.BoolVar(&verbose, "v", false, "be verbose")
	flags.BoolVar(&debug, "vv", false, "enable debug mode")
	flags.BoolVar(&trace, "vvv", false, "enable trace mode")
	// flags.BoolVar(&ashex, "hex", false, "hex output mode")
	flags.StringVar(&csvfile, "csv", "", "csv output `filename`")
	// flags.StringVar(&dbname, "db", "", "database")
	// flags.StringVar(&cmd, "cmd", "", "run `command`")
	// flags.IntVar(&packid, "pack", 0, "use pack `number`")
	// flags.StringVar(&tablename, "table", "", "use table `name` (optional, for multi-table files)")
}

func main() {
	if err := run(); err != nil {
		log.Error(err)
	}
}

func printhelp() {
	fmt.Println("Usage:\n  compress [command] [database][/table][/pack] [flags]")
	fmt.Println(cmdinfo)
	fmt.Println("Flags:")
	flags.PrintDefaults()
	fmt.Println()
}

func run() error {
	if len(os.Args) < 4 {
		printhelp()
		return nil
	}
	err := flags.Parse(os.Args[4:])
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

	// cmd = flags.Arg(0)
	cmd = os.Args[1]
	// dbname = strings.Split(flags.Arg(1), ".db")[0] + ".db"
	dbname = strings.Split(os.Args[2], ".db")[0] + ".db"
	switch dbx := strings.Split(strings.TrimPrefix(os.Args[2], dbname), "/"); len(dbx) {
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

	var cmethod string
	if cmethod = os.Args[3]; cmethod == "" {
		cmethod = "legacy"
	}

	name := strings.TrimSuffix(filepath.Base(dbname), ".db")
	db, err := pack.OpenDatabase("bolt", filepath.Dir(dbname), name, "*", boltopts)
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
	//	if ashex {
	//		mode = pack.DumpModeHex
	//	}
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
	case "compress-pack":
		return table.CompressPack(cmethod, out, packid, mode)
	case "compress-all":
		return table.CompressAll(cmethod, out, mode, verbose)
	case "compress-index-pack":
		return table.CompressIndexPack(cmethod, out, 0, packid, mode)
	case "compress-index-all":
		return table.CompressIndexAll(cmethod, 0, out, mode, verbose)
	case "index-collisions":
		return table.IndexCollisions(cmethod, 0, out, mode, verbose)
	case "show":
		return table.ShowCompression(cmethod, out, mode, verbose)
	default:
		return fmt.Errorf("unsupported command %s", cmd)
	}
}
