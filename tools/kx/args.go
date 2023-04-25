// Copyright (c) 2018-2022 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/echa/log"
	bolt "go.etcd.io/bbolt"

	"blockwatch.cc/knoxdb/pack"
	"blockwatch.cc/knoxdb/store"
)

var (
	flags   = flag.NewFlagSet("kx", flag.ContinueOnError)
	verbose bool
	debug   bool
	trace   bool
	noflush bool
	cmdinfo = `
Available Commands:
  stats       show boltdb stats
  reindex     rebuild indexes
  rebuild     rebuild statistics
  compact     compact table
  flush       flush journals
  gc          garbage collect bolt storage space
`
)

func init() {
	flags.Usage = func() {}
	flags.BoolVar(&verbose, "v", false, "be verbose")
	flags.BoolVar(&debug, "vv", false, "debug mode")
	flags.BoolVar(&trace, "vvv", false, "trace mode")
	flags.BoolVar(&noflush, "noflush", false, "disable journal flush")
}

func printhelp() {
	fmt.Println("Usage:\n  kx [flags] [command] [database][/table][/index][/pack]")
	fmt.Println(cmdinfo)
	fmt.Println("Flags:")
	flags.PrintDefaults()
	fmt.Println()
}

type Args struct {
	cmd   string
	db    string
	table string
	id1   int
	id2   int
	bolt  *bolt.Options
}

func parseArgs() (args Args, err error) {
	err = flags.Parse(os.Args[1:])
	if err != nil {
		if err == flag.ErrHelp {
			printhelp()
			err = nil
			return
		}
		return
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

	if flags.NArg() < 2 {
		err = fmt.Errorf("Missing argument. Need command and database file!")
		return
	}
	if cmd := flags.Arg(0); cmd == "" {
		err = fmt.Errorf("Missing command. See -h")
		return
	} else {
		args.cmd = cmd
	}

	db, dbx := separateTargetDescriptors(flags.Arg(1))
	args.db = db

	switch len(dbx) {
	case 0:
		// none
	case 1:
		// 1: /:table-name
		// 2: /:pack-id
		// 3: /:index-id
		if p, ok := readNumber(dbx[0]); ok {
			args.id1 = p
		} else {
			args.table = dbx[0]
		}
	case 2:
		// 1: /:table-name/:pack-id
		// 2: /:table-name/:index-id
		// 3: /:index-id/:pack-id
		p1, ok1 := readNumber(dbx[0])
		p2, ok2 := readNumber(dbx[1])
		if !ok1 {
			args.table = dbx[0]
			args.id1 = p2
		} else if ok2 {
			args.id1 = p1
			args.id2 = p2
		} else {
			err = fmt.Errorf("invalid id '%s': %v", dbx[1], err)
			return
		}
	case 3:
		// 1: /:table-name/:index-id/:pack-id
		args.table = dbx[0]
		if p, ok := readNumber(dbx[1]); ok {
			args.id1 = p
		} else {
			err = fmt.Errorf("invalid index id '%s': %v", dbx[1], err)
			return
		}
		if p, ok := readNumber(dbx[2]); ok {
			args.id2 = p
		} else {
			err = fmt.Errorf("invalid pack id '%s': %v", dbx[2], err)
			return
		}
	default:
		err = fmt.Errorf("invalid database locator")
		return
	}

	// table name defaults to same name as db file basename
	if args.table == "" {
		args.table = strings.TrimSuffix(filepath.Base(args.db), ".db")
	}

	if debug {
		log.Debug("cmd=", args.cmd)
		log.Debug("db=", args.db)
		log.Debug("table=", args.table)
		log.Debug("id1=", args.id1)
		log.Debug("id2=", args.id2)
	}

	args.bolt = &bolt.Options{
		Timeout:      time.Second, // open timeout when file is locked
		NoGrowSync:   true,        // assuming Docker + XFS
		NoSync:       true,        // skip fsync (DANGEROUS on crashes)
		FreelistType: bolt.FreelistMapType,
	}
	return
}

// Takes target descriptor and splits it into components
//
// returns the filename of the database and an array of optional descriptors
func separateTargetDescriptors(descriptor string) (string, []string) {
	targetSplit := strings.Split(descriptor, "/")
	for i, _ := range targetSplit {
		info, err := os.Stat(strings.Join(targetSplit[0:i+1], "/"))
		if err == nil && !info.IsDir() {
			return strings.Join(targetSplit[0:i+1], "/"), targetSplit[i+1 : len(targetSplit)]
		}
	}
	return "", []string{}
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
