// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

// KnoxDB wal inspector

package main

import (
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"

	"github.com/echa/log"

	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/internal/wal"
	"blockwatch.cc/knoxdb/pkg/util"
	"github.com/jedib0t/go-pretty/v6/table"
)

var (
	flags      = flag.NewFlagSet("walview", flag.ContinueOnError)
	verbose    bool
	debug      bool
	trace      bool
	mode       wal.RecoveryMode = wal.RecoveryModeIgnore
	size       int
	lsn        uint64
	limit      int64
	headRepeat int
)

func init() {
	flags.Usage = func() {}
	flags.BoolVar(&verbose, "v", false, "be verbose")
	flags.BoolVar(&debug, "vv", false, "enable debug mode")
	flags.BoolVar(&trace, "vvv", false, "enable trace mode")
	flags.IntVar(&headRepeat, "head", 80, "repeat headers every `num` records")
	flags.IntVar(&size, "size", 128<<20, "max segment size")
	flags.Uint64Var(&lsn, "lsn", 0, "first lsn to read from")
	flags.Int64Var(&limit, "limit", 0, "stop after `limit` records")
	flags.Var(&mode, "mode", "wal recovery mode on open (ignore, skip, truncate, fail)")
}

func main() {
	defer func() {
		if e := recover(); e != nil {
			log.Error(e)
			os.Exit(1)
		}
	}()
	if err := run(); err != nil {
		log.Error(err)
		os.Exit(1)
	}
}

func printhelp() {
	fmt.Println("Usage:\n  walview [flags] [path/to/wal]")
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

	dbname := filepath.Base(filepath.Dir(filepath.Clean(flags.Arg(0))))
	opts := wal.WalOptions{
		Seed:           types.TaggedHash(types.ObjectTagDatabase, dbname),
		Path:           flags.Arg(0),
		MaxSegmentSize: size,
		RecoveryMode:   mode,
		Logger:         log.Log,
	}
	log.Debugf("Opening wal for db %s at %s in mode %s", dbname, opts.Path, opts.RecoveryMode)
	w, err := wal.Open(wal.LSN(lsn), opts)
	if err != nil {
		return err
	}
	defer w.Close()

	r := w.NewReader()
	if err := r.Seek(wal.LSN(lsn)); err != nil {
		return err
	}

	t := table.NewWriter()
	t.SetPageSize(headRepeat)
	t.SetOutputMirror(os.Stdout)
	t.SetTitle("%s WAL - size %s - max lsn 0x%016x", dbname, util.ByteSize(w.Len()), w.Len())
	t.AppendHeader(table.Row{"#", "LSN", "Type", "Tag", "TxID", "Entity", "Body"})
	var count int64
	for {
		rec, err := r.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		count++
		t.AppendRow([]any{
			count,
			"0x" + strconv.FormatUint(uint64(rec.Lsn), 16),
			rec.Type,
			rec.Tag,
			strconv.FormatUint(rec.TxID, 10),
			"0x" + strconv.FormatUint(rec.Entity, 16),
			LimitHexEllipsis(rec.Data, 64),
		})
		if limit > 0 && limit == count {
			break
		}
	}
	t.Render()
	return nil
}

func LimitHexEllipsis(buf []byte, sz int) string {
	if len(buf) > sz {
		left := hex.EncodeToString(buf[:sz/2])
		right := hex.EncodeToString(buf[len(buf)-sz/2:])
		return left + "..." + right
	}
	return hex.EncodeToString(buf)
}
