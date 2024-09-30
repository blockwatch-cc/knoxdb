// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc
package main

import (
	"flag"
	"fmt"
	"os"
	"runtime/pprof"

	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/internal/wal"
	"github.com/echa/log"
)

var (
	verbose bool
	debug   bool
	trace   bool
	profile bool
	cmd     string
	walPath string
	flags   = flag.NewFlagSet("wal", flag.ContinueOnError)
)

// Main
func init() {
	flags.Usage = func() {}
	flags.BoolVar(&verbose, "v", false, "be verbose")
	flags.BoolVar(&debug, "vv", false, "enable debug mode")
	flags.BoolVar(&trace, "vvv", false, "enable trace mode")
	flags.BoolVar(&profile, "profile", false, "enable CPU profiling")
	flags.StringVar(&walPath, "waldir", "", "wal")
	flags.StringVar(&cmd, "cmd", "write", "action")
}

func printhelp() {
	fmt.Println("Usage:\n  types [flags]")
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

	if profile {
		f, err := os.Create("types.prof")
		if err != nil {
			log.Fatal(err)
		}
		defer f.Close()
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	opt := wal.WalOptions{
		Path:           walPath,
		MaxSegmentSize: 20 << 10,
	}

	switch cmd {
	case "write":
		return Write(opt)

	case "read":
		return Read(opt)

	default:
		return fmt.Errorf("cmd is not valid")
	}

}

func Read(opt wal.WalOptions) error {
	w, err := wal.Open(wal.LSN(0), opt)
	if err != nil {
		return err
	}
	defer func() error {
		return w.Close()
	}()

	var record *wal.Record
	reader := w.NewReader()

	for {
		record, err = reader.Next()
		if err != nil {
			break
		}
		fmt.Println(record)
	}

	return err
}

func Write(opt wal.WalOptions) error {
	w, err := wal.Create(opt)
	if err != nil {
		return err
	}
	defer func() error {
		err = w.Sync()
		if err != nil {
			return err
		}
		return w.Close()
	}()

	records := genWalRecords(1000)
	for _, record := range records {
		lsn, err := w.Write(record)
		if err != nil {
			return err
		}
		record.Lsn = lsn
	}

	return nil
}

func genWalRecords(sz int) []*wal.Record {
	recs := make([]*wal.Record, 0, sz)
	for i := 1; i <= sz; i++ {
		recs = append(recs, &wal.Record{
			Type:   wal.RecordTypeCommit,
			Tag:    types.ObjectTagDatabase,
			Entity: 100,
			Data:   []byte("hello"),
			TxID:   uint64(i),
		})
	}

	return recs
}
