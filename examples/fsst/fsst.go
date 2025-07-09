// Copyright (c) 2025 Blockwatch Data Inc.
// Author: abdul@blockwatch.cc

package main

import (
	"bufio"
	"flag"
	"io"
	"os"
	"runtime/pprof"

	"blockwatch.cc/knoxdb/internal/encode/fsst"
	"github.com/echa/log"
)

const (
	PERM = 0644
)

var (
	path string
	out  string
	op   string

	trace   bool
	debug   bool
	verbose bool
	profile bool
)

func init() {
	flag.StringVar(&op, "op", "compress", "operation")
	flag.StringVar(&path, "in", "", "path to file")
	flag.StringVar(&out, "out", "", "path to out file")
	flag.BoolVar(&verbose, "v", false, "be verbose")
	flag.BoolVar(&debug, "vv", false, "enable debug mode")
	flag.BoolVar(&trace, "vvv", false, "enable trace mode")
	flag.BoolVar(&profile, "profile", false, "enable CPU profiling")
}

func main() {
	log.ParseFlags("0")
	flag.Parse()

	if err := run(); err != nil {
		log.Fatal("error: %v", err)
	}
}

func run() error {
	log.Infof("Loading file %s for fsst compression", path)

	lvl := log.LevelInfo
	switch {
	case trace:
		lvl = log.LevelTrace
	case debug:
		lvl = log.LevelDebug
	case verbose:
		lvl = log.LevelInfo
	}
	log.SetLevel(lvl)

	if profile {
		f, err := os.Create("fsst.prof")
		if err != nil {
			log.Fatal(err)
		}
		defer f.Close()

		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	switch op {
	case "compress":
		return compress()
	case "decompress":
		return decompress()
	default:
		log.Warnf("%q is not supported command", op)
		return nil
	}
}

func decompress() error {
	buf, err := os.ReadFile(path)
	if err != nil {
		return err
	}

	decompressed, err := fsst.Decompress(buf)
	if err != nil {
		return err
	}
	log.Infof("Writing decompressed data to %s", out)
	err = os.WriteFile(out, decompressed, PERM)
	if err != nil {
		return err
	}

	return nil
}

func compress() error {
	f, err := os.OpenFile(path, os.O_RDONLY, PERM)
	if err != nil {
		return err
	}

	rd := bufio.NewReader(f)

	dataSz := 0
	data := make([][]byte, 0)

	for {
		read := make([]byte, fsst.FSST_MAX_SIZE)
		sz, err := rd.Read(read)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		data = append(data, read[:sz])
		dataSz += sz
	}

	compressed := fsst.Compress(data)
	log.Infof("Writing compressed data to %s", out)
	err = os.WriteFile(out, compressed, PERM)
	if err != nil {
		return err
	}

	log.Infof("Compressed %d byte(s) to %d byte(s) %d %%", dataSz, len(compressed), (100 * len(compressed) / dataSz))
	return nil
}
