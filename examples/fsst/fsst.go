// Copyright (c) 2025 Blockwatch Data Inc.
// Author: abdul@blockwatch.cc

package main

import (
	"flag"
	"os"

	"blockwatch.cc/knoxdb/internal/fsst"
	"github.com/echa/log"
)

const (
	PERM = 0644
)

var (
	path string
	out  string
	op   string
)

func init() {
	flag.StringVar(&op, "op", "compress", "operation")
	flag.StringVar(&path, "in", "", "path to file")
	flag.StringVar(&out, "out", "", "path to out file")
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
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}

	compressed := fsst.Compress([][]uint8{data})
	log.Infof("Writing compressed data to %s", out)
	err = os.WriteFile(out, compressed, PERM)
	if err != nil {
		return err
	}

	log.Infof("Compressed %d byte(s) to %d byte(s)", len(data), len(compressed))
	return nil
}
