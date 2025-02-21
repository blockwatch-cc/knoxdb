package main

import (
	"blockwatch.cc/knoxdb/internal/alp"
	"github.com/echa/log"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	values := []float64{
		1.1,
		2.1,
		3.21,
		3.22,
		2.33,
	}

	log.Infof("current value: %v\n", values)
	enc, err := alp.Compress(values)
	if err != nil {
		return err
	}
	log.Infof("encoded values: %v\n", enc.State.EncodedIntegers)

	decompressedValues, err := alp.Decompress(enc)
	if err != nil {
		return err
	}
	log.Infof("decoded values: %v\n", decompressedValues)

	return nil
}
