package main

import (
	"flag"
	"fmt"

	"blockwatch.cc/knoxdb/internal/encode/alp"
	"github.com/echa/log"
)

var (
	op string
)

func main() {
	flag.StringVar(&op, "op", "alp", "")
	flag.Parse()

	var err error
	switch op {
	case "alp":
		err = runALP()
	case "alprd":
		err = runALPrd()
	}

	if err != nil {
		log.Fatal(err)
	}
}

func runALP() error {
	values := []float64{
		1.1,
		2.1,
		3.21,
		3.22,
		2.33,
	}

	log.Infof("current value: %v\n", values)
	state := alp.Compress(values)
	log.Infof("encoded values: %v\n", state.EncodedIntegers)

	decompressedValues := make([]float64, len(values))
	alp.Decompress(decompressedValues, state.EncodingIndice.Factor, state.EncodingIndice.Exponent, state.FOR, state.Exceptions, state.ExceptionPositions, state.EncodedIntegers)
	log.Infof("decoded values: %v\n", decompressedValues)

	return nil
}

func runALPrd() error {
	values := []float64{
		1.5, 1.5, 2.35, 2.35, 3.60, 3.60,
	}

	log.Infof("current value: %v\n", values)
	state := alp.RDCompress[float64, uint64](values)
	log.Infof("left part encoded: %v\n", state.LeftPartEncoded)
	log.Infof("right part encoded: %v\n", state.RightPartEncoded)

	decompressedValues := alp.RDDecompress[float64, uint64](state)
	log.Infof("decoded values: %v\n", decompressedValues)

	for i, v := range decompressedValues {
		if v != values[i] {
			fmt.Println("failed !!!")
		}
	}

	return nil
}
