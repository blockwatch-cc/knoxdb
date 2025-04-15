package main

import (
	"log"

	"blockwatch.cc/knoxdb/internal/encode/fcbench"
)

func main() {
	cfg := fcbench.BenchmarkConfig{
		VectorLengths: []int{128, 256, 512, 1024},
		DatasetSize:   1000000,
		DatasetType:   "timeseries",
		OutputDir:     "./bench_results",
		Encoders:      []string{"delta", "gorilla", "rle"},
		Repeat:        3,
	}
	results, err := fcbench.RunBenchmarks(cfg)
	if err != nil {
		log.Fatal(err)
	}
	for _, r := range results {
		log.Printf("Encoder: %s, Vector: %d, Ratio: %.2f, Throughput: %.2f val/s",
			r.EncoderName, r.VectorLength, r.CompressionRatio, r.Throughput)
	}
}
