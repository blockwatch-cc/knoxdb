// Copyright (c) 2025 Blockwatch Data Inc.
// Author: oliver@blockwatch.cc

package main

import (
	"flag"
	"log"
	"os"
	"strings"
	"time"

	"blockwatch.cc/knoxdb/internal/encode/fcbench"
)

// main is the entry point for the KnoxDB FCBench benchmarking tool.
// Usage:
//   - Default: Run recommended combinations (float encoders ["alp"] on
//     float datasets [timeseries, hpc, observation]; integer encoders
//     ["delta", "run", "bp", "dict", "s8"] on transaction dataset).
//   - --all: Run all 28 encoder-dataset combinations (4 datasets × 7 encoders),
//     including incompatible ones (e.g., timeseries × delta), which may fail to
//     expose core issues.
//   - --dataset=timeseries,hpc: Run specific datasets with all or selected encoders.
//   - --encoder=alp,delta: Run specific encoders on all or selected datasets.
//   - --recommended: Explicitly run recommended combinations (same as default).
//
// Output: CSVs in ./bench_results and logs detailing successes/failures.
// Note: Incompatible combinations (e.g., float dataset with integer encoder) are
// expected to fail (e.g., "integer encoder not supported"), helping identify
// error-handling issues in KnoxDB.

func main() {
	// Parse command-line flags
	all := flag.Bool("all", false, "Run all encoder-dataset combinations (including incompatible ones)")
	recommended := flag.Bool("recommended", false, "Run only recommended combinations (default)")
	dataset := flag.String("dataset", "", "Run specific dataset(s): timeseries,hpc,observation,transaction (comma-separated)")
	encoder := flag.String("encoder", "", "Run specific encoder(s): alp,delta,run,bp,dict,s8 (comma-separated)")
	flag.Parse()

	// Ensure output directory exists
	if err := os.MkdirAll("./bench_results", 0755); err != nil {
		log.Fatalf("Failed to create output directory: %v", err)
	}

	log.Println("KnoxDB FCBench - Starting benchmark...")

	// Determine run mode
	if *all && !*recommended {
		log.Println("Running all possible encoder-dataset combinations...")
		runAllCombinations(strings.Split(*dataset, ","), strings.Split(*encoder, ","))
	} else if *dataset != "" || *encoder != "" {
		log.Println("Running selected encoder-dataset combinations...")
		runSelectedCombinations(strings.Split(*dataset, ","), strings.Split(*encoder, ","))
	} else {
		log.Println("Running recommended benchmarks...")
		runRecommendedBenchmarks()
	}
}

// runAllCombinations runs every encoder-dataset combination, logging errors for incompatible pairs
func runAllCombinations(datasets, encoders []string) {
	allDatasets := []string{"timeseries", "hpc", "observation", "transaction"}
	allEncoders := []string{"alp", "delta", "run", "bp", "dict", "s8"}

	// Filter datasets and encoders if specified
	if len(datasets) > 0 && datasets[0] != "" {
		filtered := []string{}
		for _, d := range datasets {
			if contains(allDatasets, d) {
				filtered = append(filtered, d)
			} else {
				log.Printf("Warning: Unknown dataset '%s' ignored", d)
			}
		}
		if len(filtered) > 0 {
			allDatasets = filtered
		}
	}
	if len(encoders) > 0 && encoders[0] != "" {
		filtered := []string{}
		for _, e := range encoders {
			if contains(allEncoders, e) {
				filtered = append(filtered, e)
			} else {
				log.Printf("Warning: Unknown encoder '%s' ignored", e)
			}
		}
		if len(filtered) > 0 {
			allEncoders = filtered
		}
	}

	var success, failure int
	for _, dataset := range allDatasets {
		log.Printf("\n=== %s BENCHMARKS ===\n", strings.ToUpper(dataset))
		for _, enc := range allEncoders {
			cfg := createBenchmarkConfig(dataset, []string{enc})
			log.Printf("Running %s with %s...\n", dataset, enc)
			results, err := fcbench.RunBenchmarks(cfg)
			if err != nil {
				log.Printf("  Failed: %v\n", err)
				failure++
			} else {
				log.Printf("  Succeeded: %d results\n", len(results))
				printResults(results)
				success++
			}
		}
	}

	logSummary(success, failure)
}

// runSelectedCombinations runs user-specified encoder-dataset pairs
func runSelectedCombinations(datasets, encoders []string) {
	allDatasets := []string{"timeseries", "hpc", "observation", "transaction"}
	allEncoders := []string{"alp", "delta", "run", "bp", "dict", "s8"}

	// Use all if none specified
	if len(datasets) == 0 || datasets[0] == "" {
		datasets = allDatasets
	}
	if len(encoders) == 0 || encoders[0] == "" {
		encoders = allEncoders
	}

	var success, failure int
	for _, dataset := range datasets {
		if !contains(allDatasets, dataset) {
			log.Printf("Skipping unknown dataset: %s\n", dataset)
			continue
		}
		log.Printf("\n=== %s BENCHMARKS ===\n", strings.ToUpper(dataset))
		for _, enc := range encoders {
			if !contains(allEncoders, enc) {
				log.Printf("Skipping unknown encoder: %s\n", enc)
				continue
			}
			cfg := createBenchmarkConfig(dataset, []string{enc})
			log.Printf("Running %s with %s...\n", dataset, enc)
			results, err := fcbench.RunBenchmarks(cfg)
			if err != nil {
				log.Printf("  Failed: %v\n", err)
				failure++
			} else {
				log.Printf("  Succeeded: %d results\n", len(results))
				printResults(results)
				success++
			}
		}
	}

	logSummary(success, failure)
}

// runRecommendedBenchmarks runs only the recommended combinations
func runRecommendedBenchmarks() {
	var success, failure int

	// Time Series with Float Encoders
	log.Println("\n=== TIME SERIES WITH FLOAT ENCODERS ===")
	if results, err := runBenchmark("timeseries", []string{"alp"}); err != nil {
		log.Printf("Time Series benchmark failed: %v\n", err)
		failure++
	} else {
		printResults(results)
		success++
	}

	// HPC with Float Encoders
	log.Println("\n=== HPC WITH FLOAT ENCODERS ===")
	if results, err := runBenchmark("hpc", []string{"alp"}); err != nil {
		log.Printf("HPC benchmark failed: %v\n", err)
		failure++
	} else {
		printResults(results)
		success++
	}

	// Observation with Float Encoders
	log.Println("\n=== OBSERVATION WITH FLOAT ENCODERS ===")
	if results, err := runBenchmark("observation", []string{"alp"}); err != nil {
		log.Printf("Observation benchmark failed: %v\n", err)
		failure++
	} else {
		printResults(results)
		success++
	}

	// Transaction with Integer Encoders
	log.Println("\n=== TRANSACTION WITH INTEGER ENCODERS ===")
	if results, err := runBenchmark("transaction", []string{"delta", "run", "bp", "dict", "s8"}); err != nil {
		log.Printf("Transaction benchmark failed: %v\n", err)
		failure++
	} else {
		printResults(results)
		success++
	}

	logSummary(success, failure)
}

// runBenchmark runs a single benchmark for a dataset and encoders
func runBenchmark(dataset string, encoders []string) ([]fcbench.BenchmarkResult, error) {
	cfg := createBenchmarkConfig(dataset, encoders)
	log.Printf("Running %s with %v...\n", dataset, encoders)
	return fcbench.RunBenchmarks(cfg)
}

// createBenchmarkConfig creates a config for a given dataset and encoders
func createBenchmarkConfig(dataset string, encoders []string) fcbench.BenchmarkConfig {
	cfg := fcbench.BenchmarkConfig{
		VectorLengths: []int{128, 256, 512, 1024, 2048, 4096, 8192, 16384},
		DatasetSize:   1000000,
		DatasetType:   dataset,
		OutputDir:     "./bench_results",
		Encoders:      encoders,
		Repeat:        3,
	}

	switch dataset {
	case "timeseries":
		cfg.ZipfConfig = fcbench.ZipfConfig{
			S: 1.5, V: 1.0, N: 1000, Count: 1000000,
		}
		cfg.MarkovConfig = fcbench.MarkovConfig{
			MeanA: 10.0, MeanB: 20.0, Stddev: 2.0,
			ProbStayA: 0.9, ProbStayB: 0.8, Count: 1000000,
		}
	case "hpc":
		cfg.HPCConfig = fcbench.Grid3D{
			X: 100, Y: 100, Z: 100,
			Base:  0.0,
			Noise: 0.1,
		}
	case "observation":
		cfg.HDRConfig = fcbench.HDRImageConfig{
			Width:     1000,
			Height:    1000,
			Hotspots:  50,
			BaseLevel: 0.1,
			MaxLevel:  10.0,
		}
	case "transaction":
		cfg.TPCConfig = fcbench.TPCConfig{
			Count:           1000000,
			CustomerZipf:    fcbench.ZipfConfig{S: 1.5, V: 1.0, N: 10000},
			ProductZipf:     fcbench.ZipfConfig{S: 1.2, V: 1.0, N: 50000},
			QuantityZipf:    fcbench.ZipfConfig{S: 1.8, V: 1.0, N: 100},
			AmountNormal:    fcbench.NormalConfig{Mean: 100.0, Stddev: 25.0},
			DiscountUniform: fcbench.UniformConfig{Min: 0.0, Max: 0.3},
			StartDate:       time.Now().AddDate(0, -6, 0),
			EndDate:         time.Now(),
		}
	}

	return cfg
}

// Helper function to print benchmark results
func printResults(results []fcbench.BenchmarkResult) {
	log.Printf("Benchmark completed successfully with %d results", len(results))
	for _, r := range results {
		log.Printf("Encoder: %s, Vector: %d, Ratio: %.2f, Throughput: %.2f val/s",
			r.EncoderName, r.VectorLength, r.CompressionRatio, r.Throughput)
	}
}

// Helper function to check if a slice contains a string
func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

// Helper function to log summary of results
func logSummary(success, failure int) {
	log.Printf("\n=== BENCHMARK SUMMARY ===\n")
	log.Printf("Successful runs: %d\n", success)
	log.Printf("Failed runs: %d\n", failure)
	log.Printf("Total runs: %d\n", success+failure)
	if failure > 0 {
		log.Println("Some benchmarks failed; check logs for details.")
	} else {
		log.Println("All benchmarks completed successfully!")
	}
}
