# KnoxDB FCBench

A benchmarking suite for KnoxDB's vector encoding algorithms, inspired by [FCBench](https://github.com/hpdps-group/FCBench).

## Overview

KnoxDB FCBench is a synthetic benchmarking tool designed to evaluate the performance of KnoxDB's encoding algorithms on diverse datasets, including time series, scientific computing grids, observational images, and transactional records. It measures compression ratios, encoding speed, and throughput across configurable vector lengths (e.g., 128, 256, 512, 1024) and data distributions, outputting results to CSV files for further analysis.

The tool supports:
- **Recommended combinations**: Float encoders (`alp`, `alprd`) for float datasets (`timeseries`, `hpc`, `observation`) and integer encoders (`delta`, `run`, `bp`, `dict`, `s8`) for the transaction dataset.
- **Comprehensive testing**: All encoder-dataset pairs (28 combinations) to diagnose edge cases and error handling in KnoxDB.
- **Flexible configuration**: Command-line flags to select specific datasets, encoders, or modes.

Results are stored in `./bench_results` as CSV files, including encoder configurations from the `Info()` method, enabling detailed performance analysis.

## Inspiration from FCBench

KnoxDB FCBench draws inspiration from FCBench, sharing:
- Evaluation of encoding algorithms for float64 and int64 data.
- Synthetic datasets with statistical properties (e.g., Zipf, Markov).
- Testing across multiple vector lengths.
- Metrics for compression ratio, encoding time, and throughput.
- CSV output for analysis.

Unlike FCBench's broad compressor focus, KnoxDB FCBench targets KnoxDB's specialized encoders, with enhanced flexibility for debugging core implementation issues.

## Dataset and Encoder Compatibility

| Dataset Type   | Data Field Type | Compatible Encoders                | Required Config               | Notes                                      |
|----------------|-----------------|------------------------------------|-------------------------------|--------------------------------------------|
| `timeseries`   | `float64` (Reading) | `alp`, `alprd`                 | `ZipfConfig`, `MarkovConfig`   | IoT/sensor data with Markov patterns       |
| `hpc`          | `float64` (3D grid) | `alp`, `alprd`                 | `Grid3D`                       | 3D grid for scientific computing           |
| `observation`  | `float64` (intensities) | `alp`, `alprd`             | `HDRImageConfig`               | HDR-like image intensities                 |
| `transaction`  | `int64` (Quantity) | `delta`, `run`, `bp`, `dict`, `s8` | `TPCConfig`                | TPC-like transaction quantities            |

**Note**: Incompatible combinations (e.g., `timeseries` × `delta`) are expected to fail with errors like `"integer encoder not supported"`, aiding in debugging KnoxDB's error handling.

## Supported Encoders

### Integer Encoders
- `delta`: Delta encoding, for slowly changing values.
- `run`: Run-length encoding, for repeated values.
- `bp`: Bit-packed encoding, for small integers.
- `dict`: Dictionary encoding, for low-cardinality values.
- `s8`: Simple-8 encoding, for varied integer distributions.

### Floating-Point Encoders
- `alp`: ALP floating-point encoder, standard compression.
- `alprd`: ALP-RD floating-point encoder, optimized for data series.

**Note**: `alprd` has shown reliable performance in benchmarks, but `alp` is fully supported. Ensure proper dataset initialization for float encoders.

## Supported Dataset Types

- `hpc`: 3D grids with spatial continuity for scientific computing.
- `timeseries`: IoT/sensor data with Zipf-distributed device IDs and Markov-switching readings.
- `observation`: HDR-like images with hotspot intensities.
- `transaction`: TPC-like transactions with Zipf-distributed quantities and customers.

## Usage

### Command-Line

Run benchmarks from the project directory:

```bash
cd internal/encode/fcbench
go run cmd/main.go
```

Flags:
- `--all`: Test all 28 encoder-dataset combinations, including incompatible pairs for diagnostic purposes.
- `--dataset=timeseries,hpc`: Run specific datasets (comma-separated: timeseries, hpc, observation, transaction).
- `--encoder=alp,delta`: Run specific encoders (comma-separated: alp, alprd, delta, run, bp, dict, s8).
- `--recommended`: Run recommended combinations only (default behavior).

Output:
- CSVs: Stored in `./bench_results` (e.g., `fcbench_timeseries_1234567890.csv`).
- Logs: Detail successes and failures, with errors for incompatible pairs (e.g., "float encoder not supported").

Examples:

Recommended benchmarks:
```bash
go run cmd/main.go
```

All combinations:
```bash
go run cmd/main.go --all
```

Specific dataset and encoder:
```bash
go run cmd/main.go --dataset=observation --encoder=alprd
```

### Configuration

Configure benchmarks via `BenchmarkConfig` in `main.go`:

```go
cfg := fcbench.BenchmarkConfig{
    VectorLengths: []int{128, 256, 512, 1024}, // Vector lengths
    DatasetSize:   1000000,                    // Data points
    DatasetType:   "timeseries",               // Dataset
    OutputDir:     "./bench_results",          // CSV output
    Encoders:      []string{"alp", "alprd"},   // Encoders
    Repeat:        3,                          // Repetitions
    ZipfConfig:    fcbench.ZipfConfig{S: 1.5, V: 1.0, N: 1000, Count: 1000000},
    MarkovConfig:  fcbench.MarkovConfig{MeanA: 10.0, MeanB: 20.0, Stddev: 2.0, ProbStayA: 0.9, ProbStayB: 0.8, Count: 1000000},
}
```

See `main.go` for full dataset configurations (`Grid3D`, `HDRImageConfig`, `TPCConfig`).

### Running Programmatically

```go
package main

import (
    "log"
    "os"

    "blockwatch.cc/knoxdb/internal/encode/fcbench"
)

func main() {
    if err := os.MkdirAll("./bench_results", 0755); err != nil {
        log.Fatalf("Failed to create output directory: %v", err)
    }

    cfg := fcbench.BenchmarkConfig{
        VectorLengths: []int{128, 256, 512, 1024},
        DatasetSize:   1000000,
        DatasetType:   "timeseries",
        OutputDir:     "./bench_results",
        Encoders:      []string{"alprd"},
        Repeat:        3,
        ZipfConfig:    fcbench.ZipfConfig{S: 1.5, V: 1.0, N: 1000, Count: 1000000},
        MarkovConfig:  fcbench.MarkovConfig{MeanA: 10.0, MeanB: 20.0, Stddev: 2.0, ProbStayA: 0.9, ProbStayB: 0.8, Count: 1000000},
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
```

## Output

CSV files in `./bench_results` include:
- timestamp
- dataset_type
- encoder_name
- vector_length
- encoder_config (from Info())
- original_size_bytes
- compressed_size_bytes
- compression_ratio
- encode_time_ns
- throughput_values_per_sec

## Data Generation

Distributions:
- `GenerateZipf`: Zipf-distributed integers using math/rand/v2 (power-law).
- `GenerateNormal`, `GenerateMarkov`: Normal and Markov distributions using gonum.org/v1/gonum/stat/distuv.
- `GenerateUniform`: Uniform distribution using standard Go rand.

Datasets:
- `GenerateGrid3D`: 3D grid for hpc with spatial continuity.
- `GenerateTimeSeries`: Sensor readings with Zipf device IDs and Markov patterns.
- `GenerateHDRImage`: HDR-like intensities with hotspots for observation.
- `GenerateTPCDataset`: Transaction records with Zipf quantities for transaction.