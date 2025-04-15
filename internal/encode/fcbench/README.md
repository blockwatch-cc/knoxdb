# KnoxDB FCBench

A benchmarking suite for KnoxDB's vector encoding algorithms, inspired by FCBench.

## Overview

KnoxDB FCBench is a synthetic benchmarking tool designed to evaluate the performance of various encoding algorithms on different types of time series and floating-point data. It measures compression ratios, encoding speed, and throughput across different vector lengths and data distributions.

The benchmark generates realistic synthetic datasets with configurable statistical properties, simulating common scenarios in scientific computing, IoT/sensor networks, observational data, and transactional databases.

## Inspiration from FCBench

This benchmarking suite draws inspiration from [FCBench](https://github.com/hpdps-group/FCBench), a comprehensive floating-point compressor benchmark developed by the HPDPS Group. Key similarities include:

- Focus on evaluating encoding algorithms for floating-point and integer data
- Generation of synthetic datasets with specific statistical distributions
- Testing across various vector lengths
- Measurement of compression ratios and encoding performance
- Output of results in CSV format for analysis

## Supported Encoders

The following encoders are available for benchmarking:

### Integer Encoders
- `delta`: Delta encoding, efficient for slowly changing values
- `run`: Run-length encoding, efficient for repeated values
- `bp`: Bit-packed encoding
- `dict`: Dictionary encoding
- `s8`: Simple-8 encoding

### Floating-Point Encoders
- `alp`: ALP floating-point encoding
- `alprd`: ALP-RD floating-point encoding

## Supported Dataset Types

The benchmark can generate several types of synthetic datasets:

- `hpc`: Scientific computing data as 3D grids with spatial continuity
- `timeseries`: IoT/sensor time series with Markov-switching behavior
- `observation`: Observation/imaging data with HDR-like characteristics
- `transaction`: TPC-like transactional database records

## Usage

### Configuration

Create a `BenchmarkConfig` structure to configure the benchmark run:

```go
cfg := fcbench.BenchmarkConfig{
    VectorLengths: []int{128, 256, 512, 1024},  // Vector lengths to test
    DatasetSize:   1000000,                     // Number of data points to generate
    DatasetType:   "timeseries",                // Type of dataset to generate
    OutputDir:     "./bench_results",           // Directory for CSV output
    Encoders:      []string{"delta", "run", "alp"}, // Encoders to benchmark
    Repeat:        3,                           // Number of repetitions per test
    
    // Dataset-specific configurations (only required for the selected DatasetType)
    HPCConfig: fcbench.Grid3D{
        X: 100, Y: 100, Z: 100, 
        Base: 0.0, 
        Noise: 0.1,
    },
    
    ZipfConfig: fcbench.ZipfConfig{
        S: 1.5, V: 1.0, N: 1000, Count: 1000000,
    },
    
    MarkovConfig: fcbench.MarkovConfig{
        MeanA: 10.0, MeanB: 20.0, Stddev: 2.0,
        ProbStayA: 0.9, ProbStayB: 0.8, Count: 1000000,
    },
    
    HDRConfig: fcbench.HDRImageConfig{
        Width: 1000, Height: 1000, Hotspots: 10,
        BaseLevel: 0.1, MaxLevel: 10.0,
    },
    
    TPCConfig: fcbench.TPCConfig{
        Count: 1000000,
        CustomerZipf: fcbench.ZipfConfig{S: 1.5, V: 1.0, N: 10000},
        ProductZipf: fcbench.ZipfConfig{S: 1.2, V: 1.0, N: 50000},
        QuantityZipf: fcbench.ZipfConfig{S: 1.8, V: 1.0, N: 100},
        AmountNormal: fcbench.NormalConfig{Mean: 100.0, Stddev: 25.0},
        DiscountUniform: fcbench.UniformConfig{Min: 0.0, Max: 0.3},
        StartDate: time.Now().AddDate(0, -6, 0),
        EndDate: time.Now(),
    },
}
```

### Running the Benchmark

```go
package main

import (
    "log"
    "time"

    "blockwatch.cc/knoxdb/internal/encode/fcbench"
)

func main() {
    cfg := fcbench.BenchmarkConfig{
        VectorLengths: []int{128, 256, 512, 1024},
        DatasetSize:   1000000,
        DatasetType:   "timeseries",
        OutputDir:     "./bench_results",
        Encoders:      []string{"delta", "run", "alp"},
        Repeat:        3,
        
        // Configure timeseries dataset parameters
        ZipfConfig: fcbench.ZipfConfig{
            S: 1.5, V: 1.0, N: 1000, Count: 1000000,
        },
        MarkovConfig: fcbench.MarkovConfig{
            MeanA: 10.0, MeanB: 20.0, Stddev: 2.0,
            ProbStayA: 0.9, ProbStayB: 0.8, Count: 1000000,
        },
    }
    
    results, err := fcbench.RunBenchmarks(cfg)
    if err != nil {
        log.Fatal(err)
    }
    
    // Print summary of results
    for _, r := range results {
        log.Printf("Encoder: %s, Vector: %d, Ratio: %.2f, Throughput: %.2f val/s",
            r.EncoderName, r.VectorLength, r.CompressionRatio, r.Throughput)
    }
}
```

### Running from the Command Line

Make sure you have properly configured the `cmd/main.go` file with your desired benchmark parameters, then run:

```bash
$ mkdir -p bench_results
$ cd internal/encode/fcbench
$ go run cmd/main.go
```

The benchmark will generate output files in the `./bench_results` directory (or your configured output directory) with filenames following the pattern `fcbench_[datasettype]_[timestamp].csv`.


### Output

Results are saved as CSV files in the specified output directory. The CSV includes the following columns:

- `timestamp`: When the benchmark was run
- `dataset_type`: Type of dataset used
- `encoder_name`: Name of the encoder
- `vector_length`: Length of vectors tested
- `encoder_config`: Configuration information about the encoder
- `original_size_bytes`: Size of uncompressed data
- `compressed_size_bytes`: Size after encoding
- `compression_ratio`: Ratio of original to compressed size
- `encode_time_ns`: Time taken for encoding in nanoseconds
- `throughput_values_per_sec`: Values encoded per second

## Data Generation

The benchmark includes several data generators to simulate different data patterns:

### Distribution Generators
- `GenerateZipf`: Creates Zipf-distributed integers (power-law distribution)
- `GenerateNormal`: Creates normally distributed floating-point values
- `GenerateUniform`: Creates uniformly distributed floating-point values
- `GenerateMarkov`: Creates time series with Markov-switching behavior

### Dataset Generators
- `GenerateGrid3D`: Creates 3D grid data for scientific computing
- `GenerateTimeSeries`: Creates synthetic time series with device IDs and readings
- `GenerateHDRImage`: Creates 2D matrix simulating HDR image data
- `GenerateTPCDataset`: Creates synthetic transaction records