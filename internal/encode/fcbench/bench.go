// Copyright (c) 2025 Blockwatch Data Inc.
// Author: oliver@blockwatch.cc
//
// Package fcbench provides a benchmarking suite for KnoxDB, inspired by FCBench.
// It generates synthetic datasets and tests encoding performance across various
// vector lengths and encoder types, exporting results to CSV for analysis.

package fcbench

import (
	"fmt"
	"math/rand"
	"path/filepath"
	"reflect"
	"strconv"
	"time"

	"blockwatch.cc/knoxdb/internal/encode"
	"blockwatch.cc/knoxdb/internal/encode/alp"
)

// Encoder defines the interface for KnoxDB encoders.
type Encoder struct {
	ienc encode.IntegerContainer[int64]
	info string
}

func (e *Encoder) EncodeInt(v []int64) (int, error) {
	if e.ienc == nil {
		return 0, fmt.Errorf("encoder: EncodeInt: integer encoder not supported")
	}
	ctx := encode.AnalyzeInt(v, e.ienc.Type() == encode.TIntegerDictionary)
	e.ienc.Encode(ctx, v, encode.MAX_CASCADE)
	e.info = e.ienc.Info()
	sz := e.ienc.Size()
	ctx.Close()
	return sz, nil
}

func (e *Encoder) EncodeFloat(v []float64) (int, error) {
	if len(v) == 0 {
		return 0, nil
	}
	// always create a new ALP encoder
	ctx := encode.AnalyzeFloat(v, false, true)

	// ensure internal ALP encoder exists (noyt initialized for constant data min=max)
	if ctx.AlpEncoder == nil {
		ctx.AlpEncoder = alp.NewEncoder[float64]().Analyze(v)
	}

	// select encoder based on detected scheme
	var enc encode.FloatContainer[float64]
	switch ctx.AlpEncoder.State().Scheme {
	case alp.AlpScheme:
		enc = encode.NewFloat[float64](encode.TFloatAlp)
	case alp.AlpRdScheme:
		enc = encode.NewFloat[float64](encode.TFloatAlpRd)
	}
	enc.Encode(ctx, v, encode.MAX_CASCADE)
	sz := enc.Size()
	e.info = enc.Info()
	enc.Close()
	ctx.Close()
	return sz, nil
}

func (e *Encoder) Info() string {
	return e.info
}

// getEncoder retrieves an encoder instance by name.
// Supported encoders are:
//   - "delta": Delta encoding, efficient for slowly changing values
//   - "run": run end encoding, efficient for repeated values
//   - "bp": bit packed
//   - "dict": dictionary
//   - "s8": simple 8
//   - "alp": ALP floating point
//   - "alprd": ALP-RD floating point
//
// Returns an error if the requested encoder is not supported.
func getEncoder(name string) (*Encoder, error) {
	switch name {
	case "delta":
		return &Encoder{ienc: encode.NewInt[int64](encode.TIntegerDelta)}, nil
	case "run":
		return &Encoder{ienc: encode.NewInt[int64](encode.TIntegerRunEnd)}, nil
	case "bp":
		return &Encoder{ienc: encode.NewInt[int64](encode.TIntegerBitpacked)}, nil
	case "dict":
		return &Encoder{ienc: encode.NewInt[int64](encode.TIntegerDictionary)}, nil
	case "s8":
		return &Encoder{ienc: encode.NewInt[int64](encode.TIntegerSimple8)}, nil
	case "alp":
		return &Encoder{}, nil
	default:
		return nil, fmt.Errorf("getEncoder: unknown encoder: %s", name)
	}
}

// =====================================================================
// BENCHMARKING UTILITIES
// =====================================================================

// BenchmarkConfig defines the parameters for a benchmarking run.
// It controls all aspects of the benchmark including which encoders to test,
// what data to generate, and how to output results.
type BenchmarkConfig struct {
	VectorLengths []int
	DatasetSize   int
	DatasetType   string
	OutputDir     string
	Encoders      []string
	Repeat        int
	Seed          int64          // Seed for reproducible random generation
	HPCConfig     Grid3D         // For "hpc"
	ZipfConfig    ZipfConfig     // For "timeseries" and "transaction"
	MarkovConfig  MarkovConfig   // For "timeseries"
	HDRConfig     HDRImageConfig // For "observation"
	TPCConfig     TPCConfig      // For "transaction"
}

// BenchmarkResult captures metrics for a single benchmark run.
type BenchmarkResult struct {
	DatasetType      string
	EncoderName      string
	VectorCount      int
	VectorLength     int
	EncoderConfig    string
	OriginalSize     int64
	CompressedSize   int64
	CompressionRatio float64
	EncodeTimeNs     int64
	Throughput       float64
	Timestamp        string
}

// RunBenchmarks executes the benchmarking suite according to the provided configuration.
// It generates synthetic data based on the specified dataset type, runs each
// configured encoder against the data with various vector lengths, measures
// performance metrics, and exports the results to a CSV file.
// Returns the benchmark results and any errors encountered during execution.
func RunBenchmarks(cfg BenchmarkConfig) ([]BenchmarkResult, error) {
	if len(cfg.VectorLengths) == 0 {
		return nil, fmt.Errorf("RunBenchmarks: BenchmarkConfig.VectorLengths must not be empty")
	}
	for _, vl := range cfg.VectorLengths {
		if vl <= 0 {
			return nil, fmt.Errorf("RunBenchmarks: BenchmarkConfig.VectorLengths must contain positive values")
		}
	}
	if cfg.DatasetSize <= 0 {
		return nil, fmt.Errorf("RunBenchmarks: BenchmarkConfig.DatasetSize must be positive")
	}
	if cfg.Repeat <= 0 {
		return nil, fmt.Errorf("RunBenchmarks: BenchmarkConfig.Repeat must be positive")
	}
	if len(cfg.Encoders) == 0 {
		return nil, fmt.Errorf("RunBenchmarks: BenchmarkConfig.Encoders must not be empty")
	}
	if cfg.OutputDir == "" {
		return nil, fmt.Errorf("RunBenchmarks: BenchmarkConfig.OutputDir must not be empty")
	}
	if cfg.Seed != 0 {
		rand.Seed(cfg.Seed)
	} else {
		rand.Seed(time.Now().UnixNano())
	}
	var data interface{}
	switch cfg.DatasetType {
	case "hpc":
		data = GenerateGrid3D(cfg.HPCConfig)
	case "timeseries":
		data = GenerateTimeSeries(cfg.DatasetSize, cfg.ZipfConfig, cfg.MarkovConfig)
	case "observation":
		data = GenerateHDRImage(cfg.HDRConfig)
	case "transaction":
		data = GenerateTPCDataset(cfg.TPCConfig)
	default:
		return nil, fmt.Errorf("RunBenchmarks: unsupported dataset type: %s", cfg.DatasetType)
	}
	results := []BenchmarkResult{}
	for _, encName := range cfg.Encoders {
		enc, err := getEncoder(encName)
		if err != nil {
			return nil, fmt.Errorf("RunBenchmarks: failed to get encoder %s: %w", encName, err)
		}
		for _, vecLen := range cfg.VectorLengths {
			for rep := 0; rep < cfg.Repeat; rep++ {
				var intData []int64
				var floatData []float64
				var sliceErr error
				switch d := data.(type) {
				case []float64:
					floatData, sliceErr = sliceFloatData(d, vecLen, cfg.DatasetSize)
				case []TimeSeriesRow:
					floatData, sliceErr = sliceTimeSeriesData(d, vecLen, cfg.DatasetSize)
				case [][]float64:
					floatData, sliceErr = sliceHDRData(d, vecLen, cfg.DatasetSize)
				case []TransactionRow:
					intData, sliceErr = sliceTPCData(d, vecLen, cfg.DatasetSize)
				default:
					return nil, fmt.Errorf("RunBenchmarks: unsupported data type: %v", reflect.TypeOf(data))
				}
				if sliceErr != nil {
					return nil, fmt.Errorf("RunBenchmarks: failed to slice data: %w", sliceErr)
				}
				var result BenchmarkResult
				if len(intData) > 0 {
					result, err = benchmarkIntEncoder(enc, encName, intData, vecLen, cfg.DatasetType)
				} else {
					result, err = benchmarkFloatEncoder(enc, encName, floatData, vecLen, cfg.DatasetType)
				}
				if err != nil {
					return nil, fmt.Errorf("RunBenchmarks: failed to benchmark encoder %s: %w", encName, err)
				}
				results = append(results, result)
			}
		}
	}
	outputFile := filepath.Join(cfg.OutputDir, fmt.Sprintf("fcbench_%s_%d.csv", cfg.DatasetType, time.Now().Unix()))
	if err := exportCSV(outputFile, []string{
		"timestamp",
		"dataset_type",
		"encoder_name",
		"vector_length",
		"encoder_config",
		"original_size_bytes",
		"compressed_size_bytes",
		"compression_ratio",
		"encode_time_ns",
		"throughput_values_per_sec",
		"vector_count",
	}, resultsToCSVRecords(results)); err != nil {
		return nil, fmt.Errorf("RunBenchmarks: failed to export results: %w", err)
	}
	return results, nil
}

// resultsToCSVRecords converts benchmark results to CSV records.
func resultsToCSVRecords(results []BenchmarkResult) [][]string {
	records := make([][]string, len(results))
	for i, r := range results {
		records[i] = []string{
			r.Timestamp,
			r.DatasetType,
			r.EncoderName,
			strconv.Itoa(r.VectorLength),
			r.EncoderConfig,
			strconv.FormatInt(r.OriginalSize, 10),
			strconv.FormatInt(r.CompressedSize, 10),
			strconv.FormatFloat(r.CompressionRatio, 'f', 6, 64),
			strconv.FormatInt(r.EncodeTimeNs, 10),
			strconv.FormatFloat(r.Throughput, 'f', 6, 64),
			strconv.Itoa(r.VectorCount),
		}
	}
	return records
}

// sliceFloatData slices a float64 dataset into vectors of specified length.
// It randomly selects starting positions to ensure diverse data patterns.
func sliceFloatData(data []float64, vecLen, maxSize int) ([]float64, error) {
	if vecLen <= 0 {
		return nil, fmt.Errorf("sliceFloatData: vecLen must be positive")
	}
	if maxSize <= 0 {
		return nil, fmt.Errorf("sliceFloatData: maxSize must be positive")
	}
	if len(data) < vecLen {
		return nil, fmt.Errorf("sliceFloatData: data size %d smaller than vector length %d", len(data), vecLen)
	}
	count := (maxSize + vecLen - 1) / vecLen // ceil()
	result := make([]float64, count*vecLen)
	for i := 0; i < count; i++ {
		start := rand.Intn(len(data) - vecLen + 1)
		copy(result[i*vecLen:(i+1)*vecLen], data[start:start+vecLen])
	}
	return result[:maxSize], nil
}

// sliceTimeSeriesData extracts readings from time series data into float64 vectors.
// It focuses on the Reading field of TimeSeriesRow for encoding benchmarks.
func sliceTimeSeriesData(data []TimeSeriesRow, vecLen, maxSize int) ([]float64, error) {
	if vecLen <= 0 {
		return nil, fmt.Errorf("sliceTimeSeriesData: vecLen must be positive")
	}
	if maxSize <= 0 {
		return nil, fmt.Errorf("sliceTimeSeriesData: maxSize must be positive")
	}
	if len(data) < vecLen {
		return nil, fmt.Errorf("sliceTimeSeriesData: data size %d smaller than vector length %d", len(data), vecLen)
	}
	count := (maxSize + vecLen - 1) / vecLen // ceil()
	result := make([]float64, count*vecLen)
	for i := 0; i < count; i++ {
		start := rand.Intn(len(data) - vecLen + 1)
		for j := 0; j < vecLen; j++ {
			result[i*vecLen+j] = data[start+j].Reading
		}
	}
	return result[:maxSize], nil
}

// sliceHDRData flattens and slices HDR image data (2D) into 1D float64 vectors.
// This simulates processing image data in encoding applications.
func sliceHDRData(data [][]float64, vecLen, maxSize int) ([]float64, error) {
	if vecLen <= 0 {
		return nil, fmt.Errorf("sliceHDRData: vecLen must be positive")
	}
	if maxSize <= 0 {
		return nil, fmt.Errorf("sliceHDRData: maxSize must be positive")
	}
	flat := make([]float64, 0, len(data)*len(data[0]))
	for _, row := range data {
		flat = append(flat, row...)
	}
	return sliceFloatData(flat, vecLen, maxSize)
}

// sliceTPCData extracts quantities from transaction data into int64 vectors.
// It focuses on the Quantity field of TransactionRow for integer encoding benchmarks.
func sliceTPCData(data []TransactionRow, vecLen, maxSize int) ([]int64, error) {
	if vecLen <= 0 {
		return nil, fmt.Errorf("sliceTPCData: vecLen must be positive")
	}
	if maxSize <= 0 {
		return nil, fmt.Errorf("sliceTPCData: maxSize must be positive")
	}
	if len(data) < vecLen {
		return nil, fmt.Errorf("sliceTPCData: data size %d smaller than vector length %d", len(data), vecLen)
	}

	count := (maxSize + vecLen - 1) / vecLen // ceil()
	result := make([]int64, count*vecLen)    // ← **int64**

	for i := 0; i < count; i++ {
		start := rand.Intn(len(data) - vecLen + 1)
		for j := 0; j < vecLen; j++ {
			result[i*vecLen+j] = data[start+j].Quantity // stays int64
		}
	}
	return result[:maxSize], nil // returns []int64
}

// benchmarkIntEncoder runs a benchmark on an int64 vector using the specified encoder.
// It measures encoding time, compression ratio, and throughput in values per second.
func benchmarkIntEncoder(enc *Encoder, encName string, data []int64, vecLen int, datasetType string) (BenchmarkResult, error) {
	result := BenchmarkResult{
		DatasetType:  datasetType,
		EncoderName:  encName,
		VectorLength: vecLen,
		Timestamp:    time.Now().Format("2006-01-02 15:04:05"),
	}
	start := time.Now()
	for i := 0; i < len(data); i += vecLen {
		chunk := data[i:min(i+vecLen, len(data))]
		sz, err := enc.EncodeInt(chunk)
		if err != nil {
			return result, fmt.Errorf("benchmarkIntEncoder: encode error: %w", err)
		}
		result.CompressedSize += int64(sz)
		result.EncoderConfig = enc.Info()
		result.VectorCount++
	}
	encodeDuration := time.Since(start)
	result.EncodeTimeNs = encodeDuration.Nanoseconds()
	result.OriginalSize = int64(len(data) * 8)
	if result.CompressedSize > 0 {
		result.CompressionRatio = float64(result.OriginalSize) / float64(result.CompressedSize)
	}
	if result.EncodeTimeNs > 0 {
		result.Throughput = float64(len(data)) / (float64(result.EncodeTimeNs) / 1e9)
	}
	return result, nil
}

// benchmarkFloatEncoder runs a benchmark on a float64 vector using the specified encoder.
// It measures encoding time, compression ratio, and throughput in values per second.
func benchmarkFloatEncoder(enc *Encoder, encName string, data []float64, vecLen int, datasetType string) (BenchmarkResult, error) {
	result := BenchmarkResult{
		DatasetType:  datasetType,
		EncoderName:  encName,
		VectorLength: vecLen,
		Timestamp:    time.Now().Format("2006-01-02 15:04:05"),
	}
	start := time.Now()
	for i := 0; i < len(data); i += vecLen {
		chunk := data[i:min(i+vecLen, len(data))]
		sz, err := enc.EncodeFloat(chunk)
		if err != nil {
			return result, fmt.Errorf("benchmarkFloatEncoder: encode error: %w", err)
		}
		result.CompressedSize += int64(sz)
		result.EncoderConfig = enc.Info()
		result.VectorCount++
	}
	encodeDuration := time.Since(start)
	result.EncodeTimeNs = encodeDuration.Nanoseconds()
	result.OriginalSize = int64(len(data) * 8)
	if result.CompressedSize > 0 {
		result.CompressionRatio = float64(result.OriginalSize) / float64(result.CompressedSize)
	}
	if result.EncodeTimeNs > 0 {
		result.Throughput = float64(len(data)) / (float64(result.EncodeTimeNs) / 1e9)
	}
	return result, nil
}
