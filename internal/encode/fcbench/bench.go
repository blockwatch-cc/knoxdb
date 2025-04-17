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

// =====================================================================
// ENCODER WRAPPER
// =====================================================================

type Encoder struct {
	ienc encode.IntegerContainer[int64]
	info string
}

func (e *Encoder) EncodeInt(v []int64) (int, error) {
	if e.ienc == nil {
		return 0, fmt.Errorf("encoder: EncodeInt: integer encoder not configured")
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
	ctx := encode.AnalyzeFloat(v, false, true)
	if ctx.AlpEncoder == nil {
		ctx.AlpEncoder = alp.NewEncoder[float64]().Analyze(v)
	}

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

func (e *Encoder) Info() string { return e.info }

// =====================================================================
// ENCODER FACTORY
// =====================================================================

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
		return nil, fmt.Errorf("getEncoder: unknown encoder %q", name)
	}
}

// =====================================================================
// BENCHMARKING CONFIG / RESULT
// =====================================================================

type BenchmarkConfig struct {
	VectorLengths []int
	DatasetSize   int
	DatasetType   string
	OutputDir     string
	Encoders      []string
	Repeat        int
	Seed          int64 // Seed for reproducible random generation

	// dataset‑specific generator knobs
	HPCConfig    Grid3D         // For "hpc"
	ZipfConfig   ZipfConfig     // For "timeseries" and "transaction"
	MarkovConfig MarkovConfig   // For "timeseries"
	HDRConfig    HDRImageConfig // For "observation"
	TPCConfig    TPCConfig      // For "transaction"
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

// =====================================================================
// RESERVOIR SAMPLING (uniform k‑out‑of‑n)
// =====================================================================

func reservoir[T any](src []T, k int, rng *rand.Rand) []T {
	if k >= len(src) {
		out := make([]T, len(src))
		copy(out, src)
		return out
	}
	out := make([]T, k)
	copy(out, src[:k])
	for i := k; i < len(src); i++ {
		if j := rng.Intn(i + 1); j < k {
			out[j] = src[i]
		}
	}
	return out
}

func packFloatVectors(src []float64, vecLen int) []float64 {
	need := ((len(src) + vecLen - 1) / vecLen) * vecLen
	out := make([]float64, need)
	copy(out, src)
	return out[:len(src)]
}

func packIntVectors(src []int64, vecLen int) []int64 {
	need := ((len(src) + vecLen - 1) / vecLen) * vecLen
	out := make([]int64, need)
	copy(out, src)
	return out[:len(src)]
}

// =====================================================================
// PUBLIC DRIVER
// =====================================================================

// RunBenchmarks executes the benchmarking suite according to the provided configuration.
// It generates synthetic data based on the specified dataset type, runs each
// configured encoder against the data with various vector lengths, measures
// performance metrics, and exports the results to a CSV file.
// Returns the benchmark results and any errors encountered during execution.
func RunBenchmarks(cfg BenchmarkConfig) ([]BenchmarkResult, error) {
	if len(cfg.VectorLengths) == 0 {
		return nil, fmt.Errorf("VectorLengths empty")
	}
	if cfg.DatasetSize <= 0 || cfg.Repeat <= 0 {
		return nil, fmt.Errorf("DatasetSize and Repeat must be >0")
	}
	if len(cfg.Encoders) == 0 {
		return nil, fmt.Errorf("no encoder selected")
	}
	if cfg.OutputDir == "" {
		return nil, fmt.Errorf("OutputDir empty")
	}

	// deterministic RNG when cfg.Seed != 0
	rng := rand.New(rand.NewSource(
		func() int64 {
			if cfg.Seed != 0 {
				return cfg.Seed
			}
			return time.Now().UnixNano()
		}()))

	// --- generate synthetic data ------------------------------------------------
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
		return nil, fmt.Errorf("unsupported dataset %q", cfg.DatasetType)
	}

	// --- run benchmarks ---------------------------------------------------------
	var results []BenchmarkResult
	for _, encName := range cfg.Encoders {
		encProto, err := getEncoder(encName)
		if err != nil {
			return nil, err
		}
		for _, vlen := range cfg.VectorLengths {
			for rep := 0; rep < cfg.Repeat; rep++ {
				enc := *encProto // fresh state

				var (
					ints   []int64
					floats []float64
					sErr   error
				)
				switch col := data.(type) {
				case []float64:
					floats, sErr = sliceFloatData(col, vlen, cfg.DatasetSize, rng)
				case []TimeSeriesRow:
					floats, sErr = sliceTimeSeriesData(col, vlen, cfg.DatasetSize, rng)
				case [][]float64:
					floats, sErr = sliceHDRData(col, vlen, cfg.DatasetSize, rng)
				case []TransactionRow:
					ints, sErr = sliceTPCData(col, vlen, cfg.DatasetSize, rng)
				default:
					return nil, fmt.Errorf("unsupported data kind %v", reflect.TypeOf(data))
				}
				if sErr != nil {
					return nil, sErr
				}

				var res BenchmarkResult
				if len(ints) > 0 {
					res, err = benchmarkIntEncoder(&enc, encName, ints, vlen, cfg.DatasetType)
				} else {
					res, err = benchmarkFloatEncoder(&enc, encName, floats, vlen, cfg.DatasetType)
				}
				if err != nil {
					return nil, err
				}
				results = append(results, res)
			}
		}
	}

	// --- CSV export -------------------------------------------------------------
	out := filepath.Join(cfg.OutputDir,
		fmt.Sprintf("fcbench_%s_%d.csv", cfg.DatasetType, time.Now().Unix()))
	headers := []string{
		"timestamp", "dataset_type", "encoder_name", "vector_length",
		"encoder_config", "original_size_bytes", "compressed_size_bytes",
		"compression_ratio", "encode_time_ns", "throughput_values_per_sec",
		"vector_count",
	}
	if err := exportCSV(out, headers, resultsToCSVRecords(results)); err != nil {
		return nil, err
	}
	return results, nil
}

// =====================================================================
// CSV HELPER
// =====================================================================

func resultsToCSVRecords(res []BenchmarkResult) [][]string {
	rows := make([][]string, len(res))
	for i, r := range res {
		rows[i] = []string{
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
	return rows
}

// =====================================================================
// SAMPLING HELPERS
// =====================================================================

func sliceFloatData(col []float64, vecLen, maxSize int, rng *rand.Rand) ([]float64, error) {
	if vecLen <= 0 || maxSize <= 0 || len(col) < vecLen {
		return nil, fmt.Errorf("bad params")
	}
	sample := reservoir(col, maxSize, rng)
	return packFloatVectors(sample, vecLen), nil
}

func sliceTimeSeriesData(col []TimeSeriesRow, vecLen, maxSize int, rng *rand.Rand) ([]float64, error) {
	if vecLen <= 0 || maxSize <= 0 || len(col) < vecLen {
		return nil, fmt.Errorf("bad params")
	}
	tmp := make([]float64, len(col))
	for i, r := range col {
		tmp[i] = r.Reading
	}
	sample := reservoir(tmp, maxSize, rng)
	return packFloatVectors(sample, vecLen), nil
}

func sliceHDRData(img [][]float64, vecLen, maxSize int, rng *rand.Rand) ([]float64, error) {
	flat := make([]float64, 0, len(img)*len(img[0]))
	for _, row := range img {
		flat = append(flat, row...)
	}
	if vecLen <= 0 || maxSize <= 0 || len(flat) < vecLen {
		return nil, fmt.Errorf("bad params")
	}
	sample := reservoir(flat, maxSize, rng)
	return packFloatVectors(sample, vecLen), nil
}

func sliceTPCData(col []TransactionRow, vecLen, maxSize int, rng *rand.Rand) ([]int64, error) {
	if vecLen <= 0 || maxSize <= 0 || len(col) < vecLen {
		return nil, fmt.Errorf("bad params")
	}
	tmp := make([]int64, len(col))
	for i, r := range col {
		tmp[i] = r.Quantity
	}
	sample := reservoir(tmp, maxSize, rng)
	return packIntVectors(sample, vecLen), nil
}

// =====================================================================
// BENCHMARK CORE
// =====================================================================

func benchmarkIntEncoder(enc *Encoder, name string, data []int64, vecLen int,
	dtype string) (BenchmarkResult, error) {

	br := BenchmarkResult{DatasetType: dtype, EncoderName: name,
		VectorLength: vecLen, Timestamp: time.Now().Format("2006-01-02 15:04:05")}
	start := time.Now()
	for i := 0; i < len(data); i += vecLen {
		chunk := data[i:min(i+vecLen, len(data))]
		sz, err := enc.EncodeInt(chunk)
		if err != nil {
			return br, err
		}
		br.CompressedSize += int64(sz)
		br.VectorCount++
		br.EncoderConfig = enc.Info()
	}
	elapsed := time.Since(start)
	br.EncodeTimeNs = elapsed.Nanoseconds()
	br.OriginalSize = int64(len(data) * 8)
	if br.CompressedSize > 0 {
		br.CompressionRatio = float64(br.OriginalSize) / float64(br.CompressedSize)
	}
	if br.EncodeTimeNs > 0 {
		br.Throughput = float64(len(data)) / (float64(br.EncodeTimeNs) / 1e9)
	}
	return br, nil
}

func benchmarkFloatEncoder(enc *Encoder, name string, data []float64, vecLen int,
	dtype string) (BenchmarkResult, error) {

	br := BenchmarkResult{DatasetType: dtype, EncoderName: name,
		VectorLength: vecLen, Timestamp: time.Now().Format("2006-01-02 15:04:05")}
	start := time.Now()
	for i := 0; i < len(data); i += vecLen {
		chunk := data[i:min(i+vecLen, len(data))]
		sz, err := enc.EncodeFloat(chunk)
		if err != nil {
			return br, err
		}
		br.CompressedSize += int64(sz)
		br.VectorCount++
		br.EncoderConfig = enc.Info()
	}
	elapsed := time.Since(start)
	br.EncodeTimeNs = elapsed.Nanoseconds()
	br.OriginalSize = int64(len(data) * 8)
	if br.CompressedSize > 0 {
		br.CompressionRatio = float64(br.OriginalSize) / float64(br.CompressedSize)
	}
	if br.EncodeTimeNs > 0 {
		br.Throughput = float64(len(data)) / (float64(br.EncodeTimeNs) / 1e9)
	}
	return br, nil
}

// min helper
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
