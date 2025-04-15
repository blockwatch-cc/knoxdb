// Package fcbench provides a benchmarking suite for KnoxDB, inspired by FCBench.
// It generates synthetic datasets and tests encoding performance across various
// vector lengths and encoder types, exporting results to CSV for analysis.
package fcbench

import (
    "encoding/csv"
    "fmt"
    "math"
    "math/rand"
    "os"
    "path/filepath"
    "reflect"
    "strconv"
    "time"

    "github.com/blockwatch-cc/knoxdb/internal/hash"
    "github.com/blockwatch-cc/knoxdb/v2/encode"
    "gonum.org/v1/gonum/stat/distuv"
)

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
        encInfo := enc.Info()
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
                    result, err = benchmarkIntEncoder(enc, encName, encInfo, intData, vecLen, cfg.DatasetType)
                } else {
                    result, err = benchmarkFloatEncoder(enc, encName, encInfo, floatData, vecLen, cfg.DatasetType)
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
        }
    }
    return records
}

// getEncoder retrieves an encoder instance by name.
// Supported encoders are:
//   - "delta": Delta encoding, efficient for slowly changing values
//   - "gorilla": Gorilla time series encoding, good for floating-point values
//   - "rle": Run-Length encoding, efficient for repeated values
//
// Returns an error if the requested encoder is not supported.
func getEncoder(name string) (encode.Encoder, error) {
    switch name {
    case "delta":
        return encode.NewDelta(), nil
    case "gorilla":
        return encode.NewGorilla(), nil
    case "rle":
        return encode.NewRunLength(), nil
    default:
        return nil, fmt.Errorf("getEncoder: unknown encoder: %s", name)
    }
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
    count := min(maxSize, len(data)) / vecLen
    result := make([]float64, count*vecLen)
    for i := 0; i < count; i++ {
        start := rand.Intn(len(data) - vecLen + 1)
        copy(result[i*vecLen:(i+1)*vecLen], data[start:start+vecLen])
    }
    return result, nil
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
    count := min(maxSize, len(data)) / vecLen
    result := make([]float64, count*vecLen)
    for i := 0; i < count; i++ {
        start := rand.Intn(len(data) - vecLen + 1)
        for j := 0; j < vecLen; j++ {
            result[i*vecLen+j] = data[start+j].Reading
        }
    }
    return result, nil
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
    count := min(maxSize, len(data)) / vecLen
    result := make([]int64, count*vecLen)
    for i := 0; i < count; i++ {
        start := rand.Intn(len(data) - vecLen + 1)
        for j := 0; j < vecLen; j++ {
            result[i*vecLen+j] = data[start+j].Quantity
        }
    }
    return result, nil
}

// benchmarkIntEncoder runs a benchmark on an int64 vector using the specified encoder.
// It measures encoding time, compression ratio, and throughput in values per second.
func benchmarkIntEncoder(enc encode.Encoder, encName, encInfo string, data []int64, vecLen int, datasetType string) (BenchmarkResult, error) {
    result := BenchmarkResult{
        DatasetType:   datasetType,
        EncoderName:   encName,
        VectorLength:  vecLen,
        EncoderConfig: encInfo,
        Timestamp:     time.Now().Format("2006-01-02 15:04:05"),
    }
    start := time.Now()
    var compressed []byte
    var err error
    for i := 0; i < len(data); i += vecLen {
        chunk := data[i:min(i+vecLen, len(data))]
        compressedChunk, encErr := enc.EncodeInt(chunk)
        if encErr != nil {
            return result, fmt.Errorf("benchmarkIntEncoder: encode error: %w", encErr)
        }
        compressed = append(compressed, compressedChunk...)
    }
    encodeDuration := time.Since(start)
    result.EncodeTimeNs = encodeDuration.Nanoseconds()
    result.OriginalSize = int64(len(data) * 8)
    result.CompressedSize = int64(len(compressed))
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
func benchmarkFloatEncoder(enc encode.Encoder, encName, encInfo string, data []float64, vecLen int, datasetType string) (BenchmarkResult, error) {
    result := BenchmarkResult{
        DatasetType:   datasetType,
        EncoderName:   encName,
        VectorLength:  vecLen,
        EncoderConfig: encInfo,
        Timestamp:     time.Now().Format("2006-01-02 15:04:05"),
    }
    start := time.Now()
    var compressed []byte
    var err error
    for i := 0; i < len(data); i += vecLen {
        chunk := data[i:min(i+vecLen, len(data))]
        compressedChunk, encErr := enc.EncodeFloat(chunk)
        if encErr != nil {
            return result, fmt.Errorf("benchmarkFloatEncoder: encode error: %w", encErr)
        }
        compressed = append(compressed, compressedChunk...)
    }
    encodeDuration := time.Since(start)
    result.EncodeTimeNs = encodeDuration.Nanoseconds()
    result.OriginalSize = int64(len(data) * 8)
    result.CompressedSize = int64(len(compressed))
    if result.CompressedSize > 0 {
        result.CompressionRatio = float64(result.OriginalSize) / float64(result.CompressedSize)
    }
    if result.EncodeTimeNs > 0 {
        result.Throughput = float64(len(data)) / (float64(result.EncodeTimeNs) / 1e9)
    }
    return result, nil
}

// min returns the minimum of two integers.
// Used for bounds checking and safely accessing slices.
func min(a, b int) int {
    if a < b {
        return a
    }
    return b
}

// =====================================================================
// BLOOM FILTER BENCHMARKING
// =====================================================================

// BloomFilterStats holds metrics about bloom filter performance.
// It captures both accuracy metrics (like false positive rate) and
// performance metrics (like build and query times).
type BloomFilterStats struct {
    BitsPerElement    int
    FalsePositives    int
    TruePositives     int
    TrueNegatives     int
    FalsePositiveRate float64
    BuildTimeNs       int64
    QueryTimeNs       int64
}

// SimpleBloomFilter implements a basic Bloom filter using xxHash64 algorithm.
// It uses two hash functions with different seeds and a configurable number
// of hash iterations to achieve the desired false positive rate.
const (
    hashSeed1 = 0xCAFE // Seed for the first hash function
    hashSeed2 = 0xBABE // Seed for the second hash function
)

type SimpleBloomFilter struct {
    bits      []byte
    numHashes int
    size      uint64 // Number of bits
}

// NewBloomFilter creates a new Bloom filter with the specified size in bits
// and number of hash functions to use. The filter is initially empty.
// Panics if size is 0 or numHashes is not positive.
func NewBloomFilter(size uint64, numHashes int) *SimpleBloomFilter {
    if size == 0 {
        panic("NewBloomFilter: size must be greater than 0")
    }
    if numHashes <= 0 {
        panic("NewBloomFilter: numHashes must be greater than 0")
    }
    // Calculate number of bytes needed (1 byte = 8 bits)
    byteSize := (size + 7) / 8
    return &SimpleBloomFilter{
        bits:      make([]byte, byteSize),
        numHashes: numHashes,
        size:      size,
    }
}

// Add inserts an element into the Bloom filter.
// It calculates multiple hash values using two base hash functions
// and sets the corresponding bits in the filter.
func (bf *SimpleBloomFilter) Add(data []byte) {
    h1 := hash.Sum64(data, hashSeed1)
    h2 := hash.Sum64(data, hashSeed2)
    for i := 0; i < bf.numHashes; i++ {
        idx := (h1 + uint64(i)*h2) % bf.size
        byteIdx := idx / 8
        bitIdx := idx % 8
        bf.bits[byteIdx] |= 1 << bitIdx
    }
}

// Contains checks if an element might be in the Bloom filter.
// Returns true if the element may be present (with a possibility of false positive),
// and false if it is definitely not present.
func (bf *SimpleBloomFilter) Contains(data []byte) bool {
    h1 := hash.Sum64(data, hashSeed1)
    h2 := hash.Sum64(data, hashSeed2)
    for i := 0; i < bf.numHashes; i++ {
        idx := (h1 + uint64(i)*h2) % bf.size
        byteIdx := idx / 8
        bitIdx := idx % 8
        if (bf.bits[byteIdx] & (1 << bitIdx)) == 0 {
            return false
        }
    }
    return true
}

// MeasureBloomFilter benchmarks Bloom filter performance on string data.
// It splits the input data into training and testing sets, builds a filter with
// the training data, and then measures true positives, false positives, and
// performance metrics when querying the filter with both members and non-members.
// Returns detailed statistics about the filter's performance.
func MeasureBloomFilter(data []string, bitsPerElement int, numHashes int) (*BloomFilterStats, error) {
    if len(data) == 0 {
        return nil, fmt.Errorf("MeasureBloomFilter: data must not be empty")
    }
    if len(data) < 2 {
        return nil, fmt.Errorf("MeasureBloomFilter: data must have at least 2 elements for training and testing")
    }
    if bitsPerElement <= 0 {
        return nil, fmt.Errorf("MeasureBloomFilter: bitsPerElement must be positive")
    }
    if numHashes <= 0 {
        return nil, fmt.Errorf("MeasureBloomFilter: numHashes must be positive")
    }
    stats := &BloomFilterStats{
        BitsPerElement: bitsPerElement,
    }
    splitIdx := len(data) * 2 / 3
    trainData := data[:splitIdx]
    testData := data[splitIdx:]
    // Generate non-members by ensuring uniqueness
    testNonMembers := make([]string, len(testData))
    for i, item := range testData {
        testNonMembers[i] = item + "_nonmember_" + strconv.Itoa(i)
    }
    filterSize := uint64(len(trainData) * bitsPerElement)
    bf := NewBloomFilter(filterSize, numHashes)
    startBuild := time.Now()
    for _, item := range trainData {
        bf.Add([]byte(item))
    }
    buildDuration := time.Since(startBuild)
    stats.BuildTimeNs = buildDuration.Nanoseconds()
    startQuery := time.Now()
    for _, item := range testData {
        if bf.Contains([]byte(item)) {
            stats.TruePositives++
        }
    }
    for _, item := range testNonMembers {
        if bf.Contains([]byte(item)) {
            stats.FalsePositives++
        } else {
            stats.TrueNegatives++
        }
    }
    queryDuration := time.Since(startQuery)
    stats.QueryTimeNs = queryDuration.Nanoseconds()
    if len(testNonMembers) > 0 {
        stats.FalsePositiveRate = float64(stats.FalsePositives) / float64(len(testNonMembers))
    }
    return stats, nil
}
