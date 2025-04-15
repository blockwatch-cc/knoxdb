// Package fcbench provides a benchmarking suite for KnoxDB, inspired by FCBench.
// It generates synthetic datasets and tests encoding performance across various
// vector lengths and encoder types, exporting results to CSV for analysis.
package fcbench

import (
	"math"
	"math/rand/v2"
	"time"

	"gonum.org/v1/gonum/stat/distuv"
)

// =====================================================================
// DISTRIBUTION GENERATORS
// =====================================================================

// ZipfConfig specifies parameters for generating Zipf-distributed integers.
type ZipfConfig struct {
	S     float64 // The "s" parameter (>1) - larger values give more skew
	V     float64 // The "v" parameter - typically 1.0
	N     int     // Maximum possible value + 1
	Count int     // Number of values to generate
}

// GenerateZipf creates an array of Zipf-distributed integers.
func GenerateZipf(cfg ZipfConfig) []uint64 {
	if cfg.S <= 1 {
		panic("GenerateZipf: ZipfConfig.S must be greater than 1")
	}
	if cfg.V <= 0 {
		panic("GenerateZipf: ZipfConfig.V must be greater than 0")
	}
	if cfg.N <= 0 {
		panic("GenerateZipf: ZipfConfig.N must be greater than 0")
	}
	if cfg.Count < 0 {
		panic("GenerateZipf: ZipfConfig.Count must be non-negative")
	}
	r := rand.New(rand.NewPCG(uint64(0), uint64(0)))
	z := rand.NewZipf(r, cfg.S, cfg.V, uint64(cfg.N-1))
	out := make([]uint64, cfg.Count)
	for i := 0; i < cfg.Count; i++ {
		out[i] = z.Uint64()
	}
	return out
}

// NormalConfig specifies parameters for generating normally distributed floating-point values.
type NormalConfig struct {
	Mean   float64 // Center of the distribution
	Stddev float64 // Standard deviation (spread)
	Count  int     // Number of values to generate
}

// GenerateNormal creates an array of normally distributed values.
func GenerateNormal(cfg NormalConfig) []float64 {
	if cfg.Stddev <= 0 {
		panic("GenerateNormal: NormalConfig.Stddev must be greater than 0")
	}
	if cfg.Count < 0 {
		panic("GenerateNormal: NormalConfig.Count must be non-negative")
	}
	n := distuv.Normal{
		Mu:    cfg.Mean,
		Sigma: cfg.Stddev,
	}
	out := make([]float64, cfg.Count)
	for i := 0; i < cfg.Count; i++ {
		out[i] = n.Rand()
	}
	return out
}

// UniformConfig specifies parameters for generating uniformly distributed values.
type UniformConfig struct {
	Min   float64 // Minimum value
	Max   float64 // Maximum value
	Count int     // Number of values to generate
}

// GenerateUniform creates an array of uniformly distributed values.
func GenerateUniform(cfg UniformConfig) []float64 {
	if cfg.Min > cfg.Max {
		panic("GenerateUniform: UniformConfig.Min must be less than or equal to Max")
	}
	if cfg.Count < 0 {
		panic("GenerateUniform: UniformConfig.Count must be non-negative")
	}
	out := make([]float64, cfg.Count)
	span := cfg.Max - cfg.Min
	for i := 0; i < cfg.Count; i++ {
		out[i] = cfg.Min + rand.Float64()*span
	}
	return out
}

// MarkovConfig defines state-transition rules for a 2-state Markov process.
type MarkovConfig struct {
	MeanA     float64 // Mean value when in state A
	MeanB     float64 // Mean value when in state B
	Stddev    float64 // Standard deviation in both states
	ProbStayA float64 // Probability of staying in state A (0.0-1.0)
	ProbStayB float64 // Probability of staying in state B (0.0-1.0)
	Count     int     // Number of values to generate
}

// GenerateMarkov creates a time series with Markov-switching behavior.
func GenerateMarkov(cfg MarkovConfig) []float64 {
	if cfg.Stddev <= 0 {
		panic("GenerateMarkov: MarkovConfig.Stddev must be greater than 0")
	}
	if cfg.ProbStayA < 0 || cfg.ProbStayA > 1 {
		panic("GenerateMarkov: MarkovConfig.ProbStayA must be between 0 and 1")
	}
	if cfg.ProbStayB < 0 || cfg.ProbStayB > 1 {
		panic("GenerateMarkov: MarkovConfig.ProdStayB must be between 0 and 1")
	}
	if cfg.Count < 0 {
		panic("GenerateMarkov: MarkovConfig.Count must be non-negative")
	}
	distA := distuv.Normal{Mu: cfg.MeanA, Sigma: cfg.Stddev}
	distB := distuv.Normal{Mu: cfg.MeanB, Sigma: cfg.Stddev}
	state := 0 // Start in state A
	out := make([]float64, cfg.Count)
	for i := 0; i < cfg.Count; i++ {
		if state == 0 {
			out[i] = distA.Rand()
			if rand.Float64() > cfg.ProbStayA {
				state = 1
			}
		} else {
			out[i] = distB.Rand()
			if rand.Float64() > cfg.ProbStayB {
				state = 0
			}
		}
	}
	return out
}

// =====================================================================
// HPC DATASET GENERATION
// =====================================================================

// Grid3D contains parameters for generating a 3D grid of floating-point values.
type Grid3D struct {
	X, Y, Z int     // Dimensions of the 3D grid
	Base    float64 // Base value
	Noise   float64 // Noise amplitude (0 = no noise)
}

// GenerateGrid3D creates a 3D grid with smooth, spatially-continuous values.
func GenerateGrid3D(cfg Grid3D) []float64 {
	if cfg.X <= 0 || cfg.Y <= 0 || cfg.Z <= 0 {
		panic("GenerateGrid3D: Grid3D dimensions X, Y, Z must be greater than 0")
	}
	data := make([]float64, 0, cfg.X*cfg.Y*cfg.Z)
	for z := 0; z < cfg.Z; z++ {
		for y := 0; y < cfg.Y; y++ {
			for x := 0; x < cfg.X; x++ {
				val := cfg.Base +
					math.Sin(float64(x)/10) +
					math.Cos(float64(y)/10) +
					math.Sin(float64(z)/15)*0.5
				if cfg.Noise > 0 {
					val += (rand.Float64()*2 - 1) * cfg.Noise
				}
				data = append(data, val)
			}
		}
	}
	return data
}

// =====================================================================
// TIME SERIES DATASET GENERATION
// =====================================================================

// TimeSeriesRow represents a single row in a synthetic time-series dataset.
type TimeSeriesRow struct {
	Timestamp int64   // Unix timestamp
	DeviceID  int64   // Device identifier
	Reading   float64 // Sensor reading
}

// GenerateTimeSeries creates a synthetic time series dataset.
func GenerateTimeSeries(count int, deviceZipf ZipfConfig, readingMarkov MarkovConfig) []TimeSeriesRow {
	if count < 0 {
		panic("GenerateTimeSeries: count must be non-negative")
	}
	if deviceZipf.Count != count {
		deviceZipf.Count = count
	}
	if readingMarkov.Count != count {
		readingMarkov.Count = count
	}
	timestamps := make([]int64, count)
	base := time.Now().Unix() - int64(count*10)
	for i := range timestamps {
		timestamps[i] = base + int64(i*10)
	}
	devs := GenerateZipf(deviceZipf)
	reads := GenerateMarkov(readingMarkov)
	out := make([]TimeSeriesRow, count)
	for i := 0; i < count; i++ {
		out[i] = TimeSeriesRow{
			Timestamp: timestamps[i],
			DeviceID:  int64(devs[i]),
			Reading:   reads[i],
		}
	}
	return out
}

// =====================================================================
// OBSERVATION DATASET GENERATION
// =====================================================================

// HDRImageConfig defines parameters for generating HDR-like image data.
type HDRImageConfig struct {
	Width     int
	Height    int
	Hotspots  int
	BaseLevel float64
	MaxLevel  float64
}

// GenerateHDRImage creates a 2D matrix of intensities simulating an HDR image.
func GenerateHDRImage(cfg HDRImageConfig) [][]float64 {
	if cfg.Width <= 0 || cfg.Height <= 0 {
		panic("GenerateHDRImage: HDRImageConfig.Width and Height must be greater than 0")
	}
	if cfg.Hotspots < 0 {
		panic("GenerateHDRImage: HDRImageConfig.Hotspots must be non-negative")
	}
	if cfg.BaseLevel > cfg.MaxLevel {
		panic("GenerateHDRImage: HDRImageConfig.BaseLevel must be less than or equal to MaxLevel")
	}
	img := make([][]float64, cfg.Height)
	for y := range img {
		img[y] = make([]float64, cfg.Width)
		for x := range img[y] {
			img[y][x] = cfg.BaseLevel
		}
	}
	for i := 0; i < cfg.Hotspots; i++ {
		centerX := rand.IntN(cfg.Width)
		centerY := rand.IntN(cfg.Height)
		intensity := cfg.BaseLevel + rand.Float64()*(cfg.MaxLevel-cfg.BaseLevel)
		sigma := 5.0 + rand.Float64()*20.0
		for y := 0; y < cfg.Height; y++ {
			for x := 0; x < cfg.Width; x++ {
				dist2 := math.Pow(float64(x-centerX), 2) + math.Pow(float64(y-centerY), 2)
				gaussianFactor := math.Exp(-dist2 / (2 * sigma * sigma))
				img[y][x] += intensity * gaussianFactor
			}
		}
	}
	return img
}

// =====================================================================
// DATABASE/TPC DATASET GENERATION
// =====================================================================

// TransactionRow represents a synthetic transaction record.
type TransactionRow struct {
	TransactionID int64
	CustomerID    int64
	ProductID     int64
	Quantity      int64
	Amount        float64
	Discount      float64
	Date          int64
}

// TPCConfig defines parameters for generating TPC-like transaction data.
type TPCConfig struct {
	Count           int
	CustomerZipf    ZipfConfig
	ProductZipf     ZipfConfig
	QuantityZipf    ZipfConfig
	AmountNormal    NormalConfig
	DiscountUniform UniformConfig
	StartDate       time.Time
	EndDate         time.Time
}

// GenerateTPCDataset creates a synthetic transaction dataset.
func GenerateTPCDataset(cfg TPCConfig) []TransactionRow {
	if cfg.Count < 0 {
		panic("GenerateTPCDataset: TPCConfig.Count must be non-negative")
	}
	if cfg.CustomerZipf.Count != cfg.Count {
		cfg.CustomerZipf.Count = cfg.Count
	}
	if cfg.ProductZipf.Count != cfg.Count {
		cfg.ProductZipf.Count = cfg.Count
	}
	if cfg.QuantityZipf.Count != cfg.Count {
		cfg.QuantityZipf.Count = cfg.Count
	}
	if cfg.AmountNormal.Count != cfg.Count {
		cfg.AmountNormal.Count = cfg.Count
	}
	if cfg.DiscountUniform.Count != cfg.Count {
		cfg.DiscountUniform.Count = cfg.Count
	}
	customers := GenerateZipf(cfg.CustomerZipf)
	products := GenerateZipf(cfg.ProductZipf)
	quantities := GenerateZipf(cfg.QuantityZipf)
	amounts := GenerateNormal(cfg.AmountNormal)
	discounts := GenerateUniform(cfg.DiscountUniform)
	dates := generateClusteredDates(cfg.StartDate, cfg.EndDate, cfg.Count)
	transactions := make([]TransactionRow, cfg.Count)
	for i := 0; i < cfg.Count; i++ {
		transactions[i] = TransactionRow{
			TransactionID: int64(i + 1),
			CustomerID:    int64(customers[i]),
			ProductID:     int64(products[i]),
			Quantity:      int64(quantities[i]) + 1,
			Amount:        amounts[i],
			Discount:      discounts[i],
			Date:          dates[i],
		}
	}
	return transactions
}

// generateClusteredDates creates timestamps with realistic patterns.
func generateClusteredDates(start, end time.Time, count int) []int64 {
	if count < 0 {
		panic("generateClusteredDates: count must be non-negative")
	}
	if start.After(end) {
		panic("generateClusteredDates: StartDate must be before EndDate")
	}
	startUnix := start.Unix()
	endUnix := end.Unix()
	timespan := endUnix - startUnix
	dates := make([]int64, count)
	for i := 0; i < count; i++ {
		maxRetries := 1000
		var timestamp int64
		for retries := 0; retries < maxRetries; retries++ {
			rawTimestamp := startUnix + rand.Int64N(timespan)
			t := time.Unix(rawTimestamp, 0)
			weekdayFactor := 1.0
			if t.Weekday() == time.Saturday {
				weekdayFactor = 0.6
			} else if t.Weekday() == time.Sunday {
				weekdayFactor = 0.4
			}
			hourFactor := 1.0
			hour := t.Hour()
			if hour < 6 || hour > 22 {
				hourFactor = 0.3
			} else if hour < 9 || hour > 17 {
				hourFactor = 0.7
			}
			if rand.Float64() < (weekdayFactor * hourFactor) {
				timestamp = rawTimestamp
				break
			}
			if retries == maxRetries-1 {
				timestamp = rawTimestamp // Fallback to avoid infinite loop
			}
		}
		dates[i] = timestamp
	}
	return dates
}
