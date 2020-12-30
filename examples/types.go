// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/echa/log"
	bolt "go.etcd.io/bbolt"

	"blockwatch.cc/knoxdb/encoding/decimal"
	"blockwatch.cc/knoxdb/pack"
	_ "blockwatch.cc/knoxdb/store/bolt"
	"blockwatch.cc/knoxdb/vec"
)

type Enum int

const (
	EnumInvalid Enum = iota // 0
	EnumOne                 // 1 (success)
	EnumTwo
	EnumThree
	EnumFour
)

func (t Enum) IsValid() bool {
	return t != EnumInvalid
}

func (t *Enum) UnmarshalText(data []byte) error {
	v := ParseEnum(string(data))
	if !v.IsValid() {
		return fmt.Errorf("invalid enum '%s'", string(data))
	}
	*t = v
	return nil
}

func (t *Enum) MarshalText() ([]byte, error) {
	return []byte(t.String()), nil
}

func ParseEnum(s string) Enum {
	switch s {
	case "1", "one":
		return EnumOne
	case "2", "two":
		return EnumTwo
	case "3", "three":
		return EnumThree
	case "4", "four":
		return EnumFour
	default:
		return EnumInvalid
	}
}

func (t Enum) String() string {
	switch t {
	case EnumOne:
		return "one"
	case EnumTwo:
		return "two"
	case EnumThree:
		return "three"
	case EnumFour:
		return "four"
	default:
		return ""
	}
}

type Types struct {
	RowId     uint64             `pack:"I,pk,snappy"          json:"row_id"`
	Timestamp time.Time          `pack:"T,snappy"             json:"time"`
	Hash      []byte             `pack:"H"                    json:"hash"`
	String    string             `pack:"s,snappy"             json:"string"`
	Bool      bool               `pack:"b,snappy"             json:"bool"`
	Enum      Enum               `pack:"e,snappy"             json:"enum"`
	Int64     int64              `pack:"i,snappy"             json:"int64"`
	Int32     int32              `pack:"j,snappy"             json:"int32"`
	Int16     int16              `pack:"k,snappy"             json:"int16"`
	Int8      int8               `pack:"l,snappy"             json:"int8"`
	Uint64    uint64             `pack:"m,snappy"             json:"uint64"`
	Uint32    uint32             `pack:"n,snappy"             json:"uint32"`
	Uint16    uint16             `pack:"o,snappy"             json:"uint16"`
	Uint8     uint8              `pack:"p,snappy"             json:"uint8"`
	Float64   float64            `pack:"g,snappy"             json:"float64"`
	Float32   float32            `pack:"f,snappy"             json:"float32"`
	D32       decimal.Decimal32  `pack:"d32,scale=5,snappy"   json:"decimal32"`
	D64       decimal.Decimal64  `pack:"d64,scale=15,snappy"  json:"decimal64"`
	D128      decimal.Decimal128 `pack:"d128,scale=18,snappy" json:"decimal128"`
	D256      decimal.Decimal256 `pack:"d256,scale=24,snappy" json:"decimal256"`
	I128      vec.Int128         `pack:"i128,snappy"          json:"int128"`
	I256      vec.Int256         `pack:"i256,snappy"          json:"int256"`
}

func (t Types) ID() uint64 {
	return t.RowId
}

func (t *Types) SetID(i uint64) {
	t.RowId = i
}

var _ pack.Item = (*Types)(nil)

const (
	TypesPackSizeLog2         = 15  // 32k packs ~4M
	TypesJournalSizeLog2      = 16  // 64k - search for spending op, so keep small
	TypesCacheSize            = 128 // 128=512MB
	TypesFillLevel            = 100
	TypesIndexPackSizeLog2    = 15   // 16k packs (32k split size) ~256k
	TypesIndexJournalSizeLog2 = 16   // 64k
	TypesIndexCacheSize       = 1024 // ~256M
	TypesIndexFillLevel       = 90
	TypesTableKey             = "types"
	DbLabel                   = "TEST-TYPES"
)

var (
	verbose  bool
	debug    bool
	trace    bool
	dbname   string
	flags    = flag.NewFlagSet("types", flag.ContinueOnError)
	boltopts = &bolt.Options{
		Timeout:      time.Second, // open timeout when file is locked
		NoGrowSync:   true,        // assuming Docker + XFS
		ReadOnly:     false,
		NoSync:       true, // skip fsync (DANGEROUS on crashes)
		FreelistType: bolt.FreelistMapType,
	}
)

func Create(path string, dbOpts interface{}) (*pack.Table, error) {
	fields, err := pack.Fields(Types{})
	if err != nil {
		return nil, err
	}
	db, err := pack.CreateDatabaseIfNotExists(filepath.Dir(path), TypesTableKey, "*", boltopts)
	if err != nil {
		return nil, fmt.Errorf("creating %s database: %v", TypesTableKey, err)
	}

	table, err := db.CreateTableIfNotExists(
		TypesTableKey,
		fields,
		pack.Options{
			PackSizeLog2:    TypesPackSizeLog2,
			JournalSizeLog2: TypesJournalSizeLog2,
			CacheSize:       TypesCacheSize,
			FillLevel:       TypesFillLevel,
		})
	if err != nil {
		db.Close()
		return nil, err
	}

	_, err = table.CreateIndexIfNotExists(
		"hash",
		fields.Find("H"),   // op hash field (32 byte op hashes)
		pack.IndexTypeHash, // hash table, index stores hash(field) -> pk value
		pack.Options{
			PackSizeLog2:    TypesIndexPackSizeLog2,
			JournalSizeLog2: TypesIndexJournalSizeLog2,
			CacheSize:       TypesIndexCacheSize,
			FillLevel:       TypesIndexFillLevel,
		})
	if err != nil {
		table.Close()
		db.Close()
		return nil, err
	}

	return table, nil
}

func Open(path string) (*pack.Table, error) {
	db, err := pack.OpenDatabase(filepath.Dir(path), TypesTableKey, "*", boltopts)
	if err != nil {
		return nil, err
	}
	return db.Table(
		TypesTableKey,
		pack.Options{
			JournalSizeLog2: TypesJournalSizeLog2,
			CacheSize:       TypesCacheSize,
		},
		pack.Options{
			JournalSizeLog2: TypesIndexJournalSizeLog2,
			CacheSize:       TypesIndexCacheSize,
		})
}

func Close(table *pack.Table) error {
	if table == nil {
		return nil
	}
	if err := table.Close(); err != nil {
		return err
	}
	return table.Database().Close()
}

// GenerateRandomKey creates a random key with the given length in bytes.
// On failure, returns nil.
//
// Callers should explicitly check for the possibility of a nil return, treat
// it as a failure of the system random number generator, and not continue.
func GenerateRandomKey(length int) []byte {
	k := make([]byte, length)
	if _, err := io.ReadFull(rand.Reader, k); err != nil {
		return nil
	}
	return k
}

func NewRandomTypes(i int) *Types {
	return &Types{
		RowId:     0, // empty, will be set by insert
		Timestamp: time.Now().UTC(),
		Hash:      GenerateRandomKey(20),
		String:    hex.EncodeToString(GenerateRandomKey(4)),
		Bool:      true,
		Enum:      Enum(i%4 + 1),
		Int64:     int64(i),
		Int32:     int32(i),
		Int16:     int16(i),
		Int8:      int8(i),
		Uint64:    uint64(i * 1000000),
		Uint32:    uint32(i * 1000000),
		Uint16:    uint16(i),
		Uint8:     uint8(i),
		Float32:   float32(i * 1000000),
		Float64:   float64(i * 1000000),
		D32:       decimal.NewDecimal32(int32(100123456789-i), 5),
		D64:       decimal.NewDecimal64(1123456789123456789-int64(i), 15),
		D128:      decimal.NewDecimal128(vec.MustParseInt128(strconv.Itoa(i)+"00000000000000000000"), 18),
		D256:      decimal.NewDecimal256(vec.MustParseInt256(strconv.Itoa(i)+"0000000000000000000000000000000000000000"), 24),
		I128:      vec.MustParseInt128(strconv.Itoa(i) + "000000000000000000000000000000"),
		I256:      vec.MustParseInt256(strconv.Itoa(i) + "000000000000000000000000000000000000000000000000000000000000"),
	}
}

// Main
func init() {
	flags.Usage = func() {}
	flags.BoolVar(&verbose, "v", false, "be verbose")
	flags.BoolVar(&debug, "vv", false, "enable debug mode")
	flags.BoolVar(&trace, "vvv", false, "enable trace mode")
	flags.StringVar(&dbname, "db", "", "database")
}

func printhelp() {
	fmt.Println("Usage:\n  types [flags]")
	fmt.Println("Flags:")
	flags.PrintDefaults()
	fmt.Println()
}

func main() {
	if err := run(); err != nil {
		log.Error(err)
	}
}

func run() error {
	if err := flags.Parse(os.Args[1:]); err != nil {
		if err == flag.ErrHelp {
			printhelp()
			return nil
		}
		return err
	}
	lvl := log.LevelInfo
	switch true {
	case trace:
		lvl = log.LevelTrace
	case debug:
		lvl = log.LevelDebug
	case verbose:
		lvl = log.LevelInfo
	}
	log.SetLevel(lvl)
	pack.UseLogger(log.Log)

	table, err := Create(".", nil)
	if err != nil {
		return err
	}
	log.Infof("Created Table %s", table.Name())

	// fill with random data
	for i := 0; i < 64*1024+1; i++ {
		err = table.Insert(context.Background(), NewRandomTypes(i))
		if err != nil {
			return err
		}
	}

	log.Infof("Written %d entries", table.Stats().TupleCount)

	if err := Close(table); err != nil {
		return err
	}
	log.Info("Closed Table")

	return nil
}
