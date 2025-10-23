// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc
package main

import (
	"context"
	"encoding/hex"
	"flag"
	"fmt"
	"os"
	"runtime/pprof"
	"strconv"
	"time"

	"github.com/echa/log"

	"blockwatch.cc/knoxdb/pkg/knox"
	"blockwatch.cc/knoxdb/pkg/num"
	"blockwatch.cc/knoxdb/pkg/schema"
	"blockwatch.cc/knoxdb/pkg/util"
)

type MyEnum string

const (
	MyEnumOne   = "one"
	MyEnumTwo   = "two"
	MyEnumThree = "three"
	MyEnumFour  = "four"
)

var myEnums = []string{MyEnumOne, MyEnumTwo, MyEnumThree, MyEnumFour}

type Types struct {
	Id        uint64         `knox:"id,pk"`
	Timestamp time.Time      `knox:"time"`
	Date      time.Time      `knox:"date,date"`
	Hash      [32]byte       `knox:"hash,index=bloom:3"`
	String    string         `knox:"string"`
	Bool      bool           `knox:"bool"`
	MyEnum    MyEnum         `knox:"my_enum,enum"`
	Int64     int64          `knox:"int64"`
	Int32     int32          `knox:"int32"`
	Int16     int16          `knox:"int16"`
	Int8      int8           `knox:"int8"`
	Int_64    int            `knox:"int_as_int64"`
	Uint64    uint64         `knox:"uint64,index=bloom:2"`
	Uint32    uint32         `knox:"uint32"`
	Uint16    uint16         `knox:"uint16"`
	Uint8     uint8          `knox:"uint8"`
	Uint_64   uint           `knox:"uint_as_uint64"`
	Float64   float64        `knox:"float64"`
	Float32   float32        `knox:"float32"`
	D32       num.Decimal32  `knox:"decimal32,scale=5"`
	D64       num.Decimal64  `knox:"decimal64,scale=15"`
	D128      num.Decimal128 `knox:"decimal128,scale=18"`
	D256      num.Decimal256 `knox:"decimal256,scale=24"`
	I128      num.Int128     `knox:"int128"`
	I256      num.Int256     `knox:"int256"`
	Big       num.Big        `knox:"big"`
}

const (
	TypesCacheSize            = 128 // MB
	TypesPackSizeLog2         = 15  // 32k packs ~6M
	TypesJournalSizeLog2      = 17  // 128k
	TypesFillLevel            = 1.0
	TypesIndexPackSizeLog2    = 11 // 2k packs
	TypesIndexJournalSizeLog2 = 17 // 128k journal
	TypesIndexFillLevel       = 1.0
)

var (
	verbose bool
	debug   bool
	trace   bool
	profile bool
	dbname  string
	flags   = flag.NewFlagSet("types", flag.ContinueOnError)
)

// Main
func init() {
	flags.Usage = func() {}
	flags.BoolVar(&verbose, "v", false, "be verbose")
	flags.BoolVar(&debug, "vv", false, "enable debug mode")
	flags.BoolVar(&trace, "vvv", false, "enable trace mode")
	flags.BoolVar(&profile, "p", false, "enable CPU profiling")
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
	switch {
	case trace:
		lvl = log.LevelTrace
	case debug:
		lvl = log.LevelDebug
	case verbose:
		lvl = log.LevelInfo
	}
	log.SetLevel(lvl)

	if profile {
		f, err := os.Create("cpu.prof")
		if err != nil {
			log.Fatal(err)
		}
		defer f.Close()
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	ctx := context.Background()

	db, table, err := OpenOrCreate(ctx)
	if err != nil {
		return err
	}
	defer db.Close(ctx)

	// Step 1
	//
	// fill with random data
	log.Infof("Writing data...")
	start := time.Now()
	c := 2 * 64 * 1024
	data := make([]*Types, 0, c)
	for i := 1; i <= c; i++ {
		data = append(data, NewRandomTypes(i))
	}
	_, err = table.Insert(ctx, data)
	if err != nil {
		return err
	}
	// sync to flush journal (until WAL works)
	err = db.Sync(ctx)
	if err != nil {
		return err
	}

	log.Infof("Written %d records in %s", c, time.Since(start))
	log.Infof("Total %d records", table.Metrics().TupleCount)

	// Step 2
	//
	// read records back
	var count int
	start = time.Now()
	err = knox.NewGenericQuery[Types]().
		WithTable(table).
		WithTag("three_million_records").
		WithLimit(3000000).
		WithStats(true).
		// WithDebug(true).
		Stream(ctx, func(_ *Types) error {
			count++
			return nil
		})
	if err != nil {
		log.Errorf("Decode: %v", err)
	} else {
		dur := time.Since(start)
		log.Infof("Decoded %d records in %s (%d/s)", count, dur, count*1000000000/int(dur))
	}

	// read a single entry
	var single Types
	_, err = knox.NewGenericQuery[Types]().
		WithTable(table).
		WithTag("two_conditions_single").
		AndGte("int64", 42).
		AndLt("int64", 1024).
		WithStats(true).
		// WithDebug(true).
		Execute(ctx, &single)
	if err != nil {
		return fmt.Errorf("single: %v", err)
	}
	log.Infof("Single value int64=%d pk=%d", single.Int64, single.Id)

	// read up to 10 records via query interface
	multi := make([]Types, 10)
	_, err = knox.NewQuery().
		WithTable(table).
		WithTag("no_condition_limit").
		WithLimit(10).
		WithStats(true).
		// WithDebug(true).
		Execute(ctx, &multi)
	if err != nil {
		return fmt.Errorf("multi: %v", err)
	}
	log.Infof("%d Multi values", len(multi))
	// for i, v := range multi {
	// 	log.Tracef("%d int64=%d pk=%d", i, v.Int64, v.Id)
	// }

	// Step 3
	//
	// delete some records
	n, err := knox.NewQuery().
		WithTag("delete").
		WithTable(table).
		AndLt("int64", 1024).
		// WithDebug(true).
		Delete(ctx)
	if err != nil {
		log.Errorf("Decode: %v", err)
	} else {
		log.Infof("Deleted %d records", n)
	}
	err = db.Sync(ctx)
	if err != nil {
		return err
	}

	log.Info("Closing DB")

	return nil
}

func OpenOrCreate(ctx context.Context) (db knox.Database, table knox.Table, err error) {
	ok, err := knox.IsDatabaseExist(
		ctx,
		"types",
		knox.DefaultDatabaseOptions.WithPath("./db"),
	)
	if err != nil {
		return nil, nil, err
	}
	if ok {
		return Open(ctx)
	} else {
		return Create(ctx)
	}
}

func Create(ctx context.Context) (db knox.Database, table knox.Table, err error) {
	var s *schema.Schema
	s, err = schema.SchemaOf(&Types{})
	if err != nil {
		return
	}

	opts := knox.DefaultDatabaseOptions.
		WithPath("./db").
		WithNamespace("cx.bwd.knox.types-demo").
		WithCacheSize(1 << 20 * TypesCacheSize).
		WithLogger(log.Log)

	log.Info("Creating DB")
	db, err = knox.CreateDatabase(ctx, "types", opts)
	if err != nil {
		return
	}

	log.Info("Creating Enum")
	_, err = db.CreateEnum(ctx, "my_enum")
	if err != nil {
		return
	}
	err = db.ExtendEnum(ctx, "my_enum", myEnums...)
	if err != nil {
		return
	}

	log.Infof("Creating Table %s", s.Name)
	log.Debugf("Schema %s", s)
	table, err = db.CreateTable(ctx, s, knox.TableOptions{
		Engine:      "pack",
		Driver:      "bolt",
		PackSize:    1 << TypesPackSizeLog2,
		JournalSize: 1 << TypesJournalSizeLog2,
		PageFill:    TypesFillLevel,
		NoSync:      true,
	})
	if err != nil {
		return
	}
	ts := table.Schema()

	// s, err = ts.SelectFields("hash", "$rid")
	// if err != nil {
	// 	return
	// }
	// s.WithName("types_hash_index")
	// log.Infof("Creating Index %s", s.Name())
	// log.Debugf("Schema %s", s)
	// err = db.CreateIndex(ctx, "types_hash_index", table, s, knox.IndexOptions{
	// 	Engine:      "pack",
	// 	Driver:      "bolt",
	// 	Type:        knox.IndexTypeHash,
	// 	PackSize:    1 << TypesIndexPackSizeLog2,
	// 	JournalSize: 1 << TypesIndexJournalSizeLog2,
	// 	PageFill:    TypesIndexFillLevel,
	// 	Logger:      log.Log,
	// })
	// if err != nil {
	// 	return
	// }

	s, err = ts.SelectFields("id", "$rid")
	if err != nil {
		return
	}
	s.WithName("types_pk_index")
	log.Infof("Creating Index %s", s.Name)
	log.Debugf("Schema %s", s)
	err = db.CreateIndex(ctx, "types_pk_index", table, s, knox.IndexOptions{
		Engine:      "pack",
		Driver:      "bolt",
		Type:        knox.IndexTypeInt,
		PackSize:    1 << TypesIndexPackSizeLog2,
		JournalSize: 1 << TypesIndexJournalSizeLog2,
		PageFill:    TypesIndexFillLevel,
		Logger:      log.Log,
		NoSync:      true,
	})
	if err != nil {
		return
	}

	return
}

func Open(ctx context.Context) (db knox.Database, table knox.Table, err error) {
	log.Info("Opening DB")
	db, err = knox.OpenDatabase(ctx, "types", knox.DatabaseOptions{
		Path:      "./db",
		Namespace: "cx.bwd.knox.types-demo",
		Logger:    log.Log,
	})
	if err != nil {
		return
	}

	log.Info("Use table types")
	table, err = db.FindTable("types")
	if err != nil {
		return
	}
	log.Debugf("Schema %s", table.Schema())
	log.Infof("%d records", table.Metrics().TupleCount)
	return
}

func NewRandomTypes(i int) *Types {
	return &Types{
		Id:        0, // empty, will be set by insert
		Timestamp: time.Now().UTC(),
		Date:      time.Now().UTC(),
		Hash:      [32]byte(util.RandBytes(32)),
		String:    hex.EncodeToString(util.RandBytes(4)),
		Bool:      true,
		MyEnum:    MyEnum(myEnums[i%4]),
		// typed ints
		Int64: int64(i),
		Int32: int32(i),
		Int16: int16(i % (1<<16 - 1)),
		Int8:  int8(i % (1<<8 - 1)),
		// int to typed int
		Int_64: i,
		// typed uints
		Uint64: uint64(i),
		Uint32: uint32(i),
		Uint16: uint16(i),
		Uint8:  uint8(i),
		// uint to typed uint
		Uint_64: uint(i),
		Float32: float32(i),
		Float64: float64(i),
		// decimals
		D32:  num.NewDecimal32(int32(i)*100000, 5),
		D64:  num.NewDecimal64(int64(i)*1000000000000, 15),
		D128: num.NewDecimal128(num.MustParseInt128(strconv.Itoa(i)+"00000000000000000000"), 18),
		D256: num.NewDecimal256(num.MustParseInt256(strconv.Itoa(i)+"0000000000000000000000000000000000000000"), 24),
		I128: num.MustParseInt128(strconv.Itoa(i) + "000000000000000000000000000000"),
		I256: num.MustParseInt256(strconv.Itoa(i) + "000000000000000000000000000000000000000000000000000000000000"),
		Big:  num.NewBig(int64(i)),
	}
}
