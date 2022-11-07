// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/echa/log"
	bolt "go.etcd.io/bbolt"

	"blockwatch.cc/knoxdb/pack"
	_ "blockwatch.cc/knoxdb/store/bolt"
)

type OpStatus uint

const (
	OpStatusInvalid OpStatus = iota // 0
	OpStatusApplied                 // 1 (success)
	OpStatusFailed
	OpStatusSkipped
	OpStatusBacktracked
)

func (t OpStatus) IsValid() bool {
	return t != OpStatusInvalid
}

func (t OpStatus) IsSuccess() bool {
	return t == OpStatusApplied
}

func (t *OpStatus) UnmarshalText(data []byte) error {
	v := ParseOpStatus(string(data))
	if !v.IsValid() {
		return fmt.Errorf("invalid operation status '%s'", string(data))
	}
	*t = v
	return nil
}

func (t *OpStatus) MarshalText() ([]byte, error) {
	return []byte(t.String()), nil
}

func ParseOpStatus(s string) OpStatus {
	switch s {
	case "applied":
		return OpStatusApplied
	case "failed":
		return OpStatusFailed
	case "skipped":
		return OpStatusSkipped
	case "backtracked":
		return OpStatusBacktracked
	default:
		return OpStatusInvalid
	}
}

func (t OpStatus) String() string {
	switch t {
	case OpStatusApplied:
		return "applied"
	case OpStatusFailed:
		return "failed"
	case OpStatusSkipped:
		return "skipped"
	case OpStatusBacktracked:
		return "backtracked"
	default:
		return ""
	}
}

type OpType uint

const (
	OpTypeBake                      OpType = iota // 0
	OpTypeActivateAccount                         // 1
	OpTypeDoubleBakingEvidence                    // 2
	OpTypeDoubleEndorsementEvidence               // 3
	OpTypeSeedNonceRevelation                     // 4
	OpTypeTransaction                             // 5
	OpTypeOrigination                             // 6
	OpTypeDelegation                              // 7
	OpTypeReveal                                  // 8
	OpTypeEndorsement                             // 9
	OpTypeProposals                               // 10
	OpTypeBallot                                  // 11
	OpTypeUnfreeze                                // 12
	OpTypeInvoice                                 // 13
	OpTypeAirdrop                                 // 14
	OpTypeSeedSlash                               // 15
	OpTypeMigration                               // 16 indexer only
	OpTypeFailingNoop                             // 17 v009
	OpTypeBatch                     = 254         // indexer only, output-only
	OpTypeInvalid                   = 255
)

func (t OpType) IsValid() bool {
	return t != OpTypeInvalid
}

func (t *OpType) UnmarshalText(data []byte) error {
	v := ParseOpType(string(data))
	if !v.IsValid() {
		return fmt.Errorf("invalid operation type '%s'", string(data))
	}
	*t = v
	return nil
}

func (t *OpType) MarshalText() ([]byte, error) {
	return []byte(t.String()), nil
}

func ParseOpType(s string) OpType {
	switch s {
	case "bake":
		return OpTypeBake
	case "activate_account":
		return OpTypeActivateAccount
	case "double_baking_evidence":
		return OpTypeDoubleBakingEvidence
	case "double_endorsement_evidence":
		return OpTypeDoubleEndorsementEvidence
	case "seed_nonce_revelation":
		return OpTypeSeedNonceRevelation
	case "transaction":
		return OpTypeTransaction
	case "origination":
		return OpTypeOrigination
	case "delegation":
		return OpTypeDelegation
	case "reveal":
		return OpTypeReveal
	case "endorsement":
		return OpTypeEndorsement
	case "proposals":
		return OpTypeProposals
	case "ballot":
		return OpTypeBallot
	case "unfreeze":
		return OpTypeUnfreeze
	case "invoice":
		return OpTypeInvoice
	case "airdrop":
		return OpTypeAirdrop
	case "seed_slash":
		return OpTypeSeedSlash
	case "migration":
		return OpTypeMigration
	case "batch":
		return OpTypeBatch
	case "failing_noop":
		return OpTypeFailingNoop
	default:
		return OpTypeInvalid
	}
}

func (t OpType) String() string {
	switch t {
	case OpTypeBake:
		return "bake"
	case OpTypeActivateAccount:
		return "activate_account"
	case OpTypeDoubleBakingEvidence:
		return "double_baking_evidence"
	case OpTypeDoubleEndorsementEvidence:
		return "double_endorsement_evidence"
	case OpTypeSeedNonceRevelation:
		return "seed_nonce_revelation"
	case OpTypeTransaction:
		return "transaction"
	case OpTypeOrigination:
		return "origination"
	case OpTypeDelegation:
		return "delegation"
	case OpTypeReveal:
		return "reveal"
	case OpTypeEndorsement:
		return "endorsement"
	case OpTypeProposals:
		return "proposals"
	case OpTypeBallot:
		return "ballot"
	case OpTypeUnfreeze:
		return "unfreeze"
	case OpTypeInvoice:
		return "invoice"
	case OpTypeAirdrop:
		return "airdrop"
	case OpTypeSeedSlash:
		return "seed_slash"
	case OpTypeMigration:
		return "migration"
	case OpTypeBatch:
		return "batch"
	case OpTypeFailingNoop:
		return "failing_noop"
	default:
		return ""
	}
}

type Op struct {
	RowId        uint64    `knox:"I,pk"             json:"row_id"`         // internal: unique row id
	Type         OpType    `knox:"t,u8,bloom"       json:"type"`           // indexer op type
	Hash         []byte    `knox:"H,snappy,bloom=3" json:"hash"`           // op hash
	Height       int64     `knox:"h,i32"            json:"height"`         // block height
	Cycle        int64     `knox:"c,i16"            json:"cycle"`          // block cycle
	Timestamp    time.Time `knox:"T"                json:"time"`           // block time
	OpN          int       `knox:"n,i32"            json:"op_n"`           // unique in-block pos
	OpP          int       `knox:"P,i16"            json:"op_p"`           // op list pos (list can be derived from type)
	Status       OpStatus  `knox:"?,u8"             json:"status"`         // op status
	IsSuccess    bool      `knox:"!,snappy"         json:"is_success"`     // success flag
	IsContract   bool      `knox:"C,snappy"         json:"is_contract"`    // contract call flag (target is contract)
	IsInternal   bool      `knox:"N,snappy"         json:"is_internal"`    // internal contract call or op
	IsEvent      bool      `knox:"m,snappy"         json:"is_event"`       // this is an implicit event
	IsRollup     bool      `knox:"u,snappy"         json:"is_rollup"`      // this is an rollup operation
	Counter      int64     `knox:"j,i32"            json:"counter"`        // signer counter
	GasLimit     int64     `knox:"l,i32"            json:"gas_limit"`      // gas limit
	GasUsed      int64     `knox:"G,i32"            json:"gas_used"`       // gas used
	StorageLimit int64     `knox:"Z,i32"            json:"storage_limit"`  // storage size limit
	StoragePaid  int64     `knox:"$,i32"            json:"storage_paid"`   // storage allocated/paid
	Volume       int64     `knox:"v"                json:"volume"`         // transacted tez volume
	Fee          int64     `knox:"f"                json:"fee"`            // tx fees
	Reward       int64     `knox:"r"                json:"reward"`         // baking/endorsement reward
	Deposit      int64     `knox:"d"                json:"deposit"`        // baker deposit
	Burned       int64     `knox:"b"                json:"burned"`         // burned tez (for storage allocation)
	SenderId     uint64    `knox:"S,u32,bloom"      json:"sender_id"`      // sender id, also on internal ops
	ReceiverId   uint64    `knox:"R,u32,bloom"      json:"receiver_id"`    // receiver id
	CreatorId    uint64    `knox:"M,u32"            json:"creator_id"`     // creator id, direct source for internal ops
	BakerId      uint64    `knox:"D,u32,bloom"      json:"baker_id"`       // delegate id
	Data         string    `knox:"a,snappy"         json:"data"`           // custom op data
	Parameters   []byte    `knox:"p,snappy"         json:"parameters"`     // call params
	StorageHash  uint64    `knox:"s"                json:"storage_hash"`   // storage hash
	Errors       []byte    `knox:"e,snappy"         json:"errors"`         // call errors
	TDD          float64   `knox:"x,d32,scale=6"    json:"days_destroyed"` // token days destroyed
	Entrypoint   int       `knox:"E,i8"             json:"entrypoint_id"`  // update contract counters, search by entrypoint
}

func (o Op) ID() uint64 {
	return o.RowId
}

func (o *Op) SetID(i uint64) {
	o.RowId = i
}

// Static check to ensure Op implements the pack.Item interface.
var _ pack.Item = (*Op)(nil)

const (
	PackSizeLog2         = 15  // 32k packs ~4M
	JournalSizeLog2      = 16  // 64k - search for spending op, so keep small
	CacheSize            = 128 // 128=512MB
	FillLevel            = 100
	IndexPackSizeLog2    = 15   // 16k packs (32k split size) ~256k
	IndexJournalSizeLog2 = 16   // 64k
	IndexCacheSize       = 1024 // ~256M
	IndexFillLevel       = 90
)

var (
	verbose  bool
	debug    bool
	trace    bool
	cache    bool
	dbname   string
	flags    = flag.NewFlagSet("opdb", flag.ContinueOnError)
	boltopts = &bolt.Options{
		Timeout:      time.Second, // open timeout when file is locked
		NoGrowSync:   true,        // assuming Docker + XFS
		ReadOnly:     false,       // set true to disallow write transactions
		NoSync:       true,        // skip fsync (DANGEROUS on crashes)
		FreelistType: bolt.FreelistMapType,
	}
)

// Create a new database at `path` and a new table with the same name as
// the file's basename (without extension). Uses the type of `schema`
// as template for extracting table columns. Schema must be pointer to struct
// which uses struct tags `knox:""` to configure column names, overwrite type
// detection and configure compression settings. Optional parameter `opts`
// allows to configure settings of the underlying boltdb engine.
//
// ```
// // creates new database `op.db` in path `./db` with table `op` from type Op
// t, err := Create("./db/op.db", &Op{}, nil)
// ```

func Create(path string, schema, opts interface{}) (*pack.Table, error) {
	fields, err := pack.Fields(schema)
	if err != nil {
		return nil, err
	}
	name := filepath.Base(path)
	name = name[:len(name)-len(filepath.Ext(name))]
	db, err := pack.CreateDatabaseIfNotExists(filepath.Dir(path), name, "*", opts)
	if err != nil {
		return nil, fmt.Errorf("creating %s database: %v", name, err)
	}

	table, err := db.CreateTableIfNotExists(
		name,
		fields,
		pack.Options{
			PackSizeLog2:    PackSizeLog2,
			JournalSizeLog2: JournalSizeLog2,
			CacheSize:       CacheSize,
			FillLevel:       FillLevel,
		})
	if err != nil {
		db.Close()
		return nil, err
	}

	_, err = table.CreateIndexIfNotExists(
		"hash",              // index name
		fields.Find("hash"), // op hash field (32 byte op hashes)
		pack.IndexTypeHash,  // hash table, index stores hash(field) -> pk value
		pack.Options{
			PackSizeLog2:    IndexPackSizeLog2,
			JournalSizeLog2: IndexJournalSizeLog2,
			CacheSize:       IndexCacheSize,
			FillLevel:       IndexFillLevel,
		})
	if err != nil {
		table.Close()
		db.Close()
		return nil, err
	}

	return table, nil
}

// Open an existing database at `path` and looks for a table with the
// same name as the file's basename (without extension). Optional parameter `opts`
// allows to configure settings of the underlying boltdb engine.
//
// # Example
//
// ```
// // opens file `op.db` in path `./db` and looks for table `op`
// t, err := Open("./db/op.db")
// ```
func Open(path string, opts interface{}) (*pack.Table, error) {
	name := filepath.Base(path)
	name = name[:len(name)-len(filepath.Ext(name))]
	db, err := pack.OpenDatabase(filepath.Dir(path), name, "*", opts)
	if err != nil {
		return nil, err
	}
	return db.Table(
		name,
		pack.Options{
			JournalSizeLog2: JournalSizeLog2,
			CacheSize:       CacheSize,
		},
		pack.Options{
			JournalSizeLog2: IndexJournalSizeLog2,
			CacheSize:       IndexCacheSize,
		})
}

// Closes table and database. Must be called before shutdown to flush any state
// changes to disk.
func Close(table *pack.Table) error {
	if table == nil {
		return nil
	}
	if err := table.Close(); err != nil {
		return err
	}
	return table.Database().Close()
}

// Example using the simplified Query API (pack/query.go)
func ListOpTypes(ctx context.Context, table *pack.Table, typ OpType, limit int) ([]*Op, error) {
	// Construct a query by appending multiple options; each function returns a
	// different Query struct with the respoecive option set, but keeps the original
	// query untouched. That way, partical queries can be re-used later if needed.
	//
	q := pack.NewQuery("list_"+typ.String()).
		WithTable(table).
		AndEqual("type", typ).
		WithLimit(limit).
		WithCache(cache)

	// Execute is a shortcut for Stream & Decode which takes a pointer to one of
	// - a single struct (an implicity limit of 1 is used for this query)
	// - a slice of structs
	// - a slice of pointers to structs
	//
	// Data is automatically extracted into the provided struct/slice. Slice
	// elements are allocated and appended on-the-fly as needed.
	//
	ops := make([]*Op, 0)
	if err := q.Execute(ctx, &ops); err != nil {
		return nil, err
	}
	return ops, nil
}

func init() {
	flags.Usage = func() {}
	flags.BoolVar(&verbose, "v", false, "be verbose")
	flags.BoolVar(&debug, "vv", false, "enable debug mode")
	flags.BoolVar(&trace, "vvv", false, "enable trace mode")
	flags.BoolVar(&cache, "cache", false, "enable db cache")
	flags.StringVar(&dbname, "db", "", "database")
}

func printhelp() {
	fmt.Println("Usage:\n  opdb [flags]")
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

	// open existing table
	table, err := Open(dbname, boltopts)
	if err != nil {
		return err
	}
	ctx := context.Background()

	sender_id, err := strconv.ParseUint(flags.Arg(0), 10, 64)
	if err != nil {
		return err
	}
	fmt.Printf("Acc id = %s %d\n", flags.Arg(0), sender_id)
	// return nil

	// Example 1
	// Note: loads all data into memory, use low limit
	// ops, err := ListOpTypes(ctx, table, OpTypeTransaction, 100000)
	// if err != nil {
	// 	return err
	// }
	// // do smth with ops
	var vol int64
	// for _, o := range ops {
	// 	vol += o.Volume
	// }
	// fmt.Printf("Volume in first %d transactions is %f\n", len(ops), float64(vol)/1000000)
	// vol = 0

	// Example 2
	// Note: uses streaming and constant memory to visit all table rows
	var count int
	start := time.Now()
	err = pack.NewQuery("stream_tx").
		WithTable(table).
		AndEqual("type", OpTypeTransaction).
		WithCache(cache).
		Stream(ctx, func(r pack.Row) error {
			var o Op
			if err := r.Decode(&o); err != nil {
				return err
			}
			vol += o.Volume
			count++
			return nil
		})
	fmt.Printf("Total volume in all %d transactions is %f, runtime=%s\n", count, float64(vol)/1000000, time.Since(start))

	// Example 3
	// queries all transactions sent by senderid
	start = time.Now()
	count = 0
	vol = 0
	err = pack.NewQuery("stream_tx").
		WithTable(table).
		AndEqual("type", OpTypeTransaction).
		AndEqual("receiver_id", sender_id).
		WithCache(cache).
		Stream(ctx, func(r pack.Row) error {
			var o Op
			if err := r.Decode(&o); err != nil {
				return err
			}
			vol += o.Volume
			count++
			return nil
		})

	if err := Close(table); err != nil {
		return err
	}
	fmt.Printf("Total volume sent by account %d in %d transactions is %f, runtime=%s\n", sender_id, count, float64(vol)/1000000, time.Since(start))

	return nil
}
