// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/echa/log"
	bolt "go.etcd.io/bbolt"

	"blockwatch.cc/packdb-pro/pack"
	_ "blockwatch.cc/packdb-pro/store/bolt"
	// "blockwatch.cc/packdb-pro/util"
)

type OpStatus int

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

type OpType int

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
	default:
		return ""
	}
}

type Op struct {
	RowId        uint64    `pack:"I,pk,snappy"   json:"row_id"`                         // internal: unique row id
	Timestamp    time.Time `pack:"T,snappy"      json:"time"`                           // bc: op block time
	Height       int64     `pack:"h,snappy"      json:"height"`                         // bc: block height op was mined at
	Cycle        int64     `pack:"c,snappy"      json:"cycle"`                          // bc: block cycle (tezos specific)
	Hash         []byte    `pack:"H"             json:"hash"`                           // bc: unique op_id (op hash)
	Counter      int64     `pack:"j,snappy"      json:"counter"`                        // bc: counter
	OpN          int       `pack:"n,snappy"      json:"op_n"`                           // bc: gobal position in block (block.Operations.([][]*OperationHeader) list position)
	OpC          int       `pack:"o,snappy"      json:"op_c"`                           // bc: position in OperationHeader.Contents.([]Operation) list
	OpI          int       `pack:"i,snappy"      json:"op_i"`                           // bc: position in internal operation result list
	OpL          int       `pack:"L,snappy"      json:"op_l"`                           // bc: operation list (i.e. 0 for endorsements, etc corresponding to validation pass)
	OpP          int       `pack:"P,snappy"      json:"op_p"`                           // bc: operation list position (use in combination with op_l to lookup op on RPC)
	Type         OpType    `pack:"t,snappy"      json:"type"`                           // stats: operation type as defined byprotocol
	Status       OpStatus  `pack:"?,snappy"      json:"status"`                         // stats: operation status
	IsSuccess    bool      `pack:"!,snappy"      json:"is_success"`                     // bc: operation succesful flag
	IsContract   bool      `pack:"C,snappy"      json:"is_contract"`                    // bc: operation succesful flag
	GasLimit     int64     `pack:"l,snappy"      json:"gas_limit"`                      // stats: gas limit
	GasUsed      int64     `pack:"G,snappy"      json:"gas_used"`                       // stats: gas used
	GasPrice     float64   `pack:"g,convert,precision=5,snappy"      json:"gas_price"`  // stats: gas price in tezos per unit gas, relative to tx fee
	StorageLimit int64     `pack:"Z,snappy"      json:"storage_limit"`                  // stats: storage size limit
	StorageSize  int64     `pack:"z,snappy"      json:"storage_size"`                   // stats: storage size used/allocated by this op
	StoragePaid  int64     `pack:"$,snappy"      json:"storage_paid"`                   // stats: extra storage size paid by this op
	Volume       int64     `pack:"v,snappy"      json:"volume"`                         // stats: sum of transacted tezos volume
	Fee          int64     `pack:"f,snappy"      json:"fee"`                            // stats: transaction fees
	Reward       int64     `pack:"r,snappy"      json:"reward"`                         // stats: baking and endorsement rewards
	Deposit      int64     `pack:"d,snappy"      json:"deposit"`                        // stats: bonded deposits for baking and endorsement
	Burned       int64     `pack:"b,snappy"      json:"burned"`                         // stats: burned tezos
	SenderId     uint64    `pack:"S,snappy"      json:"sender_id"`                      // internal: op sender
	ReceiverId   uint64    `pack:"R,snappy"      json:"receiver_id"`                    // internal: op receiver
	ManagerId    uint64    `pack:"M,snappy"      json:"manager_id"`                     // internal: op manager for originations
	DelegateId   uint64    `pack:"D,snappy"      json:"delegate_id"`                    // internal: op delegate for originations and delegations
	IsInternal   bool      `pack:"N,snappy"      json:"is_internal"`                    // bc: internal from contract call
	HasData      bool      `pack:"w,snappy"      json:"has_data"`                       // internal: flag to signal if data is available
	Data         string    `pack:"a,snappy"      json:"data"`                           // bc: extra op data
	Parameters   []byte    `pack:"p,snappy"      json:"parameters"`                     // bc: input params
	Storage      []byte    `pack:"s,snappy"      json:"storage"`                        // bc: result storage
	BigMapDiff   []byte    `pack:"B,snappy"      json:"big_map_diff"`                   // bc: result big map diff
	Errors       string    `pack:"e,snappy"      json:"errors"`                         // bc: result errors
	TDD          float64   `pack:"x,convert,precision=6,snappy"  json:"days_destroyed"` // stats: token days destroyed
	BranchId     uint64    `pack:"X,snappy"      json:"branch_id"`                      // bc: branch block the op is based on
	BranchHeight int64     `pack:"#,snappy"      json:"branch_height"`                  // bc: height of the branch block
	BranchDepth  int64     `pack:"<,snappy"      json:"branch_depth"`                   // stats: diff between branch block and current block
	IsImplicit   bool      `pack:"m,snappy"      json:"is_implicit"`                    // bc: implicit operation not published on chain
	Entrypoint   int       `pack:"E,snappy"      json:"entrypoint_id"`                  // entrypoint sequence id
	IsOrphan     bool      `pack:"O,snappy"      json:"is_orphan"`
	// IsBatch      bool      `pack:"y,snappy"      json:"is_batch"`
}

func (o Op) ID() uint64 {
	return o.RowId
}

func (o *Op) SetID(i uint64) {
	o.RowId = i
}

type Op2 struct {
	RowId  uint64 `pack:"I,pk,snappy"   json:"row_id"` // internal: unique row id
	Volume int32  `pack:"v,snappy"      json:"volume"` // stats: sum of transacted tezos volume
}

func (o Op2) ID() uint64 {
	return o.RowId
}

func (o *Op2) SetID(i uint64) {
	o.RowId = i
}

var _ pack.Item = (*Op)(nil)

const (
	OpPackSizeLog2         = 15  // 32k packs ~4M
	OpJournalSizeLog2      = 16  // 64k - search for spending op, so keep small
	OpCacheSize            = 128 // 128=512MB
	OpFillLevel            = 100
	OpIndexPackSizeLog2    = 15   // 16k packs (32k split size) ~256k
	OpIndexJournalSizeLog2 = 16   // 64k
	OpIndexCacheSize       = 1024 // ~256M
	OpIndexFillLevel       = 90
	OpTableKey             = "op"
	OpTable2Key            = "op2"
	DbLabel                = "XTZ"
)

var (
	verbose  bool
	debug    bool
	trace    bool
	dbname   string
	flags    = flag.NewFlagSet("opdb", flag.ContinueOnError)
	boltopts = &bolt.Options{
		Timeout:      time.Second, // open timeout when file is locked
		NoGrowSync:   true,        // assuming Docker + XFS
		ReadOnly:     false,
		NoSync:       true, // skip fsync (DANGEROUS on crashes)
		FreelistType: bolt.FreelistMapType,
	}
)

func Create(path string, dbOpts interface{}) (*pack.Table, error) {
	fields, err := pack.Fields(Op2{})
	if err != nil {
		return nil, err
	}
	db, err := pack.CreateDatabaseIfNotExists(filepath.Dir(path), OpTable2Key, "*", boltopts)
	if err != nil {
		return nil, fmt.Errorf("creating %s database: %v", OpTable2Key, err)
	}

	table, err := db.CreateTableIfNotExists(
		OpTable2Key,
		fields,
		pack.Options{
			PackSizeLog2:    OpPackSizeLog2,
			JournalSizeLog2: OpJournalSizeLog2,
			CacheSize:       OpCacheSize,
			FillLevel:       OpFillLevel,
		})
	if err != nil {
		db.Close()
		return nil, err
	}

	/*
		_, err = table.CreateIndexIfNotExists(
			"hash",
			fields.Find("H"),   // op hash field (32 byte op hashes)
			pack.IndexTypeHash, // hash table, index stores hash(field) -> pk value
			pack.Options{
				PackSizeLog2:    OpIndexPackSizeLog2,
				JournalSizeLog2: OpIndexJournalSizeLog2,
				CacheSize:       OpIndexCacheSize,
				FillLevel:       OpIndexFillLevel,
			})
		if err != nil {
			table.Close()
			db.Close()
			return nil, err
		}*/

	return table, nil
}

func Open(path string) (*pack.Table, error) {
	db, err := pack.OpenDatabase(filepath.Dir(path), OpTableKey, "*", boltopts)
	if err != nil {
		return nil, err
	}
	return db.Table(
		OpTableKey,
		pack.Options{
			JournalSizeLog2: OpJournalSizeLog2,
			CacheSize:       OpCacheSize,
		},
		pack.Options{
			JournalSizeLog2: OpIndexJournalSizeLog2,
			CacheSize:       OpIndexCacheSize,
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

func LookupOpByIds(ctx context.Context, table *pack.Table, ids []uint64) ([]*Op, error) {
	ops := make([]*Op, len(ids))
	var count int
	err := table.StreamLookup(ctx, ids, func(r pack.Row) error {
		if count >= len(ops) {
			return io.EOF
		}
		op := &Op{}
		if err := r.Decode(op); err != nil {
			return err
		}
		ops[count] = op
		count++
		return nil
	})
	if err != nil && err != io.EOF {
		return nil, err
	}
	if count == 0 {
		return nil, fmt.Errorf("no op found")
	}
	ops = ops[:count]
	return ops, nil
}

func ListOpTypes(ctx context.Context, table *pack.Table, typ OpType, limit int) ([]*Op, error) {
	q := pack.Query{
		Name:       "list_op_type",
		Conditions: make(pack.ConditionList, 0),
		Limit:      limit,
	}
	q.Conditions = append(q.Conditions, pack.Condition{
		Field: table.Fields().Find("t"), // search for type field
		Mode:  pack.FilterModeEqual,
		Value: int64(typ), // must be int64
	})

	ops := make([]*Op, 0)
	err := table.Stream(ctx, q, func(r pack.Row) error {
		op := &Op{}
		if err := r.Decode(op); err != nil {
			return err
		}
		ops = append(ops, op)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return ops, nil
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
	// table, err := Open(".")
	// if err != nil {
	// 	return err
	// }

	table2, err := Create(".", nil)
	if err != nil {
		return err
	}
	fmt.Printf("Created Table 2\n")

	for i := 0; i < 1; i++ {
		var row_op = Op2{
			Volume: int32(i),
		}

		err = table2.Insert(context.Background(), &row_op)
		if err != nil {
			return err
		}
	}
	// ops, err := ListOpTypes(context.Background(), table, OpTypeTransaction, 100)
	// if err != nil {
	// 	return err
	// }

	// // do smth with the ops
	// var totalVolume int64
	// for _, o := range ops {
	// 	totalVolume += o.Volume
	// }
	// fmt.Printf("Total Volume is %f\n", float64(totalVolume)/1000000)

	if err := Close(table2); err != nil {
		return err
	}
	fmt.Print("Closed Table 2\n")

	return nil
}
