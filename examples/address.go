package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"io"

	_ "blockwatch.cc/knoxdb/store/bolt"
	_ "go.etcd.io/bbolt"

	"blockwatch.cc/knoxdb/pack"
	"github.com/echa/log"
)

func main() {
	log.SetLevel(log.LevelDebug)
	pack.UseLogger(log.Log)

	if err := run(); err != nil {
		fmt.Printf("Error: %v\n", err)
	}
}

// var KeyStream20 chan []byte
// var KeyStream32 chan []byte

// func RunKeyGenerator20() {
// 	// start asynchronous key generator
// 	KeyStream20 = make(chan []byte, 100)
// 	go func(ch chan []byte) {
// 		for {
// 			ch <- GenerateRandomKey(20)
// 		}
// 	}(KeyStream20)
// }

// func RunKeyGenerator32() {
// 	// start asynchronous key generator
// 	KeyStream32 = make(chan []byte, 100)
// 	go func(ch chan []byte) {
// 		for {
// 			ch <- GenerateRandomKey(32)
// 		}
// 	}(KeyStream32)
// }

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

type Account struct {
	RowId              uint64 `pack:"I,pk,snappy" json:"row_id"`
	Hash               []byte `pack:"H"           json:"hash"`
	DelegateId         uint64 `pack:"D,snappy"    json:"delegate_id"`
	ManagerId          uint64 `pack:"M,snappy"    json:"manager_id"`
	Pubkey             []byte `pack:"k"           json:"pubkey"`
	Type               int    `pack:"t,snappy"    json:"address_type"`
	FirstIn            int64  `pack:"i,snappy"    json:"first_in"`
	FirstOut           int64  `pack:"o,snappy"    json:"first_out"`
	LastIn             int64  `pack:"J,snappy"    json:"last_in"`
	LastOut            int64  `pack:"O,snappy"    json:"last_out"`
	FirstSeen          int64  `pack:"0,snappy"    json:"first_seen"`
	LastSeen           int64  `pack:"l,snappy"    json:"last_seen"`
	DelegatedSince     int64  `pack:"+,snappy"    json:"delegated_since"`
	DelegateSince      int64  `pack:"*,snappy"    json:"delegate_since"`
	TotalReceived      int64  `pack:"R,snappy"    json:"total_received"`
	TotalSent          int64  `pack:"S,snappy"    json:"total_sent"`
	TotalBurned        int64  `pack:"B,snappy"    json:"total_burned"`
	TotalFeesPaid      int64  `pack:"F,snappy"    json:"total_fees_paid"`
	TotalRewardsEarned int64  `pack:"W,snappy"    json:"total_rewards_earned"`
	TotalFeesEarned    int64  `pack:"E,snappy"    json:"total_fees_earned"`
	TotalLost          int64  `pack:"L,snappy"    json:"total_lost"` // lost due to denounciation
	FrozenDeposits     int64  `pack:"z,snappy"    json:"frozen_deposits"`
	FrozenRewards      int64  `pack:"Z,snappy"    json:"frozen_rewards"`
	FrozenFees         int64  `pack:"Y,snappy"    json:"frozen_fees"`
	UnclaimedBalance   int64  `pack:"U,snappy"    json:"unclaimed_balance"` // vesting or not activated
	SpendableBalance   int64  `pack:"s,snappy"    json:"spendable_balance"`
	DelegatedBalance   int64  `pack:"~,snappy"    json:"delegated_balance"`
	TotalDelegations   int64  `pack:">,snappy"    json:"total_delegations"`  // from delegate ops
	ActiveDelegations  int64  `pack:"a,snappy"    json:"active_delegations"` // with non-zero balance
	IsFunded           bool   `pack:"f,snappy"    json:"is_funded"`
	IsActivated        bool   `pack:"A,snappy"    json:"is_activated"` // bc: fundraiser account
	IsVesting          bool   `pack:"V,snappy"    json:"is_vesting"`   // bc: vesting contract account
	IsSpendable        bool   `pack:"p,snappy"    json:"is_spendable"` // manager can move funds without running any code
	IsDelegatable      bool   `pack:"?,snappy"    json:"is_delegatable"`
	IsDelegated        bool   `pack:"=,snappy"    json:"is_delegated"`
	IsRevealed         bool   `pack:"r,snappy"    json:"is_revealed"`
	IsDelegate         bool   `pack:"d,snappy"    json:"is_delegate"`
	IsActiveDelegate   bool   `pack:"v,snappy"    json:"is_active_delegate"`
	IsContract         bool   `pack:"c,snappy"    json:"is_contract"` // smart contract with code
	BlocksBaked        int    `pack:"b,snappy"    json:"blocks_baked"`
	BlocksMissed       int    `pack:"m,snappy"    json:"blocks_missed"`
	BlocksStolen       int    `pack:"n,snappy"    json:"blocks_stolen"`
	BlocksEndorsed     int    `pack:"e,snappy"    json:"blocks_endorsed"`
	SlotsEndorsed      int    `pack:"x,snappy"    json:"slots_endorsed"`
	SlotsMissed        int    `pack:"y,snappy"    json:"slots_missed"`
	NOps               int    `pack:"1,snappy"    json:"n_ops"`         // stats: successful operation count
	NOpsFailed         int    `pack:"2,snappy"    json:"n_ops_failed"`  // stats: failed operation coiunt
	NTx                int    `pack:"3,snappy"    json:"n_tx"`          // stats: number of Tx operations
	NDelegation        int    `pack:"4,snappy"    json:"n_delegation"`  // stats: number of Delegations operations
	NOrigination       int    `pack:"5,snappy"    json:"n_origination"` // stats: number of Originations operations
	NProposal          int    `pack:"6,snappy"    json:"n_proposal"`    // stats: number of Proposals operations
	NBallot            int    `pack:"7,snappy"    json:"n_ballot"`      // stats: number of Ballots operations
	TokenGenMin        int64  `pack:"g,snappy"    json:"token_gen_min"` // hops
	TokenGenMax        int64  `pack:"G,snappy"    json:"token_gen_max"` // hops
	GracePeriod        int64  `pack:"P,snappy"    json:"grace_period"`  // deactivation cycle
	CallStats          []byte `pack:"C,snappy"    json:"call_stats"`    // per entrypoint call statistics for contracts

	// used during block processing, not stored in DB
	IsNew      bool `pack:"-" json:"-"` // first seen this block
	WasFunded  bool `pack:"-" json:"-"` // true if account was funded before processing this block
	IsDirty    bool `pack:"-" json:"-"` // indicates an update happened
	MustDelete bool `pack:"-" json:"-"` // indicates the account should be deleted (during rollback)
}

func (a Account) ID() uint64 {
	return uint64(a.RowId)
}

func (a *Account) SetID(id uint64) {
	a.RowId = id
}

var opts = pack.Options{
	PackSizeLog2:    10, // 2 << 10
	JournalSizeLog2: 11, // 2 << 11
	CacheSize:       2,
	FillLevel:       90,
}

func CreateTable() (*pack.DB, *pack.Table, error) {
	fields, err := pack.Fields(Account{})
	if err != nil {
		return nil, nil, err
	}
	db, err := pack.CreateDatabaseIfNotExists(".", "address", "TEST", nil)
	if err != nil {
		return nil, nil, fmt.Errorf("creating database: %v", err)
	}

	table, err := db.CreateTableIfNotExists(
		"address",
		fields,
		opts)
	if err != nil {
		db.Close()
		return nil, nil, err
	}

	_, err = table.CreateIndexIfNotExists(
		"hash",
		fields.Find("H"),   // tx hash field (32 byte tx hashes)
		pack.IndexTypeHash, // hash table, index stores hash(field) -> pk value
		opts)
	if err != nil {
		table.Close()
		db.Close()
		return nil, nil, err
	}

	return db, table, nil
}

func run() error {
	log.Info("Open...")
	ctx := context.Background()
	db, table, err := CreateTable()
	if err != nil {
		return err
	}
	defer table.Close()
	defer db.Close()
	accs := make([]pack.Item, 0)
	accHashes := make(map[uint64][]byte)
	accKeys := make(map[uint64][]byte)
	for round := 0; round < 32; round++ {
		for i := uint64(1); i <= 2<<10; i++ {
			acc := &Account{
				RowId:  i + uint64(round*(2<<10)),
				Hash:   GenerateRandomKey(20),
				Pubkey: GenerateRandomKey(32),
			}
			h := make([]byte, len(acc.Hash))
			copy(h, acc.Hash)
			accHashes[acc.RowId] = h
			h = make([]byte, len(acc.Pubkey))
			copy(h, acc.Pubkey)
			accKeys[acc.RowId] = h
			accs = append(accs, acc)
		}
		log.Info("Insert...")
		if err := table.Insert(ctx, accs); err != nil {
			return err
		}
		crossCheck(ctx, table, accHashes, accKeys)
	}

	log.Info("Flush...")
	if err := table.Flush(ctx); err != nil {
		return err
	}
	crossCheck(ctx, table, accHashes, accKeys)

	// replace all keys
	accs = accs[:0]
	err = table.Stream(ctx, pack.Query{
		Name:    "replace",
		NoCache: true,
		Fields:  table.Fields(),
	}, func(r pack.Row) error {
		acc := &Account{}
		if err := r.Decode(acc); err != nil {
			return err
		}
		if acc.RowId%2 == 1 {
			acc.Hash = GenerateRandomKey(20)
			h := make([]byte, len(acc.Hash))
			copy(h, acc.Hash)
			accHashes[acc.RowId] = h
			// fmt.Printf("%02d new hash %x => %x\n", acc.RowId, acc.Hash, acc.Pubkey)
			accs = append(accs, acc)
		}
		return nil
	})

	log.Info("Update...")
	if err := table.Update(ctx, accs); err != nil {
		return err
	}
	crossCheck(ctx, table, accHashes, accKeys)

	log.Info("Flush...")
	if err := table.Flush(ctx); err != nil {
		return err
	}
	crossCheck(ctx, table, accHashes, accKeys)

	return nil
}

func dump(ctx context.Context, table *pack.Table) error {
	// walk and print addresses and keys
	acc := &Account{}
	err := table.Stream(ctx, pack.Query{
		Name:    "check",
		NoCache: true,
		Fields:  table.Fields(),
	}, func(r pack.Row) error {
		if err := r.Decode(acc); err != nil {
			return err
		}
		fmt.Printf("%02d %x => %x\n", acc.RowId, acc.Hash, acc.Pubkey)
		return nil
	})
	return err
}

func crossCheck(ctx context.Context, table *pack.Table, accHashes, accKeys map[uint64][]byte) error {
	// walk and print addresses and keys
	acc := &Account{}
	err := table.Stream(ctx, pack.Query{
		Name:    "check",
		NoCache: true,
		Fields:  table.Fields(),
	}, func(r pack.Row) error {
		if err := r.Decode(acc); err != nil {
			return err
		}
		if bytes.Compare(acc.Hash, accHashes[acc.RowId]) != 0 {
			fmt.Printf("ERROR %02d wrong hash %x => %x\n", acc.RowId, acc.Hash, accHashes[acc.RowId])
		}
		if bytes.Compare(acc.Pubkey, accKeys[acc.RowId]) != 0 {
			fmt.Printf("ERROR %02d wrong key %x => %x\n", acc.RowId, acc.Pubkey, accKeys[acc.RowId])
		}
		return nil
	})
	return err
}
