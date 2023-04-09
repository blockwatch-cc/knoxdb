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
	RowId              uint64 `knox:"I,pk,snappy" json:"row_id"`
	Hash               []byte `knox:"H"           json:"hash"`
	DelegateId         uint64 `knox:"D,snappy"    json:"delegate_id"`
	ManagerId          uint64 `knox:"M,snappy"    json:"manager_id"`
	Pubkey             []byte `knox:"k"           json:"pubkey"`
	Type               int    `knox:"t,snappy"    json:"address_type"`
	FirstIn            int64  `knox:"i,snappy"    json:"first_in"`
	FirstOut           int64  `knox:"o,snappy"    json:"first_out"`
	LastIn             int64  `knox:"J,snappy"    json:"last_in"`
	LastOut            int64  `knox:"O,snappy"    json:"last_out"`
	FirstSeen          int64  `knox:"0,snappy"    json:"first_seen"`
	LastSeen           int64  `knox:"l,snappy"    json:"last_seen"`
	DelegatedSince     int64  `knox:"+,snappy"    json:"delegated_since"`
	DelegateSince      int64  `knox:"*,snappy"    json:"delegate_since"`
	TotalReceived      int64  `knox:"R,snappy"    json:"total_received"`
	TotalSent          int64  `knox:"S,snappy"    json:"total_sent"`
	TotalBurned        int64  `knox:"B,snappy"    json:"total_burned"`
	TotalFeesPaid      int64  `knox:"F,snappy"    json:"total_fees_paid"`
	TotalRewardsEarned int64  `knox:"W,snappy"    json:"total_rewards_earned"`
	TotalFeesEarned    int64  `knox:"E,snappy"    json:"total_fees_earned"`
	TotalLost          int64  `knox:"L,snappy"    json:"total_lost"` // lost due to denounciation
	FrozenDeposits     int64  `knox:"z,snappy"    json:"frozen_deposits"`
	FrozenRewards      int64  `knox:"Z,snappy"    json:"frozen_rewards"`
	FrozenFees         int64  `knox:"Y,snappy"    json:"frozen_fees"`
	UnclaimedBalance   int64  `knox:"U,snappy"    json:"unclaimed_balance"` // vesting or not activated
	SpendableBalance   int64  `knox:"s,snappy"    json:"spendable_balance"`
	DelegatedBalance   int64  `knox:"~,snappy"    json:"delegated_balance"`
	TotalDelegations   int64  `knox:">,snappy"    json:"total_delegations"`  // from delegate ops
	ActiveDelegations  int64  `knox:"a,snappy"    json:"active_delegations"` // with non-zero balance
	IsFunded           bool   `knox:"f,snappy"    json:"is_funded"`
	IsActivated        bool   `knox:"A,snappy"    json:"is_activated"` // bc: fundraiser account
	IsVesting          bool   `knox:"V,snappy"    json:"is_vesting"`   // bc: vesting contract account
	IsSpendable        bool   `knox:"p,snappy"    json:"is_spendable"` // manager can move funds without running any code
	IsDelegatable      bool   `knox:"?,snappy"    json:"is_delegatable"`
	IsDelegated        bool   `knox:"=,snappy"    json:"is_delegated"`
	IsRevealed         bool   `knox:"r,snappy"    json:"is_revealed"`
	IsDelegate         bool   `knox:"d,snappy"    json:"is_delegate"`
	IsActiveDelegate   bool   `knox:"v,snappy"    json:"is_active_delegate"`
	IsContract         bool   `knox:"c,snappy"    json:"is_contract"` // smart contract with code
	BlocksBaked        int    `knox:"b,snappy"    json:"blocks_baked"`
	BlocksMissed       int    `knox:"m,snappy"    json:"blocks_missed"`
	BlocksStolen       int    `knox:"n,snappy"    json:"blocks_stolen"`
	BlocksEndorsed     int    `knox:"e,snappy"    json:"blocks_endorsed"`
	SlotsEndorsed      int    `knox:"x,snappy"    json:"slots_endorsed"`
	SlotsMissed        int    `knox:"y,snappy"    json:"slots_missed"`
	NOps               int    `knox:"1,snappy"    json:"n_ops"`         // stats: successful operation count
	NOpsFailed         int    `knox:"2,snappy"    json:"n_ops_failed"`  // stats: failed operation coiunt
	NTx                int    `knox:"3,snappy"    json:"n_tx"`          // stats: number of Tx operations
	NDelegation        int    `knox:"4,snappy"    json:"n_delegation"`  // stats: number of Delegations operations
	NOrigination       int    `knox:"5,snappy"    json:"n_origination"` // stats: number of Originations operations
	NProposal          int    `knox:"6,snappy"    json:"n_proposal"`    // stats: number of Proposals operations
	NBallot            int    `knox:"7,snappy"    json:"n_ballot"`      // stats: number of Ballots operations
	TokenGenMin        int64  `knox:"g,snappy"    json:"token_gen_min"` // hops
	TokenGenMax        int64  `knox:"G,snappy"    json:"token_gen_max"` // hops
	GracePeriod        int64  `knox:"P,snappy"    json:"grace_period"`  // deactivation cycle
	CallStats          []byte `knox:"C,snappy"    json:"call_stats"`    // per entrypoint call statistics for contracts

	// used during block processing, not stored in DB
	IsNew      bool `knox:"-" json:"-"` // first seen this block
	WasFunded  bool `knox:"-" json:"-"` // true if account was funded before processing this block
	IsDirty    bool `knox:"-" json:"-"` // indicates an update happened
	MustDelete bool `knox:"-" json:"-"` // indicates the account should be deleted (during rollback)
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
