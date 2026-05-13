// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package engine

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"path/filepath"
	"runtime"
	"sync"

	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/internal/wal"
	"blockwatch.cc/knoxdb/pkg/schema"
	"blockwatch.cc/knoxdb/pkg/store"
	"blockwatch.cc/knoxdb/pkg/util"
	"github.com/echa/log"
)

// TODO Design
// - handle schema evolution (latest schema is referenced by object, list of prev schemas?)
// - foreign tables + engines
// - views
// - snapshots
// - streams
//
// buckets
// - options: key=name_hash, val=options
// - schemas: key=name_hash:schema_hash, val=schema
// - database
//   - checkpoint
// - tables
//   - name_hash
//     - name
//     - schema_hash
//     - last_id -> state
//     - num_tuples -> state
//     - opts -> options
// - indexes
//   - name_hash
//     - name
//     - schema_hash
//     - table_hash
//     - type
//     - status (empty,rebuilding,ready) -> state
//     - opts -> options
// - enums
//   - name_hash
//     - name
//     - data (string values)
// - views (todo)
//   - name_hash
//     - name
//     - schema_hash
//     - query
//     - opts -> options
// - snapshots (todo)
// - streams (todo)
//

const (
	CATALOG_NAME    = "_catalog.db"
	CATALOG_TYPE    = "knoxdb.schemas.catalog.v1"
	CATALOG_VERSION = 1
)

var (
	// buckets
	databaseKey  = []byte("database")  // unused
	schemasKey   = []byte("schemas")   // tag:schema => serialized schema
	optionsKey   = []byte("options")   // tag => serialized options (db, table, store, view, ..)
	tablesKey    = []byte("tables")    // tag => name=str, schema=u64
	indexesKey   = []byte("indexes")   // tag => name=str, schema=u64, table=u64, status=u8
	viewsKey     = []byte("views")     // key => name=str, schema=u64, data=query
	enumsKey     = []byte("enums")     // key => name=str, data=package (id, string)
	snapshotsKey = []byte("snapshots") // key => serialized snapshot config
	streamsKey   = []byte("streams")   // key => serialized stream config

	// keys
	schemaKey = []byte("schema")
	tableKey  = []byte("table")
	nameKey   = []byte("name")
	// statusKey     = []byte("status")
	dataKey       = []byte("data")
	checkpointKey = []byte("checkpoint")
)

var (
	BE = binary.BigEndian
	LE = binary.LittleEndian
)

var defaultDatabaseOptions = Options{
	Path:            "./db",
	CacheSize:       16 << 20,
	WalSegmentSize:  128 << 20,
	WalRecoveryMode: wal.RecoveryModeTruncate,
	MaxWorkers:      runtime.NumCPU(),
	MaxTasks:        16,
	Engine:          "pack",
	PackSize:        1 << 14, // 16k
	JournalSize:     1 << 15, // 32k
	JournalSegments: 16,
	Driver:          "bolt",
	TxMaxSize:       10 << 24, // 16 MB
	PageSize:        1 << 16,  // 64kB
	PageFill:        0.9,
	Log:             log.Disabled,
}

// knoxdb.schemas.catalog.v1
type Catalog struct {
	mu         sync.RWMutex           // guard write access to catalog internals
	db         store.DBManager        // catalog database file
	path       string                 // database path, used for object file cleanup
	name       string                 // database name
	id         uint64                 // database tag
	wal        *wal.Wal               // copy of wal managed by engine
	checkpoint wal.LSN                // latest wal checkpoint that is safe in db
	pending    map[types.XID][]Object // active txids pending updates waiting for commit/abort
	log        log.Logger             // logger handle
}

func NewCatalog(name string) *Catalog {
	return &Catalog{
		name:    name,
		id:      types.TaggedHash(types.ObjectTagDatabase, name),
		pending: make(map[types.XID][]Object),
		log:     log.Disabled,
	}
}

func (c *Catalog) WithWal(w *wal.Wal) *Catalog {
	c.wal = w
	return c
}

func (c *Catalog) WithLogger(l log.Logger) *Catalog {
	c.log = l
	return c
}

func (c *Catalog) Create(ctx context.Context, opts Options) error {
	c.path = filepath.Join(opts.Path, c.name)
	c.log.Debugf("create catalog at %s", c.path)
	db, err := store.Create(opts.CatalogOptions(c.name)...)
	if err != nil {
		return fmt.Errorf("create catalog %s: %w", c.name, err)
	}

	// init table storage
	err = db.Update(func(tx store.Tx) error {
		for _, key := range [][]byte{
			databaseKey,
			schemasKey,
			optionsKey,
			tablesKey,
			indexesKey,
			viewsKey,
			enumsKey,
			snapshotsKey,
			streamsKey,
		} {
			if _, err := tx.CreateBucket(key); err != nil {
				return err
			}
		}
		var b [8]byte
		bucket, err := tx.Bucket(databaseKey)
		if err != nil {
			return err
		}
		return bucket.Put(checkpointKey, b[:])
	})
	if err != nil {
		_ = db.Close()
		return err
	}
	c.db = db

	return nil
}

func (c *Catalog) Open(ctx context.Context, opts Options) error {
	c.path = filepath.Join(opts.Path, c.name)
	c.log.Debugf("open catalog at %s", c.path)
	db, err := store.Open(opts.CatalogOptions(c.name)...)
	if err != nil {
		c.log.Errorf("open catalog %s: %v", c.name, err)
		return ErrDatabaseCorrupt
	}

	// load catalog checkpoint
	err = db.View(func(tx store.Tx) error {
		val, err := store.GetKey(tx, databaseKey, checkpointKey)
		if err != nil {
			return ErrNoKey
		}
		c.checkpoint = wal.LSN(BE.Uint64(val))
		return nil
	})
	if err != nil {
		db.Close()
		return err
	}

	c.db = db

	return nil
}

func (c *Catalog) Close(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.db == nil {
		return nil
	}
	clear(c.pending)

	// store checkpoint record in wal and write checkpoint
	var err error
	if c.wal != nil {
		err = c.doCheckpoint(ctx)
	}

	// close db
	if err2 := c.db.Close(); err2 != nil && err == nil {
		err = err2
	}
	c.db = nil
	c.wal = nil
	c.log = nil

	return err
}

func (c *Catalog) ForceClose() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.db == nil {
		return nil
	}
	clear(c.pending)
	err := c.db.Close()
	c.db = nil
	c.wal = nil
	c.log = nil

	return err
}

// returns catalog checkpoint position in WAL
func (c *Catalog) Checkpoint() wal.LSN {
	return c.checkpoint
}

func (c *Catalog) PutCheckpoint(ctx context.Context, lsn wal.LSN) error {
	writeCheckpoint := func(tx store.Tx) error {
		bucket, err := tx.Bucket(databaseKey)
		if err != nil {
			return ErrDatabaseCorrupt
		}
		var b [8]byte
		BE.PutUint64(b[:], uint64(lsn))
		err = bucket.Put(checkpointKey, b[:])
		if err != nil {
			return err
		}
		c.checkpoint = lsn
		return nil
	}

	// when run with a managed tx we reuse it here, otherwise we open
	// a separate storage tx
	if etx := GetTx(ctx); etx != nil {
		tx, err := etx.CatalogTx(c.db, true)
		if err != nil {
			return err
		}
		return writeCheckpoint(tx)
	} else {
		return c.db.Update(writeCheckpoint)
	}
}

func (c *Catalog) GetSchema(ctx context.Context, key uint64) (*schema.Schema, error) {
	tx, err := GetTx(ctx).CatalogTx(c.db, false)
	if err != nil {
		return nil, err
	}
	buf, err := store.GetKey(tx, schemasKey, util.U64Bytes(key))
	if err != nil {
		return nil, ErrNoKey
	}
	s := schema.NewSchema()
	if err := s.UnmarshalBinary(buf); err != nil {
		return nil, err
	}
	return s, nil
}

func (c *Catalog) PutSchema(ctx context.Context, s *schema.Schema) error {
	tx, err := GetTx(ctx).CatalogTx(c.db, true)
	if err != nil {
		return err
	}
	bucket, err := tx.Bucket(schemasKey)
	if err != nil {
		return ErrDatabaseCorrupt
	}
	buf, err := s.MarshalBinary()
	if err != nil {
		return err
	}
	return bucket.Put(util.U64Bytes(s.Hash), buf)
}

func (c *Catalog) GetIndexSchema(ctx context.Context, key uint64) (*schema.IndexSchema, error) {
	tx, err := GetTx(ctx).CatalogTx(c.db, false)
	if err != nil {
		return nil, err
	}
	buf, err := store.GetKey(tx, schemasKey, util.U64Bytes(key))
	if err != nil {
		return nil, ErrNoKey
	}
	s := &schema.IndexSchema{}
	if err := s.UnmarshalBinary(buf); err != nil {
		return nil, err
	}
	s.Base, err = c.GetSchema(ctx, s.Base.Hash)
	if err != nil {
		return nil, err
	}
	return s, nil
}

func (c *Catalog) PutIndexSchema(ctx context.Context, s *schema.IndexSchema) error {
	tx, err := GetTx(ctx).CatalogTx(c.db, true)
	if err != nil {
		return err
	}
	bucket, err := tx.Bucket(schemasKey)
	if err != nil {
		return ErrDatabaseCorrupt
	}
	buf, err := s.MarshalBinary()
	if err != nil {
		return err
	}
	return bucket.Put(util.U64Bytes(s.Hash()), buf)
}

func (c *Catalog) DelSchema(ctx context.Context, key uint64) error {
	tx, err := GetTx(ctx).CatalogTx(c.db, true)
	if err != nil {
		return err
	}
	bucket, err := tx.Bucket(schemasKey)
	if err != nil {
		return ErrDatabaseCorrupt
	}
	return bucket.Delete(util.U64Bytes(key))
}

func (c *Catalog) GetOptions(ctx context.Context, key uint64, opts any) error {
	s, err := schema.SchemaOf(opts)
	if err != nil {
		return err
	}
	tx, err := GetTx(ctx).CatalogTx(c.db, false)
	if err != nil {
		return err
	}
	buf, err := store.GetKey(tx, optionsKey, util.U64Bytes(key))
	if err != nil {
		return ErrNoKey
	}
	dec := schema.NewDecoder(s)
	if err := dec.Decode(buf, opts); err != nil {
		return err
	}
	return nil
}

func (c *Catalog) PutOptions(ctx context.Context, key uint64, opts any) error {
	if opts == nil {
		return nil
	}
	s, err := schema.SchemaOf(opts)
	if err != nil {
		return err
	}
	tx, err := GetTx(ctx).CatalogTx(c.db, true)
	if err != nil {
		return err
	}
	bucket, err := tx.Bucket(optionsKey)
	if err != nil {
		return ErrDatabaseCorrupt
	}
	enc := schema.NewEncoder(s)
	buf, err := enc.Encode(opts, nil)
	if err != nil {
		return err
	}
	return bucket.Put(util.U64Bytes(key), buf)
}

func (c *Catalog) DelOptions(ctx context.Context, key uint64) error {
	tx, err := GetTx(ctx).CatalogTx(c.db, true)
	if err != nil {
		return err
	}
	bucket, err := tx.Bucket(optionsKey)
	if err != nil {
		return ErrDatabaseCorrupt
	}
	return bucket.Delete(util.U64Bytes(key))
}

func (c *Catalog) ListTables(ctx context.Context) ([]uint64, error) {
	return c.listObjectKeys(ctx, tablesKey)
}

func (c *Catalog) GetTable(ctx context.Context, key uint64) (s *schema.Schema, o Options, err error) {
	var tx store.Tx
	tx, err = GetTx(ctx).CatalogTx(c.db, false)
	if err != nil {
		return
	}
	skey, err := store.GetKey(tx, tablesKey, util.U64Bytes(key), schemaKey)
	if err != nil {
		err = ErrNoKey
		return
	}
	s, err = c.GetSchema(ctx, BE.Uint64(skey))
	if err != nil {
		return
	}
	err = c.GetOptions(ctx, key, &o)
	return
}

func (c *Catalog) AddTable(ctx context.Context, key uint64, s *schema.Schema, o Options) error {
	if err := c.PutSchema(ctx, s); err != nil {
		return err
	}

	if err := c.PutOptions(ctx, key, &o); err != nil {
		return err
	}

	// create table bucket, add table name and current schema
	tx, err := GetTx(ctx).CatalogTx(c.db, true)
	if err != nil {
		return err
	}
	bucket, err := tx.Bucket(tablesKey)
	if err != nil {
		return ErrDatabaseCorrupt
	}
	bucket, err = bucket.CreateBucket(util.U64Bytes(key))
	if err != nil {
		return err
	}
	if err := bucket.Put(schemaKey, util.U64Bytes(s.Hash)); err != nil {
		return err
	}
	if err := bucket.Put(nameKey, []byte(s.Name)); err != nil {
		return err
	}

	return nil
}

func (c *Catalog) DropTable(ctx context.Context, key uint64) error {
	// TODO: we don't have a reference to previous schema versions/hashes for removal
	tx, err := GetTx(ctx).CatalogTx(c.db, true)
	if err != nil {
		return err
	}
	tables, err := tx.Bucket(tablesKey)
	if err != nil {
		return ErrDatabaseCorrupt
	}
	bucket, err := tables.Bucket(util.U64Bytes(key))
	if err != nil {
		return ErrNoTable
	}
	skey, err := bucket.Get(schemaKey)
	if err != nil {
		return ErrNoKey
	}
	if err := tables.DeleteBucket(util.U64Bytes(key)); err != nil {
		return err
	}
	if err := c.DelOptions(ctx, key); err != nil {
		return err
	}
	if err := c.DelSchema(ctx, BE.Uint64(skey)); err != nil {
		return err
	}

	return nil
}

func (c *Catalog) GetIndex(ctx context.Context, key uint64) (s *schema.IndexSchema, o Options, err error) {
	var tx store.Tx
	tx, err = GetTx(ctx).CatalogTx(c.db, false)
	if err != nil {
		return
	}
	skey, err := store.GetKey(tx, indexesKey, util.U64Bytes(key), schemaKey)
	if err != nil {
		err = ErrNoKey
		return
	}
	s, err = c.GetIndexSchema(ctx, BE.Uint64(skey))
	if err != nil {
		return
	}
	err = c.GetOptions(ctx, key, &o)
	return
}

func (c *Catalog) ListIndexes(ctx context.Context, key uint64) ([]uint64, error) {
	tx, err := GetTx(ctx).CatalogTx(c.db, false)
	if err != nil {
		return nil, err
	}
	bucket, err := tx.Bucket(indexesKey)
	if err != nil {
		return nil, ErrDatabaseCorrupt
	}
	res := make([]uint64, 0)
	for k, b := range bucket.Buckets() {
		tkey, err := b.Get(tableKey)
		if err != nil {
			return nil, ErrNoKey
		}
		if BE.Uint64(tkey) == key {
			res = append(res, BE.Uint64(k))
		}
	}
	return res, nil
}

func (c *Catalog) AddIndex(ctx context.Context, ikey, tkey uint64, s *schema.IndexSchema, o Options) error {
	if err := c.PutIndexSchema(ctx, s); err != nil {
		return err
	}

	if err := c.PutOptions(ctx, ikey, &o); err != nil {
		return err
	}
	// create index bucket, add index name, current schema, table hash
	tx, err := GetTx(ctx).CatalogTx(c.db, true)
	if err != nil {
		return err
	}
	bucket, err := tx.Bucket(indexesKey)
	if err != nil {
		return ErrDatabaseCorrupt
	}
	bucket, err = bucket.CreateBucket(util.U64Bytes(ikey))
	if err != nil {
		return err
	}
	if err := bucket.Put(schemaKey, util.U64Bytes(s.Hash())); err != nil {
		return err
	}
	if err := bucket.Put(nameKey, []byte(s.Name)); err != nil {
		return err
	}
	if err := bucket.Put(tableKey, util.U64Bytes(tkey)); err != nil {
		return err
	}

	return nil
}

func (c *Catalog) DropIndex(ctx context.Context, key uint64) error {
	tx, err := GetTx(ctx).CatalogTx(c.db, true)
	if err != nil {
		return err
	}
	indexes, err := tx.Bucket(indexesKey)
	if err != nil {
		return ErrDatabaseCorrupt
	}
	bucket, err := indexes.Bucket(util.U64Bytes(key))
	if err != nil {
		return ErrNoIndex
	}
	skey, err := bucket.Get(schemaKey)
	if err != nil {
		return ErrNoKey
	}
	if err := indexes.DeleteBucket(util.U64Bytes(key)); err != nil {
		return err
	}
	if err := c.DelOptions(ctx, key); err != nil {
		return err
	}
	if err := c.DelSchema(ctx, BE.Uint64(skey)); err != nil {
		return err
	}

	return nil
}

func (c *Catalog) ListEnums(ctx context.Context) ([]uint64, error) {
	return c.listObjectKeys(ctx, enumsKey)
}

func (c *Catalog) GetEnum(ctx context.Context, key uint64) (e *schema.EnumDictionary, err error) {
	var tx store.Tx
	tx, err = GetTx(ctx).CatalogTx(c.db, false)
	if err != nil {
		return
	}
	bucket, err := store.GetBucket(tx, enumsKey, util.U64Bytes(key))
	if err != nil {
		err = ErrDatabaseCorrupt
		return
	}
	name, err := bucket.Get(nameKey)
	if err != nil {
		err = ErrNoKey
		return
	}
	data, err := bucket.Get(dataKey)
	if err != nil {
		err = ErrNoKey
		return
	}
	e = schema.NewEnumDictionary(string(name))
	err = e.UnmarshalBinary(data)
	return
}

func (c *Catalog) PutEnum(ctx context.Context, e *schema.EnumDictionary) error {
	tx, err := GetTx(ctx).CatalogTx(c.db, true)
	if err != nil {
		return err
	}
	bucket, err := store.GetBucket(tx, enumsKey, util.U64Bytes(e.Tag()))
	if err != nil {
		return ErrDatabaseCorrupt
	}
	buf, err := e.MarshalBinary()
	if err != nil {
		return err
	}
	return bucket.Put(dataKey, buf)
}

func (c *Catalog) AddEnum(ctx context.Context, e *schema.EnumDictionary) error {
	// create enum bucket, add enum name and data
	tx, err := GetTx(ctx).CatalogTx(c.db, true)
	if err != nil {
		return err
	}
	enums, err := tx.Bucket(enumsKey)
	if err != nil {
		return ErrDatabaseCorrupt
	}
	bucket, err := enums.CreateBucket(util.U64Bytes(e.Tag()))
	if err != nil {
		return err
	}
	if err := bucket.Put(nameKey, []byte(e.Name())); err != nil {
		return err
	}
	buf, err := e.MarshalBinary()
	if err != nil {
		return err
	}
	if err := bucket.Put(dataKey, buf); err != nil {
		return err
	}
	return nil
}

func (c *Catalog) DropEnum(ctx context.Context, key uint64) error {
	tx, err := GetTx(ctx).CatalogTx(c.db, true)
	if err != nil {
		return err
	}
	enums, err := tx.Bucket(enumsKey)
	if err != nil {
		return ErrDatabaseCorrupt
	}
	if err := enums.DeleteBucket(util.U64Bytes(key)); err != nil {
		return err
	}

	return nil
}

func (c *Catalog) listObjectKeys(ctx context.Context, bucketKey []byte) ([]uint64, error) {
	tx, err := GetTx(ctx).CatalogTx(c.db, false)
	if err != nil {
		return nil, err
	}
	bucket, err := tx.Bucket(bucketKey)
	if err != nil {
		return nil, ErrDatabaseCorrupt
	}
	res := make([]uint64, 0)
	for k := range bucket.Buckets() {
		res = append(res, BE.Uint64(k))
	}
	return res, nil
}

func (c *Catalog) append(ctx context.Context, o Object) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// fetch tx and append catalog flag (to forward commit call here)
	tx := GetTx(ctx).WithFlags(TxFlagCatalog)

	// write wal record
	buf, err := o.Encode()
	if err != nil {
		return err
	}

	_, err = c.wal.Write(&wal.Record{
		Type:   o.Action(),
		Tag:    types.ObjectTagDatabase,
		Entity: c.id,
		TxID:   tx.id,
		Data:   [][]byte{buf},
	})
	if err != nil {
		return err
	}

	// keep for commit/abort
	c.pending[tx.id] = append(c.pending[tx.id], o)

	return nil
}

func (c *Catalog) CommitTx(ctx context.Context, xid types.XID) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// check if this txn has any pending actions
	pending, ok := c.pending[xid]
	if !ok {
		return nil
	}

	// execute actions
	err := c.runCommitActions(ctx, pending)
	if err != nil {
		return err
	}

	// remove actions
	delete(c.pending, xid)

	// db store commit and checkpoint
	return c.doCheckpoint(ctx)
}

func (c *Catalog) AbortTx(ctx context.Context, xid types.XID) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	pending, ok := c.pending[xid]
	if !ok {
		return nil
	}

	// execute actions
	err := c.runAbortActions(ctx, pending)
	if err != nil {
		return err
	}

	// remove actions
	delete(c.pending, xid)

	return nil
}

// make catalog changes permanent and write checkpoint
func (c *Catalog) doCheckpoint(ctx context.Context) error {
	// must have no more pending txn
	if len(c.pending) > 0 {
		return nil
	}

	// only checkpoint when a managed tx is not failed and
	// we're not in read-only mode or wal is disabled in tx
	if GetEngine(ctx).IsReadOnly() {
		return nil
	}

	tx := GetTx(ctx)
	if tx != nil {
		if tx.Err() != nil {
			return nil
		}
		if tx.IsReadOnly() {
			return nil
		}
		if !tx.UseWal() {
			return nil
		}
	}

	// write checkpoint record to wal and sync so put below
	// stores a valid/existing wal lsn
	lsn, err := c.wal.WriteAndSync(&wal.Record{
		Type:   wal.RecordTypeCheckpoint,
		Tag:    types.ObjectTagDatabase,
		Entity: c.id,
	})
	if err != nil {
		return err
	}

	// store checkpoint in catalog db
	if err = c.PutCheckpoint(ctx, lsn); err != nil {
		return err
	}

	// // ensure changes are safe in catalog db
	// if tx != nil && tx.catTx != nil && tx.catTx.IsWriteable() {
	// 	// until this commit succeeds changes are not durable,
	// 	// but when it does the previous wal checkpoint will be
	// 	// referenced at next startup
	// 	err := tx.catTx.Commit()
	// 	if err != nil {
	// 		return err
	// 	}
	// 	tx.catTx = nil
	// 	tx.rtflags &^= TxFlagsCatalog
	// }

	return nil
}

func (c *Catalog) runAbortActions(ctx context.Context, pending []Object) error {
	for _, obj := range pending {
		var err error
		switch obj.Action() {
		case wal.RecordTypeInsert:
			err = obj.Drop(ctx)
		case wal.RecordTypeUpdate:
			// ignore
		case wal.RecordTypeDelete:
			// ignore
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *Catalog) runCommitActions(ctx context.Context, pending []Object) error {
	for _, obj := range pending {
		var err error
		switch obj.Action() {
		case wal.RecordTypeInsert:
			err = obj.Create(ctx)
		case wal.RecordTypeUpdate:
			err = obj.Update(ctx)
		case wal.RecordTypeDelete:
			err = obj.Drop(ctx)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// requires transaction in context
func (c *Catalog) Recover(ctx context.Context) error {
	c.log.Debugf("catalog: run wal recovery from lsn 0x%x", c.checkpoint)

	// read all wal records, of any record exists it must be rolled back
	// unless its txid is committed
	r := c.wal.NewReader()
	defer r.Close()
	defer clear(c.pending)

	// setup reader
	r.WithTag(types.ObjectTagDatabase)
	err := r.Seek(c.checkpoint)
	if err != nil {
		return err
	}

	// track max committed/aborted xid seen
	var xmax types.XID

	// we may have data from multiple txn in the wal and each txn may
	// have created, updated or removed multiple objects. some txn
	// may have committed, some may have aborted, some may have neither
	// during a crash. we assume object actions are idempotent (they check
	// state and skip when any update has already happened). Hence
	// we can safely replay any committed txn in wal order at commit time
	// (or abort it and clean up side effects like created files).
	for {
		rec, err := r.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		// handle catalog object creation, update and deletion on commit,
		// or rollback on abort
		// e.log.Debugf("Record %s", rec)

		// reconstruct and execute pending object actions
		switch rec.Type {
		case wal.RecordTypeCommit:
			err = c.runCommitActions(ctx, c.pending[rec.TxID])
			delete(c.pending, rec.TxID)
			xmax = max(xmax, rec.TxID)

		case wal.RecordTypeAbort:
			err = c.runAbortActions(ctx, c.pending[rec.TxID])
			delete(c.pending, rec.TxID)
			xmax = max(xmax, rec.TxID)

		case wal.RecordTypeInsert,
			wal.RecordTypeUpdate,
			wal.RecordTypeDelete:
			var obj Object
			obj, err = c.decodeWalRecord(ctx, rec)
			if obj != nil {
				c.pending[rec.TxID] = append(c.pending[rec.TxID], obj)
			}

		case wal.RecordTypeCheckpoint:
			// unlikely, but in case wal write succeeded and subsequent
			// catalog db store tx failed
			err = c.PutCheckpoint(ctx, rec.Lsn)

		default:
			err = fmt.Errorf("unexpected wal record: %s", rec)
		}
		if err != nil {
			return err
		}
	}

	// abort any pending object actions
	for xid := range c.pending {
		err = c.runAbortActions(ctx, c.pending[xid])
		delete(c.pending, xid)
		if err != nil {
			return err
		}
	}

	// update engine horizon
	GetEngine(ctx).UpdateTxHorizon(xmax)

	c.log.Debug("catalog: recovery done, writing new checkpoint")

	return c.doCheckpoint(ctx)
}

func (c *Catalog) decodeWalRecord(ctx context.Context, rec *wal.Record) (Object, error) {
	var obj Object
	switch types.ObjectTag(rec.Data[0][0]) {
	case types.ObjectTagTable:
		obj = &TableObject{cat: c}
	case types.ObjectTagEnum:
		obj = &EnumObject{cat: c}
	case types.ObjectTagIndex:
		obj = &IndexObject{cat: c}
	// case types.ObjectTagView:
	// 	obj = &ViewObject{cat: c}
	// case types.ObjectTagStream:
	// 	obj = &StreamObject{cat: c}
	// case types.ObjectTagSnapshot:
	// 	obj = &SnapshotObject{cat: c}
	default:
		return nil, ErrInvalidObjectType
	}
	if err := obj.Decode(ctx, rec); err != nil {
		return nil, err
	}
	return obj, nil
}
