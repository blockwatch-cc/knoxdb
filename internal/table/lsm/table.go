// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package lsm

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"

	"blockwatch.cc/knoxdb/internal/engine"
	"blockwatch.cc/knoxdb/internal/query"
	"blockwatch.cc/knoxdb/internal/store"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/bitmap"
	"blockwatch.cc/knoxdb/pkg/schema"
	"github.com/echa/log"
	"golang.org/x/exp/slices"
)

var _ engine.TableEngine = (*Table)(nil)

func init() {
	engine.RegisterTableFactory(engine.TableKindLSM, NewTable)
}

var (
	BE = binary.BigEndian    // byte order for keys
	NE = binary.NativeEndian // byte order for values (LE)

	DefaultTableOptions = engine.TableOptions{
		Driver:     "bolt",
		PageSize:   1 << 16,
		PageFill:   0.9,
		TxMaxSize:  1 << 24, // 16 MB,
		ReadOnly:   false,
		NoSync:     false,
		NoGrowSync: false,
		Logger:     log.Disabled,
	}
)

type TableState struct {
	Sequence uint64 // next free sequence
	Rows     uint64 // total non-deleted rows
}

type Table struct {
	engine     *engine.Engine       // engine access
	schema     *schema.Schema       // table schema
	tableId    uint64               // unique tagged name hash
	pkindex    int                  // field index for primary key (if any)
	opts       engine.TableOptions  // copy of config options
	db         store.DB             // lower-level KV store (e.g. boltdb or badger)
	key        []byte               // name of the data bucket
	isZeroCopy bool                 // storage reads are zero copy (copy to safe references)
	noClose    bool                 // don't close underlying store db on Close
	state      TableState           // volatile state, synced with catalog
	indexes    []engine.IndexEngine // list of indexes
	stats      engine.TableStats    // usage statistics
	log        log.Logger
}

func NewTable() engine.TableEngine {
	return &Table{}
}

func (t *Table) Create(ctx context.Context, s *schema.Schema, opts engine.TableOptions) error {
	// require primary key
	pki := s.PkIndex()
	if pki < 0 {
		return engine.ErrNoPk
	}

	e := engine.GetTransaction(ctx).Engine()

	// init names
	name := s.Name()
	typ := s.TypeLabel(e.Namespace())

	// setup store
	t.engine = e
	t.schema = s
	t.tableId = s.TaggedHash(types.HashTagTable)
	t.pkindex = pki
	t.opts = DefaultTableOptions.Merge(opts)
	t.key = []byte(name)
	t.state.Sequence = 1
	t.stats.Name = name
	t.db = opts.DB
	t.noClose = true
	t.log = opts.Logger

	// create db if not passed in options
	if t.db == nil {
		path := filepath.Join(e.RootPath(), name+".db")
		t.log.Debugf("Creating LSM table %q with opts %#v", path, opts)
		db, err := store.Create(t.opts.Driver, path, t.opts.ToDriverOpts())
		if err != nil {
			return fmt.Errorf("creating table %s: %v", typ, err)
		}
		err = db.SetManifest(store.Manifest{
			Name:    name,
			Schema:  typ,
			Version: int(s.Version()),
		})
		if err != nil {
			_ = db.Close()
			return err
		}
		t.db = db
		t.noClose = false
	}
	t.isZeroCopy = t.db.IsZeroCopyRead()

	// init table storage
	tx, err := engine.GetTransaction(ctx).StoreTx(t.db, true)
	if err != nil {
		return err
	}
	if _, err := store.CreateBucket(tx, t.key, engine.ErrTableExists); err != nil {
		return err
	}

	// init catalog state
	t.engine.Catalog().SetState(t.tableId, 1, 0)

	t.log.Debugf("Created table %s", typ)
	return nil
}

func (t *Table) Open(ctx context.Context, s *schema.Schema, opts engine.TableOptions) error {
	e := engine.GetTransaction(ctx).Engine()

	// init names
	name := s.Name()
	typ := s.TypeLabel(e.Namespace())

	// setup table
	t.engine = e
	t.schema = s
	t.tableId = s.TaggedHash(types.HashTagTable)
	t.pkindex = s.PkIndex()
	t.opts = DefaultTableOptions.Merge(opts)
	t.key = []byte(name)
	t.state.Sequence, t.state.Rows = e.Catalog().GetState(t.tableId)
	t.stats.Name = name
	t.stats.TupleCount = int64(t.state.Rows)
	t.db = opts.DB
	t.noClose = true
	t.log = opts.Logger

	// open db if not passed in options
	if t.db == nil {
		path := filepath.Join(e.RootPath(), name+".db")
		t.log.Debugf("Opening LSM table %q with opts %#v", path, opts)
		db, err := store.Open(t.opts.Driver, path, t.opts.ToDriverOpts())
		if err != nil {
			t.log.Errorf("opening table %s: %v", typ, err)
			return engine.ErrNoTable
		}
		t.db = db
		t.noClose = false

		// check manifest matches
		mft, err := t.db.Manifest()
		if err != nil {
			t.log.Errorf("missing manifest: %v", err)
			_ = t.Close(ctx)
			return engine.ErrDatabaseCorrupt
		}
		err = mft.Validate(name, "*", typ, -1)
		if err != nil {
			t.log.Errorf("schema mismatch: %v", err)
			_ = t.Close(ctx)
			return schema.ErrSchemaMismatch
		}
	}
	t.isZeroCopy = t.db.IsZeroCopyRead()

	// check table storage
	tx, err := engine.GetTransaction(ctx).StoreTx(t.db, false)
	if err != nil {
		return err
	}
	b := tx.Bucket(t.key)
	if b == nil {
		t.log.Error("missing table data: %v", engine.ErrNoBucket)
		tx.Rollback()
		_ = t.Close(ctx)
		return engine.ErrDatabaseCorrupt
	}
	stats := b.Stats()
	t.stats.TotalSize = int64(stats.Size) // estimate only

	t.log.Debugf("Table %s opened with %d rows", typ, t.state.Rows)
	return nil
}

func (t *Table) Close(ctx context.Context) (err error) {
	if !t.noClose && t.db != nil {
		t.log.Debugf("Closing table %s", t.schema.TypeLabel(t.engine.Namespace()))
		err = t.db.Close()
		t.db = nil
	}
	t.engine = nil
	t.schema = nil
	t.tableId = 0
	t.pkindex = 0
	t.key = nil
	t.noClose = false
	t.isZeroCopy = false
	t.opts = engine.TableOptions{}
	t.stats = engine.TableStats{}
	t.state = TableState{}
	t.indexes = nil
	return
}

func (t *Table) Schema() *schema.Schema {
	return t.schema
}

func (t *Table) Indexes() []engine.IndexEngine {
	return t.indexes
}

func (t *Table) name() string {
	return t.schema.Name()
}

func (t *Table) Stats() engine.TableStats {
	stats := t.stats
	stats.TupleCount = int64(t.state.Rows)
	return stats
}

func (t *Table) Drop(ctx context.Context) error {
	typ := t.schema.TypeLabel(t.engine.Namespace())
	if t.noClose {
		tx, err := engine.GetTransaction(ctx).StoreTx(t.db, true)
		if err != nil {
			return err
		}
		t.log.Debugf("dropping table %s", typ)
		if err := tx.Root().DeleteBucket(t.key); err != nil {
			return err
		}
		if err := tx.Commit(); err != nil {
			return err
		}
		return nil
	}
	path := t.db.Path()
	t.db.Close()
	t.db = nil
	t.log.Debugf("dropping table %s with path %s", typ, path)
	if err := os.Remove(path); err != nil {
		return err
	}
	return nil
}

func (t *Table) Sync(_ context.Context) error {
	return nil
}

func (t *Table) Compact(ctx context.Context) error {
	return t.db.GC(ctx, t.opts.PageFill)
}

func (t *Table) Truncate(ctx context.Context) error {
	tx, err := engine.GetTransaction(ctx).StoreTx(t.db, true)
	if err != nil {
		return err
	}
	if err := tx.Root().DeleteBucket(t.key); err != nil {
		return err
	}
	if _, err := tx.Root().CreateBucket(t.key); err != nil {
		return err
	}
	t.engine.Catalog().SetState(t.tableId, 1, 0)
	t.stats.DeletedTuples += int64(t.state.Rows)
	t.stats.TupleCount = 0
	t.state.Rows = 0
	t.state.Sequence = 1
	return nil
}

func (t *Table) UseIndex(idx engine.IndexEngine) {
	t.indexes = append(t.indexes, idx)
}

func (t *Table) UnuseIndex(idx engine.IndexEngine) {
	idxId := idx.Schema().TaggedHash(types.HashTagIndex)
	t.indexes = slices.DeleteFunc(t.indexes, func(v engine.IndexEngine) bool {
		return v.Schema().TaggedHash(types.HashTagIndex) == idxId
	})
}

// Table interface
// TODO: better to use typed buffer here []WireMessage or similar
func (t *Table) InsertRows(ctx context.Context, buf []byte) (uint64, error) {
	if len(buf) == 0 {
		return 0, nil
	}
	if len(buf) < t.schema.WireSize() {
		return 0, engine.ErrShortMessage
	}

	// open write transaction
	tx, err := engine.GetTransaction(ctx).StoreTx(t.db, true)
	if err != nil {
		return 0, err
	}
	atomic.AddInt64(&t.stats.InsertCalls, 1)

	// keep a pre-image of the state
	firstPk := t.state.Sequence
	state := t.state

	// cleanup on exit
	defer func() {
		// rollback table state to original value
		if state.Sequence > 0 {
			t.state = state
		}
	}()

	// split buf into wire messages
	view, buf, _ := schema.NewView(t.schema).Cut(buf)

	// process each message independently, assign PK and insert
	for view.IsValid() {
		// assign primary key by writing directly into wire format buffer
		nextPk := t.state.Sequence
		view.SetPk(nextPk)

		// write value to storage, returns any previous value
		// which we need to update indexes below
		prev, err := t.putTx(tx, engine.Key64Bytes(nextPk), view.Bytes())
		if err != nil {
			return 0, err
		}

		// update indexes, note indexes may be hosted in different
		// db files and engines, so store.Tx may not be relevant
		for _, idx := range t.indexes {
			idx.Add(ctx, prev, view.Bytes())
		}

		// process next message, if any
		view, buf, _ = view.Cut(buf)

		// advance table sequence
		t.state.Sequence++
	}

	// update state in catalog (will commit with main tx)
	t.engine.Catalog().SetState(t.tableId, t.state.Sequence, t.state.Rows)
	state = TableState{}

	// return first primary key assigned
	return firstPk, nil
}

func (t *Table) UpdateRows(ctx context.Context, buf []byte) (uint64, error) {
	if len(buf) == 0 {
		return 0, nil
	}
	if len(buf) < t.schema.WireSize() {
		return 0, engine.ErrShortMessage
	}

	// open write transaction
	tx, err := engine.GetTransaction(ctx).StoreTx(t.db, true)
	if err != nil {
		return 0, err
	}
	atomic.AddInt64(&t.stats.UpdateCalls, 1)

	// split buf into wire messages
	view, buf, _ := schema.NewView(t.schema).Cut(buf)
	var n uint64

	// process each message independently, assign PK and insert
	for view.IsValid() {
		// check primary key exists
		pk := view.GetPk()

		// write value to storage, returns any previous value
		// which we need to update indexes below
		prev, err := t.putTx(tx, engine.Key64Bytes(pk), view.Bytes())
		if err != nil {
			return 0, err
		}

		// fail when prev is nil (no previous value exists)
		if prev == nil {
			return 0, fmt.Errorf("update: missing pk %d", pk)
		}

		// update indexes, note indexes may be hosted in different
		// db files and engines, so store.Tx may not be relevant
		for _, idx := range t.indexes {
			idx.Add(ctx, prev, view.Bytes())
		}

		// process next message, if any
		view, buf, _ = view.Cut(buf)
		n++
	}

	return n, nil
}

// Table Query Interface
// - requires main data bucket to be indexed by pk (uint64)
// - generate index scan ranges from query conditions
// - run index scans -> bitsets
// - merge bitsets along condition tree
// - resolve result from value bucket via final bitset
// - append row data to Result
// - result decoder can skip unused fields

func (t *Table) Query(ctx context.Context, q engine.QueryPlan) (engine.QueryResult, error) {
	plan, ok := q.(*query.QueryPlan)
	if !ok {
		return nil, fmt.Errorf("invalid query plan type %T", q)
	}

	res := NewResult(plan.ResultSchema, int(plan.Limit))

	err := t.doQuery(ctx, plan, res)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func (t *Table) Stream(ctx context.Context, q engine.QueryPlan, fn func(engine.QueryRow) error) error {
	plan, ok := q.(*query.QueryPlan)
	if !ok {
		return fmt.Errorf("invalid query plan type %T", q)
	}

	res := NewStreamResult(plan.ResultSchema, fn)

	err := t.doQuery(ctx, plan, res)
	if err != nil && err != engine.EndStream {
		return err
	}

	return nil
}

func (t *Table) doQuery(ctx context.Context, plan *query.QueryPlan, res QueryResultConsumer) error {
	var (
		bits                       bitmap.Bitmap
		key                        [8]byte
		nRowsScanned, nRowsMatched uint32
	)

	// open read transaction
	tx, err := engine.GetTransaction(ctx).StoreTx(t.db, false)
	if err != nil {
		return err
	}

	// cleanup and log on exit
	defer func() {
		plan.Stats.Tick("scan_time")
		plan.Stats.Count("rows_scanned", int(nRowsScanned))
		plan.Stats.Count("rows_matched", int(nRowsMatched))
		atomic.AddInt64(&t.stats.QueryCalls, 1)
		atomic.AddInt64(&t.stats.QueriedTuples, int64(nRowsMatched))
		bits.Free()
	}()

	bucket := tx.Bucket(t.key)
	if bucket == nil {
		return engine.ErrNoBucket
	}

	// prepare result converter (table schema -> result schema)
	conv := schema.NewConverter(t.schema, plan.ResultSchema, binary.NativeEndian)

	// handle cases
	switch {
	case plan.Filters.IsEmpty():
		// No conds: walk entire table
		c := bucket.Cursor(store.ForwardCursor)
		defer c.Close()
		for ok := c.First(); ok; ok = c.Next() {
			// skip offset
			if plan.Offset > 0 {
				plan.Offset--
				continue
			}

			// convert result schema and store
			if err := res.Append(conv.Extract(c.Value()), t.isZeroCopy); err != nil {
				return err
			}

			// apply limit
			nRowsScanned++
			nRowsMatched++
			if plan.Limit > 0 && nRowsMatched >= plan.Limit {
				break
			}
		}

	case plan.Filters.IsProcessed():
		// 1: full index query -> everything is resolved, walk bitset
		it := plan.Filters.Bits.Bitmap.NewIterator()
		for id := it.Next(); id > 0; id = it.Next() {
			// skip offset
			if plan.Offset > 0 {
				plan.Offset--
				continue
			}
			BE.PutUint64(key[:], id)
			val := bucket.Get(key[:])
			if val == nil {
				// warn on indexed but missing pks
				plan.Log.Warnf("query %s: missing index scan PK %d on table %s", plan.Key, id, t.name())
				continue
			}

			// convert result schema and store
			if err := res.Append(conv.Extract(val), t.isZeroCopy); err != nil {
				return err
			}

			// apply limit
			nRowsScanned++
			nRowsMatched++
			if plan.Limit > 0 && nRowsMatched >= plan.Limit {
				break
			}
		}
	case !plan.Filters.OrKind && plan.Filters.Bits.IsValid():
		// 2: partial index query & root = AND: walk bitset but check each value
		it := plan.Filters.Bits.Bitmap.NewIterator()
		view := schema.NewView(t.schema)
		for id := it.Next(); id > 0; id = it.Next() {
			BE.PutUint64(key[:], id)
			buf := bucket.Get(key[:])
			if buf == nil {
				// warn on indexed but missing pks
				plan.Log.Warnf("query %s: missing index scan PK %d on table %s", plan.Key, id, t.name())
				continue
			}

			// check conditions
			nRowsScanned++
			if !MatchNode(&plan.Filters, view.Reset(buf)) {
				continue
			}

			// skip offset
			if plan.Offset > 0 {
				plan.Offset--
				continue
			}

			// convert result schema and store
			if err := res.Append(conv.Extract(buf), t.isZeroCopy); err != nil {
				return err
			}

			// apply limit
			nRowsMatched++
			if plan.Limit > 0 && nRowsMatched >= plan.Limit {
				break
			}
		}
	default:
		// 3: partial index query & root = OR: walk full table and check each value
		// 4: no index query: walk full table and check each value
		c := bucket.Cursor(store.ForwardCursor)
		defer c.Close()
		view := schema.NewView(t.schema)

		// construct prefix scan from unprocessed pk condition(s) if any
		var first, last [8]byte
		from, to := PkRange(&plan.Filters, t.schema)
		BE.PutUint64(first[:], from)
		BE.PutUint64(last[:], to)

		for ok := c.Seek(first[:]); ok && bytes.Compare(c.Key(), last[:]) <= 0; ok = c.Next() {
			buf := c.Value()

			// check conditions
			nRowsScanned++
			if !MatchNode(&plan.Filters, view.Reset(buf)) {
				continue
			}

			// skip offset
			if plan.Offset > 0 {
				plan.Offset--
				continue
			}

			// convert result schema and store
			if err := res.Append(conv.Extract(buf), t.isZeroCopy); err != nil {
				return err
			}

			// apply limit
			nRowsMatched++
			if plan.Limit > 0 && nRowsMatched >= plan.Limit {
				break
			}
		}
	}

	return nil
}

func (t *Table) Delete(ctx context.Context, q engine.QueryPlan) (uint64, error) {
	var (
		key                        [8]byte
		nRowsScanned, nRowsMatched uint32
	)

	plan, ok := q.(*query.QueryPlan)
	if !ok {
		return 0, fmt.Errorf("invalid query plan type %T", q)
	}

	// open write transaction
	tx, err := engine.GetTransaction(ctx).StoreTx(t.db, true)
	if err != nil {
		return 0, err
	}

	// cleanup on exit
	defer func() {
		plan.Stats.Tick("scan_time")
		plan.Stats.Count("rows_scanned", int(nRowsScanned))
		plan.Stats.Count("rows_matched", int(nRowsMatched))
		atomic.AddInt64(&t.stats.DeleteCalls, 1)
	}()

	bucket := tx.Bucket(t.key)
	if bucket == nil {
		return 0, engine.ErrNoBucket
	}

	// handle cases
	switch {
	case plan.Filters.IsEmpty():
		// nothing to delete
		return 0, nil

	case plan.Filters.IsProcessed():
		// 1: full index query -> everything is resolved, walk bitset
		it := plan.Filters.Bits.Bitmap.NewIterator()
		for pk := it.Next(); pk > 0; pk = it.Next() {
			BE.PutUint64(key[:], pk)
			prev, err := t.delTx(tx, key[:])
			if err != nil {
				return 0, err
			}
			if prev == nil {
				continue
			}
			nRowsMatched++

			// update indexes
			for _, idx := range t.indexes {
				idx.Del(ctx, prev)
			}
		}

	case !plan.Filters.OrKind && plan.Filters.Bits.IsValid():
		// 2: partial index query & root = AND: walk bitset but check each value
		it := plan.Filters.Bits.Bitmap.NewIterator()
		view := schema.NewView(t.schema)
		for id := it.Next(); id > 0; id = it.Next() {
			BE.PutUint64(key[:], id)
			buf := bucket.Get(key[:])
			if buf == nil {
				continue
			}

			// check conditions
			nRowsScanned++
			if !MatchNode(&plan.Filters, view.Reset(buf)) {
				continue
			}

			// delete
			prev, err := t.delTx(tx, key[:])
			if err != nil {
				return 0, err
			}
			nRowsMatched++

			// update indexes
			for _, idx := range t.indexes {
				idx.Del(ctx, prev)
			}
		}
	default:
		// 3: partial index query & root = OR: walk full table and check each value
		// 4: no index query: walk full table and check each value
		c := bucket.Cursor(store.ForwardCursor)
		view := schema.NewView(t.schema)
		for ok := c.First(); ok; ok = c.Next() {
			buf := c.Value()

			// check conditions
			nRowsScanned++
			if !MatchNode(&plan.Filters, view.Reset(buf)) {
				continue
			}

			// delete
			prev, err := t.delTx(tx, key[:])
			if err != nil {
				return 0, err
			}
			nRowsMatched++

			// update indexes
			for _, idx := range t.indexes {
				idx.Del(ctx, prev)
			}
		}
	}

	return uint64(nRowsMatched), nil
}

func (t *Table) Count(ctx context.Context, q engine.QueryPlan) (uint64, error) {
	var (
		bits                       bitmap.Bitmap
		key                        [8]byte
		nRowsScanned, nRowsMatched uint32
	)

	plan, ok := q.(*query.QueryPlan)
	if !ok {
		return 0, fmt.Errorf("invalid query plan type %T", q)
	}

	// open read transaction
	tx, err := engine.GetTransaction(ctx).StoreTx(t.db, false)
	if err != nil {
		return 0, err
	}

	// cleanup and log on exit
	defer func() {
		plan.Stats.Tick("scan_time")
		plan.Stats.Count("rows_scanned", int(nRowsScanned))
		plan.Stats.Count("rows_matched", int(nRowsMatched))
		atomic.AddInt64(&t.stats.QueryCalls, 1)
		atomic.AddInt64(&t.stats.QueriedTuples, int64(nRowsMatched))
		bits.Free()
	}()

	bucket := tx.Bucket(t.key)
	if bucket == nil {
		return 0, engine.ErrNoBucket
	}

	// handle cases
	switch {
	case plan.Filters.IsEmpty():
		// No conds: walk entire table
		c := bucket.Cursor(store.IndexCursor)
		defer c.Close()
		for ok := c.First(); ok; ok = c.Next() {
			nRowsMatched++
		}

	case plan.Filters.IsProcessed():
		// 1: full index query -> everything is resolved, count bitset
		nRowsMatched = uint32(plan.Filters.Bits.Count())

	case !plan.Filters.OrKind && plan.Filters.Bits.IsValid():
		// 2: partial index query & root = AND: walk bitset but check each value
		it := plan.Filters.Bits.Bitmap.NewIterator()
		view := schema.NewView(t.schema)
		for id := it.Next(); id > 0; id = it.Next() {
			BE.PutUint64(key[:], id)
			buf := bucket.Get(key[:])
			if buf == nil {
				// warn on indexed but missing pks
				plan.Log.Warnf("query %s: missing index scan PK %d on table %s", plan.Key, id, t.name())
				continue
			}

			// check conditions
			nRowsScanned++
			if !MatchNode(&plan.Filters, view.Reset(buf)) {
				continue
			}

			nRowsMatched++
		}

	default:
		// 3: partial index query & root = OR: walk full table and check each value
		// 4: no index query: walk full table and check each value
		c := bucket.Cursor(store.ForwardCursor)
		view := schema.NewView(t.schema)
		for ok := c.First(); ok; ok = c.Next() {
			buf := c.Value()

			// check conditions
			nRowsScanned++
			if !MatchNode(&plan.Filters, view.Reset(buf)) {
				continue
			}

			nRowsMatched++
		}
	}

	return uint64(nRowsMatched), nil
}

func (t *Table) Lookup(ctx context.Context, pks []uint64) (engine.QueryResult, error) {
	res := NewResult(t.schema, len(pks))
	err := t.doLookup(ctx, pks, res)
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (t *Table) StreamLookup(ctx context.Context, pks []uint64, fn func(engine.QueryRow) error) error {
	res := NewStreamResult(t.schema, fn)
	err := t.doLookup(ctx, pks, res)
	if err != nil && err != engine.EndStream {
		return err
	}
	return nil
}

func (t *Table) doLookup(ctx context.Context, pks []uint64, res QueryResultConsumer) error {
	var (
		key          [8]byte
		nRowsMatched uint32
	)

	// open read transaction
	tx, err := engine.GetTransaction(ctx).StoreTx(t.db, false)
	if err != nil {
		return err
	}

	// cleanup on exit
	defer func() {
		atomic.AddInt64(&t.stats.QueryCalls, 1)
		atomic.AddInt64(&t.stats.QueriedTuples, int64(nRowsMatched))
		tx.Rollback()
	}()

	bucket := tx.Bucket(t.key)
	if bucket == nil {
		return engine.ErrNoBucket
	}

	for _, pk := range pks {
		if pk == 0 {
			continue
		}

		BE.PutUint64(key[:], pk)
		buf := bucket.Get(key[:])
		if buf == nil {
			continue
		}

		nRowsMatched++
		if err := res.Append(buf, t.isZeroCopy); err != nil {
			return err
		}
	}
	return nil
}

// low-level interface for KV storage access
func (t *Table) getTx(tx store.Tx, key []byte) []byte {
	bucket := tx.Bucket(t.key)
	if bucket == nil {
		return nil
	}
	buf := bucket.Get(key)
	if buf == nil {
		return nil
	}
	atomic.AddInt64(&t.stats.BytesRead, int64(len(buf)))
	return buf
}

func (t *Table) putTx(tx store.Tx, key, val []byte) ([]byte, error) {
	prevSize, sz := -1, len(key)+len(val)
	bucket := tx.Bucket(t.key)
	if bucket == nil {
		return nil, engine.ErrNoBucket
	}
	buf := bucket.Get(key)
	if buf != nil {
		prevSize = len(buf) + len(key)
	} else {
		t.state.Rows++
	}
	err := bucket.Put(key, val)
	if err != nil {
		return nil, err
	}
	if prevSize >= 0 {
		// update
		atomic.AddInt64(&t.stats.UpdatedTuples, 1)
		atomic.AddInt64(&t.stats.TotalSize, int64(sz-prevSize))
	} else {
		// insert
		atomic.AddInt64(&t.stats.InsertedTuples, 1)
		atomic.AddInt64(&t.stats.TupleCount, 1)
		atomic.AddInt64(&t.stats.TotalSize, int64(sz))
	}
	atomic.AddInt64(&t.stats.BytesWritten, int64(sz))
	return buf, nil
}

func (t *Table) delTx(tx store.Tx, key []byte) ([]byte, error) {
	prevSize := -1
	bucket := tx.Bucket(t.key)
	if bucket == nil {
		return nil, engine.ErrNoBucket
	}
	buf := bucket.Get(key)
	if buf != nil {
		prevSize = len(buf)
		t.state.Rows--
	}
	err := bucket.Delete(key)
	if err == nil && prevSize >= 0 {
		atomic.AddInt64(&t.stats.TupleCount, -1)
		atomic.AddInt64(&t.stats.DeletedTuples, 1)
		atomic.AddInt64(&t.stats.TotalSize, -int64(prevSize))
	}
	return buf, err
}
