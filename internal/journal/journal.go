// Copyright (c) 2018-2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package journal

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"
	"sort"

	"blockwatch.cc/knoxdb/internal/bitset"
	"blockwatch.cc/knoxdb/internal/pack"
	"blockwatch.cc/knoxdb/internal/store"
	"blockwatch.cc/knoxdb/pkg/bitmap"
	"blockwatch.cc/knoxdb/pkg/schema"
	"blockwatch.cc/knoxdb/pkg/util"
)

// TODO: replay wal on load to init journal records

const sizeStep int = 1 << 8 // 256

var ErrNoKey = errors.New("update without primary key")

// RoundSize rounds size up to a multiple of sizeStep
func roundSize(sz int) int {
	return (sz + (sizeStep - 1)) & ^(sizeStep - 1)
}

// Append-only journal and in-memory tombstone
//
// - supports INSERT, UPSERT, UPDATE, DELETE
// - supports walking the journal by pk order (required for queries and flush)
// - journal pack data is only sorted by insert order, not necessarily pk order
// - primary key to position mapping is always sorted in pk order
// - tombstone is an efficient bitmap
// - re-inserting deleted entries is safe
//
// To avoid sorting the journal after insert, but still process journal entries
// in pk sort order (in queries or flush and in both directions), we keep a
// mapping from pk to journal position in `keys` which is always sorted by pk.
//
// # How the journal is used
//
// Lookup: (non-order-preserving) matches against pk values only. For best performance
// we pre-sort the pk's we want to look up.
//
// Query, Stream, Count: runs a full pack match on the journal's data pack. Note
// the resulting bitset is in storage order, not pk order!. Then a query walks
// all table packs and cross-check with tomb + journal to decide if/which rows
// to return to the caller. Ticks off journal matches from the match bitset as
// they are visted along the way. Finally, walks all tail entries that only exist
// in the journal but were never flushed to a table pack. Since the bitset is
// in storage order we must translate it into pk order for this step to work. This is
// what SortedIndexes() and SortedIndexesReversed() are for.
type Journal struct {
	Data *pack.Package  // journal pack storing live data
	Keys JournalRecords // 0: pk, 1: index in journal; sorted by pk, always sorted

	Tomb    bitmap.Bitmap  // deleted primary keys as xroar bitmap
	Deleted *bitset.Bitset // tracks which journal positions are in tomb

	newKeys  JournalRecords // new keys added during insert/update
	view     *schema.View   // wire data view
	maxid    uint64         // the highest primary key in the journal, used for sorting
	maxsize  int            // max number of entries before flush
	sortData bool           // true = data pack is unsorted
}

type JournalRecord struct {
	Pk  uint64
	Idx int
}

type JournalRecords []JournalRecord

func (l JournalRecords) Len() int           { return len(l) }
func (l JournalRecords) Less(i, j int) bool { return l[i].Pk < l[j].Pk }
func (l JournalRecords) Swap(i, j int)      { l[i], l[j] = l[j], l[i] }

type Tombstone struct {
	Id uint64 `knox:"I,pk"`
}

func NewJournal(s *schema.Schema, size int) *Journal {
	pkg := pack.New().
		WithMaxRows(size).
		WithKey(pack.JournalKeyId).
		WithSchema(s).
		Alloc()
	return &Journal{
		maxsize: size,
		Data:    pkg,
		Keys:    make(JournalRecords, 0, roundSize(size)),
		Tomb:    bitmap.New(),
		Deleted: bitset.NewBitset(roundSize(size)).Resize(0),
		newKeys: make(JournalRecords, 0, roundSize(size)),
		view:    schema.NewView(s),
	}
}

func (j *Journal) Open(ctx context.Context, tx store.Tx, bkey []byte) error {
	return j.LoadLegacy(ctx, tx, bkey)
}

func (j *Journal) Close() {
	j.Data.Release()
	j.Data = nil
	j.Tomb.Bitmap = nil
	j.Keys = nil
	j.Deleted = nil
	j.maxid = 0
	j.newKeys = nil
	j.view = nil
}

func (j *Journal) LoadLegacy(ctx context.Context, tx store.Tx, bucket []byte) error {
	// we need to alloc a new data pack without blocks for load to fill from disk
	s := j.Data.Schema()
	j.Data.Release()
	j.Data = pack.New().
		WithMaxRows(j.maxsize).
		WithKey(pack.JournalKeyId).
		WithSchema(s)
	if _, err := j.Data.Load(ctx, tx, false, 0, bucket, nil, 0); err != nil {
		return err
	}
	j.sortData = false
	for i, n := range j.Data.PkColumn() {
		j.Keys = append(j.Keys, JournalRecord{n, i})
		j.sortData = j.sortData || n < j.maxid
		j.maxid = util.Max(j.maxid, n)
	}
	// ensure invariant, keep keys always sorted
	if j.sortData {
		sort.Sort(j.Keys)
	}

	// tomb
	j.Deleted.Resize(len(j.Keys))
	var key [4]byte
	binary.BigEndian.PutUint32(key[:], pack.TombstoneKeyId)
	if buf := tx.Bucket(bucket).Get(key[:]); buf != nil {
		if err := j.Tomb.UnmarshalBinary(buf); err != nil {
			return err
		}
	}
	var idx, last int
	it := j.Tomb.Bitmap.NewIterator()
	for pk := it.Next(); pk > 0; pk = it.Next() {
		idx, last = j.PkIndex(pk, last)
		if idx < 0 {
			continue
		}
		if last >= len(j.Keys) {
			break
		}
		j.Deleted.Set(idx)
		j.Data.SetValue(j.Data.PkIdx(), idx, uint64(0))
	}
	return nil
}

func (j *Journal) StoreLegacy(ctx context.Context, tx store.Tx, bucket []byte) (int, int, error) {
	// reconstructed deleted pks from key list
	var idx, last int
	it := j.Tomb.Bitmap.NewIterator()
	for pk := it.Next(); pk > 0; pk = it.Next() {
		idx, last = j.PkIndex(pk, last)
		if idx < 0 {
			continue
		}
		if last >= len(j.Keys) {
			break
		}
		j.Data.SetValue(j.Data.PkIdx(), idx, pk)
	}
	n, err := j.Data.Store(ctx, tx, 0, bucket, 0.9, nil)
	if err != nil {
		return 0, 0, err
	}

	var key [4]byte
	binary.BigEndian.PutUint32(key[:], pack.TombstoneKeyId)
	buf, err := j.Tomb.MarshalBinary()
	if err != nil {
		return 0, 0, err
	}
	tx.Bucket(bucket).Put(key[:], buf)
	m := len(buf)

	// reset deleted pks to zero
	last = 0
	it = j.Tomb.Bitmap.NewIterator()
	for pk := it.Next(); pk > 0; pk = it.Next() {
		idx, last = j.PkIndex(pk, last)
		if idx < 0 {
			continue
		}
		if last >= len(j.Keys) {
			break
		}
		j.Data.SetValue(j.Data.PkIdx(), idx, uint64(0))
	}
	return n, m, nil
}

func (j *Journal) Len() int {
	return j.Data.Len()
}

func (j *Journal) TombLen() int {
	return j.Tomb.Count()
}

func (j *Journal) HeapSize() int {
	sz := len(j.Keys)*16 + j.Tomb.Size() + 82
	for _, v := range j.Data.Blocks() {
		sz += v.HeapSize()
	}
	return sz
}

func (j *Journal) IsFull() bool {
	return j.Data.IsFull() || j.Tomb.Count() >= j.maxsize
}

// InsertBatch adds multiple records in wire format and generates new
// pks >= nextSeq in sequential order. Records with existing pks are
// inserted again under a new pk. Returns number of records inserted
// and the remaining buffer contents in case the journal is full.
// All operations are error-free (assuming wire messages are well
// formed) which simplifies code as there is no need to roll back
// journal after error.
func (j *Journal) InsertBatch(buf []byte, nextSeq uint64) (uint64, []byte) {
	// cannot insert into full journal, must flush first
	if len(buf) == 0 || j.Data.IsFull() {
		return 0, buf
	}

	// split buf into wire messages
	j.view, buf, _ = j.view.Cut(buf)
	j.newKeys = j.newKeys[:0]

	// process each message independently, assign PK and insert
	// this cannot produce any errors (assuming messages are well formed)
	var count uint64
	for j.view.IsValid() {
		// assign primary key by writing directly into wire format buffer
		pk := nextSeq
		nextSeq++
		j.view.SetPk(pk)
		count++

		// append to data pack
		j.Data.AppendWire(j.view.Bytes())

		// update keys
		j.newKeys = append(j.newKeys, JournalRecord{pk, j.Data.Len() - 1})

		// set sortData flag
		j.sortData = j.sortData || pk < j.maxid

		// update maxid
		j.maxid = max(j.maxid, pk)

		// stop when journal is full
		if j.Data.IsFull() {
			break
		}

		// process next message, if any
		j.view, buf, _ = j.view.Cut(buf)
	}

	// merge new keys (sorted) into sorted key list
	j.mergeKeys(j.newKeys)
	j.Deleted.Resize(len(j.Keys))
	j.view.Reset(nil)

	return count, buf
}

// Updates multiple records in wire format by inserting or overwriting
// them in the journal. Returns the number of processed records and
// the remaining buffer contents in case the journal is full.
// Wire buffer is not required to contain pk sorted records.
func (j *Journal) UpdateBatch(buf []byte) (uint64, []byte, error) {
	// cannot insert into full journal
	if len(buf) == 0 {
		return 0, buf, nil
	}

	// split buf into wire messages
	lastBuf := buf
	j.view, buf, _ = j.view.Cut(buf)
	j.newKeys = j.newKeys[:0]

	// process each message independently, assign PK and insert
	var (
		err   error
		idx   int
		count uint64
	)
	for j.view.IsValid() {
		// ensure primary key is set
		pk := j.view.GetPk()
		if pk == 0 {
			err = ErrNoKey
			break
		}

		// identify insert method (append / update)
		idx, _ = j.PkIndex(pk, 0)
		if idx < 0 {
			// stop when journal is full
			if j.IsFull() {
				buf = lastBuf
				break
			}

			// append to data pack (this record does not yet exist in the journal)
			j.Data.AppendWire(j.view.Bytes())
			count++

			// create mapped keys
			j.newKeys = append(j.newKeys, JournalRecord{pk, j.Data.Len() - 1})

			// set sortData flag
			j.sortData = j.sortData || pk < j.maxid
			j.maxid = max(j.maxid, pk)

		} else {
			// undelete and replace in data pack (this record already exists)
			j.undelete(pk)
			j.Data.SetWire(idx, j.view.Bytes())
			count++
		}

		// process next message, if any
		lastBuf = buf
		j.view, buf, _ = j.view.Cut(buf)
	}
	if err != nil {
		j.view.Reset(nil)
		return count, lastBuf, err
	}

	// undelete if deleted, must call before mergeKeys

	// sort and merge new journal keys
	sort.Sort(j.newKeys)
	j.mergeKeys(j.newKeys)
	j.Deleted.Resize(len(j.Keys))
	j.view.Reset(nil)

	return count, buf, nil
}

func (j *Journal) mergeKeys(newKeys JournalRecords) {
	if len(newKeys) == 0 {
		return
	}

	// sanity-check for unsorted keys
	// if isSorted && !sort.IsSorted(newKeys) {
	// 	panic("pack: mergeKeys input is unsorted, but sorted flag is set")
	// }

	// merge newKeys into key list (both lists are sorted)
	if cap(j.Keys) < len(j.Keys)+len(newKeys) {
		cp := make(JournalRecords, len(j.Keys), roundSize(len(j.Keys)+len(newKeys)))
		copy(cp, j.Keys)
		j.Keys = cp
	}

	// fast path for append-only
	if len(j.Keys) == 0 || newKeys[0].Pk > j.Keys[len(j.Keys)-1].Pk {
		j.Keys = append(j.Keys, newKeys...)
		return
	}

	// merge backward

	// keep position of the last value in keys
	last := len(j.Keys) - 1

	// extend keys len
	j.Keys = j.Keys[:len(j.Keys)+len(newKeys)]

	// ignore equal keys, they cannot exist here (as safety measure, we still copy them)
	for in1, in2, out := last, len(newKeys)-1, len(j.Keys)-1; in2 >= 0; {
		// insert new keys as long as they are larger or all old keys have been
		// copied (i.e. any new key is smaller than the first old key)
		for in2 >= 0 && (in1 < 0 || j.Keys[in1].Pk < newKeys[in2].Pk) {
			j.Keys[out] = newKeys[in2]
			in2--
			out--
		}

		// insert old keys as long as they are larger (safety: although no
		// duplicate keys are allowed, we simply copy them using >= instead of >)
		for in1 >= 0 && (in2 < 0 || j.Keys[in1].Pk >= newKeys[in2].Pk) {
			j.Keys[out] = j.Keys[in1]
			in1--
			out--
		}
	}
}

// Registers pks for deletion. Records may or may not exist, the
// journal accepts any non zero primary key here. Data is later
// reconciled on flush and invalid pks are simply ignored.
func (j *Journal) DeleteBatch(pks bitmap.Bitmap) uint64 {
	if !pks.IsValid() || pks.Count() == 0 {
		return 0
	}

	// mark in-journal entries deleted
	var last, idx int
	it := pks.Bitmap.NewIterator()
	for pk := it.Next(); pk > 0; pk = it.Next() {
		// find existing key and position in journal
		if idx, last = j.PkIndex(pk, last); idx >= 0 {
			// overwrite journal pk col with zero (this signals to query and
			// flush operations that this item is deleted and should be skipped)
			j.Data.SetValue(j.Data.PkIdx(), idx, uint64(0))

			// remember the journal position was deleted, so that a subsequent
			// insert/upsert call can properly undelete
			j.Deleted.Set(idx)
		}
		// stop journal scan if deleted ids are larger than whats stored in the
		// journal right now
		if last == j.Data.Len() {
			break
		}
	}

	// get before count
	before := j.Tomb.Count()

	// merge pks into tomb
	j.Tomb.Or(pks)

	// get after count
	after := j.Tomb.Count()

	// return diff
	return uint64(after - before)
}

// To support insert/update-after-delete we remove entries from the
// tomb and we reconstruct the previous state of the undeleted entry
// in our data pack (i.e. we restore its primary key) and reset the
// deleted flag. pks must be storted.
func (j *Journal) undelete(pk uint64) {
	if !j.IsDeleted(pk) {
		return
	}
	// reset the deleted bit and restore pk
	idx, _ := j.PkIndex(pk, 0)
	if idx > -1 {
		j.Deleted.Clear(idx)
		j.Data.SetValue(j.Data.PkIdx(), idx, pk)
	}
	// remove from tomb
	j.Tomb.Remove(pk)
}

// Efficient check if a pk is in the tomb or not. Use `last` to skip already
// processed entries when walking through a sorted list of pks.
func (j *Journal) IsDeleted(pk uint64) bool {
	return j.Tomb.Contains(pk)
}

// Returns the journal index at which pk is stored or -1 when pk is not found and the last
// index that matched. Using the last argument allows to skip searching a part of the journal
// for better efficiency in loops. This works only if subsequent calls can guarantee that
// queried primary keys are sorted, i.e. the next pk is larger than the previous pk.
//
// var last, index int
//
//	for last < journal.Len() {
//	   index, last = journal.PkIndex(pk, last)
//	}
//
// Invariant: keys list is always sorted
func (j *Journal) PkIndex(pk uint64, last int) (int, int) {
	// early stop when key or last are out of range
	if pk > j.maxid || last >= len(j.Keys) {
		return -1, len(j.Keys)
	}

	// find pk in keys list, use last as hint to limit search space
	last += sort.Search(len(j.Keys)-last, func(i int) bool { return j.Keys[last+i].Pk >= pk })

	// return index	if found or -1 otherwise
	// Note: when entry is deleted, we still return its position since other
	// parts of this algorithm rely on this behaviour
	if last < len(j.Keys) && j.Keys[last].Pk == pk {
		return j.Keys[last].Idx, last
	}
	return -1, last
}

// Checks invariants
func (j *Journal) checkInvariants(when string) error {
	// check invariants
	if a, b := j.Data.Len(), len(j.Keys); a != b {
		return fmt.Errorf("journal %s: INVARIANT VIOLATION: data-pack-len=%d key-len=%d", when, a, b)
	}
	if a, b := j.Data.Len(), j.Deleted.Len(); a != b {
		return fmt.Errorf("journal %s: INVARIANT VIOLATION: data-pack-len=%d deleted-bitset-len=%d", when, a, b)
	}
	for i, v := range j.Keys {
		if i == 0 {
			continue
		}
		if j.Keys[i-1].Pk > v.Pk {
			return fmt.Errorf("journal %s: INVARIANT VIOLATION: unsorted keys", when)
		}
		if j.Keys[i-1].Pk == v.Pk {
			return fmt.Errorf("journal %s: INVARIANT VIOLATION: duplicate key", when)
		}
	}
	// no duplicate pks in pack (consider deleted keys == 0)
	pks := j.Data.PkColumn()
	sorted := make([]uint64, len(pks))
	copy(sorted, pks)
	slices.Sort(sorted)
	var last uint64
	for _, v := range sorted {
		if last == 0 {
			last = v
			continue
		}
		if v == last {
			return fmt.Errorf("journal %s: INVARIANT VIOLATION: duplicate pk %d in data pack", when, v)
		}
	}
	return nil
}

func (j *Journal) Reset() {
	j.Data.Clear()
	if len(j.Keys) > 0 {
		j.Keys[0].Idx = 0
		j.Keys[0].Pk = 0
		for bp := 1; bp < len(j.Keys); bp *= 2 {
			copy(j.Keys[bp:], j.Keys[:bp])
		}
		j.Keys = j.Keys[:0]
	}
	j.Tomb.Bitmap.Reset()
	j.maxid = 0
	j.sortData = false
	j.Deleted.Reset()
}
