// Copyright (c) 2018-2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package pack

import (
	"fmt"
	"reflect"
	"sort"

	"blockwatch.cc/knoxdb/vec"
)

const sizeStep int = 1 << 8 // 256

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
// - tombstone is always sorted
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
//
// TODO
// - write all incoming inserts/updates/deletes to a WAL
// - load and reconstructed journal + tomb from WAL
type Journal struct {
	lastid   uint64 // the highest primary key in the journal, used for sorting
	maxid    uint64 // the highest primary key in the table, used to generate new ids
	maxsize  int    // max number of entries before flush
	sortData bool   // true = data pack is unsorted

	data    *Package         // journal pack storing live data
	keys    journalEntryList // 0: pk, 1: index in journal; sorted by pk, always sorted
	tomb    []uint64         // list of deleted primary keys, always sorted
	deleted *vec.Bitset      // tracks which journal positions are in tomb
	wal     *Wal             // write-ahead log (unused for now, will ensure the D in ACID)
	prefix  string           // table name, used for debugging, maybe WAL file name
}

type journalEntry struct {
	pk  uint64
	idx int
}

type journalEntryList []journalEntry

func (l journalEntryList) Len() int           { return len(l) }
func (l journalEntryList) Less(i, j int) bool { return l[i].pk < l[j].pk }
func (l journalEntryList) Swap(i, j int)      { l[i], l[j] = l[j], l[i] }

func NewJournal(maxid uint64, size int, name string) *Journal {
	pkg := NewPackage(size, nil)
	pkg.key = journalKey
	return &Journal{
		maxid:   maxid,
		maxsize: size,
		data:    pkg,
		keys:    make(journalEntryList, 0, roundSize(size)),
		tomb:    make([]uint64, 0, roundSize(size)),
		deleted: vec.NewCustomBitset(roundSize(size)).Resize(0),
		prefix:  name,
	}
}

func (j *Journal) InitFields(fields FieldList) error {
	return j.data.InitFields(fields, nil)
}

func (j *Journal) InitType(typ interface{}) error {
	tinfo, err := getTypeInfo(typ)
	if err != nil {
		return err
	}
	fields, err := Fields(typ)
	if err != nil {
		return err
	}
	if j.prefix == "" {
		j.prefix = tinfo.name
	}
	return j.data.InitFields(fields, tinfo)
}

func (j *Journal) Open(path string) error {
	w, err := OpenWal(path, j.prefix)
	if err != nil {
		return fmt.Errorf("pack: opening WAL for %s journal failed: %v", j.prefix, err)
	}
	j.wal = w
	return nil
}

func (j *Journal) Close() {
	if j.wal != nil {
		j.wal.Close()
		j.wal = nil
	}
	j.data.Release()
	j.data = nil
	j.tomb = nil
	j.keys = nil
	j.deleted = nil
	j.lastid = 0
	j.maxid = 0
}

func (j *Journal) LoadLegacy(tx *Tx, bucketName []byte) error {
	j.Reset()
	if _, err := loadPackTx(tx, bucketName, encodePackKey(journalKey), j.data, j.maxsize); err != nil {
		return err
	}
	j.sortData = false
	for i, n := range j.data.PkColumn() {
		j.keys = append(j.keys, journalEntry{n, i})
		j.sortData = j.sortData || n < j.lastid
		j.lastid = max(j.lastid, n)
	}
	// ensure invariant, keep keys always sorted
	if j.sortData {
		sort.Sort(j.keys)
	}
	tomb, err := loadPackTx(tx, bucketName, encodePackKey(tombstoneKey), nil, j.maxsize)
	if err != nil {
		return fmt.Errorf("pack: cannot open tombstone for table %s: %v", string(bucketName), err)
	}
	tomb.InitType(Tombstone{})
	pk := tomb.PkColumn()
	if cap(j.tomb) < len(pk) {
		j.tomb = make([]uint64, len(pk), roundSize(len(pk)))
	}
	j.tomb = j.tomb[:len(pk)]
	copy(j.tomb, pk)
	tomb.Release()
	j.deleted.Resize(len(j.keys))
	var idx, last int
	for _, v := range j.tomb {
		idx, last = j.PkIndex(v, last)
		if idx < 0 {
			continue
		}
		if last >= len(j.keys) {
			break
		}
		j.deleted.Set(idx)
		j.data.SetFieldAt(j.data.pkindex, idx, uint64(0))
	}
	return nil
}

func (j *Journal) StoreLegacy(tx *Tx, bucketName []byte) (int, int, error) {
	// reconstructed deleted pks from key list
	var idx, last int
	for _, v := range j.tomb {
		idx, last = j.PkIndex(v, last)
		if idx < 0 {
			continue
		}
		if last >= len(j.keys) {
			break
		}
		j.data.SetFieldAt(j.data.pkindex, idx, v)
	}
	n, err := storePackTx(tx, bucketName, encodePackKey(journalKey), j.data, defaultJournalFillLevel)
	if err != nil {
		return 0, 0, err
	}
	tomb := NewPackage(len(j.tomb), nil)
	tomb.InitType(Tombstone{})
	defer tomb.Release()
	for _, v := range j.tomb {
		ts := Tombstone{v}
		_ = tomb.Push(ts)
	}
	m, err := storePackTx(tx, bucketName, encodePackKey(tombstoneKey), tomb, defaultJournalFillLevel)
	if err != nil {
		return n, 0, err
	}
	// reset deleted pks to zero
	idx, last = 0, 0
	for _, v := range j.tomb {
		idx, last = j.PkIndex(v, last)
		if idx < 0 {
			continue
		}
		if last >= len(j.keys) {
			break
		}
		j.data.SetFieldAt(j.data.pkindex, idx, uint64(0))
	}
	return n, m, nil
}

func (j *Journal) Len() int {
	return j.data.Len()
}

func (j *Journal) TombLen() int {
	return len(j.tomb)
}

func (j *Journal) HeapSize() int {
	return j.data.HeapSize() + len(j.keys)*16 + len(j.tomb)*8 + 82
}

func (j *Journal) ShouldFlush() bool {
	return j.data.Len()+len(j.tomb) > j.maxsize
}

func (j *Journal) IsSorted() bool {
	return !j.sortData
}

func (j *Journal) MaxId() uint64 {
	return j.maxid
}

func (j *Journal) LastId() uint64 {
	return j.lastid
}

func (j *Journal) next() uint64 {
	j.maxid++
	return j.maxid
}

// check ID and generate next sequence if missing
func (j *Journal) getOrAssignPk(val any) (uint64, bool) {
	if j.data.pkindex < 0 {
		return 0, false
	}
	pkv := reflect.Indirect(reflect.ValueOf(val)).Field(j.data.pkindex)
	pk := pkv.Uint()
	if pk == 0 {
		pk = j.next()
		pkv.SetUint(pk)
		return pk, true
	}
	return pk, false
}

func (j *Journal) getPk(val any) uint64 {
	if j.data.pkindex < 0 {
		return 0
	}
	pkv := reflect.Indirect(reflect.ValueOf(val)).Field(j.data.pkindex)
	return pkv.Uint()
}

func (j *Journal) Insert(val any) error {
	updateIdx := -1
	pk, isNew := j.getOrAssignPk(val)
	if !isNew {
		updateIdx, _ = j.PkIndex(pk, 0)
	}

	// write insert record to WAL
	j.wal.Write(WalRecordTypeInsert, pk, val)

	if updateIdx < 0 {
		// append to data pack
		if err := j.data.Push(val); err != nil {
			return err
		}

		// undelete if deleted (must do before mergeKeys call!)
		j.undelete([]uint64{pk})

		// update keys
		j.mergeKeys(journalEntryList{journalEntry{pk, j.data.Len() - 1}})
		j.deleted.Resize(len(j.keys))

		// set sortData flag
		j.sortData = j.sortData || pk < j.lastid
	} else {
		// replace in data pack, this also resets a zero pk after deletion
		if err := j.data.ReplaceAt(updateIdx, val); err != nil {
			return err
		}
		// undelete if deleted
		j.undelete([]uint64{pk})
	}

	// update lastid and maxid
	j.lastid = max(j.lastid, pk)
	j.maxid = max(j.maxid, pk)

	return nil
}

// Inserts multiple items, returns number of successfully processed items.
// Inserts with pk == 0 will generate a new pk > maxpk in sequential order.
// Inserts with an external pk (pk > 0) will insert or upsert and track the
// maximum pk seen.
func (j *Journal) InsertBatch(batch reflect.Value) (int, error) {
	// when inserting with external pk, make sure batch is sorted
	if batch.Len() == 0 {
		return 0, nil
	}
	SortBatch(j.data.pkindex, batch, nil)

	var count, last int
	newKeys := make(journalEntryList, 0, batch.Len())
	newPks := make([]uint64, 0, batch.Len())

	for i, l := 0, batch.Len(); i < l; i++ {
		// check ID and generate next sequence if missing
		val := batch.Index(i).Interface()
		updateIdx := -1
		pk, isNew := j.getOrAssignPk(val)
		if !isNew {
			updateIdx, last = j.PkIndex(pk, last)
		}

		if updateIdx < 0 {
			// write insert record to WAL
			j.wal.Write(WalRecordTypeInsert, pk, val)

			// append to data pack
			if err := j.data.Push(val); err != nil {
				return count, err
			}

			// update keys
			newKeys = append(newKeys, journalEntry{pk, j.data.Len() - 1})

			// set sortData flag
			j.sortData = j.sortData || pk < j.lastid

			// update lastid and maxid
			j.lastid = max(j.lastid, pk)
			j.maxid = max(j.maxid, pk)
		} else {
			// write update record to WAL
			j.wal.Write(WalRecordTypeUpdate, pk, val)

			// replace in data pack, this also resets a zero pk after deletion
			if err := j.data.ReplaceAt(updateIdx, val); err != nil {
				return count, err
			}
		}
		newPks = append(newPks, pk)
		count++
	}

	// undelete if deleted
	j.undelete(newPks)

	// merge new keys (sorted) into sorted key list
	j.mergeKeys(newKeys)
	j.deleted.Resize(len(j.keys))

	return count, nil
}

// Assumes no duplicates. pkg is not trusted to be sorted. It may come from a
// desc-ordered query result or from a result that has been sorted by a different
// field than the primary key. Primary key may exist (>0), but is generated when
// missing.
func (j *Journal) InsertPack(pkg *Package, pos, n int) (int, error) {
	l := pkg.Len()
	if l == 0 || n == 0 || n+pos > l {
		return 0, nil
	}

	// analyze primary keys of the data we insert
	pkcol := pkg.PkColumn()
	pks := pkcol[pos : pos+n]
	minid, maxid := vec.Uint64.MinMax(pks)
	isSorted := minid == 0 && maxid == 0
	isSorted = isSorted || sort.SliceIsSorted(pks, func(i, j int) bool { return pks[i] < pks[j] })

	var count, last int
	newKeys := make(journalEntryList, 0, n)

	if minid > j.lastid {
		// write insert records to WAL
		j.wal.WritePack(WalRecordTypeInsert, pkg, pos, n)

		// fast path (all ids are > last)
		jLen := j.data.Len()
		if err := j.data.AppendFrom(pkg, pos, n); err != nil {
			return 0, err
		}
		count += n
		for i, v := range pks {
			newKeys = append(newKeys, journalEntry{v, jLen + i})
		}
		j.lastid = maxid

	} else {
		// slow path, some ids are < last or 0 (must be created)
		for i, pk := range pks {
			// generate new id and overwrite in source slice
			updateIdx := -1
			if pk == 0 {
				pk = j.next()
				pkcol[pos+i] = pk
				j.sortData = true
			} else {
				updateIdx, last = j.PkIndex(pk, last)
			}

			if updateIdx < 0 {
				// write insert record to WAL
				j.wal.WritePack(WalRecordTypeInsert, pkg, pos+i, 1)

				// append to journal
				if err := j.data.AppendFrom(pkg, pos+i, 1); err != nil {
					return count, err
				}
				newKeys = append(newKeys, journalEntry{pk, j.data.Len() - 1})
			} else {
				// write update record to WAL
				j.wal.WritePack(WalRecordTypeUpdate, pkg, pos+i, 1)

				// replace in data pack, this also resets a zero pk after deletion
				if err := j.data.ReplaceFrom(pkg, updateIdx, pos+i, 1); err != nil {
					return count, err
				}
			}
			count++
			j.lastid = max(j.lastid, pk)
		}
	}

	// undelete if deleted, must call before mergeKeys!
	j.undelete(pks)

	// update keys and flags
	if !isSorted {
		sort.Sort(newKeys)
	}
	j.mergeKeys(newKeys)
	j.deleted.Resize(len(j.keys))
	j.sortData = j.sortData || !isSorted
	j.maxid = max(j.maxid, j.lastid)

	return count, nil
}

func (j *Journal) Update(val any) error {
	// require primary key
	pk := j.getPk(val)
	if pk == 0 {
		return fmt.Errorf("pack: missing primary key on val %T", val)
	}

	// write update record to WAL
	j.wal.Write(WalRecordTypeUpdate, pk, val)

	// find existing key and position in journal
	if idx, _ := j.PkIndex(pk, 0); idx < 0 {
		// append to data pack if not exists
		if err := j.data.Push(val); err != nil {
			return err
		}

		// undelete if deleted, must call before mergeKeys
		j.undelete([]uint64{pk})

		// update keys
		j.mergeKeys(journalEntryList{journalEntry{pk, j.data.Len() - 1}})
		j.deleted.Resize(len(j.keys))

		// set sortData flag
		j.sortData = j.sortData || pk < j.lastid

		// update maxid (Note: since we just check if primary key exists in
		// the journal, but not in the entire table, an update can be an insert)
		j.lastid = max(j.lastid, pk)
		j.maxid = max(j.maxid, pk)
	} else {
		// replace in data pack if exists, this also resets a zero pk after deletion
		if err := j.data.ReplaceAt(idx, val); err != nil {
			return err
		}

		// undelete if deleted
		j.undelete([]uint64{pk})
	}

	return nil
}

// Updates multiple items by inserting or overwriting them in the journal,
// returns the number of successsfully processed items. Batch is expected
// to be sorted.
func (j *Journal) UpdateBatch(batch reflect.Value) (int, error) {
	if batch.Len() == 0 {
		return 0, nil
	}
	// sort for improved update performance
	newPks := make([]uint64, batch.Len())

	// require primary keys for all items
	for i, l := 0, batch.Len(); i < l; i++ {
		val := batch.Index(i).Interface()
		pk := j.getPk(val)
		if pk == 0 {
			return 0, fmt.Errorf("pack: missing primary key on val %T", val)
		}
		newPks[i] = pk
	}
	SortBatch(j.data.pkindex, batch, newPks)

	// write update record to WAL
	j.wal.WriteMulti(WalRecordTypeUpdate, newPks, batch.Interface())

	var last, idx, count int
	newKeys := make(journalEntryList, 0, batch.Len())
	for i, l := 0, batch.Len(); i < l; i++ {
		val := batch.Index(i).Interface()
		pk := newPks[i]
		idx, last = j.PkIndex(pk, last)
		if idx < 0 {
			// append to data pack if not exists
			if err := j.data.Push(val); err != nil {
				return count, err
			}
			count++

			// create mapped keys
			newKeys = append(newKeys, journalEntry{pk, j.data.Len() - 1})

			// set sortData flag
			j.sortData = j.sortData || pk < j.lastid
			j.lastid = max(j.lastid, pk)

		} else {
			// replace in data pack if exists
			if err := j.data.ReplaceAt(idx, val); err != nil {
				return count, err
			}
			count++
		}
		// newPks = append(newPks, pk)
	}

	// undelete if deleted, must call before mergeKeys
	j.undelete(newPks)

	// merge new journal keys (they are known to be sorted because batch was sorted)
	j.mergeKeys(newKeys)
	j.deleted.Resize(len(j.keys))

	// update maxid (Note: since we just check if primary key exists in
	// the journal, but not in the entire table, an update be a hidden insert)
	j.maxid = max(j.maxid, j.lastid)

	return count, nil
}

func (j *Journal) mergeKeys(newKeys journalEntryList) {
	if len(newKeys) == 0 {
		return
	}

	// sanity-check for unsorted keys
	// if isSorted && !sort.IsSorted(newKeys) {
	// 	panic("pack: mergeKeys input is unsorted, but sorted flag is set")
	// }

	// merge newKeys into key list (both lists are sorted)
	if cap(j.keys) < len(j.keys)+len(newKeys) {
		cp := make(journalEntryList, len(j.keys), roundSize(len(j.keys)+len(newKeys)))
		copy(cp, j.keys)
		j.keys = cp
	}

	// fast path for append-only
	if len(j.keys) == 0 || newKeys[0].pk > j.keys[len(j.keys)-1].pk {
		j.keys = append(j.keys, newKeys...)
		return
	}

	// merge backward

	// keep position of the last value in keys
	last := len(j.keys) - 1

	// extend keys len
	j.keys = j.keys[:len(j.keys)+len(newKeys)]

	// ignore equal keys, they cannot exist here (as safety measure, we still copy them)
	for in1, in2, out := last, len(newKeys)-1, len(j.keys)-1; in2 >= 0; {
		// insert new keys as long as they are larger or all old keys have been
		// copied (i.e. any new key is smaller than the first old key)
		for in2 >= 0 && (in1 < 0 || j.keys[in1].pk < newKeys[in2].pk) {
			j.keys[out] = newKeys[in2]
			in2--
			out--
		}

		// insert old keys as long as they are larger (safety: although no
		// duplicate keys are allowed, we simply copy them using >= instead of >)
		for in1 >= 0 && (in2 < 0 || j.keys[in1].pk >= newKeys[in2].pk) {
			j.keys[out] = j.keys[in1]
			in1--
			out--
		}
	}
}

func (j *Journal) Delete(pk uint64) (int, error) {
	// pk must exist
	if pk <= 0 || pk > j.maxid {
		return 0, fmt.Errorf("pack: delete pk out-of-bounds")
	}

	// write delete record to WAL
	j.wal.Write(WalRecordTypeDelete, pk, nil)

	// find if key exists in journal and mark entry as deleted
	idx, _ := j.PkIndex(pk, 0)
	if idx >= 0 {
		// overwrite journal pk col with zero (this signals to query and
		// flush operations that this item is deleted and should be skipped)
		j.data.SetFieldAt(j.data.pkindex, idx, uint64(0))

		// remember the journal position was deleted, so that a subsequent
		// insert/upsert call can properly undelete
		j.deleted.Set(idx)
	}

	// grow tomb capacity if too small
	if cap(j.tomb) < len(j.tomb)+1 {
		cp := make([]uint64, len(j.tomb), roundSize(len(j.tomb)+1))
		copy(cp, j.tomb)
		j.tomb = cp
	}

	// find insert position
	idx = sort.Search(len(j.tomb), func(i int) bool { return j.tomb[i] >= pk })
	if idx < len(j.tomb) && j.tomb[idx] == pk {
		// ignore duplicate
		return 0, nil
	}

	// insert new pk
	j.tomb = j.tomb[:len(j.tomb)+1]
	copy(j.tomb[idx+1:], j.tomb[idx:])
	j.tomb[idx] = pk

	return 1, nil
}

func (j *Journal) DeleteBatch(pks []uint64) (int, error) {
	if len(pks) == 0 {
		return 0, nil
	}

	// the algorithm below requires ids to be sorted and unique
	pks = vec.Uint64.Unique(pks)
	pks = vec.Uint64.RemoveZeros(pks)
	if len(pks) == 0 {
		return 0, nil
	}

	// pks must exist
	if pks[len(pks)-1] > j.maxid {
		return 0, fmt.Errorf("pack: delete pk out-of-bounds")
	}

	// write delete records to WAL
	j.wal.WriteMulti(WalRecordTypeDelete, pks, nil)

	var last, idx int
	for _, pk := range pks {
		// find existing key and position in journal
		if idx, last = j.PkIndex(pk, last); idx >= 0 {
			// overwrite journal pk col with zero (this signals to query and
			// flush operations that this item is deleted and should be skipped)
			j.data.SetFieldAt(j.data.pkindex, idx, uint64(0))

			// remember the journal position was deleted, so that a subsequent
			// insert/upsert call can properly undelete
			j.deleted.Set(idx)
		}
		// stop journal scan if deleted ids are larger than whats stored in the
		// journal right now
		if last == j.data.Len() {
			break
		}
	}

	// Merge-sort ids into tomb, this keeps tomb always sorted and is
	// the most efficient sort strategy. The algo below finds the next
	// insert position in tomb, then inserts all pks from ids that are
	// smaller than the next value in the tomb. Duplicate pks are skipped.

	// grow tomb capacity first so we're sure we can hold the final result
	if cap(j.tomb) < len(j.tomb)+len(pks) {
		cp := make([]uint64, len(j.tomb), roundSize(len(j.tomb)+len(pks)))
		copy(cp, j.tomb)
		j.tomb = cp
	}

	// fast path for append-only
	if len(j.tomb) == 0 || pks[0] > j.tomb[len(j.tomb)-1] {
		j.tomb = append(j.tomb, pks...)
		return len(pks), nil
	}

	// merge backwards
	// keep position of the last value in tomb
	last, count, move := len(j.tomb)-1, len(pks), 0

	// extend tomb len
	j.tomb = j.tomb[:len(j.tomb)+len(pks)]

	// fill from back
	for in1, in2, out := last, len(pks)-1, len(j.tomb)-1; in2 >= 0; {
		// skip duplicate ids (that already exist in tomb)
		for in2 >= 0 && in1 >= 0 && j.tomb[in1] == pks[in2] {
			move++
			count--
			in2--
		}

		// insert new keys as long as they are larger or all keys are
		// copied (i.e. all remaining new keys are smaller than the first old key)
		for in2 >= 0 && (in1 < 0 || j.tomb[in1] < pks[in2]) {
			j.tomb[out] = pks[in2]
			in2--
			out--
		}

		// insert old keys as long as they are larger
		for in1 >= 0 && (in2 < 0 || j.tomb[in1] > pks[in2]) {
			j.tomb[out] = j.tomb[in1]
			in1--
			out--
		}
	}

	// correct for duplicates
	if move > 0 {
		copy(j.tomb[:len(j.tomb)-move], j.tomb[move:])
		j.tomb = j.tomb[:len(j.tomb)-move]
	}

	return count, nil
}

// To support insert/update-after-delete we remove entries from the
// tomb and we reconstruct the previous state of the undeleted entry
// in our data pack (i.e. we restore its primary key) and reset the
// deleted flag. pks must be storted.
func (j *Journal) undelete(pks []uint64) {
	var idx, last, lastTomb int
	for len(pks) > 0 {
		// reset the deleted bit and restore pk
		idx, last = j.PkIndex(pks[0], last)
		if idx > -1 {
			j.deleted.Clear(idx)
			j.data.SetFieldAt(j.data.pkindex, idx, pks[0])
		}
		// find the next match
		next := sort.Search(len(j.tomb)-lastTomb, func(k int) bool { return j.tomb[lastTomb+k] >= pks[0] })
		if lastTomb+next < len(j.tomb) && j.tomb[lastTomb+next] == pks[0] {
			// count consecutive matches
			count := 1
			for {
				if count >= len(pks) {
					break
				}
				if lastTomb+next+count >= len(j.tomb) {
					break
				}
				if j.tomb[lastTomb+next+count] != pks[count] {
					break
				}
				count++
				// unset deleted bit and restore pk
				idx, last = j.PkIndex(pks[count], last)
				if idx > -1 {
					j.deleted.Clear(idx)
					j.data.SetFieldAt(j.data.pkindex, idx, pks[count])
				}
			}
			// delete all matches from tomb and undelete list
			pks = pks[count:]
			j.tomb = append(j.tomb[:lastTomb+next], j.tomb[lastTomb+next+count:]...)

			// prepare next iteration (continue scan at match position, this is
			// where the next unprocessed element has been moved)
			lastTomb += next
		} else {
			// allow non-deleted entries (common case)
			pks = pks[1:]
		}
	}
}

// Efficient check if a pk is in the tomb or not. Use `last` to skip already
// processed entries when walking through a sorted list of pks.
func (j *Journal) IsDeleted(pk uint64, last int) (bool, int) {
	// early return when out of bounds
	if last >= len(j.tomb) {
		return false, len(j.tomb)
	}

	// find pk in tomb, always sorted
	idx := sort.Search(len(j.tomb)-last, func(i int) bool { return j.tomb[last+i] >= pk })

	// return true + new index for direct match
	if last+idx < len(j.tomb) && j.tomb[last+idx] == pk {
		return true, last + idx
	}

	// reached end of tomb, no more matches
	if last+idx == len(j.tomb) {
		return false, len(j.tomb)
	}

	// pk is not in tomb, but more search results are available
	return false, last
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
	if pk > j.lastid || last >= len(j.keys) {
		return -1, len(j.keys)
	}

	// find pk in keys list, use last as hint to limit search space
	last += sort.Search(len(j.keys)-last, func(i int) bool { return j.keys[last+i].pk >= pk })

	// return index	if found or -1 otherwise
	// Note: when entry is deleted, we still return its position since other
	// parts of this algorithm rely on this behaviour
	if last < len(j.keys) && j.keys[last].pk == pk {
		return j.keys[last].idx, last
	}
	return -1, last
}

// Checks invariants
func (j *Journal) checkInvariants(when string) error {
	// check invariants
	if a, b := j.data.Len(), len(j.keys); a != b {
		return fmt.Errorf("journal %s: INVARIANT VIOLATION: data-pack-len=%d key-len=%d", when, a, b)
	}
	if a, b := j.data.Len(), j.deleted.Len(); a != b {
		return fmt.Errorf("journal %s: INVARIANT VIOLATION: data-pack-len=%d deleted-bitset-len=%d", when, a, b)
	}
	for i, v := range j.keys {
		if i == 0 {
			continue
		}
		if j.keys[i-1].pk > v.pk {
			return fmt.Errorf("journal %s: INVARIANT VIOLATION: unsorted keys", when)
		}
		if j.keys[i-1].pk == v.pk {
			return fmt.Errorf("journal %s: INVARIANT VIOLATION: duplicate key", when)
		}
	}
	for i, v := range j.tomb {
		if i == 0 {
			continue
		}
		if j.tomb[i-1] > v {
			return fmt.Errorf("journal %s: INVARIANT VIOLATION: unsorted tomb %#v", when, j.tomb)
		}
		if j.tomb[i-1] == v {
			return fmt.Errorf("journal %s: INVARIANT VIOLATION: duplicate tomb pk %#v", when, j.tomb)
		}
	}
	// no duplicate pks in pack (consider deleted keys == 0)
	pks := j.data.PkColumn()
	sorted := make([]uint64, len(pks))
	copy(sorted, pks)
	sorted = vec.Uint64.Sort(sorted)
	for i, v := range sorted {
		if i == 0 || v == 0 || sorted[i-1] == 0 {
			continue
		}
		if have, want := v, sorted[i-1]; have == want {
			return fmt.Errorf("journal %s: INVARIANT VIOLATION: duplicate pk %d in data pack", when, v)
		}
	}
	return nil
}

type dualSorter struct {
	pk []uint64
	id []int
}

func (s dualSorter) Len() int           { return len(s.pk) }
func (s dualSorter) Less(i, j int) bool { return s.pk[i] < s.pk[j] }
func (s dualSorter) Swap(i, j int) {
	s.pk[i], s.pk[j] = s.pk[j], s.pk[i]
	s.id[i], s.id[j] = s.id[j], s.id[i]
}

// On lookup/query we run matching algos on the journal pack which produce a bitset
// of all matches. The algo below takes this bitset and translates it into a pk
// sorted index list.
//
// 1. Cond.MatchPack() -> Bitset (1s at unsorted journal matches)
// 2. Bitset.Indexes() -> []int (positions in unsorted journal)
// 3. data.Column(pkid) -> []uint64 (lookup pks at indexes)
// 4. Joined sort index/pks by pk
// 5. Return pk-sorted index list
func (j *Journal) SortedIndexes(b *vec.Bitset) ([]int, []uint64) {
	ds := dualSorter{
		pk: make([]uint64, b.Count()),
		id: b.Indexes(nil),
	}
	// fill pks
	pk := j.data.PkColumn()
	for i, n := range ds.id {
		ds.pk[i] = pk[n]
	}
	sort.Sort(ds)

	// strip all entries that have been marked as deleted (pk == 0)
	firstNonZero := sort.Search(len(ds.pk), func(k int) bool { return ds.pk[k] > 0 })
	ds.id = ds.id[firstNonZero:]
	ds.pk = ds.pk[firstNonZero:]

	// return data pack positions and corresponding pks
	return ds.id, ds.pk
}

func (j *Journal) SortedIndexesReversed(b *vec.Bitset) ([]int, []uint64) {
	id, pk := j.SortedIndexes(b)
	for i, j := 0, len(id)-1; i < j; i, j = i+1, j-1 {
		id[i], id[j] = id[j], id[i]
		pk[i], pk[j] = pk[j], pk[i]
	}
	return id, pk
}

func (j *Journal) DataPack() *Package {
	return j.data
}

func (j *Journal) Reset() {
	// Note: don't alter j.maxid
	j.data.Clear()
	if len(j.keys) > 0 {
		j.keys[0].idx = 0
		j.keys[0].pk = 0
		for bp := 1; bp < len(j.keys); bp *= 2 {
			copy(j.keys[bp:], j.keys[:bp])
		}
		j.keys = j.keys[:0]
	}
	if len(j.tomb) > 0 {
		j.tomb[0] = 0
		for bp := 1; bp < len(j.tomb); bp *= 2 {
			copy(j.tomb[bp:], j.tomb[:bp])
		}
		j.tomb = j.tomb[:0]
	}
	j.lastid = 0
	j.sortData = false
	j.deleted.Reset()
	j.wal.Reset()
}

func SortBatch(idx int, rval reflect.Value, pks []uint64) {
	if idx < 0 {
		return
	}

	// sort with known pks
	if pks != nil {
		if pks[0] == 0 {
			return
		}
		NewBatchSorter(rval, pks).Sort()
	} else {
		// reflect only sort
		if reflect.Indirect(rval.Index(0)).Field(idx).Uint() == 0 {
			return
		}
		sort.Slice(rval.Interface(), func(i, j int) bool {
			idA := reflect.Indirect(rval.Index(i)).Field(idx).Uint()
			idB := reflect.Indirect(rval.Index(j)).Field(idx).Uint()
			return idA < idB
		})
	}
}

type BatchSorter struct {
	val  reflect.Value
	pks  []uint64
	swap func(i, j int)
}

func NewBatchSorter(val reflect.Value, pks []uint64) BatchSorter {
	return BatchSorter{
		val:  val,
		pks:  pks,
		swap: reflect.Swapper(val.Interface()),
	}
}

func (s BatchSorter) Sort() bool {
	if sort.IsSorted(s) {
		return false
	}
	sort.Sort(s)
	return true
}

func (s BatchSorter) Len() int { return s.val.Len() }

func (s BatchSorter) Less(i, j int) bool { return s.pks[i] < s.pks[j] }

func (s BatchSorter) Swap(i, j int) {
	s.swap(i, j)
	s.pks[i], s.pks[j] = s.pks[j], s.pks[i]
}
