// Copyright (c) 2018-2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package pack

import (
	"fmt"
	"sort"

	"blockwatch.cc/knoxdb/store"
	"blockwatch.cc/knoxdb/util"
	"blockwatch.cc/knoxdb/vec"
)

// Append-only journal and in-memory tombstone
// - keeps mapping from primary keys to journal positions as cache
// - journal data is never sorted
// - primary keys are always sorted (insert with id, update, delete, query, flush)
// - tombstone is always sorted
//
// TODO
// - write all incoming inserts/updates/deletes to a WAL
// - load and reconstructed journal + tomb from WAL
//
// Re-inserting deleted entries is safe because on deletion the pk column is
// overwritten with zero value. Such rows are never flushed. Insert after delete
// is also safe and explicitly handled by the journal by removing the pk from its
// tombstone list.
//
type Journal struct {
	lastid   uint64 // the highest primary key in the journal, used for sorting
	maxid    uint64 // the highest primary key in the table, used to generate new ids
	maxsize  int    // max number of entries before flush
	sortData bool   // true = data pack is unsorted

	data    *Package         // journal pack storing live data
	keys    journalEntryList // 0: pk, 1: index in journal; sorted by pk, may be unsorted
	tomb    []uint64         // list of deleted primary keys, always sorted
	deleted *vec.BitSet      // tracks which journal positions are in tomb
}

type journalEntry struct {
	pk  uint64
	idx int
}

type journalEntryList []journalEntry

func (l journalEntryList) Len() int           { return len(l) }
func (l journalEntryList) Less(i, j int) bool { return l[i].pk < l[j].pk }
func (l journalEntryList) Swap(i, j int)      { l[i], l[j] = l[j], l[i] }

func NewJournal(maxid uint64, size int) *Journal {
	return &Journal{
		maxid:   maxid,
		maxsize: size,
		data:    NewPackage(size),
		keys:    make(journalEntryList, 0, size),
		tomb:    make([]uint64, 0, size),
		deleted: vec.NewCustomBitSet(size).Grow(0),
	}
}

func (j *Journal) InitFields(fields []Field) error {
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
	return j.data.InitFields(fields, tinfo)
}

func (j *Journal) LoadLegacy(dbTx store.Tx, bucketName []byte) error {
	if _, err := loadPackTx(dbTx, bucketName, encodePackKey(journalKey), j.data); err != nil {
		return err
	}
	j.sortData = false
	col, _ := j.data.Column(j.data.pkindex)
	for i, n := range col.([]uint64) {
		j.keys = append(j.keys, journalEntry{n, i})
		j.sortData = j.sortData || n < j.lastid
		j.lastid = util.MaxU64(j.lastid, n)
	}
	// ensure invariant that keys are always sorted
	if j.sortData {
		sort.Sort(j.keys)
	}
	tomb, err := loadPackTx(dbTx, bucketName, encodePackKey(tombstoneKey), nil)
	if err != nil {
		return fmt.Errorf("pack: cannot open tombstone for table %s: %v", string(bucketName), err)
	}
	tomb.InitType(Tombstone{})
	col, _ = tomb.Column(0)
	pk := col.([]uint64)
	if cap(j.tomb) < len(pk) {
		j.tomb = make([]uint64, len(pk))
	}
	j.tomb = j.tomb[:len(pk)]
	copy(j.tomb, pk)
	tomb.Release()
	j.deleted.Grow(len(j.keys))
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
	}
	return nil
}

func (j *Journal) StoreLegacy(dbTx store.Tx, bucketName []byte) (int, error) {
	n, err := storePackTx(dbTx, bucketName, encodePackKey(journalKey), j.data, defaultJournalFillLevel)
	if err != nil {
		return 0, err
	}
	tomb := NewPackage(len(j.tomb))
	tomb.InitType(Tombstone{})
	defer tomb.Release()
	for _, v := range j.tomb {
		ts := Tombstone{v}
		_ = tomb.Push(ts)
	}
	m, err := storePackTx(dbTx, bucketName, encodePackKey(tombstoneKey), tomb, defaultJournalFillLevel)
	if err != nil {
		return n, err
	}
	n += m
	return n, nil
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

func (j *Journal) MaxId() uint64 {
	return j.maxid
}

func (j *Journal) next() uint64 {
	j.maxid++
	return j.maxid
}

func (j *Journal) Insert(item Item) error {
	// check ID and generate next sequence if missing
	pk := item.ID()
	if pk == 0 {
		pk = j.next()
		item.SetID(pk)
	}

	// write insert record to WAL
	// TODO

	// append to data pack
	if err := j.data.Push(item); err != nil {
		return err
	}

	// undelete if deleted
	j.undelete([]uint64{pk}, true)

	// update keys
	j.mergeKeys(journalEntryList{journalEntry{pk, j.data.Len() - 1}}, pk > j.lastid)
	j.deleted.Grow(len(j.keys))

	// set sortData flag
	j.sortData = j.sortData || pk < j.lastid

	// update lastid and maxid
	j.lastid = util.MaxU64(j.lastid, pk)
	j.maxid = util.MaxU64(j.maxid, pk)

	return nil
}

// Inserts multiple items, returns number of successfully processed items.
// Insert with pk == 0 will generate a new pk in sequential order.
// insert with external pk set (pk > 0) will insert or upsert and track the
// maximum pk seen.
func (j *Journal) InsertBatch(batch []Item) (int, error) {
	// when inserting with external pk, make sure batch is sorted
	SortItems(batch)

	var count, last int
	newKeys := make(journalEntryList, 0, len(batch))
	newPks := make([]uint64, 0, len(batch))

	for _, item := range batch {
		// check ID and generate next sequence if missing
		pk := item.ID()
		updateIdx := -1
		if pk == 0 {
			pk = j.next()
			item.SetID(pk)
		} else {
			updateIdx, last = j.PkIndex(pk, last)
		}

		// write insert record to WAL
		// TODO

		if updateIdx < 0 {
			// append to data pack
			if err := j.data.Push(item); err != nil {
				return count, err
			}

			// update keys
			newKeys = append(newKeys, journalEntry{pk, j.data.Len() - 1})

			// set sortData flag
			j.sortData = j.sortData || pk < j.lastid

			// update lastid and maxid
			j.lastid = util.MaxU64(j.lastid, pk)
			j.maxid = util.MaxU64(j.maxid, pk)
		} else {
			// replace in data pack
			if err := j.data.ReplaceAt(updateIdx, item); err != nil {
				return count, err
			}
		}
		newPks = append(newPks, pk)
		count++
	}

	// undelete if deleted
	j.undelete(newPks, true)

	// merge new keys (sorted) into sorted key list
	j.mergeKeys(newKeys, true)
	j.deleted.Grow(len(j.keys))

	return count, nil
}

// Assumes no duplicates, packs may be sorted (unless coming from desc result or
// a different field sort was applied before) and pks exist.
func (j *Journal) InsertPack(pkg *Package, pos, n int) (int, error) {
	l := pkg.Len()
	if l == 0 || n == 0 || n+pos > l {
		return 0, nil
	}

	// analyze primary keys of the data we insert
	col, _ := pkg.Column(pkg.pkindex)
	pkcol := col.([]uint64)
	pks := pkcol[pos : pos+n]
	minid, maxid := vec.Uint64Slice(pks).MinMax()
	isSorted := minid == 0 && maxid == 0
	isSorted = isSorted || sort.SliceIsSorted(pks, func(i, j int) bool { return pks[i] < pks[j] })

	var count, last int
	newKeys := make(journalEntryList, 0, n)

	if minid > j.lastid {
		// fast path (all ids are > last)
		jLen := j.data.Len()
		if err := j.data.AppendFrom(pkg, pos, n, true); err != nil {
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
			} else {
				updateIdx, last = j.PkIndex(pk, last)
			}

			if updateIdx < 0 {
				// write insert record to WAL
				// TODO

				// append to journal
				if err := j.data.AppendFrom(pkg, pos+i, 1, true); err != nil {
					return count, err
				}
				newKeys = append(newKeys, journalEntry{pk, j.data.Len() - 1})
			} else {
				// write update record to WAL
				// TODO

				// replace in journal
				if err := j.data.ReplaceFrom(pkg, updateIdx, pos+i, 1); err != nil {
					return count, err
				}
			}
			count++
			j.lastid = util.MaxU64(j.lastid, pk)
		}
	}

	// undelete if deleted
	j.undelete(pks, true)

	// update keys and flags
	j.mergeKeys(newKeys, isSorted)
	j.deleted.Grow(len(j.keys))
	j.sortData = j.sortData || !isSorted
	j.maxid = util.MaxU64(j.maxid, j.lastid)

	return count, nil
}

func (j *Journal) Update(item Item) error {
	// require primary key
	pk := item.ID()
	if pk == 0 {
		return fmt.Errorf("pack: missing primary key on %T item", item)
	}

	// write update record to WAL
	// TODO

	// undelete if deleted
	j.undelete([]uint64{pk}, true)

	// find existing key and position in journal
	if idx, _ := j.PkIndex(pk, 0); idx < 0 {
		// append to data pack if not exists
		if err := j.data.Push(item); err != nil {
			return err
		}

		// update keys
		j.mergeKeys(journalEntryList{journalEntry{pk, j.data.Len() - 1}}, pk > j.lastid)
		j.deleted.Grow(len(j.keys))

		// set sortData flag
		j.sortData = j.sortData || pk < j.lastid

		// update maxid (Note: since we just check if primary key exists in
		// the journal, but not in the entire table, an update can be an insert)
		j.lastid = util.MaxU64(j.lastid, pk)
		j.maxid = util.MaxU64(j.maxid, pk)
	} else {
		// replace in data pack if exists
		if err := j.data.ReplaceAt(idx, item); err != nil {
			return err
		}
	}

	return nil
}

// Updates multiple items by inserting or overwriting them in the journal,
// returns the number of successsfully processed items. Batch is expected
// to be sorted.
func (j *Journal) UpdateBatch(batch []Item) (int, error) {
	// require primary keys for all items
	for _, item := range batch {
		if item.ID() == 0 {
			return 0, fmt.Errorf("pack: missing primary key on %T item", item)
		}
	}

	// sort for improved update performance
	SortItems(batch)
	var last, idx, count int
	newPks := make([]uint64, 0, len(batch))

	newKeys := make(journalEntryList, 0, len(batch))
	for _, item := range batch {
		pk := item.ID()
		idx, last = j.PkIndex(pk, last)
		if idx < 0 {
			// append to data pack if not exists
			if err := j.data.Push(item); err != nil {
				return count, err
			}
			count++

			// create mapped keys
			newKeys = append(newKeys, journalEntry{pk, j.data.Len() - 1})

			// set sortData flag
			j.sortData = j.sortData || pk < j.lastid
			j.lastid = util.MaxU64(j.lastid, pk)

		} else {
			// replace in data pack if exists
			if err := j.data.ReplaceAt(idx, item); err != nil {
				return count, err
			}
			count++
		}
		newPks = append(newPks, pk)
	}

	// undelete if deleted
	j.undelete(newPks, true)

	// merge new journal keys (they are known to be sorted because batch was sorted)
	j.mergeKeys(newKeys, true)
	j.deleted.Grow(len(j.keys))

	// update maxid (Note: since we just check if primary key exists in
	// the journal, but not in the entire table, an update be a hidden insert)
	j.maxid = util.MaxU64(j.maxid, j.lastid)

	return count, nil
}

func (j *Journal) mergeKeys(newKeys journalEntryList, isSorted bool) {
	if len(newKeys) == 0 {
		return
	}

	if !isSorted {
		sort.Sort(newKeys)
	}

	// merge newKeys into key list (both lists are sorted)
	// Note: deleted entries have pk == 0
	if cap(j.keys) < len(j.keys)+len(newKeys) {
		cp := make(journalEntryList, len(j.keys), len(j.keys)+len(newKeys))
		copy(cp, j.keys)
		j.keys = cp
	}

	for i := 0; i < len(j.keys) && len(newKeys) > 0; {
		// find the next insert position, binary search is ok because keys
		// is sorted at each round of this algorithm
		i += sort.Search(len(j.keys)-i, func(k int) bool { return j.keys[k+i].pk > newKeys[0].pk })
		if i >= len(j.keys) {
			// done
			break
		}

		// sanity check for duplicate pks (should have been replaced)
		// if j.keys[i].pk == newKeys[0].pk {
		// 	log.Errorf("pack: pk %d inserted, but already exists in journal at pos %d", newKeys[0].pk, newKeys[0].idx)
		// 	newKeys = newKeys[1:]
		// 	continue
		// }

		// take all elements in ids that are smaller than the next value in tomb
		var k int
		for k < len(newKeys) && newKeys[k].pk < j.keys[i].pk {
			k++
		}

		// make room for k elements at position i in tomb
		j.keys = j.keys[:len(j.keys)+k]
		copy(j.keys[i+k:], j.keys[i:])

		// insert k elements from ids into tomb at position i
		copy(j.keys[i:], newKeys[:k])

		// shorten ids by k processed elements
		newKeys = newKeys[k:]

		// update tomb insert index
		i += k
	}
	// append remainder (when all keys have been processed before, this is a noop)
	j.keys = append(j.keys, newKeys...)
}

func (j *Journal) DeleteBatch(ids []uint64) (int, error) {
	if len(ids) == 0 {
		return 0, nil
	}

	// the algorithm below requires ids to be sorted and unique
	ids = vec.UniqueUint64Slice(ids)
	for ids[0] == 0 {
		ids = ids[1:]
	}

	// ids must exist
	if ids[len(ids)-1] > j.maxid {
		return 0, fmt.Errorf("pack: delete ids out-of-bounds")
	}

	// write delete records to WAL
	// TODO
	// for _, id := range ids {
	// }

	var last, idx int
	for _, id := range ids {
		// find existing key and position in journal
		if idx, last = j.PkIndex(id, last); idx >= 0 {
			// overwrite journal pk col with zero (this signals to not query and
			// flush operations that this item is deleted and should be skipped)
			j.data.SetFieldAt(j.data.pkindex, idx, uint64(0))

			// remember the journal position was deleted, so that a subsequent
			// insert/upsert call can properly undelete
			j.deleted.Set(idx)
		}
		// stop journal scan if deleted ids are larger than whats stored in the
		// journal right now
		if last > j.data.Len() {
			break
		}
	}

	// Merge-sort ids into tomb, this keeps tomb always sorted and is
	// the most efficient sort strategy. The algo below finds the next
	// insert position in tomb, then inserts all eligible elements from ids
	// in each step. Duplicate id values that already exist in tomb are skipped.

	// grow tomb capacity first so we're sure we can hold the final result
	if cap(j.tomb) < len(j.tomb)+len(ids) {
		cp := make([]uint64, len(j.tomb), len(j.tomb)+len(ids))
		copy(cp, j.tomb)
		j.tomb = cp
	}

	count := len(ids)
	for i := 0; i < len(j.tomb) && len(ids) > 0; {
		for i < len(j.tomb) && j.tomb[i] < ids[0] {
			i++
		}
		if i == len(j.tomb) {
			// done
			break
		}

		// skip duplicate ids (that already exist in tomb)
		if j.tomb[i] == ids[0] {
			ids = ids[1:]
			continue
		}

		// take all elements in ids that are smaller than the next value in tomb
		var k int
		for k < len(ids) && ids[k] < j.tomb[i] {
			k++
		}

		// make room for k elements at position i in tomb
		j.tomb = j.tomb[:len(j.tomb)+k]
		copy(j.tomb[i+k:], j.tomb[i:])

		// insert k elements from ids into tomb at position i
		copy(j.tomb[i:], ids[:k])

		// shorten ids by k processed elements
		ids = ids[k:]

		// update tomb insert index
		i += k
	}

	// append remainder (when all ids have been processed before, this is a noop)
	j.tomb = append(j.tomb, ids...)
	return count, nil
}

func (j *Journal) undelete(pks []uint64, isSorted bool) {
	if !isSorted {
		vec.Uint64Slice(pks).Sort()
	}
	var idx, last, lastTomb int
	for len(pks) > 0 {
		// reset the deleted bit
		idx, last = j.PkIndex(pks[0], last)
		if idx > -1 {
			j.deleted.Clear(idx)
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
				// unset deleted bit
				idx, last = j.PkIndex(pks[count], last)
				if idx > -1 {
					j.deleted.Clear(idx)
				}
			}
			// delete all matches from tomb and undelete list
			pks = pks[count:]
			j.tomb = append(j.tomb[:lastTomb+next], j.tomb[lastTomb+next+count:]...)

			// prepare next iteration (continue scan at match position, this is
			// where the next unprocessed element has been moved)
			lastTomb += next
		} else {
			// unlikely, but in case we allow non-deleted entries, this is safe
			pks = pks[1:]
		}
	}
}

// How the journal is used right now:
//
// Lookup: (non-order-preserving) matches pk values only (better performance
// when sorted, but we can use the keys list and map to positions)

// Query, Stream, Count: run full pack match on journal, then walk packs and
// cross-check with tomb + journal to decide if/which rows to return; tick off
// journal matches as visted, skip journal checks when bitset is empty
//
// Forward order
// - match can work on Journal.DataPack(), bit set is in storage order
// - pack walk is in pk order and checks are via Journal.PkIndex() == storgae index
// - bitset tick-off happens with storage index (OK)
// - finally, last journal matches are processed (NEED SortedIndexes func)
//
// Reverse order considerations
// - journal match in reverse sorted indexes order as first step
// - skipping primary keys of all previously stored rows (i.e. process new inserts only)
// - other than that forward order considerations apply
//
func (j *Journal) IsDeleted(pk uint64, last int) (bool, int) {
	// find pk in tomb, always sorted
	idx := sort.Search(len(j.tomb)-last, func(i int) bool { return j.tomb[last+i] >= pk })
	return last+idx < len(j.tomb), last + idx
}

// Returns the journal index at which pk is stored or -1 when pk is not found and the last
// index that matched. Using the last argument allows to skip searching a part of the journal
// for better efficiency in loops. This works only if subsequent calls can guarantee that
// queried primary keys are sorted, i.e. the next pk is larger than the previous pk.
//
// var last, index int
// for last < journal.Len() {
//    index, last = journal.PkIndex(pk, last)
// }
//
func (j *Journal) PkIndex(pk uint64, last int) (int, int) {
	// Invariant: keys list is always sorted

	// find pk in keys list, use last as hint to limit search space
	idx := sort.Search(len(j.keys)-last, func(i int) bool { return j.keys[last+i].pk >= pk })

	// return index	if found or -1 otherwise
	if last+idx < len(j.keys) && j.keys[last+idx].pk == pk {
		return j.keys[last+idx].idx, last + idx
	}
	return -1, len(j.keys)
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
	if !sort.IsSorted(j.keys) {
		return fmt.Errorf("journal %s: INVARIANT VIOLATION: keys are unsorted", when)
	}
	if !sort.IsSorted(vec.Uint64Slice(j.tomb)) {
		return fmt.Errorf("journal: %s: INVARIANT VIOLATION: tomb is unsorted", when)
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

// To avoid sorting the journal after insert, but still process journal entries
// in pk sort order, we generate a sorted list of indexes to visit.
//
// Generating that list happens indirectly and makes use of a key-to-position map.
// On lookup/query we run matching algos on the journal pack which produce a bitset
// of all matches. The algo below takes this bitset and translates it into a pk
// sorted index list.
//
// 1. Cond.MatchPack() -> BitSet (1s at unsorted journal matches)
// 2. BitSet.Indexes() -> []int (positions in unsorted journal)
// 3. data.Column(pkid) -> []uint64 (lookup pks at indexes)
// 4. Joined sort index/pks by pk
// 5. Return pk-sorted index list
//
func (j *Journal) SortedIndexes(b *vec.BitSet) ([]int, []uint64) {
	ds := dualSorter{
		pk: make([]uint64, b.Count()),
		id: b.Indexes(nil),
	}
	// fill pks
	col, _ := j.data.Column(j.data.pkindex)
	pk, _ := col.([]uint64)
	for i, n := range ds.id {
		ds.pk[i] = pk[n]
	}
	sort.Sort(ds)

	// strip all entries that have been marked as deleted (pk == 0)
	firstNonZero := sort.Search(len(ds.pk), func(k int) bool { return ds.pk[k] > 0 })
	ds.id = ds.id[firstNonZero:]
	ds.pk = ds.pk[firstNonZero:]

	// ?? maybe return the pks as well since they are useful
	return ds.id, ds.pk
}

func (j *Journal) SortedIndexesReversed(b *vec.BitSet, maxPackedPk uint64) ([]int, []uint64) {
	id, pk := j.SortedIndexes(b)
	end := len(id)
	for i, j := 0, len(id)-1; i < j; i, j = i+1, j-1 {
		id[i], id[j] = id[j], id[i]
		pk[i], pk[j] = pk[j], pk[i]
		if pk[i] > maxPackedPk {
			end = i
		}
	}
	return id[:end], pk[:end]
}

// The old packdb flush mechanism
// - requires sorted tomb and data packs
//   - data must be sorted if sortData is true
//   - tomb is always sorted
// - locks the entire table, so journal access is automatically running under lock
func (j *Journal) Sort() {
	if j.sortData {
		_ = j.data.PkSort()
		j.sortData = false
		// rebuild keys
		for i := range j.keys {
			j.keys[i].idx = i
		}
	}
}

// cols must be storted
func (j *Journal) KeyColumns() (pk []uint64, tomb []uint64) {
	j.Sort()
	col, _ := j.data.Column(j.data.pkindex)
	pk, _ = col.([]uint64)
	tomb = j.tomb
	return
}

func (j *Journal) DataPack() *Package {
	return j.data
}

func (j *Journal) Reset() {
	// Note: don't alter j.maxid
	j.data.Clear()
	j.keys = j.keys[:0]
	j.tomb = j.tomb[:0]
	j.lastid = 0
	j.sortData = false
	j.deleted.Reset()
}
