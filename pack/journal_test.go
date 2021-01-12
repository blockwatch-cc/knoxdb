// Copyright (c) 2018-2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc
//

package pack

import (
	"fmt"
	"math/rand"
	"testing"

	"blockwatch.cc/knoxdb/encoding/block"
	"blockwatch.cc/knoxdb/util"
	"blockwatch.cc/knoxdb/vec"
)

var journalTestSizes = []int{1 << 8, 1 << 10, 1 << 15, 1 << 16}

type JournalTestType struct {
	Pk uint64 `knox:"I,pk"`
	N  int    `knox:"n,i32"`
}

func (j JournalTestType) ID() uint64 {
	return j.Pk
}

func (j *JournalTestType) SetID(id uint64) {
	j.Pk = id
}

func makeJournalTestData(n int) []Item {
	items := make([]Item, n)
	for i := 0; i < n; i++ {
		items[i] = &JournalTestType{
			Pk: 0,
			N:  i,
		}
	}
	return items
}

func makeJournalTestDataWithRandomPk(n int) ItemList {
	// generate random values
	values := randUint64Slice(n, 1)

	// strip duplicates and sort
	values = vec.UniqueUint64Slice(values)

	items := make(ItemList, len(values))
	for i := range values {
		items[i] = &JournalTestType{
			Pk: values[i],
			N:  i,
		}
	}
	return items
}

func randJournalData(n, sz int) []ItemList {
	res := make([]ItemList, n)
	for i := range res {
		res[i] = makeJournalTestDataWithRandomPk(sz)
	}
	return res
}

func shuffleItems(items ItemList) ItemList {
	rand.Shuffle(len(items), func(i, j int) { items[i], items[j] = items[j], items[i] })
	return items
}

func checkJournalSizes(t *testing.T, j *Journal, size, tomb, delcount int) {
	if got, want := j.Len(), size; got != want {
		t.Errorf("invalid journal size: got=%d want=%d", got, want)
	}
	if got, want := j.DataPack().Len(), size; got != want {
		t.Errorf("invalid pack size: got=%d want=%d", got, want)
	}
	if got, want := len(j.keys), size; got != want {
		t.Errorf("invalid keys size: got=%d want=%d", got, want)
	}
	if got, want := j.deleted.Len(), size; got != want {
		t.Errorf("invalid bitset size: got=%d want=%d", got, want)
	}
	if got, want := j.TombLen(), tomb; got != want {
		t.Errorf("invalid tomb size: got=%d want=%d", got, want)
	}
	if got, want := j.deleted.Count(), delcount; got != want {
		t.Errorf("invalid bitset count: got=%d want=%d", got, want)
	}
}

func checkJournalCaps(t *testing.T, j *Journal, data, keys, tomb int) {
	if got, want := j.DataPack().Cap(), data; got != want {
		t.Errorf("invalid data pack cap: got=%d want=%d", got, want)
	}
	if got, want := cap(j.keys), keys; got != want {
		t.Errorf("invalid keys cap: got=%d want=%d", got, want)
	}
	if got, want := cap(j.tomb), tomb; got != want {
		t.Errorf("invalid tomb cap: got=%d want=%d", got, want)
	}
	if got, want := j.deleted.Cap(), tomb; got != want {
		t.Errorf("invalid bitset cap: got=%d want=%d", got, want)
	}
}

func TestJournalNew(t *testing.T) {
	for i, sz := range journalTestSizes {
		t.Run(fmt.Sprintf("%d_new", sz), func(t *testing.T) {
			j := NewJournal(uint64(i), sz)
			// sizes & caps (note, pack storage is allocated on Init)
			checkJournalSizes(t, j, 0, 0, 0)
			checkJournalCaps(t, j, 0, sz, sz)

			// other
			if got, want := j.maxid, uint64(i); got != want {
				t.Errorf("invalid max id: got=%d want=%d", got, want)
			}
			if got, want := j.lastid, uint64(0); got != want {
				t.Errorf("invalid last id: got=%d want=%d", got, want)
			}
			if got, want := j.sortData, false; got != want {
				t.Errorf("invalid sortData: got=%t want=%t", got, want)
			}
		})
	}
}

func TestJournalInit(t *testing.T) {
	for i, sz := range journalTestSizes {
		t.Run(fmt.Sprintf("%d_init", sz), func(t *testing.T) {
			// packs use a default minimum defined in block
			expDataCap := util.Max(block.DefaultMaxPointsPerBlock, sz)

			// create journal
			j := NewJournal(uint64(i), sz)
			fields, err := Fields(JournalTestType{})
			if err != nil {
				t.Errorf("unexpected fields init error: %v", err)
			}
			// regular init
			if err := j.InitFields(fields); err != nil {
				t.Errorf("unexpected init error: %v", err)
			}
			// sizes & caps (Note: min data block size is 32k)
			checkJournalSizes(t, j, 0, 0, 0)
			checkJournalCaps(t, j, expDataCap, sz, sz)
			// 2nd init
			if err := j.InitFields(fields); err == nil {
				t.Errorf("no error on 2nd init")
			}

			j = NewJournal(uint64(i), sz)
			if err := j.InitType(JournalTestType{}); err != nil {
				t.Errorf("unexpected init error: %v", err)
			}
			// sizes & caps (Note: min data block size is 32k)
			checkJournalSizes(t, j, 0, 0, 0)
			checkJournalCaps(t, j, expDataCap, sz, sz)
			// 2nd init
			if err := j.InitType(JournalTestType{}); err == nil {
				t.Errorf("no error on 2nd init")
			}

			// empty type is not ok
			j = NewJournal(uint64(i), sz)
			if err := j.InitType(nil); err == nil {
				t.Errorf("no error when type is nil: %v", err)
			}

			// empty fields are not ok
			j = NewJournal(uint64(i), sz)
			if err := j.InitFields([]Field{}); err == nil {
				t.Errorf("no error when fields are empty")
			}

			// nil fields are not OK
			j = NewJournal(uint64(i), sz)
			if err := j.InitFields(nil); err == nil {
				t.Errorf("no error when fields are nil")
			}

			// non-pk type
			j = NewJournal(uint64(i), sz)
			type noPkType struct {
				X uint64 `pack:"x"`
				A []byte `pack:"a"`
			}
			if err := j.InitType(noPkType{}); err == nil {
				t.Errorf("no error when pk field is missing")
			}

			// empty type
			j = NewJournal(uint64(i), sz)
			type emptyType struct {
				X uint64 `pack:"-"`
				A []byte `pack:"-"`
			}
			if err := j.InitType(emptyType{}); err == nil {
				t.Errorf("no error when all type fields are ignored")
			}

			// empty type
			j = NewJournal(uint64(i), sz)
			type privateType struct {
				x uint64 `pack:"x,pk"`
				y []byte `pack:"y"`
			}
			if err := j.InitType(privateType{}); err == nil {
				t.Errorf("no error when all type fields are private")
			}
		})
	}
}

func TestJournalInsert(t *testing.T) {
	rand.Seed(0)
	for i, sz := range journalTestSizes {
		t.Run(fmt.Sprintf("%d_insert", sz), func(t *testing.T) {
			expDataCap := util.Max(block.DefaultMaxPointsPerBlock, sz)
			j := NewJournal(uint64(i), sz)
			j.InitType(JournalTestType{})
			items := makeJournalTestData(4)

			// 1st insert
			//
			err := j.Insert(items[0])
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			// item update
			if got, want := items[0].ID(), uint64(i)+1; got != want {
				t.Errorf("invalid item id: got=%d want=%d", got, want)
			}
			// counters and state
			if got, want := j.maxid, uint64(i)+1; got != want {
				t.Errorf("invalid max id: got=%d want=%d", got, want)
			}
			if got, want := j.lastid, uint64(i)+1; got != want {
				t.Errorf("invalid last id: got=%d want=%d", got, want)
			}
			if got, want := j.sortData, false; got != want {
				t.Errorf("invalid sortData: got=%t want=%t", got, want)
			}
			// sizes
			checkJournalCaps(t, j, expDataCap, sz, sz)
			checkJournalSizes(t, j, 1, 0, 0)
			// invariants
			if err := j.checkInvariants("insert"); err != nil {
				t.Error(err)
			}

			// 2nd insert
			//
			err = j.Insert(items[1])
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			// item update
			if got, want := items[1].ID(), uint64(i)+2; got != want {
				t.Errorf("invalid item id: got=%d want=%d", got, want)
			}
			// counters and state
			if got, want := j.maxid, uint64(i)+2; got != want {
				t.Errorf("invalid max id: got=%d want=%d", got, want)
			}
			if got, want := j.lastid, uint64(i)+2; got != want {
				t.Errorf("invalid last id: got=%d want=%d", got, want)
			}
			if got, want := j.sortData, false; got != want {
				t.Errorf("invalid sortData: got=%t want=%t", got, want)
			}
			// sizes
			checkJournalCaps(t, j, expDataCap, sz, sz)
			checkJournalSizes(t, j, 2, 0, 0)
			// invariants
			if err := j.checkInvariants("insert"); err != nil {
				t.Error(err)
			}

			// 3rd insert (with existing pk)
			//
			items[2].SetID(uint64(42))
			err = j.Insert(items[2])
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			// item update
			if got, want := items[2].ID(), uint64(42); got != want {
				t.Errorf("invalid item id: got=%d want=%d", got, want)
			}
			// counters and state
			if got, want := j.maxid, uint64(42); got != want {
				t.Errorf("invalid max id: got=%d want=%d", got, want)
			}
			if got, want := j.lastid, uint64(42); got != want {
				t.Errorf("invalid last id: got=%d want=%d", got, want)
			}
			if got, want := j.sortData, false; got != want {
				t.Errorf("invalid sortData: got=%t want=%t", got, want)
			}
			// sizes
			checkJournalCaps(t, j, expDataCap, sz, sz)
			checkJournalSizes(t, j, 3, 0, 0)
			// invariants
			if err := j.checkInvariants("insert"); err != nil {
				t.Error(err)
			}

			// 4th insert (with existing smaller pk)
			//
			items[3].SetID(uint64(41))
			err = j.Insert(items[3])
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			// item update
			if got, want := items[3].ID(), uint64(41); got != want {
				t.Errorf("invalid item id: got=%d want=%d", got, want)
			}
			// counters and state
			if got, want := j.maxid, uint64(42); got != want {
				t.Errorf("invalid max id: got=%d want=%d", got, want)
			}
			if got, want := j.lastid, uint64(42); got != want {
				t.Errorf("invalid last id: got=%d want=%d", got, want)
			}
			if got, want := j.sortData, true; got != want {
				t.Errorf("invalid sortData: got=%t want=%t", got, want)
			}
			// sizes
			checkJournalCaps(t, j, expDataCap, sz, sz)
			checkJournalSizes(t, j, 4, 0, 0)
			// invariants
			if err := j.checkInvariants("insert"); err != nil {
				t.Error(err)
			}
		})
	}
}

func TestJournalInsertBatch(t *testing.T) {
	rand.Seed(0)
	for _, sz := range journalTestSizes {
		for k, batch := range randJournalData(20, sz) {
			t.Run(fmt.Sprintf("%d_%d_insert_batch", sz, k), func(t *testing.T) {
				expDataCap := util.Max(block.DefaultMaxPointsPerBlock, sz)
				j := NewJournal(0, sz)
				j.InitType(JournalTestType{})
				max := batch[len(batch)-1].ID()

				// random test data is sorted
				n, err := j.InsertBatch(batch)
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}

				// sizes
				checkJournalCaps(t, j, expDataCap, sz, sz)
				checkJournalSizes(t, j, sz, 0, 0)

				// invariants
				if err := j.checkInvariants("insert_batch_sort"); err != nil {
					t.Error(err)
				}

				// counters and state
				if got, want := n, len(batch); got != want {
					t.Errorf("invalid insert count: got=%d want=%d", got, want)
				}
				if got, want := j.maxid, max; got != want {
					t.Errorf("invalid max id: got=%d want=%d", got, want)
				}
				if got, want := j.lastid, max; got != want {
					t.Errorf("invalid last id: got=%d want=%d", got, want)
				}
				if got, want := j.sortData, false; got != want {
					t.Errorf("invalid sortData: got=%t want=%t", got, want)
				}

				// retry with unsorted data
				batch = shuffleItems(batch)
				j = NewJournal(0, sz)
				j.InitType(JournalTestType{})
				n, err = j.InsertBatch(batch)
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}

				// sizes
				checkJournalCaps(t, j, expDataCap, sz, sz)
				checkJournalSizes(t, j, sz, 0, 0)

				// invariants
				if err := j.checkInvariants("insert_batch_rnd"); err != nil {
					t.Error(err)
				}

				// counters and state
				if got, want := n, len(batch); got != want {
					t.Errorf("invalid insert count: got=%d want=%d", got, want)
				}
				if got, want := j.maxid, max; got != want {
					t.Errorf("invalid max id: got=%d want=%d", got, want)
				}
				if got, want := j.lastid, max; got != want {
					t.Errorf("invalid last id: got=%d want=%d", got, want)
				}
				if got, want := j.sortData, false; got != want {
					t.Errorf("invalid sortData: got=%t want=%t", got, want)
				}

				// 2 inserts half/half (journal will become unsorted)
				batch = shuffleItems(batch)
				j = NewJournal(0, sz)
				j.InitType(JournalTestType{})
				_, err = j.InsertBatch(batch[:sz/2])
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}

				// sizes
				checkJournalCaps(t, j, expDataCap, sz, sz)
				checkJournalSizes(t, j, sz/2, 0, 0)

				_, err = j.InsertBatch(batch[sz/2:])
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}

				// sizes
				checkJournalCaps(t, j, expDataCap, sz, sz)
				checkJournalSizes(t, j, sz, 0, 0)

				// invariants
				if err := j.checkInvariants("insert_batch_1/2"); err != nil {
					t.Error(err)
				}

				// counters and state
				if got, want := n, len(batch); got != want {
					t.Errorf("invalid insert count: got=%d want=%d", got, want)
				}
				if got, want := j.maxid, max; got != want {
					t.Errorf("invalid max id: got=%d want=%d", got, want)
				}
				if got, want := j.lastid, max; got != want {
					t.Errorf("invalid last id: got=%d want=%d", got, want)
				}
				if got, want := j.sortData, true; got != want {
					t.Errorf("invalid sortData: got=%t want=%t", got, want)
				}

			})
		}
	}
}

// add same data twice, second time will update only
func TestJournalUpsertPack(t *testing.T) {
	rand.Seed(0)
	for _, sz := range journalTestSizes {
		for k, batch := range randJournalData(20, sz) {
			t.Run(fmt.Sprintf("%d_%d_insert_batch", sz, k), func(t *testing.T) {
				expDataCap := util.Max(block.DefaultMaxPointsPerBlock, sz)
				j := NewJournal(0, sz)
				j.InitType(JournalTestType{})
				max := batch[sz/2-1].ID()

				// random test data is sorted, insert half
				n, err := j.InsertBatch(batch[:sz/2])
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}

				// sizes
				checkJournalCaps(t, j, expDataCap, sz, sz)
				checkJournalSizes(t, j, sz/2, 0, 0)

				// invariants
				if err := j.checkInvariants("insert_batch"); err != nil {
					t.Error(err)
				}

				// counters and state
				if got, want := n, len(batch)/2; got != want {
					t.Errorf("invalid insert count: got=%d want=%d", got, want)
				}
				if got, want := j.maxid, max; got != want {
					t.Errorf("invalid max id: got=%d want=%d", got, want)
				}
				if got, want := j.lastid, max; got != want {
					t.Errorf("invalid last id: got=%d want=%d", got, want)
				}
				if got, want := j.sortData, false; got != want {
					t.Errorf("invalid sortData: got=%t want=%t", got, want)
				}

				// 2nd insert, same data
				n, err = j.InsertBatch(batch[:sz/2])
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}

				// sizes
				checkJournalCaps(t, j, expDataCap, sz, sz)
				checkJournalSizes(t, j, sz/2, 0, 0)

				// invariants
				if err := j.checkInvariants("insert_batch"); err != nil {
					t.Error(err)
				}

				// counters and state
				if got, want := n, len(batch)/2; got != want {
					t.Errorf("invalid insert count: got=%d want=%d", got, want)
				}
				if got, want := j.maxid, max; got != want {
					t.Errorf("invalid max id: got=%d want=%d", got, want)
				}
				if got, want := j.lastid, max; got != want {
					t.Errorf("invalid last id: got=%d want=%d", got, want)
				}
				if got, want := j.sortData, false; got != want {
					t.Errorf("invalid sortData: got=%t want=%t", got, want)
				}
			})
		}
	}
}

func TestJournalInsertPack(t *testing.T)      {}
func TestJournalUpdate(t *testing.T)          {}
func TestJournalUpdateBatch(t *testing.T)     {}
func TestJournalDeleteBatch(t *testing.T)     {}
func TestJournalIndexes(t *testing.T)         {}
func TestJournalIndexesReversed(t *testing.T) {}
func TestJournalSort(t *testing.T)            {}
func TestJournalKeyColumns(t *testing.T)      {}
