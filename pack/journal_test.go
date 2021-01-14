// Copyright (c) 2018-2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc
//

package pack

import (
	"fmt"
	"io"
	"math/rand"
	"testing"

	"blockwatch.cc/knoxdb/encoding/block"
	"blockwatch.cc/knoxdb/util"
	"blockwatch.cc/knoxdb/vec"
)

var journalTestSizes = []int{1 << 8, 1 << 10, 1 << 12}

const journalRndRuns = 20

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

func makeJournalTestData(n int) ItemList {
	items := make(ItemList, n)
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

func makeJournalFromPks(pks, del []uint64) *Journal {
	j := NewJournal(0, len(pks))
	j.InitType(JournalTestType{})
	for i := range pks {
		item := &JournalTestType{
			Pk: pks[i],
			N:  i,
		}
		j.Insert(item)
	}
	for _, v := range del {
		j.Delete(v)
	}
	return j
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

// generates n unique random numbers between 1..max
func randN(n, max int) []int {
	res := make([]int, n)
	m := make(map[int]struct{})
	for i := range res {
		for {
			res[i] = rand.Intn(max-1) + 1
			if _, ok := m[res[i]]; !ok {
				m[res[i]] = struct{}{}
				break
			}
		}
	}
	return res
}

// creates nn slices each containing n unique random numbers between 0..max
func randNN(nn, n, max int) [][]int {
	res := make([][]int, nn)
	for i := range res {
		res[i] = randN(n, max)
	}
	return res
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
	if got, want := j.DataPack().Cap(), roundSize(data); got != want {
		t.Errorf("invalid data pack cap: got=%d want=%d", got, want)
	}
	if got, want := cap(j.keys), roundSize(keys); got != want {
		t.Errorf("invalid keys cap: got=%d want=%d", got, want)
	}
	if got, want := cap(j.tomb), roundSize(tomb); got != want {
		t.Errorf("invalid tomb cap: got=%d want=%d", got, want)
	}
	if got, want := j.deleted.Cap(), roundSize(tomb); got != want {
		t.Errorf("invalid bitset cap: got=%d want=%d", got, want)
	}
}

func comparePackWithBatch(t *testing.T, name string, j *Journal, batch ItemList) {
	t.Run(name, func(t *testing.T) {
		if got, want := j.DataPack().Len(), len(batch); got != want {
			t.Errorf("mismatched pack/batch len: got=%d want=%d", got, want)
			t.FailNow()
		}
		res := Result{pkg: j.DataPack()}
		err := res.ForEach(JournalTestType{}, func(i int, v interface{}) error {
			val, ok := v.(*JournalTestType)
			if !ok {
				t.Errorf("package type mismatch, got=%T want=JournalTestType", v)
				return io.EOF
			}
			cmp, ok := batch[i].(*JournalTestType)
			if !ok {
				t.Errorf("batch type mismatch, got=%T want=*JournalTestType", v)
				return io.EOF
			}
			if got, want := val.Pk, cmp.Pk; got != want {
				t.Errorf("mismatched pk at pos %d: got=%d want=%d", i, got, want)
				return io.EOF
			}
			if got, want := val.N, cmp.N; got != want {
				t.Errorf("mismatched value at pos %d: got=%d want=%d", i, got, want)
				return io.EOF
			}
			// ignore deleted entries when cross-checking
			if val.Pk != 0 {
				idx, _ := j.PkIndex(val.Pk, 0)
				if got, want := idx, i; got != want {
					t.Errorf("mismatched PkIndex for pk %d: got=%d want=%d", val.Pk, got, want)
					return io.EOF
				}
			}
			return nil
		})
		if err != nil {
			if err == io.EOF {
				t.FailNow()
			} else {
				t.Errorf("unexpected pack walk error: %v", err)
			}
		}
	})
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
		for k, batch := range randJournalData(journalRndRuns, sz) {
			t.Run(fmt.Sprintf("%d_%d", sz, k), func(t *testing.T) {
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
				if err := j.checkInvariants("sorted"); err != nil {
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

				// contents
				comparePackWithBatch(t, "sorted", j, batch)

				// retry with unsorted data (batch will be sorted by Insert!)
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
				if err := j.checkInvariants("rnd"); err != nil {
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

				// contents
				comparePackWithBatch(t, "rnd", j, batch)

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
				if err := j.checkInvariants("1/2"); err != nil {
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

				// contents
				comparePackWithBatch(t, "1/2", j, batch)
			})
		}
	}
}

// add same data twice, second time will update (i.e. upsert)
func TestJournalUpsertBatch(t *testing.T) {
	rand.Seed(0)
	for _, sz := range journalTestSizes {
		for k, batch := range randJournalData(journalRndRuns, sz) {
			t.Run(fmt.Sprintf("%d_%d", sz, k), func(t *testing.T) {
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
				if err := j.checkInvariants("first-half"); err != nil {
					t.Error(err)
				}

				// counters and state
				if got, want := n, len(batch)/2; got != want {
					t.Errorf("invalid upsert count: got=%d want=%d", got, want)
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

				// contents
				comparePackWithBatch(t, "first-half", j, batch[:sz/2])

				// 2nd insert, same data
				n, err = j.InsertBatch(batch[:sz/2])
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}

				// sizes
				checkJournalCaps(t, j, expDataCap, sz, sz)
				checkJournalSizes(t, j, sz/2, 0, 0)

				// invariants
				if err := j.checkInvariants("second-half"); err != nil {
					t.Error(err)
				}

				// counters and state
				if got, want := n, len(batch)/2; got != want {
					t.Errorf("invalid upsert count: got=%d want=%d", got, want)
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

				// contents
				comparePackWithBatch(t, "second-half", j, batch[:sz/2])
			})
		}
	}
}

func TestJournalInsertPack(t *testing.T) {}

func TestJournalUpdate(t *testing.T) {
	rand.Seed(0)
	for _, sz := range journalTestSizes {
		for k, batch := range randJournalData(journalRndRuns, sz) {
			t.Run(fmt.Sprintf("%d_%d", sz, k), func(t *testing.T) {
				expDataCap := util.Max(block.DefaultMaxPointsPerBlock, sz)
				j := NewJournal(0, sz)
				j.InitType(JournalTestType{})
				max := batch[len(batch)-1].ID()

				// random test data is sorted
				n, err := j.InsertBatch(batch)
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}

				// pick a random item from batch, change its value, update and check
				for i, idx := range randN(100, sz) {
					t.Run(fmt.Sprintf("rand_%03d", i), func(t *testing.T) {
						val := batch[idx].(*JournalTestType)
						val.N++
						err := j.Update(val)
						if err != nil {
							t.Errorf("unexpected error: %v", err)
						}
						// sizes
						checkJournalCaps(t, j, expDataCap, sz, sz)
						checkJournalSizes(t, j, sz, 0, 0)

						// invariants
						if err := j.checkInvariants("post-update"); err != nil {
							t.Error(err)
						}

						// counters and state
						if got, want := n, len(batch); got != want {
							t.Errorf("invalid update count: got=%d want=%d", got, want)
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
						// contents
						comparePackWithBatch(t, "post-update", j, batch)
					})
				}
			})
		}
	}
}

func TestJournalUpdateBatch(t *testing.T) {
	rand.Seed(0)
	for _, sz := range journalTestSizes {
		for k, batch := range randJournalData(journalRndRuns, sz) {
			t.Run(fmt.Sprintf("%d_%d", sz, k), func(t *testing.T) {
				expDataCap := util.Max(block.DefaultMaxPointsPerBlock, sz)
				j := NewJournal(0, sz)
				j.InitType(JournalTestType{})
				max := batch[len(batch)-1].ID()

				// random test data is sorted
				n, err := j.InsertBatch(batch)
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}

				// pick a random number of items from batch, change their values,
				// update and check
				for l, idxs := range randNN(100, 100, sz) {
					t.Run(fmt.Sprintf("rand_%03d", l), func(t *testing.T) {
						// this changes the batch because ItemList contains
						// pointers to structs
						newBatch := make([]Item, len(idxs))
						for i := range newBatch {
							val := batch[idxs[i]].(*JournalTestType)
							val.N += sz
							newBatch[i] = val
						}
						_, err := j.UpdateBatch(newBatch)
						if err != nil {
							t.Errorf("unexpected error: %v", err)
						}
						// sizes
						checkJournalCaps(t, j, expDataCap, sz, sz)
						checkJournalSizes(t, j, sz, 0, 0)

						// invariants
						if err := j.checkInvariants("post-update"); err != nil {
							t.Error(err)
						}

						// counters and state
						if got, want := n, len(batch); got != want {
							t.Errorf("invalid update count: got=%d want=%d", got, want)
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
						// expected contents is in original batch order, but with
						// updated contents
						comparePackWithBatch(t, "post-update", j, batch)
					})
				}
			})
		}
	}
}

func TestJournalDelete(t *testing.T) {
	rand.Seed(0)
	for _, sz := range journalTestSizes {
		for k, batch := range randJournalData(journalRndRuns, sz) {
			t.Run(fmt.Sprintf("%d_%d", sz, k), func(T *testing.T) {
				expDataCap := util.Max(block.DefaultMaxPointsPerBlock, sz)
				j := NewJournal(0, sz)
				j.InitType(JournalTestType{})
				max := batch[len(batch)-1].ID()

				// random test data is sorted
				_, err := j.InsertBatch(batch)
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}

				// pick a random item to delete
				for i, idx := range randN(sz/8, sz) {
					T.Run(fmt.Sprintf("rand_%03d", i), func(t *testing.T) {
						// value to delete
						val := batch[idx].(*JournalTestType)
						// pk := val.Pk
						// remove from batch (so test does not find it)
						// batch = append(batch[:idx], batch[idx+1:]...)
						// delete from journal
						n, err := j.Delete(val.Pk)
						if err != nil {
							t.Errorf("unexpected error: %v", err)
							T.FailNow()
						}
						// sizes (journal len stays the same, but tomb grows)
						checkJournalCaps(t, j, expDataCap, sz, sz)
						checkJournalSizes(t, j, sz, i+1, i+1)

						// invariants
						if err := j.checkInvariants("post-delete"); err != nil {
							t.Error(err)
							T.FailNow()
						}

						// counters and state
						ok, _ := j.IsDeleted(val.Pk, 0)
						if got, want := ok, true; got != want {
							t.Errorf("invalid IsDeleted: got=%t want=%t", got, want)
							T.FailNow()
						}
						idx, last := j.PkIndex(val.Pk, 0)
						if got, dontwant := idx, -1; got == dontwant {
							t.Errorf("invalid PkIndex: got=%d dontwant=%d", got, dontwant)
							T.FailNow()
						}
						if got, dontwant := last, j.Len(); got == dontwant {
							t.Errorf("invalid PkIndex last: got=%d dontwant=%d", got, dontwant)
							T.FailNow()
						}
						if got, want := n, 1; got != want {
							t.Errorf("invalid delete count: got=%d want=%d", got, want)
							T.FailNow()
						}
						if got, want := j.maxid, max; got != want {
							t.Errorf("invalid max id: got=%d want=%d", got, want)
							T.FailNow()
						}
						if got, want := j.lastid, max; got != want {
							t.Errorf("invalid last id: got=%d want=%d", got, want)
							T.FailNow()
						}
						if got, want := j.sortData, false; got != want {
							t.Errorf("invalid sortData: got=%t want=%t", got, want)
							T.FailNow()
						}

						// contents
						val.Pk = 0
						comparePackWithBatch(t, "post-delete", j, batch)
					})
				}
			})
		}
	}
}

func TestJournalDeleteBatch(t *testing.T) {
	rand.Seed(0)
	for _, sz := range journalTestSizes {
		for k, originalbatch := range randJournalData(journalRndRuns, sz) {
			t.Run(fmt.Sprintf("%d_%d", sz, k), func(T *testing.T) {
				expDataCap := util.Max(block.DefaultMaxPointsPerBlock, sz)

				// pick list of a random items to delete
				for l, idxs := range randNN(10, sz/8, sz) {
					T.Run(fmt.Sprintf("rand_%03d", l), func(t *testing.T) {
						// start with a fresh batch
						batch := make(ItemList, len(originalbatch))
						for i, v := range originalbatch {
							v := v.(*JournalTestType)
							c := *v
							batch[i] = &c
						}
						// and a fresh journal
						j := NewJournal(0, sz)
						j.InitType(JournalTestType{})
						max := batch[len(batch)-1].ID()
						_, err := j.InsertBatch(batch)
						if err != nil {
							t.Errorf("unexpected error: %v", err)
						}

						// prepare values to delete
						pks := make([]uint64, len(idxs))
						for i, idx := range idxs {
							val := batch[idx].(*JournalTestType)
							pks[i] = val.Pk
							val.Pk = 0
						}

						// delete from journal
						n, err := j.DeleteBatch(pks)
						if err != nil {
							t.Errorf("unexpected error: %v", err)
							T.FailNow()
						}
						// sizes (journal size stays the same, tomb grows)
						checkJournalCaps(t, j, expDataCap, sz, sz)
						checkJournalSizes(t, j, sz, len(pks), len(pks))

						// invariants
						if err := j.checkInvariants("post-delete"); err != nil {
							t.Error(err)
							T.FailNow()
						}

						// counters and state
						var (
							ok   bool
							last int
						)
						for i, pk := range pks {
							ok, _ = j.IsDeleted(pk, 0)
							if got, want := ok, true; got != want {
								t.Errorf("invalid IsDeleted last=0 %d: got=%t want=%t", pk, got, want)
								T.FailNow()
							}
							// use `last` to skip, checks if we got the offsets right
							ok, last = j.IsDeleted(pk, last)
							if got, want := ok, true; got != want {
								t.Errorf("invalid IsDeleted last>=0 %d: got=%t want=%t", pk, got, want)
								T.FailNow()
							}
							idx, jlast := j.PkIndex(pk, 0)
							if got, want := idx, idxs[i]; got != want {
								t.Errorf("invalid PkIndex: got=%d want=%d", got, want)
								T.FailNow()
							}
							if got, dontwant := jlast, j.Len(); got == dontwant {
								t.Errorf("invalid PkIndex last: got=%d dontwant=%d", got, dontwant)
								T.FailNow()
							}
						}
						if got, want := n, len(pks); got != want {
							t.Errorf("invalid delete count: got=%d want=%d", got, want)
							T.FailNow()
						}
						if got, want := j.maxid, max; got != want {
							t.Errorf("invalid max id: got=%d want=%d", got, want)
							T.FailNow()
						}
						if got, want := j.lastid, max; got != want {
							t.Errorf("invalid last id: got=%d want=%d", got, want)
							T.FailNow()
						}
						if got, want := j.sortData, false; got != want {
							t.Errorf("invalid sortData: got=%t want=%t", got, want)
							T.FailNow()
						}

						// contents
						comparePackWithBatch(t, "post-delete", j, batch)
					})
				}
			})
		}
	}
}

type journalE2ETest struct {
	name string
	pks  []uint64 // input: pks used to insert test data into journal
	del  []uint64 // input: pks to delete from journal after insert
	bit  []byte   // input: bitset to simulate journal matches
	idx  []int    // output: expected indexes sorted in pk order
	pkx  []uint64 // output: expected pks sorted in pk order
}

var journalE2Etests = []journalE2ETest{
	journalE2ETest{
		name: "SORT-INS(8)-DEL[0:3]-MATCH[0:7]",
		pks:  []uint64{1, 2, 3, 4, 5, 6, 7, 8}, // sorted journal
		del:  []uint64{1, 2, 3, 4},             // first 50% marked as deleted
		bit:  []byte{0xFF},                     // all match
		idx:  []int{4, 5, 6, 7},                // exp: second half as result
		pkx:  []uint64{5, 6, 7, 8},             // exp: ordered pks
	},
	journalE2ETest{
		name: "SORT-INS(8)-DEL[0:3]-MATCH[2:5]",
		pks:  []uint64{1, 2, 3, 4, 5, 6, 7, 8}, // sorted journal
		del:  []uint64{1, 2, 3, 4},             // first 50% marked as deleted
		bit:  []byte{0x3C},                     // match some data pack entries only
		idx:  []int{4, 5},                      // exp: matching entries (minus deleted)
		pkx:  []uint64{5, 6},                   // exp: ordered pks
	},
	journalE2ETest{
		name: "UNSORT-INS(8)-DEL[0:3]-MATCH[0:7]",
		pks:  []uint64{1, 8, 2, 7, 3, 6, 4, 5}, // unordered journal
		del:  []uint64{1, 2, 3, 4},             // delete pks at random positions
		bit:  []byte{0xFF},                     // all match
		idx:  []int{7, 5, 3, 1},                // exp: indexes of non-deleted pks
		pkx:  []uint64{5, 6, 7, 8},             // exp: ordered pks
	},
}

func (x journalE2ETest) Run(t *testing.T) {
	t.Run(x.name, func(t *testing.T) {
		j := makeJournalFromPks(x.pks, x.del)
		ids, pks := j.SortedIndexes(vec.NewBitSetFromBytes(x.bit, len(x.bit)*8))
		if got, want := len(ids), len(x.idx); got != want {
			t.Errorf("invalid result ids len: got=%d want=%d", got, want)
		}
		if got, want := len(pks), len(x.pkx); got != want {
			t.Errorf("invalid result pks len: got=%d want=%d", got, want)
		}
		for i := range x.idx {
			if got, want := ids[i], x.idx[i]; got != want {
				t.Errorf("invalid ordered result idx %d: got=%d want=%d", i, got, want)
			}
			if got, want := pks[i], x.pkx[i]; got != want {
				t.Errorf("invalid ordered result pk %d: got=%d want=%d", i, got, want)
			}
		}
	})
}

func TestJournalIndexes(t *testing.T) {
	for _, v := range journalE2Etests {
		v.Run(t)
	}
}
