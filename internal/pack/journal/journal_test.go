// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package journal

import (
	"fmt"
	"sort"
	"testing"

	"blockwatch.cc/knoxdb/internal/bitset"
	"blockwatch.cc/knoxdb/internal/pack"
	"blockwatch.cc/knoxdb/pkg/bitmap"
	"blockwatch.cc/knoxdb/pkg/schema"
	"blockwatch.cc/knoxdb/pkg/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	journalTestSizes = []int{256, 1024, 4096}
	benchmarkSizes   = []benchmarkSize{
		{"16K", 16 * 1024},
		{"32K", 32 * 1024},
		{"64K", 64 * 1024},
	}
	testSchema = schema.MustSchemaOf(TestRecord{})
)

const journalRndRuns = 5

type TestRecord struct {
	Pk uint64 `knox:"I,pk"`
	N  int    `knox:"n"`
}

type TestRecords []*TestRecord

type benchmarkSize struct {
	name string
	l    int
}

// generates n unique random numbers between 1..max
func uniqueRandN(n, max int) []int {
	res := make([]int, n)
	m := make(map[int]struct{})
	for i := range res {
		for {
			res[i] = util.RandIntn(max-1) + 1
			if _, ok := m[res[i]]; !ok {
				m[res[i]] = struct{}{}
				break
			}
		}
	}
	return res
}

func makeJournalTestData(n int) TestRecords {
	recs := make(TestRecords, n)
	for i := 0; i < n; i++ {
		recs[i] = &TestRecord{
			Pk: 0,
			N:  i,
		}
	}
	return recs
}

func recsToJournalRecords(recs TestRecords) JournalRecords {
	l := make(JournalRecords, len(recs))
	for i := range recs {
		l[i] = JournalRecord{recs[i].Pk, i}
	}
	return l
}

func makeJournalDataSequential(sz, _ int) TestRecords {
	res := make(TestRecords, sz)
	for i := range res {
		res[i] = &TestRecord{
			Pk: uint64(1 + i),
			N:  util.RandInt(),
		}
	}
	return res
}

func encodeTestData(v []*TestRecord) []byte {
	enc := schema.NewEncoder(testSchema)
	buf, err := enc.EncodeSlice(v, nil)
	if err != nil {
		panic(err)
	}
	return buf
}

func shuffleItems(recs TestRecords) TestRecords {
	util.RandShuffle(len(recs), func(i, j int) { recs[i], recs[j] = recs[j], recs[i] })
	return recs
}

func checkJournalSizes(t *testing.T, j *Journal, size, tomb, delcount int) {
	assert.Equal(t, j.Len(), size, "invalid journal size")
	assert.Equal(t, j.Data.Len(), size, "invalid journal pack size")
	assert.Len(t, j.Keys, size, "invalid journal keys size")
	assert.Equal(t, j.Deleted.Len(), size, "invalid tomb bitset size")
	assert.Equal(t, j.TombLen(), tomb, "invalid tomb size")
	assert.Equal(t, j.Deleted.Count(), delcount, "invalid tomb bitset size")
}

func checkJournalCaps(t *testing.T, j *Journal, data, keys, tomb int) {
	assert.Equal(t, j.Data.Cap(), roundSize(data), "invalid journal pack cap")
	assert.Equal(t, cap(j.Keys), roundSize(keys), "invalid journal keys cap")
	assert.Equal(t, j.Deleted.Cap(), roundSize(tomb), "invalid tomb bitset cap")
}

func comparePackWithBatch(t *testing.T, name string, j *Journal, batch TestRecords) {
	require.Equal(t, j.Data.Len(), len(batch), "%s: mismatched pack/batch len", name)
	err := pack.ForEach[TestRecord](j.Data, func(i int, val *TestRecord) error {
		// pks are assigned on insert (assuming start is 1 in all calls below)
		// require.Equal(t, val.Pk, batch[i].Pk, "mismatched pk")
		require.Equal(t, val.Pk, uint64(i+1), "%s: mismatched pk", name)
		require.Equal(t, val.N, batch[i].N, "%s: mismatched value", name)

		// ignore deleted entries when cross-checking
		if val.Pk != 0 {
			idx, _ := j.PkIndex(val.Pk, 0)
			require.Equal(t, idx, i, "%s: mismatched PkIndex for pk", name)
		}
		return nil
	})
	require.NoError(t, err)
}

func TestJournalNew(t *testing.T) {
	for _, sz := range journalTestSizes {
		t.Run(fmt.Sprintf("%d_new", sz), func(t *testing.T) {
			j := NewJournal(testSchema, sz)
			// sizes & caps (note, pack storage is allocated on Init)
			checkJournalSizes(t, j, 0, 0, 0)
			checkJournalCaps(t, j, sz, sz, sz)

			// other
			require.Equal(t, j.maxid, uint64(0))
			require.Equal(t, j.sortData, false)
		})
	}
}

func TestJournalMerge(t *testing.T) {
	for _, sz := range journalTestSizes {
		t.Run(fmt.Sprintf("%d_merge", sz), func(t *testing.T) {
			j := NewJournal(testSchema, sz)
			recs := makeJournalDataSequential(sz, 1)
			keys := recsToJournalRecords(recs)
			util.RandShuffle(len(keys), func(i, j int) { keys[i], keys[j] = keys[j], keys[i] })

			// 4-step insert
			step := len(keys) / 4

			// merge keys
			for k := 0; k < len(keys); k += step {
				// sort just within the inserted slice
				sort.Sort(keys[k : k+step])
				j.mergeKeys(keys[k : k+step])
			}

			// check all keys are available
			require.Equal(t, len(j.Keys), len(keys), "invalid keys len")

			// check keys are sorted
			if !sort.IsSorted(j.Keys) {
				t.Errorf("unexpected non-sorted keys: %v", j.Keys)
			}
			for i, v := range j.Keys {
				if i == 0 {
					continue
				}
				require.LessOrEqualf(t, j.Keys[i-1].Pk, v.Pk,
					"INVARIANT VIOLATION: unsorted keys %d [%d] !< %d [%d]",
					j.Keys[i-1].Pk, i-1, v.Pk, i,
				)
				require.NotEqualf(t, j.Keys[i-1].Pk, v.Pk,
					"INVARIANT VIOLATION: duplicate key %d [%d:%d]",
					v.Pk, i-1, i,
				)
			}
		})
	}
}

func TestJournalInsert(t *testing.T) {
	for _, sz := range journalTestSizes {
		t.Run(fmt.Sprintf("%d_insert", sz), func(t *testing.T) {
			j := NewJournal(testSchema, sz)

			// 1st insert
			//
			rec := makeJournalTestData(1)
			buf := encodeTestData(rec)
			n, more := j.InsertBatch(buf, 42)
			require.Len(t, more, 0)
			require.Equal(t, n, uint64(1))
			// sizes
			checkJournalCaps(t, j, sz, sz, sz)
			checkJournalSizes(t, j, 1, 0, 0)
			// rec update
			require.Equal(t, j.Data.Uint64(0, 0), uint64(42), "invalid rec pk")
			// counters and state
			require.Equal(t, j.maxid, uint64(42), "invalid max id")
			require.Equal(t, j.sortData, false)
			// invariants
			require.NoError(t, j.checkInvariants("insert"))

			// 2nd insert
			//
			rec = makeJournalTestData(1)
			buf = encodeTestData(rec)
			n, more = j.InsertBatch(buf, 43)
			require.Len(t, more, 0)
			require.Equal(t, n, uint64(1))
			// sizes
			checkJournalCaps(t, j, sz, sz, sz)
			checkJournalSizes(t, j, 2, 0, 0)
			// rec update
			require.Equal(t, j.Data.Uint64(0, 1), uint64(43), "invalid rec pk")
			// counters and state
			require.Equal(t, j.maxid, uint64(43), "invalid max id")
			require.Equal(t, j.sortData, false)
			// invariants
			require.NoError(t, j.checkInvariants("insert"))

			// 3rd insert (with existing pk, will be overwritten)
			//
			rec = makeJournalTestData(1)
			rec[0].Pk = 42
			buf = encodeTestData(rec)
			n, more = j.InsertBatch(buf, 44)
			require.Len(t, more, 0)
			require.Equal(t, n, uint64(1))
			// sizes
			checkJournalCaps(t, j, sz, sz, sz)
			checkJournalSizes(t, j, 3, 0, 0)
			// rec update
			require.Equal(t, j.Data.Uint64(0, 2), uint64(44), "invalid rec pk")
			// counters and state
			require.Equal(t, j.maxid, uint64(44), "invalid max id")
			require.Equal(t, j.sortData, false)
			// invariants
			require.NoError(t, j.checkInvariants("insert"))

			// 4th insert (with existing smaller pk)
			//
			rec = makeJournalTestData(1)
			rec[0].Pk = 41
			buf = encodeTestData(rec)
			n, more = j.InsertBatch(buf, 45)
			require.Len(t, more, 0)
			require.Equal(t, n, uint64(1))
			// sizes
			checkJournalCaps(t, j, sz, sz, sz)
			checkJournalSizes(t, j, 4, 0, 0)
			// rec update
			require.Equal(t, j.Data.Uint64(0, 3), uint64(45), "invalid rec pk")
			// counters and state
			require.Equal(t, j.maxid, uint64(45), "invalid max id")
			require.Equal(t, j.sortData, false)
			// invariants
			require.NoError(t, j.checkInvariants("insert"))
		})
	}
}

func TestJournalInsertMulti(t *testing.T) {
	for _, sz := range journalTestSizes {
		for k := 0; k < journalRndRuns; k++ {
			batch := makeJournalDataSequential(sz, 1)
			t.Logf("%d_%d", sz, k)
			// 1
			//
			j := NewJournal(testSchema, sz)
			buf := encodeTestData(batch)

			// random test data is sorted
			n, more := j.InsertBatch(buf, 1)
			require.Len(t, more, 0)
			require.Equal(t, n, uint64(sz))

			// sizes
			checkJournalCaps(t, j, sz, sz, sz)
			checkJournalSizes(t, j, sz, 0, 0)

			// invariants
			require.NoError(t, j.checkInvariants("insert"))

			// counters and state
			require.Equal(t, j.maxid, uint64(sz), "invalid max id")
			require.Equal(t, j.sortData, false)

			// contents
			comparePackWithBatch(t, "sorted", j, batch)
			j.Close()

			// 2
			//
			// retry with unsorted data (batch will be sorted by Insert!)
			j = NewJournal(testSchema, sz)
			batch = shuffleItems(batch)
			buf = encodeTestData(batch)
			n, more = j.InsertBatch(buf, 1)
			require.Len(t, more, 0)
			require.Equal(t, n, uint64(sz))

			// sizes
			checkJournalCaps(t, j, sz, sz, sz)
			checkJournalSizes(t, j, sz, 0, 0)

			// invariants
			require.NoError(t, j.checkInvariants("rnd-insert"))

			// counters and state
			require.Equal(t, j.maxid, uint64(sz), "invalid max id")
			require.Equal(t, j.sortData, false)

			// contents
			comparePackWithBatch(t, "rnd", j, batch)
		}
	}
}

func TestJournalInsertFull(t *testing.T) {
	for _, sz := range journalTestSizes {
		t.Run(fmt.Sprintf("%d_insert", sz), func(t *testing.T) {
			j := NewJournal(testSchema, sz)
			rec := makeJournalDataSequential(sz, 1)
			buf := encodeTestData(rec)

			// insert all
			n, more := j.InsertBatch(buf, 1)
			require.Len(t, more, 0)
			require.Equal(t, n, uint64(sz))
			require.True(t, j.IsFull())

			// try insert more
			n, more = j.InsertBatch(buf, 1)
			require.Len(t, more, len(buf))
			require.Equal(t, n, uint64(0))
			require.True(t, j.IsFull())
		})
	}
}

func TestJournalUpdate(t *testing.T) {
	for _, sz := range journalTestSizes {
		for k := 0; k < journalRndRuns; k++ {
			batch := makeJournalDataSequential(sz, 1)
			t.Logf("%d_%d", sz, k)
			j := NewJournal(testSchema, sz)
			buf := encodeTestData(batch)

			// insert all
			n, more := j.InsertBatch(buf, 1)
			require.Len(t, more, 0)
			require.Equal(t, n, uint64(sz))

			// pick random recs from batch, update with changed value
			for _, idx := range uniqueRandN(100, sz) {
				val := batch[idx]
				val.N++
				buf := encodeTestData([]*TestRecord{val})

				n, more, err := j.UpdateBatch(buf)
				require.NoError(t, err)
				require.Len(t, more, 0)
				require.Equal(t, n, uint64(1), "rand %d", idx)

				// sizes
				checkJournalCaps(t, j, sz, sz, sz)
				checkJournalSizes(t, j, sz, 0, 0)

				// invariants
				require.NoError(t, j.checkInvariants("post-update"), "rand %d", idx)

				// counters and state
				require.Equal(t, j.maxid, uint64(sz), "rand %d: invalid max id", idx)
				require.Equal(t, j.sortData, false)

				// contents
				comparePackWithBatch(t, "post-update", j, batch)
			}
		}
	}
}

func TestJournalUpdateNoPk(t *testing.T) {
	for _, sz := range journalTestSizes {
		for k := 0; k < journalRndRuns; k++ {
			batch := makeJournalDataSequential(sz, 1)
			t.Logf("%d_%d", sz, k)
			j := NewJournal(testSchema, sz)
			buf := encodeTestData(batch)

			// insert all
			n, more := j.InsertBatch(buf, 1)
			require.Len(t, more, 0)
			require.Equal(t, n, uint64(sz))

			// pick rec, reset pk
			batch[0].Pk = 0
			buf = encodeTestData([]*TestRecord{batch[0]})
			n, more, err := j.UpdateBatch(buf)
			require.Error(t, err)
			require.Len(t, more, len(buf))
			require.Equal(t, n, uint64(0))
		}
	}
}

func TestJournalUpdateFull(t *testing.T) {
	for _, sz := range journalTestSizes {
		t.Run(fmt.Sprintf("%d_insert", sz), func(t *testing.T) {
			j := NewJournal(testSchema, sz)
			rec := makeJournalDataSequential(sz*2, 1)
			buf := encodeTestData(rec[:sz])

			// insert until full
			n, more := j.InsertBatch(buf, 1)
			require.Len(t, more, 0)
			require.Equal(t, n, uint64(sz))
			require.True(t, j.IsFull())

			// try update more
			buf = encodeTestData(rec[sz:])
			n, more, err := j.UpdateBatch(buf)
			require.NoError(t, err)
			require.Len(t, more, len(buf))
			require.Equal(t, n, uint64(0))
			require.True(t, j.IsFull())
		})
	}
}

func TestJournalUpdateMulti(t *testing.T) {
	for _, sz := range journalTestSizes {
		for k := 0; k < journalRndRuns; k++ {
			t.Logf("%d_%d", sz, 0)
			j := NewJournal(testSchema, sz)
			batch := makeJournalDataSequential(sz, 1)
			buf := encodeTestData(batch)

			// insert all
			n, more := j.InsertBatch(buf, 1)
			require.Len(t, more, 0)
			require.Equal(t, n, uint64(sz))

			// change random recs from batch
			for _, idx := range uniqueRandN(100, sz) {
				batch[idx].N++
			}

			// update all
			buf = encodeTestData(batch)
			n, more, err := j.UpdateBatch(buf)
			require.NoError(t, err)
			require.Len(t, more, 0)
			require.Equal(t, n, uint64(len(batch)))

			// sizes
			checkJournalCaps(t, j, sz, sz, sz)
			checkJournalSizes(t, j, sz, 0, 0)

			// invariants
			require.NoError(t, j.checkInvariants("post-update"))

			// counters and state
			require.Equal(t, j.maxid, uint64(sz), "invalid max id")
			require.Equal(t, j.sortData, false)

			// contents
			comparePackWithBatch(t, "post-update", j, batch)
		}
	}
}

func TestJournalDelete(t *testing.T) {
	for _, sz := range journalTestSizes {
		for k := 0; k < journalRndRuns; k++ {
			batch := makeJournalDataSequential(sz, 1)
			t.Logf("%d_%d", sz, k)
			j := NewJournal(testSchema, sz)
			buf := encodeTestData(batch)

			// insert all
			n, more := j.InsertBatch(buf, 1)
			require.Len(t, more, 0)
			require.Equal(t, n, uint64(sz))

			// pick a random rec to delete
			for i, idx := range uniqueRandN(sz/8, sz) {
				// value to delete
				val := batch[idx]
				bits := bitmap.NewFromArray([]uint64{val.Pk})
				require.True(t, bits.IsValid(), "rand %d", idx)
				require.Equal(t, bits.Count(), 1, "rand %d", idx)
				require.Equal(t, bits.Bitmap.NewIterator().Next(), val.Pk, "rand %d", idx)

				n := j.DeleteBatch(bits)
				require.Equal(t, n, uint64(1))

				// sizes (journal len stays the same, but tomb grows)
				checkJournalCaps(t, j, sz, sz, sz)
				checkJournalSizes(t, j, sz, i+1, i+1)

				// invariants
				require.NoError(t, j.checkInvariants("post-delete"), "rand %d", idx)

				// counters and state
				require.True(t, j.IsDeleted(val.Pk), "rand %d: invalid IsDeleted", idx)
				require.Equal(t, j.TombLen(), i+1, "rand %d: invalid tomb len", idx)
				require.Equal(t, j.maxid, uint64(sz), "rand %d: invalid max id", idx)
				require.Equal(t, j.sortData, false)

				// contents
				val.Pk = 0 // journal marks deleted records with zero pks !
			}
		}
	}
}

type journalE2ETest struct {
	name string
	pks  []uint64 // input: pks used to insert test data into journal
	del  []uint64 // input: pks to delete from journal after insert
	bit  []byte   // input: bitset to simulate journal matches
	idx  []uint32 // output: expected indexes sorted in pk order
	pkx  []uint64 // output: expected pks sorted in pk order
}

var journalE2Etests = []journalE2ETest{
	{
		name: "SORT-INS(8)-DEL[0:3]-MATCH[0:7]",
		pks:  []uint64{1, 2, 3, 4, 5, 6, 7, 8}, // sorted journal
		del:  []uint64{1, 2, 3, 4},             // first 50% marked as deleted
		bit:  []byte{0xFF},                     // all match
		idx:  []uint32{4, 5, 6, 7},             // exp: second half as result
		pkx:  []uint64{5, 6, 7, 8},             // exp: ordered pks
	},
	{
		name: "SORT-INS(8)-DEL[0:3]-MATCH[2:5]",
		pks:  []uint64{1, 2, 3, 4, 5, 6, 7, 8}, // sorted journal
		del:  []uint64{1, 2, 3, 4},             // first 50% marked as deleted
		bit:  []byte{0x3C},                     // match some data pack entries only
		idx:  []uint32{4, 5},                   // exp: matching entries (minus deleted)
		pkx:  []uint64{5, 6},                   // exp: ordered pks
	},
	{
		name: "UNSORT-INS(8)-DEL[0:3]-MATCH[0:7]",
		pks:  []uint64{1, 8, 2, 7, 3, 6, 4, 5}, // unordered journal
		del:  []uint64{1, 2, 3, 4},             // delete pks at random positions
		bit:  []byte{0xFF},                     // all match
		idx:  []uint32{7, 5, 3, 1},             // exp: indexes of non-deleted pks
		pkx:  []uint64{5, 6, 7, 8},             // exp: ordered pks
	},
}

func makeJournalFromPks(t *testing.T, pks, del []uint64) *Journal {
	enc := schema.NewEncoder(testSchema)
	j := NewJournal(testSchema, len(pks))
	for i := range pks {
		rec := &TestRecord{
			Pk: pks[i],
			N:  i,
		}
		buf, err := enc.Encode(rec, nil)
		require.NoError(t, err)
		require.NotNil(t, buf)
		n, more := j.InsertBatch(buf, pks[i])
		require.NotZero(t, n)
		require.Len(t, more, 0)
	}
	x := bitmap.New()
	for _, v := range del {
		x.Set(v)
	}
	n := j.DeleteBatch(x)
	require.Equal(t, int(n), len(del))
	return j
}

func (x journalE2ETest) Run(t *testing.T) {
	t.Run(x.name, func(t *testing.T) {
		j := makeJournalFromPks(t, x.pks, x.del)
		ids, pks := j.SortedIndexes(bitset.FromBuffer(x.bit, len(x.bit)*8))
		require.Len(t, ids, len(x.idx), "invalid result ids len")
		require.Len(t, pks, len(x.pkx), "invalid result pks len")
		for i := range x.idx {
			require.Equal(t, ids[i], x.idx[i], "invalid ordered result idx")
			require.Equal(t, pks[i], x.pkx[i], "invalid ordered result pk")
		}
	})
}

func TestJournalIndexes(t *testing.T) {
	for _, v := range journalE2Etests {
		v.Run(t)
	}
}

func BenchmarkJournalMerge1kRandom(b *testing.B) {
	for _, n := range benchmarkSizes {
		b.Run(n.name, func(b *testing.B) {
			j := NewJournal(testSchema, n.l+1024)
			rec := makeJournalDataSequential(n.l+1024, 1)
			keys := recsToJournalRecords(rec)
			util.RandShuffle(len(keys), func(i, j int) { keys[i], keys[j] = keys[j], keys[i] })
			sort.Sort(keys[:n.l]) // sort the keys we will add first
			sort.Sort(keys[n.l:]) // sort the keys we will add second
			b.SetBytes(int64(1024 * 16))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				j.Keys = j.Keys[:0]
				j.mergeKeys(keys[:n.l])
				b.StartTimer()
				j.mergeKeys(keys[n.l:])
			}
		})
	}
}

// size means number of recs in journal, 1 new rec with pk = 0 is added
func BenchmarkJournalInsertSingle(b *testing.B) {
	for _, n := range benchmarkSizes {
		b.Run(n.name, func(b *testing.B) {
			batch := makeJournalTestData(n.l)
			j := NewJournal(testSchema, 100*n.l)
			buf := encodeTestData(batch[:n.l])
			buf2 := encodeTestData(batch[n.l:])
			j.InsertBatch(buf, 1)
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				j.InsertBatch(buf2, uint64(n.l+i))
			}
		})
	}
}

// size means batch size, all pk = 0
func BenchmarkJournalInsertBatch(b *testing.B) {
	for _, n := range benchmarkSizes {
		b.Run(n.name, func(b *testing.B) {
			batch := makeJournalTestData(n.l)
			buf := encodeTestData(batch)
			j := NewJournal(testSchema, n.l)
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				j.Reset()
				j.InsertBatch(buf, 1)
			}
		})
	}
}

func BenchmarkJournalUpdateSingle(b *testing.B) {
	for _, n := range benchmarkSizes {
		b.Run(n.name, func(b *testing.B) {
			batch := makeJournalTestData(n.l)
			buf := encodeTestData(batch)
			buf2 := encodeTestData([]*TestRecord{batch[0]})
			j := NewJournal(testSchema, n.l)
			j.InsertBatch(buf, 1)
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				j.UpdateBatch(buf2)
			}
		})
	}
}

func BenchmarkJournalUpdateBatch(b *testing.B) {
	for _, n := range benchmarkSizes {
		b.Run(n.name, func(b *testing.B) {
			batch := makeJournalTestData(n.l)
			buf := encodeTestData(batch)
			j := NewJournal(testSchema, n.l)
			j.InsertBatch(buf, 1)
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				j.UpdateBatch(buf)
			}
		})
	}
}

func BenchmarkJournalDeleteSingle(b *testing.B) {
	for _, n := range benchmarkSizes {
		b.Run(n.name, func(b *testing.B) {
			batch := makeJournalTestData(n.l)
			buf := encodeTestData(batch)
			j := NewJournal(testSchema, n.l)
			j.InsertBatch(buf, 1)
			bits := bitmap.New()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				bits.Bitmap.Reset()
				bits.Set(uint64(i%n.l + 1))
				j.DeleteBatch(bits)
			}
		})
	}
}

func BenchmarkJournalDeleteBatch(b *testing.B) {
	for _, n := range benchmarkSizes {
		b.Run(n.name, func(b *testing.B) {
			batch := makeJournalDataSequential(n.l, 1)
			buf := encodeTestData(batch)
			j := NewJournal(testSchema, n.l)
			j.InsertBatch(buf, 1)
			bits := bitmap.New()
			for i := uint64(1); i < uint64(n.l); i += uint64(n.l) / 256 {
				bits.Set(i)
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				j.DeleteBatch(bits)
			}
		})
	}
}
