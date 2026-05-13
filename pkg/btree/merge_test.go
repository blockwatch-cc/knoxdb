// Copyright (c) 2026 Blockwatch Data Inc.
// Author: alex@blockwatch.cc
package btree

import (
	"bytes"
	"iter"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMerge_SkipTombstones(t *testing.T) {
	// Create two sequences: one with values, one with tombstones
	seq1 := func(yield func([]byte, []byte) bool) {
		if !yield([]byte("key1"), []byte("val1")) {
			return
		}
		if !yield([]byte("key3"), []byte("val3")) {
			return
		}
	}
	seq2 := func(yield func([]byte, []byte) bool) {
		if !yield([]byte("key2"), nil) {
			return
		} // tombstone
		if !yield([]byte("key4"), []byte("val4")) {
			return
		}
	}

	merged := Merge2(seq2, seq1)

	var results []struct{ k, v []byte }
	for k, v := range merged {
		results = append(results, struct{ k, v []byte }{k, v})
	}

	// Expected: key1 val1, key3 val3, key4 val4 (key2 skipped)
	expected := []struct{ k, v []byte }{
		{[]byte("key1"), []byte("val1")},
		{[]byte("key3"), []byte("val3")},
		{[]byte("key4"), []byte("val4")},
	}

	require.Len(t, results, len(expected))

	for i, exp := range expected {
		require.True(t, bytes.Equal(results[i].k, exp.k))
		require.True(t, bytes.Equal(results[i].v, exp.v))
	}
}

func TestMergeTwoLayers(t *testing.T) {
	// Create two change trees as layers
	ct1 := NewChangeTree()
	ct2 := NewChangeTree()

	// ct1 has older updates
	ct1.Put([]byte("a"), []byte("old_a"))
	ct1.Put([]byte("b"), []byte("old_b"))
	ct1.Put([]byte("c"), []byte("old_c"))

	// ct2 has newer updates, some overlapping
	ct2.Put([]byte("a"), []byte("new_a")) // newer wins
	ct2.Put([]byte("d"), []byte("new_d")) // new key
	ct2.Delete([]byte("b"))               // delete

	// Merge with ct2 as primary (newer)
	merged := Merge2(ct2.Seq(), ct1.Seq())

	// Collect all keys in forward order
	var results []struct{ k, v []byte }
	for k, v := range merged {
		results = append(results, struct{ k, v []byte }{k, v})
	}

	require.Len(t, results, 3)
	require.Equal(t, []byte("a"), results[0].k)
	require.Equal(t, []byte("new_a"), results[0].v)
	require.Equal(t, []byte("c"), results[1].k)
	require.Equal(t, []byte("old_c"), results[1].v)
	require.Equal(t, []byte("d"), results[2].k)
	require.Equal(t, []byte("new_d"), results[2].v)
}

func TestMergeForward(t *testing.T) {
	ct1 := NewChangeTree()
	ct2 := NewChangeTree()

	ct1.Put([]byte("a"), []byte("1"))
	ct1.Put([]byte("c"), []byte("3"))
	ct2.Put([]byte("b"), []byte("2"))
	ct2.Put([]byte("d"), []byte("4"))

	merged := Merge2(ct2.Seq(), ct1.Seq())

	// Forward traversal
	expectedKeys := []string{"a", "b", "c", "d"}
	expectedVals := []string{"1", "2", "3", "4"}

	i := 0
	for k, v := range merged {
		require.True(t, i < len(expectedKeys))
		require.Equal(t, expectedKeys[i], string(k))
		require.Equal(t, expectedVals[i], string(v))
		i++
	}
	require.Equal(t, len(expectedKeys), i)
}

func TestMergeDeletePrecedence(t *testing.T) {
	ct1 := NewChangeTree()
	ct2 := NewChangeTree()

	// ct1 has update
	ct1.Put([]byte("key"), []byte("old"))

	// ct2 has delete (newer)
	ct2.Delete([]byte("key"))

	merged := Merge2(ct2.Seq(), ct1.Seq()) // ct2 first

	// Should not see the key since it's deleted in newer layer
	count := 0
	for range merged {
		count++
	}
	require.Equal(t, 0, count)
}

func TestMergeUpdatePrecedence(t *testing.T) {
	ct1 := NewChangeTree()
	ct2 := NewChangeTree()

	// ct1 has old value
	ct1.Put([]byte("key"), []byte("old"))

	// ct2 has new value
	ct2.Put([]byte("key"), []byte("new"))

	merged := Merge2(ct2.Seq(), ct1.Seq()) // ct2 first

	var results []struct{ k, v []byte }
	for k, v := range merged {
		results = append(results, struct{ k, v []byte }{k, v})
	}

	require.Len(t, results, 1)
	require.Equal(t, []byte("key"), results[0].k)
	require.Equal(t, []byte("new"), results[0].v)
}

func TestMerge2Reverse(t *testing.T) {
	ct1 := NewChangeTree()
	ct2 := NewChangeTree()

	ct1.Put([]byte("a"), []byte("1"))
	ct1.Put([]byte("c"), []byte("3"))
	ct2.Put([]byte("b"), []byte("2"))
	ct2.Put([]byte("d"), []byte("4"))

	merged := Merge2R(ct2.ScanReverse(nil), ct1.ScanReverse(nil))

	expectedKeys := []string{"d", "c", "b", "a"}
	expectedVals := []string{"4", "3", "2", "1"}

	i := 0
	for k, v := range merged {
		require.True(t, i < len(expectedKeys))
		require.Equal(t, expectedKeys[i], string(k))
		require.Equal(t, expectedVals[i], string(v))
		i++
	}
	require.Equal(t, len(expectedKeys), i)
}

func TestMerge2Compare(t *testing.T) {
	// Test Merge2Compare with custom comparison
	seq1 := func(yield func([]byte, []byte) bool) {
		if !yield([]byte("a"), []byte("1")) {
			return
		}
		if !yield([]byte("c"), []byte("3")) {
			return
		}
	}
	seq2 := func(yield func([]byte, []byte) bool) {
		if !yield([]byte("b"), []byte("2")) {
			return
		}
		if !yield([]byte("d"), []byte("4")) {
			return
		}
	}

	// Custom compare function (same as ByteKeyCompare)
	customCmp := func(a [2][]byte, b [2][]byte) (int, bool) {
		return bytes.Compare(a[0], b[0]), a[1] == nil
	}

	merged := Merge2Compare(customCmp, seq2, seq1)

	expectedKeys := []string{"a", "b", "c", "d"}
	expectedVals := []string{"1", "2", "3", "4"}

	i := 0
	for k, v := range merged {
		require.True(t, i < len(expectedKeys))
		require.Equal(t, expectedKeys[i], string(k))
		require.Equal(t, expectedVals[i], string(v))
		i++
	}
	require.Equal(t, len(expectedKeys), i)
}

func TestMerge2ReverseWithTombstones(t *testing.T) {
	// Create two sequences: one with values, one with tombstones
	seq1 := func(yield func([]byte, []byte) bool) {
		if !yield([]byte("key3"), []byte("val3")) {
			return
		}
		if !yield([]byte("key1"), []byte("val1")) {
			return
		}
	}
	seq2 := func(yield func([]byte, []byte) bool) {
		if !yield([]byte("key4"), []byte("val4")) {
			return
		}
		if !yield([]byte("key2"), nil) {
			return
		} // tombstone
	}

	merged := Merge2R(seq2, seq1)

	var results []struct{ k, v []byte }
	for k, v := range merged {
		results = append(results, struct{ k, v []byte }{k, v})
	}

	expected := []struct{ k, v []byte }{
		{[]byte("key4"), []byte("val4")},
		{[]byte("key3"), []byte("val3")},
		{[]byte("key1"), []byte("val1")},
	}

	require.Len(t, results, len(expected))

	for i, exp := range expected {
		require.True(t, bytes.Equal(results[i].k, exp.k))
		require.True(t, bytes.Equal(results[i].v, exp.v))
	}
}

func TestMergeK_ThreeSequences(t *testing.T) {
	ct1 := NewChangeTree()
	ct2 := NewChangeTree()
	ct3 := NewChangeTree()

	// ct1: base layer
	ct1.Put([]byte("a"), []byte("base_a"))
	ct1.Put([]byte("b"), []byte("base_b"))
	ct1.Put([]byte("c"), []byte("base_c"))

	// ct2: middle layer
	ct2.Put([]byte("a"), []byte("mid_a")) // override
	ct2.Put([]byte("d"), []byte("mid_d")) // new
	ct2.Delete([]byte("b"))               // delete

	// ct3: top layer (highest priority)
	ct3.Put([]byte("a"), []byte("top_a")) // override
	ct3.Put([]byte("e"), []byte("top_e")) // new

	merged := MergeK(ct3.Seq(), ct2.Seq(), ct1.Seq())

	var results []struct{ k, v []byte }
	for k, v := range merged {
		results = append(results, struct{ k, v []byte }{k, v})
	}

	// Expected: a=top_a, c=base_c, d=mid_d, e=top_e (b deleted)
	expected := []struct{ k, v []byte }{
		{[]byte("a"), []byte("top_a")},
		{[]byte("c"), []byte("base_c")},
		{[]byte("d"), []byte("mid_d")},
		{[]byte("e"), []byte("top_e")},
	}

	require.Len(t, results, len(expected))
	for i, exp := range expected {
		require.Equal(t, exp.k, results[i].k)
		require.Equal(t, exp.v, results[i].v)
	}
}

func TestMergeK_Reverse(t *testing.T) {
	ct1 := NewChangeTree()
	ct2 := NewChangeTree()
	ct3 := NewChangeTree()

	ct1.Put([]byte("a"), []byte("1"))
	ct1.Put([]byte("c"), []byte("3"))
	ct2.Put([]byte("b"), []byte("2"))
	ct3.Put([]byte("d"), []byte("4"))

	// For reverse merging to work properly, sequences must be walked in reverse order
	merged := MergeKR(ct3.ScanReverse(nil), ct2.ScanReverse(nil), ct1.ScanReverse(nil))

	expectedKeys := []string{"d", "c", "b", "a"}
	expectedVals := []string{"4", "3", "2", "1"}

	i := 0
	for k, v := range merged {
		require.True(t, i < len(expectedKeys))
		require.Equal(t, expectedKeys[i], string(k))
		require.Equal(t, expectedVals[i], string(v))
		i++
	}
	require.Equal(t, len(expectedKeys), i)
}

func TestMergeK_SingleSequence(t *testing.T) {
	ct := NewChangeTree()
	ct.Put([]byte("a"), []byte("1"))
	ct.Put([]byte("b"), []byte("2"))

	merged := MergeK(ct.Seq())

	var results []struct{ k, v []byte }
	for k, v := range merged {
		results = append(results, struct{ k, v []byte }{k, v})
	}

	expected := []struct{ k, v []byte }{
		{[]byte("a"), []byte("1")},
		{[]byte("b"), []byte("2")},
	}

	require.Len(t, results, len(expected))
	for i, exp := range expected {
		require.Equal(t, exp.k, results[i].k)
		require.Equal(t, exp.v, results[i].v)
	}
}

func TestMergeK_EmptySequences(t *testing.T) {
	// Test with nil sequences
	ct := NewChangeTree()
	ct.Put([]byte("a"), []byte("1"))

	merged := MergeK(nil, ct.Seq(), nil)

	var results []struct{ k, v []byte }
	for k, v := range merged {
		results = append(results, struct{ k, v []byte }{k, v})
	}

	expected := []struct{ k, v []byte }{
		{[]byte("a"), []byte("1")},
	}

	require.Len(t, results, len(expected))
	for i, exp := range expected {
		require.Equal(t, exp.k, results[i].k)
		require.Equal(t, exp.v, results[i].v)
	}
}

func TestMergeK_NoSequences(t *testing.T) {
	merged := MergeK()

	count := 0
	for range merged {
		count++
	}
	require.Equal(t, 0, count)
}

func TestMergeK_WithTombstones(t *testing.T) {
	ct1 := NewChangeTree()
	ct2 := NewChangeTree()
	ct3 := NewChangeTree()

	// ct1: base
	ct1.Put([]byte("a"), []byte("base_a"))
	ct1.Put([]byte("b"), []byte("base_b"))

	// ct2: middle - delete b
	ct2.Delete([]byte("b"))

	// ct3: top - add c, delete a
	ct3.Put([]byte("c"), []byte("top_c"))
	ct3.Delete([]byte("a"))

	merged := MergeK(ct3.Seq(), ct2.Seq(), ct1.Seq())

	var results []struct{ k, v []byte }
	for k, v := range merged {
		results = append(results, struct{ k, v []byte }{k, v})
	}

	// Expected: only c=top_c (a and b deleted)
	expected := []struct{ k, v []byte }{
		{[]byte("c"), []byte("top_c")},
	}

	require.Len(t, results, len(expected))
	for i, exp := range expected {
		require.Equal(t, exp.k, results[i].k)
		require.Equal(t, exp.v, results[i].v)
	}
}

func TestMergeK_ReverseSequences(t *testing.T) {
	ct1 := NewChangeTree()
	ct2 := NewChangeTree()
	ct3 := NewChangeTree()

	ct1.Put([]byte("a"), []byte("1"))
	ct1.Put([]byte("c"), []byte("3"))
	ct2.Put([]byte("b"), []byte("2"))
	ct3.Put([]byte("d"), []byte("4"))

	// For reverse merging to work properly, sequences must be walked in reverse order
	merged := MergeKR(ct3.ScanReverse(nil), ct2.ScanReverse(nil), ct1.ScanReverse(nil))

	expectedKeys := []string{"d", "c", "b", "a"}
	expectedVals := []string{"4", "3", "2", "1"}

	i := 0
	for k, v := range merged {
		require.True(t, i < len(expectedKeys))
		require.Equal(t, expectedKeys[i], string(k))
		require.Equal(t, expectedVals[i], string(v))
		i++
	}
	require.Equal(t, len(expectedKeys), i)
}

func TestMergeK_ReverseWithTombstones(t *testing.T) {
	ct1 := NewChangeTree()
	ct2 := NewChangeTree()
	ct3 := NewChangeTree()

	// ct1: base
	ct1.Put([]byte("a"), []byte("base_a"))
	ct1.Put([]byte("b"), []byte("base_b"))
	ct1.Put([]byte("c"), []byte("base_c"))

	// ct2: middle - delete b
	ct2.Delete([]byte("b"))

	// ct3: top - add d, delete a
	ct3.Put([]byte("d"), []byte("top_d"))
	ct3.Delete([]byte("a"))

	// For reverse merging to work properly, sequences must be walked in reverse order
	merged := MergeKR(ct3.ScanReverse(nil), ct2.ScanReverse(nil), ct1.ScanReverse(nil))

	var results []struct{ k, v []byte }
	for k, v := range merged {
		results = append(results, struct{ k, v []byte }{k, v})
	}

	// Expected: d=top_d, c=base_c (a and b deleted), in reverse order: c, d
	expected := []struct{ k, v []byte }{
		{[]byte("d"), []byte("top_d")},
		{[]byte("c"), []byte("base_c")},
	}

	require.Len(t, results, len(expected))
	for i, exp := range expected {
		require.Equal(t, exp.k, results[i].k)
		require.Equal(t, exp.v, results[i].v)
	}
}

func TestMerge2_ReverseSequences(t *testing.T) {
	ct1 := NewChangeTree()
	ct2 := NewChangeTree()

	ct1.Put([]byte("a"), []byte("1"))
	ct1.Put([]byte("c"), []byte("3"))
	ct2.Put([]byte("b"), []byte("2"))
	ct2.Put([]byte("d"), []byte("4"))

	// For reverse merging to work properly, sequences must be walked in reverse order
	merged := Merge2R(ct2.ScanReverse(nil), ct1.ScanReverse(nil))

	expectedKeys := []string{"d", "c", "b", "a"}
	expectedVals := []string{"4", "3", "2", "1"}

	i := 0
	for k, v := range merged {
		require.True(t, i < len(expectedKeys))
		require.Equal(t, expectedKeys[i], string(k))
		require.Equal(t, expectedVals[i], string(v))
		i++
	}
	require.Equal(t, len(expectedKeys), i)
}

// TestMerge_RandomKeysWithDeletes tests 2 and 3 layer merges (forward/reverse)
// with random keys (10-1000), deletes in top layer, verifies no tombstones,
// strict ordering and no duplicate keys.
func TestMerge_RandomKeysWithDeletes(t *testing.T) {
	sizes := []int{10, 100, 1000}
	for _, n := range sizes {
		// generate n random unique keys
		keySet := make(map[string]struct{})
		var allKeys [][]byte
		for len(allKeys) < n {
			k := make([]byte, 16)
			rand.Read(k)
			if _, ok := keySet[string(k)]; !ok {
				keySet[string(k)] = struct{}{}
				allKeys = append(allKeys, k)
			}
		}

		// pick ~20% deletes for top layer
		numDeletes := n / 5
		if numDeletes == 0 {
			numDeletes = 1
		}
		deleteSet := make(map[string]struct{})
		for i := 0; i < numDeletes && i < len(allKeys); i++ {
			deleteSet[string(allKeys[i])] = struct{}{}
		}

		// base layer gets all
		ctBase := NewChangeTree()
		for _, k := range allKeys {
			ctBase.Put(k, []byte("val"))
		}

		// top layer deletes some
		ctTop := NewChangeTree()
		for dk := range deleteSet {
			ctTop.Delete([]byte(dk))
		}

		// test Merge2 forward
		runRandomMergeTest(t, Merge2(ctTop.Seq(), ctBase.Seq()), deleteSet, true)

		// test Merge2 reverse
		runRandomMergeTest(t, Merge2R(ctTop.ScanReverse(nil), ctBase.ScanReverse(nil)), deleteSet, false)

		// 3 layers: add middle
		ctMid := NewChangeTree()
		merged3 := MergeK(ctTop.Seq(), ctMid.Seq(), ctBase.Seq())
		runRandomMergeTest(t, merged3, deleteSet, true)

		merged3r := MergeKR(ctTop.ScanReverse(nil), ctMid.ScanReverse(nil), ctBase.ScanReverse(nil))
		runRandomMergeTest(t, merged3r, deleteSet, false)
	}
}

func runRandomMergeTest(t *testing.T, merged iter.Seq2[[]byte, []byte], deletes map[string]struct{}, ascending bool) {
	t.Helper()
	var prev []byte
	seen := make(map[string]struct{})
	count := 0
	for k, v := range merged {
		count++
		ks := string(k)
		if _, del := deletes[ks]; del {
			t.Errorf("deleted key %x appeared in output", k)
		}
		if _, dup := seen[ks]; dup {
			t.Errorf("duplicate key %x in merge output", k)
		}
		seen[ks] = struct{}{}
		if prev != nil {
			cmp := bytes.Compare(prev, k)
			if ascending && cmp >= 0 {
				t.Errorf("out of order: %x >= %x (ascending)", prev, k)
			}
			if !ascending && cmp <= 0 {
				t.Errorf("out of order: %x <= %x (descending)", prev, k)
			}
		}
		prev = append([]byte{}, k...)
		_ = v // values not important
	}
	if count == 0 && len(deletes) > 0 {
		// possible if all deleted, ok
	}
}
