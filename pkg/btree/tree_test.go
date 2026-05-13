package btree

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestChangeTreePutGetDelete(t *testing.T) {
	ct := NewChangeTree()

	// Test Put and Get
	key1 := []byte("key1")
	val1 := []byte("value1")
	ct.Put(key1, val1)
	val, deleted := ct.Get(key1)
	require.False(t, deleted)
	require.Equal(t, val1, val)

	// Put again, should update
	val2 := []byte("value2")
	ct.Put(key1, val2)
	val, deleted = ct.Get(key1)
	require.False(t, deleted)
	require.Equal(t, val2, val)

	// Delete
	ct.Delete(key1)
	val, deleted = ct.Get(key1)
	require.True(t, deleted)
	require.Nil(t, val)

	// Delete again, should still be deleted
	ct.Delete(key1)
	val, deleted = ct.Get(key1)
	require.True(t, deleted)
	require.Nil(t, val)

	// Put after delete
	ct.Put(key1, val1)
	val, deleted = ct.Get(key1)
	require.False(t, deleted)
	require.Equal(t, val1, val)
}

func TestChangeTreeInvariant(t *testing.T) {
	ct := NewChangeTree()

	key := []byte("key")
	val := []byte("value")

	// Put
	ct.Put(key, val)
	require.Equal(t, 1, ct.Len())
	value, deleted := ct.Get(key)
	require.False(t, deleted)
	require.NotNil(t, value)

	// Delete
	ct.Delete(key)
	require.Equal(t, 1, ct.Len())
	value, deleted = ct.Get(key)
	require.True(t, deleted)
	require.Nil(t, value)

	// Put again
	ct.Put(key, val)
	require.Equal(t, 1, ct.Len())
	value, deleted = ct.Get(key)
	require.False(t, deleted)
	require.NotNil(t, value)
}

func TestChangeTreeMerge(t *testing.T) {
	ct1 := NewChangeTree()
	ct2 := NewChangeTree()

	key1 := []byte("key1")
	val1 := []byte("value1")
	key2 := []byte("key2")
	val2 := []byte("value2")

	ct1.Put(key1, val1)
	ct2.Put(key2, val2)
	ct2.Delete(key1) // delete in ct2 should override put in ct1

	ct1.Merge(ct2)

	// key1 should be deleted
	val, deleted := ct1.Get(key1)
	require.True(t, deleted)
	require.Nil(t, val)

	// key2 should be updated
	val, deleted = ct1.Get(key2)
	require.False(t, deleted)
	require.Equal(t, val2, val)
}

func TestChangeTreeIterator(t *testing.T) {
	ct := NewChangeTree()

	keys := [][]byte{
		{0x1},
		{0x2},
		{0x3},
	}
	vals := [][]byte{
		{0x1},
		{0x2},
		{0x3},
	}

	// Put all
	for i, key := range keys {
		ct.Put(key, vals[i])
	}

	// Delete middle
	ct.Delete(keys[1])
	require.Equal(t, 3, ct.Len())

	val, deleted := ct.Get(keys[0])
	require.False(t, deleted)
	require.Equal(t, vals[0], val)

	// Iterator should return merged updates and deletes
	// with val1 = nil due to delete
	var i int
	for k, v := range ct.Scan(nil) {
		if i == 1 {
			require.Equal(t, keys[i], k)
			require.Nil(t, v)
		} else {
			require.NotNil(t, k)
			require.Equal(t, keys[i], k)
			require.Equal(t, vals[i], v)
		}
		i++
	}
}

func TestChangeTreeIteratorForward(t *testing.T) {
	ct := NewChangeTree()

	keys := [][]byte{
		{0x1},
		{0x2},
		{0x3},
	}
	vals := [][]byte{
		{0x1},
		{0x2},
		{0x3},
	}

	for i, key := range keys {
		ct.Put(key, vals[i])
	}

	var i int
	for k, v := range ct.Scan(nil) {
		require.NotNil(t, v)
		require.Equal(t, keys[i], k)
		require.Equal(t, vals[i], v)
		i++
	}
}

func TestChangeTreeIteratorBackward(t *testing.T) {
	ct := NewChangeTree()

	keys := [][]byte{
		{0x1},
		{0x2},
		{0x3},
	}
	vals := [][]byte{
		{0x1},
		{0x2},
		{0x3},
	}

	for i, key := range keys {
		ct.Put(key, vals[i])
	}

	i := 2
	for k, v := range ct.ScanReverse(nil) {
		require.NotNil(t, k)
		require.Equal(t, keys[i], k)
		require.Equal(t, vals[i], v)
		i--
	}
}

func TestChangeTreeIteratorSeek(t *testing.T) {
	ct := NewChangeTree()

	keys := [][]byte{
		{0x1},
		{0x3},
		{0x5},
		{0x7},
	}
	vals := [][]byte{
		{0x1},
		{0x3},
		{0x5},
		{0x7},
	}

	for i, key := range keys {
		ct.Put(key, vals[i])
	}

	// Seek to "2"
	for k, v := range ct.ScanRange([]byte{0x2}, nil) {
		require.NotNil(t, v)
		require.Equal(t, keys[1], k)
		require.Equal(t, vals[1], v)
		break
	}

	// Seek to "3"
	i := 0
	for k, v := range ct.ScanRange(keys[1], nil) {
		if i == 0 {
			require.NotNil(t, v)
			require.Equal(t, keys[1], k)
			require.Equal(t, vals[1], v)
			i++
		} else {
			// Next to "5"
			require.Equal(t, keys[2], k)
			require.Equal(t, vals[2], v)
			i++
			break
		}
	}
	require.Equal(t, 2, i)

	// Seek to "1" then Prev should fail
	i = 0
	for k, v := range ct.ScanRangeReverse(nil, keys[1]) {
		require.NotNil(t, v)
		require.Equal(t, keys[0], k)
		require.Equal(t, vals[0], v)
		i++
	}
	require.Equal(t, 1, i)
}

func TestChangeTreeScanRanges(t *testing.T) {
	ct := NewChangeTree()

	keys := [][]byte{
		{0x1}, // "a" equivalent
		{0x2}, // "b"
		{0x3}, // "c"
		{0x4}, // "d"
	}
	vals := [][]byte{
		{0x1},
		{0x2},
		{0x3},
		{0x4},
	}

	for i, key := range keys {
		ct.Put(key, vals[i])
	}

	// Test Scan(nil) - full scan forward
	var i int
	for k, v := range ct.Scan(nil) {
		require.Equal(t, keys[i], k)
		require.Equal(t, vals[i], v)
		i++
	}
	require.Equal(t, 4, i)

	// Test ScanReverse(nil) - full scan reverse
	i = 3
	for k, v := range ct.ScanReverse(nil) {
		require.Equal(t, keys[i], k)
		require.Equal(t, vals[i], v)
		i--
	}
	require.Equal(t, -1, i)

	// Test Scan([]byte{0x2}) - prefix scan forward (should match 0x2 only)
	i = 0
	for k, v := range ct.Scan([]byte{0x2}) {
		if i == 0 {
			require.Equal(t, keys[1], k)
			require.Equal(t, vals[1], v)
		}
		i++
	}
	require.Equal(t, 1, i)

	// Test ScanReverse([]byte{0x2}) - prefix scan reverse (should match 0x2 only)
	i = 0
	for k, v := range ct.ScanReverse([]byte{0x2}) {
		if i == 0 {
			require.Equal(t, keys[1], k)
			require.Equal(t, vals[1], v)
		}
		i++
	}
	require.Equal(t, 1, i)

	// Test ScanRange(nil, nil) - full scan forward
	i = 0
	for k, v := range ct.ScanRange(nil, nil) {
		require.Equal(t, keys[i], k)
		require.Equal(t, vals[i], v)
		i++
	}
	require.Equal(t, 4, i)

	// Test ScanRange(nil, []byte{0x3}) - from start to before 0x3
	i = 0
	for k, v := range ct.ScanRange(nil, []byte{0x3}) {
		require.Equal(t, keys[i], k)
		require.Equal(t, vals[i], v)
		i++
	}
	require.Equal(t, 2, i) // Should see 0x1, 0x2

	// Test ScanRange([]byte{0x2}, nil) - from 0x2 to end
	i = 0
	for k, v := range ct.ScanRange([]byte{0x2}, nil) {
		require.Equal(t, keys[i+1], k)
		require.Equal(t, vals[i+1], v)
		i++
	}
	require.Equal(t, 3, i) // Should see 0x2, 0x3, 0x4

	// Test ScanRange([]byte{0x2}, []byte{0x4}) - from 0x2 to before 0x4
	i = 0
	for k, v := range ct.ScanRange([]byte{0x2}, []byte{0x4}) {
		require.Equal(t, keys[i+1], k)
		require.Equal(t, vals[i+1], v)
		i++
	}
	require.Equal(t, 2, i) // Should see 0x2, 0x3

	// Test ScanRangeReverse(nil, nil) - full scan reverse
	i = 3
	for k, v := range ct.ScanRangeReverse(nil, nil) {
		require.Equal(t, keys[i], k)
		require.Equal(t, vals[i], v)
		i--
	}
	require.Equal(t, -1, i)

	// Test ScanRangeReverse(nil, []byte{0x3}) - from start to before 0x3 in reverse
	i = 0
	for k, v := range ct.ScanRangeReverse(nil, []byte{0x3}) {
		require.Equal(t, keys[1-i], k) // 0x2, then 0x1
		require.Equal(t, vals[1-i], v)
		i++
	}
	require.Equal(t, 2, i) // Should see 0x2, 0x1

	// Test ScanRangeReverse([]byte{0x2}, nil) - from 0x2 to end in reverse
	i = 0
	for k, v := range ct.ScanRangeReverse([]byte{0x2}, nil) {
		require.Equal(t, keys[3-i], k)
		require.Equal(t, vals[3-i], v)
		i++
	}
	require.Equal(t, 3, i) // Should see 0x4, 0x3, 0x2

	// Test ScanRangeReverse([]byte{0x2}, []byte{0x4}) - from 0x2 to before 0x4 in reverse
	i = 0
	for k, v := range ct.ScanRangeReverse([]byte{0x2}, []byte{0x4}) {
		require.Equal(t, keys[2-i], k)
		require.Equal(t, vals[2-i], v)
		i++
	}
	require.Equal(t, 2, i) // Should see 0x3, 0x2

	// Test ScanRange([]byte{0x4}, nil) - from 0x4 to end
	i = 0
	for k, v := range ct.ScanRange([]byte{0x4}, nil) {
		require.Equal(t, keys[3], k)
		require.Equal(t, vals[3], v)
		i++
	}
	require.Equal(t, 1, i) // Should see only 0x4

	// Test ScanRange(nil, []byte{0x1}) - from start to before 0x1 (empty)
	i = 0
	for range ct.ScanRange(nil, []byte{0x1}) {
		i++
	}
	require.Equal(t, 0, i) // Should be empty

	// Test ScanRange([]byte{0x2}, []byte{0x2}) - empty range (start == end)
	i = 0
	for range ct.ScanRange([]byte{0x2}, []byte{0x2}) {
		i++
	}
	require.Equal(t, 0, i) // Should be empty

	// Test ScanRange([]byte{0x3}, []byte{0x2}) - invalid range (start > end)
	i = 0
	for range ct.ScanRange([]byte{0x3}, []byte{0x2}) {
		i++
	}
	require.Equal(t, 0, i) // Should be empty

	// Test ScanRangeReverse([]byte{0x4}, nil) - from 0x4 to end in reverse
	i = 0
	for k, v := range ct.ScanRangeReverse([]byte{0x4}, nil) {
		require.Equal(t, keys[3], k)
		require.Equal(t, vals[3], v)
		i++
	}
	require.Equal(t, 1, i) // Should see only 0x4

	// Test ScanRangeReverse(nil, []byte{0x1}) - from start to before 0x1 in reverse (empty)
	i = 0
	for range ct.ScanRangeReverse(nil, []byte{0x1}) {
		i++
	}
	require.Equal(t, 0, i) // Should be empty

	// Test Scan([]byte{}) - empty prefix scan (should match all keys)
	i = 0
	for k, v := range ct.Scan([]byte{}) {
		require.Equal(t, keys[i], k)
		require.Equal(t, vals[i], v)
		i++
	}
	require.Equal(t, 4, i) // Should see all keys
}
