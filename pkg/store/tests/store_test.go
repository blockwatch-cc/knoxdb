// Copyright (c) 2026 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package store_test

import (
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"blockwatch.cc/knoxdb/pkg/store"
	_ "blockwatch.cc/knoxdb/pkg/store/boltdb"
	_ "blockwatch.cc/knoxdb/pkg/store/memdb"
)

var testedDBs = []string{"mem", "bolt"}

// openDB opens a testing database with the given driver
func openDB(t testing.TB, drv string) store.DBManager {
	t.Helper()
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.db")
	db, err := store.OpenOrCreate(
		store.WithDriver(drv),
		store.WithPath(path),
		store.WithNoSync(true),
		store.WithDropOnClose(false),
	)
	require.NoError(t, err)
	return db
}

// closeAndCleanup closes the database and cleans up files if necessary
func closeAndCleanup(t testing.TB, db store.DBManager) {
	t.Helper()
	driver, path := db.Type(), db.Path()
	require.NoError(t, db.Close())
	// For BoltDB, the file is removed by t.TempDir() cleanup
	// For MemDB, no file cleanup needed
	require.NoError(t, store.Drop(driver, path))
}

func v(val []byte, _ error) []byte {
	return val
}

// TestSupportedDrivers checks which drivers are registered
func TestSupportedDrivers(t *testing.T) {
	drivers := store.Supported()
	t.Logf("Supported drivers: %v", drivers)
	for _, typ := range testedDBs {
		require.Contains(t, drivers, typ)
	}
}

// TestDB_Open tests opening an in-memory database
func TestDB_Open(t *testing.T) {
	for _, typ := range testedDBs {
		db := openDB(t, typ)
		require.Equal(t, typ, db.Type())
		closeAndCleanup(t, db)
	}
}

// TestDB_BasicOperations tests basic Put/Get/Delete operations
func TestDB_BasicOperations(t *testing.T) {
	for _, tt := range testedDBs {
		t.Run(tt, func(t *testing.T) {
			db := openDB(t, tt)
			defer closeAndCleanup(t, db)

			// Test basic operations
			err := db.Update(func(tx store.Tx) error {
				bucket, err := tx.CreateBucket([]byte("test"))
				require.NoError(t, err)
				require.NoError(t, bucket.Put([]byte("key1"), []byte("value1")))
				require.NoError(t, bucket.Put([]byte("key2"), []byte("value2")))
				return nil
			})
			require.NoError(t, err)

			// Verify
			err = db.View(func(tx store.Tx) error {
				bucket, err := tx.Bucket([]byte("test"))
				require.NoError(t, err)
				require.NotNil(t, bucket)
				require.Equal(t, []byte("value1"), v(bucket.Get([]byte("key1"))))
				require.Equal(t, []byte("value2"), v(bucket.Get([]byte("key2"))))
				return nil
			})
			require.NoError(t, err)

			// Delete
			err = db.Update(func(tx store.Tx) error {
				bucket, err := tx.Bucket([]byte("test"))
				require.NoError(t, err)
				require.NoError(t, bucket.Delete([]byte("key1")))
				return nil
			})
			require.NoError(t, err)

			// Verify deletion
			err = db.View(func(tx store.Tx) error {
				bucket, err := tx.Bucket([]byte("test"))
				require.NoError(t, err)
				require.Nil(t, v(bucket.Get([]byte("key1"))))
				require.Equal(t, []byte("value2"), v(bucket.Get([]byte("key2"))))
				return nil
			})
			require.NoError(t, err)
		})
	}
}

func TestDB_Reopen(t *testing.T) {
	for _, tt := range testedDBs {
		t.Run(tt, func(t *testing.T) {
			db := openDB(t, tt)
			defer func() {
				closeAndCleanup(t, db)
			}()

			// Test basic operations
			err := db.Update(func(tx store.Tx) error {
				bucket, err := tx.CreateBucket([]byte("test"))
				require.NoError(t, err)
				require.NoError(t, bucket.Put([]byte("key1"), []byte("value1")))
				require.NoError(t, bucket.Put([]byte("key2"), []byte("value2")))
				return nil
			})
			require.NoError(t, err)

			// Verify
			err = db.View(func(tx store.Tx) error {
				bucket, err := tx.Bucket([]byte("test"))
				require.NoError(t, err)
				require.NotNil(t, bucket)
				require.Equal(t, []byte("value1"), v(bucket.Get([]byte("key1"))))
				require.Equal(t, []byte("value2"), v(bucket.Get([]byte("key2"))))
				return nil
			})
			require.NoError(t, err)

			// Close and reopen
			drv, path := db.Type(), db.Path()
			db.Close()
			tx, err := db.Begin()
			if err == nil {
				defer tx.Rollback()
			}
			require.Error(t, err)
			db, err = store.OpenOrCreate(
				store.WithDriver(drv),
				store.WithPath(path),
				store.WithNoSync(true),
				store.WithDropOnClose(false),
			)
			require.NoError(t, err)

			// Verify
			err = db.View(func(tx store.Tx) error {
				bucket, err := tx.Bucket([]byte("test"))
				require.NoError(t, err)
				require.NotNil(t, bucket)
				require.Equal(t, []byte("value1"), v(bucket.Get([]byte("key1"))))
				require.Equal(t, []byte("value2"), v(bucket.Get([]byte("key2"))))
				return nil
			})
			require.NoError(t, err)
		})
	}
}

// TestBucket_Nested tests nested bucket operations
func TestBucket_Nested(t *testing.T) {
	for _, tt := range testedDBs {
		t.Run(tt, func(t *testing.T) {
			db := openDB(t, tt)
			defer closeAndCleanup(t, db)

			err := db.Update(func(tx store.Tx) error {
				parent, err := tx.CreateBucket([]byte("parent"))
				require.NoError(t, err)
				child, err := parent.CreateBucket([]byte("child"))
				require.NoError(t, err)
				require.NoError(t, child.Put([]byte("key"), []byte("value")))
				return nil
			})
			require.NoError(t, err)

			err = db.View(func(tx store.Tx) error {
				parent, err := tx.Bucket([]byte("parent"))
				require.NoError(t, err)
				require.NotNil(t, parent)
				child, err := parent.Bucket([]byte("child"))
				require.NoError(t, err)
				require.NotNil(t, child)
				require.Equal(t, []byte("value"), v(child.Get([]byte("key"))))
				return nil
			})
			require.NoError(t, err)
		})
	}
}

// TestCursor_Iteration tests cursor iteration
func TestCursor_Iteration(t *testing.T) {
	for _, tt := range testedDBs {
		t.Run(tt, func(t *testing.T) {
			db := openDB(t, tt)
			defer closeAndCleanup(t, db)

			// Insert test data
			err := db.Update(func(tx store.Tx) error {
				bucket, err := tx.CreateBucket([]byte("test"))
				require.NoError(t, err)
				require.NoError(t, bucket.Put([]byte("key1"), []byte("val1")))
				require.NoError(t, bucket.Put([]byte("key2"), []byte("val2")))
				require.NoError(t, bucket.Put([]byte("key3"), []byte("val3")))
				return nil
			})
			require.NoError(t, err)

			// Test forward iteration
			err = db.View(func(tx store.Tx) error {
				bucket, err := tx.Bucket([]byte("test"))
				require.NoError(t, err)
				var i int
				for k, v := range bucket.Scan(nil) {
					switch i {
					case 0:
						require.Equal(t, []byte("key1"), k)
						require.Equal(t, []byte("val1"), v)
					case 1:
						require.Equal(t, []byte("key2"), k)
						require.Equal(t, []byte("val2"), v)
					case 2:
						require.Equal(t, []byte("key3"), k)
						require.Equal(t, []byte("val3"), v)
					}
					i++
				}
				require.Equal(t, 3, i)
				return nil
			})
			require.NoError(t, err)

			// Test backward iteration
			err = db.View(func(tx store.Tx) error {
				bucket, err := tx.Bucket([]byte("test"))
				require.NoError(t, err)
				var i int
				for k, v := range bucket.ScanReverse(nil) {
					switch i {
					case 0:
						require.Equal(t, []byte("key3"), k)
						require.Equal(t, []byte("val3"), v)
					case 1:
						require.Equal(t, []byte("key2"), k)
						require.Equal(t, []byte("val2"), v)
					case 2:
						require.Equal(t, []byte("key1"), k)
						require.Equal(t, []byte("val1"), v)
					}
					i++
				}
				require.Equal(t, 3, i)
				return nil
			})
			require.NoError(t, err)
		})
	}
}

// TestCursor_NestedBuckets tests that cursors skip nested buckets and only iterate over key-value pairs
func TestCursor_NestedBuckets(t *testing.T) {
	for _, tt := range testedDBs {
		t.Run(tt, func(t *testing.T) {
			db := openDB(t, tt)
			defer closeAndCleanup(t, db)

			// Insert test data with nested buckets
			err := db.Update(func(tx store.Tx) error {
				bucket, err := tx.CreateBucket([]byte("test"))
				require.NoError(t, err)
				require.NoError(t, bucket.Put([]byte("key1"), []byte("val1")))
				require.NoError(t, bucket.Put([]byte("key2"), []byte("val2")))
				// Create nested bucket
				nested, err := bucket.CreateBucket([]byte("nested"))
				require.NoError(t, err)
				require.NoError(t, nested.Put([]byte("nestedkey"), []byte("nestedval")))
				require.NoError(t, bucket.Put([]byte("key3"), []byte("val3")))
				return nil
			})
			require.NoError(t, err)

			// Test forward iteration - should skip nested bucket
			err = db.View(func(tx store.Tx) error {
				bucket, err := tx.Bucket([]byte("test"))
				require.NoError(t, err)
				var i int
				for k, v := range bucket.Scan(nil) {
					switch i {
					case 0:
						require.Equal(t, []byte("key1"), k)
						require.Equal(t, []byte("val1"), v)
					case 1:
						require.Equal(t, []byte("key2"), k)
						require.Equal(t, []byte("val2"), v)
					case 2:
						require.Equal(t, []byte("key3"), k)
						require.Equal(t, []byte("val3"), v)
					}
					i++
				}
				require.Equal(t, 3, i) // Should not see "nested"
				return nil
			})
			require.NoError(t, err)

			// Test Seek - should skip nested bucket
			err = db.View(func(tx store.Tx) error {
				bucket, err := tx.Bucket([]byte("test"))
				require.NoError(t, err)
				var i int
				for k, v := range bucket.ScanRange([]byte("key2"), nil) {
					switch i {
					case 0:
						require.Equal(t, []byte("key2"), k)
						require.Equal(t, []byte("val2"), v)
					case 1:
						require.Equal(t, []byte("key3"), k)
						require.Equal(t, []byte("val3"), v)
					}
					i++
				}
				require.Equal(t, 2, i) // Should not see "nested"
				return nil
			})
			require.NoError(t, err)
		})
	}
}

// TestTx_Rollback tests transaction rollback
func TestTx_Rollback(t *testing.T) {
	for _, tt := range testedDBs {
		t.Run(tt, func(t *testing.T) {
			db := openDB(t, tt)
			defer closeAndCleanup(t, db)

			// Insert initial data
			err := db.Update(func(tx store.Tx) error {
				bucket, err := tx.CreateBucket([]byte("test"))
				require.NoError(t, err)
				require.NoError(t, bucket.Put([]byte("key"), []byte("initial")))
				return nil
			})
			require.NoError(t, err)

			// Start transaction and modify, then rollback
			tx, err := db.Begin(store.WithTxWrite())
			defer tx.Rollback()
			require.NoError(t, err)
			bucket, err := tx.Bucket([]byte("test"))
			require.NoError(t, err)
			require.NotNil(t, bucket)
			require.NoError(t, bucket.Put([]byte("key"), []byte("modified")))
			require.NoError(t, tx.Rollback())

			// Verify rollback
			err = db.View(func(tx store.Tx) error {
				bucket, err := tx.Bucket([]byte("test"))
				require.NoError(t, err)
				require.Equal(t, []byte("initial"), v(bucket.Get([]byte("key"))))
				return nil
			})
			require.NoError(t, err)
		})
	}
}

// TestDB_SnapshotRestore tests database snapshot and restore
// func TestDB_SnapshotRestore(t *testing.T) {
// 	for _, tt := range testedDBs {
// 		t.Run(tt, func(t *testing.T) {
// 			db := openDB(t, tt)
// 			defer closeAndCleanup(t, db)

// 			// Insert data
// 			err := db.Update(func(tx store.Tx) error {
// 				bucket, err := tx.CreateBucket([]byte("test"))
// 				require.NoError(t, err)
// 				require.NoError(t, bucket.Put([]byte("key1"), []byte("val1")))
// 				return nil
// 			})
// 			require.NoError(t, err)

// 			// Snapshot to buffer
// 			var buf bytes.Buffer
// 			require.NoError(t, db.Snapshot(&buf))

// 			// Close original
// 			require.NoError(t, db.Close())

// 			// Open new db
// 			newDB := openDB(t, tt)
// 			defer closeAndCleanup(t, newDB)

// 			// Restore
// 			require.NoError(t, newDB.Restore(&buf))

// 			// Verify
// 			err = newDB.View(func(tx store.Tx) error {
// 				bucket := tx.Bucket([]byte("test"))
// 				require.NotNil(t, bucket)
// 				require.Equal(t, []byte("val1"), bucket.Get([]byte("key1")))
// 				return nil
// 			})
// 			require.NoError(t, err)
// 		})
// 	}
// }

// TestTx_Commit tests transaction commit
func TestTx_Commit(t *testing.T) {
	for _, tt := range testedDBs {
		t.Run(tt, func(t *testing.T) {
			db := openDB(t, tt)
			defer closeAndCleanup(t, db)

			tx, err := db.Begin(store.WithTxWrite())
			require.NoError(t, err)
			bucket, err := tx.CreateBucket([]byte("test"))
			require.NoError(t, err)
			require.NoError(t, bucket.Put([]byte("key"), []byte("value")))
			require.NoError(t, tx.Commit())

			// Verify
			err = db.View(func(tx store.Tx) error {
				bucket, err := tx.Bucket([]byte("test"))
				require.NoError(t, err)
				require.Equal(t, []byte("value"), v(bucket.Get([]byte("key"))))
				return nil
			})
			require.NoError(t, err)
		})
	}
}

// TestTx_ReadOnly tests read-only transactions
func TestTx_ReadOnly(t *testing.T) {
	for _, tt := range testedDBs {
		t.Run(tt, func(t *testing.T) {
			db := openDB(t, tt)
			defer closeAndCleanup(t, db)

			// Insert data
			err := db.Update(func(tx store.Tx) error {
				bucket, err := tx.CreateBucket([]byte("test"))
				require.NoError(t, err)
				require.NoError(t, bucket.Put([]byte("key"), []byte("value")))
				return nil
			})
			require.NoError(t, err)

			// Read-only tx
			tx, err := db.Begin()
			require.NoError(t, err)
			require.False(t, tx.IsWriteable())
			bucket, err := tx.Bucket([]byte("test"))
			require.NoError(t, err)
			require.Equal(t, []byte("value"), v(bucket.Get([]byte("key"))))
			require.NoError(t, tx.Rollback())
		})
	}
}

// TestBucket_DeleteBucket tests deleting buckets
func TestBucket_DeleteBucket(t *testing.T) {
	for _, tt := range testedDBs {
		t.Run(tt, func(t *testing.T) {
			db := openDB(t, tt)
			defer closeAndCleanup(t, db)

			err := db.Update(func(tx store.Tx) error {
				bucket, err := tx.CreateBucket([]byte("test"))
				require.NoError(t, err)
				require.NoError(t, bucket.Put([]byte("key"), []byte("value")))
				require.NoError(t, tx.DeleteBucket([]byte("test")))
				return nil
			})
			require.NoError(t, err)

			// Verify deleted
			err = db.View(func(tx store.Tx) error {
				b, err := tx.Bucket([]byte("test"))
				require.Error(t, err)
				require.Nil(t, b)
				return nil
			})
			require.NoError(t, err)
		})
	}
}

// TestBucket_PrefixCursor tests prefix/range cursors
func TestBucket_PrefixCursor(t *testing.T) {
	for _, tt := range testedDBs {
		t.Run(tt, func(t *testing.T) {
			db := openDB(t, tt)
			defer closeAndCleanup(t, db)

			err := db.Update(func(tx store.Tx) error {
				bucket, err := tx.CreateBucket([]byte("test"))
				require.NoError(t, err)
				require.NoError(t, bucket.Put([]byte("aa"), []byte("1")))
				require.NoError(t, bucket.Put([]byte("ab"), []byte("2")))
				require.NoError(t, bucket.Put([]byte("b"), []byte("3")))
				return nil
			})
			require.NoError(t, err)

			// Forward order
			err = db.View(func(tx store.Tx) error {
				bucket, err := tx.Bucket([]byte("test"))
				require.NoError(t, err)
				var i int
				for k := range bucket.Scan([]byte("a")) {
					switch i {
					case 0:
						require.Equal(t, []byte("aa"), k)
					case 1:
						require.Equal(t, []byte("ab"), k)
					}
					i++
				}
				require.Equal(t, 2, i) // Should not see "b"
				return nil
			})
			require.NoError(t, err)

			// Reverse order
			err = db.View(func(tx store.Tx) error {
				bucket, err := tx.Bucket([]byte("test"))
				require.NoError(t, err)
				var i int
				for k := range bucket.ScanReverse([]byte("a")) {
					switch i {
					case 0:
						require.Equal(t, []byte("ab"), k)
					case 1:
						require.Equal(t, []byte("aa"), k)
					}
					i++
				}
				require.Equal(t, 2, i) // Should not see "b"
				return nil
			})
			require.NoError(t, err)
		})
	}
}

// TestCursor_Seek tests cursor seek
func TestCursor_Seek(t *testing.T) {
	for _, tt := range testedDBs {
		t.Run(tt, func(t *testing.T) {
			db := openDB(t, tt)
			defer closeAndCleanup(t, db)

			err := db.Update(func(tx store.Tx) error {
				bucket, err := tx.CreateBucket([]byte("test"))
				require.NoError(t, err)
				require.NoError(t, bucket.Put([]byte("a"), []byte("1")))
				require.NoError(t, bucket.Put([]byte("b"), []byte("2")))
				require.NoError(t, bucket.Put([]byte("c"), []byte("3")))
				return nil
			})
			require.NoError(t, err)

			// forward order
			err = db.View(func(tx store.Tx) error {
				bucket, err := tx.Bucket([]byte("test"))
				require.NoError(t, err)
				var i int
				for k, v := range bucket.ScanRange([]byte("b"), nil) {
					switch i {
					case 0:
						require.Equal(t, []byte("b"), k)
						require.Equal(t, []byte("2"), v)
					case 1:
						require.Equal(t, []byte("c"), k)
						require.Equal(t, []byte("3"), v)
					}
					i++
				}
				require.Equal(t, 2, i) // Should not see "a"
				return nil
			})
			require.NoError(t, err)

			// reverse order
			err = db.View(func(tx store.Tx) error {
				bucket, err := tx.Bucket([]byte("test"))
				require.NoError(t, err)
				var i int
				for k, v := range bucket.ScanRangeReverse([]byte("b"), nil) {
					switch i {
					case 0:
						require.Equal(t, []byte("c"), k)
						require.Equal(t, []byte("3"), v)
					case 1:
						require.Equal(t, []byte("b"), k)
						require.Equal(t, []byte("2"), v)
					}
					t.Logf("k=%q", k)
					i++
				}
				require.Equal(t, 2, i) // Should not see "a"
				return nil
			})
			require.NoError(t, err)
		})
	}
}

// TestCursor_RangeScans tests all combinations of range scans with nil and non-nil boundaries
func TestCursor_RangeScans(t *testing.T) {
	for _, tt := range testedDBs {
		t.Run(tt, func(t *testing.T) {
			db := openDB(t, tt)
			defer closeAndCleanup(t, db)

			err := db.Update(func(tx store.Tx) error {
				bucket, err := tx.CreateBucket([]byte("test"))
				require.NoError(t, err)
				require.NoError(t, bucket.Put([]byte("a"), []byte("1")))
				require.NoError(t, bucket.Put([]byte("b"), []byte("2")))
				require.NoError(t, bucket.Put([]byte("c"), []byte("3")))
				require.NoError(t, bucket.Put([]byte("d"), []byte("4")))
				return nil
			})
			require.NoError(t, err)

			// Test ScanRange(nil, nil) - full scan
			err = db.View(func(tx store.Tx) error {
				bucket, err := tx.Bucket([]byte("test"))
				require.NoError(t, err)
				var i int
				for k, v := range bucket.ScanRange(nil, nil) {
					switch i {
					case 0:
						require.Equal(t, []byte("a"), k)
						require.Equal(t, []byte("1"), v)
					case 1:
						require.Equal(t, []byte("b"), k)
						require.Equal(t, []byte("2"), v)
					case 2:
						require.Equal(t, []byte("c"), k)
						require.Equal(t, []byte("3"), v)
					case 3:
						require.Equal(t, []byte("d"), k)
						require.Equal(t, []byte("4"), v)
					}
					i++
				}
				require.Equal(t, 4, i)
				return nil
			})
			require.NoError(t, err)

			// Test ScanRange(nil, []byte("c")) - from start to before "c"
			err = db.View(func(tx store.Tx) error {
				bucket, err := tx.Bucket([]byte("test"))
				require.NoError(t, err)
				var i int
				for k, v := range bucket.ScanRange(nil, []byte("c")) {
					switch i {
					case 0:
						require.Equal(t, []byte("a"), k)
						require.Equal(t, []byte("1"), v)
					case 1:
						require.Equal(t, []byte("b"), k)
						require.Equal(t, []byte("2"), v)
					}
					i++
				}
				require.Equal(t, 2, i) // Should not see "c" or "d"
				return nil
			})
			require.NoError(t, err)

			// Test ScanRange([]byte("b"), []byte("d")) - from "b" to before "d"
			err = db.View(func(tx store.Tx) error {
				bucket, err := tx.Bucket([]byte("test"))
				require.NoError(t, err)
				var i int
				for k, v := range bucket.ScanRange([]byte("b"), []byte("d")) {
					switch i {
					case 0:
						require.Equal(t, []byte("b"), k)
						require.Equal(t, []byte("2"), v)
					case 1:
						require.Equal(t, []byte("c"), k)
						require.Equal(t, []byte("3"), v)
					}
					i++
				}
				require.Equal(t, 2, i) // Should not see "a" or "d"
				return nil
			})
			require.NoError(t, err)

			// Test ScanRangeReverse(nil, nil) - full reverse scan
			err = db.View(func(tx store.Tx) error {
				bucket, err := tx.Bucket([]byte("test"))
				require.NoError(t, err)
				var i int
				for k, v := range bucket.ScanRangeReverse(nil, nil) {
					switch i {
					case 0:
						require.Equal(t, []byte("d"), k)
						require.Equal(t, []byte("4"), v)
					case 1:
						require.Equal(t, []byte("c"), k)
						require.Equal(t, []byte("3"), v)
					case 2:
						require.Equal(t, []byte("b"), k)
						require.Equal(t, []byte("2"), v)
					case 3:
						require.Equal(t, []byte("a"), k)
						require.Equal(t, []byte("1"), v)
					}
					i++
				}
				require.Equal(t, 4, i)
				return nil
			})
			require.NoError(t, err)

			// Test ScanRangeReverse(nil, []byte("c")) - from start to before "c" in reverse
			err = db.View(func(tx store.Tx) error {
				bucket, err := tx.Bucket([]byte("test"))
				require.NoError(t, err)
				var i int
				for k, v := range bucket.ScanRangeReverse(nil, []byte("c")) {
					switch i {
					case 0:
						require.Equal(t, []byte("b"), k)
						require.Equal(t, []byte("2"), v)
					case 1:
						require.Equal(t, []byte("a"), k)
						require.Equal(t, []byte("1"), v)
					}
					i++
				}
				require.Equal(t, 2, i) // Should not see "c" or "d"
				return nil
			})
			require.NoError(t, err)

			// Test ScanRangeReverse([]byte("b"), []byte("d")) - from "b" to before "d" in reverse
			err = db.View(func(tx store.Tx) error {
				bucket, err := tx.Bucket([]byte("test"))
				require.NoError(t, err)
				var i int
				for k, v := range bucket.ScanRangeReverse([]byte("b"), []byte("d")) {
					switch i {
					case 0:
						require.Equal(t, []byte("c"), k)
						require.Equal(t, []byte("3"), v)
					case 1:
						require.Equal(t, []byte("b"), k)
						require.Equal(t, []byte("2"), v)
					}
					i++
				}
				require.Equal(t, 2, i) // Should not see "a" or "d"
				return nil
			})
			require.NoError(t, err)

			// Test ScanRange([]byte("d"), nil) - from "d" to end
			err = db.View(func(tx store.Tx) error {
				bucket, err := tx.Bucket([]byte("test"))
				require.NoError(t, err)
				var i int
				for k, v := range bucket.ScanRange([]byte("d"), nil) {
					if i == 0 {
						require.Equal(t, []byte("d"), k)
						require.Equal(t, []byte("4"), v)
					}
					i++
				}
				require.Equal(t, 1, i) // Should only see "d"
				return nil
			})
			require.NoError(t, err)

			// Test ScanRange(nil, []byte("a")) - from start to before "a" (empty range)
			err = db.View(func(tx store.Tx) error {
				bucket, err := tx.Bucket([]byte("test"))
				require.NoError(t, err)
				var i int
				for range bucket.ScanRange(nil, []byte("a")) {
					i++
				}
				require.Equal(t, 0, i) // Should be empty
				return nil
			})
			require.NoError(t, err)

			// Test ScanRangeReverse([]byte("d"), nil) - from "d" to end in reverse
			err = db.View(func(tx store.Tx) error {
				bucket, err := tx.Bucket([]byte("test"))
				require.NoError(t, err)
				var i int
				for k, v := range bucket.ScanRangeReverse([]byte("d"), nil) {
					if i == 0 {
						require.Equal(t, []byte("d"), k)
						require.Equal(t, []byte("4"), v)
					}
					i++
				}
				require.Equal(t, 1, i) // Should only see "d"
				return nil
			})
			require.NoError(t, err)

			// Test ScanRangeReverse(nil, []byte("a")) - from start to before "a" in reverse (empty range)
			err = db.View(func(tx store.Tx) error {
				bucket, err := tx.Bucket([]byte("test"))
				require.NoError(t, err)
				var i int
				for range bucket.ScanRangeReverse(nil, []byte("a")) {
					i++
				}
				require.Equal(t, 0, i) // Should be empty
				return nil
			})
			require.NoError(t, err)

			// Test ScanRange([]byte("b"), []byte("b")) - empty range (start == end)
			err = db.View(func(tx store.Tx) error {
				bucket, err := tx.Bucket([]byte("test"))
				require.NoError(t, err)
				var i int
				for range bucket.ScanRange([]byte("b"), []byte("b")) {
					i++
				}
				require.Equal(t, 0, i) // Should be empty
				return nil
			})
			require.NoError(t, err)

			// Test ScanRange([]byte("c"), []byte("b")) - invalid range (start > end)
			err = db.View(func(tx store.Tx) error {
				bucket, err := tx.Bucket([]byte("test"))
				require.NoError(t, err)
				var i int
				for range bucket.ScanRange([]byte("c"), []byte("b")) {
					i++
				}
				require.Equal(t, 0, i) // Should be empty
				return nil
			})
			require.NoError(t, err)

			// Test Scan([]byte{}) - empty prefix scan (should match all keys)
			err = db.View(func(tx store.Tx) error {
				bucket, err := tx.Bucket([]byte("test"))
				require.NoError(t, err)
				var i int
				for k, v := range bucket.Scan([]byte{}) {
					switch i {
					case 0:
						require.Equal(t, []byte("a"), k)
						require.Equal(t, []byte("1"), v)
					case 1:
						require.Equal(t, []byte("b"), k)
						require.Equal(t, []byte("2"), v)
					case 2:
						require.Equal(t, []byte("c"), k)
						require.Equal(t, []byte("3"), v)
					case 3:
						require.Equal(t, []byte("d"), k)
						require.Equal(t, []byte("4"), v)
					}
					i++
				}
				require.Equal(t, 4, i) // Should see all keys
				return nil
			})
			require.NoError(t, err)
		})
	}
}

// TestCursor_DeleteDuringIteration tests deleting during cursor iteration
func TestCursor_DeleteDuringIteration(t *testing.T) {
	for _, tt := range testedDBs {
		t.Run(tt, func(t *testing.T) {
			db := openDB(t, tt)
			defer closeAndCleanup(t, db)

			err := db.Update(func(tx store.Tx) error {
				bucket, err := tx.CreateBucket([]byte("test"))
				require.NoError(t, err)
				require.NoError(t, bucket.Put([]byte("a"), []byte("1")))
				require.NoError(t, bucket.Put([]byte("b"), []byte("2")))
				require.NoError(t, bucket.Put([]byte("c"), []byte("3")))
				return nil
			})
			require.NoError(t, err)

			err = db.Update(func(tx store.Tx) error {
				bucket, err := tx.Bucket([]byte("test"))
				require.NoError(t, err)

				var i int
				for k := range bucket.Scan(nil) {
					switch i {
					case 0:
						require.Equal(t, []byte("a"), k)
						require.NoError(t, bucket.Delete(k)) // delete a
					case 1:
						require.Equal(t, []byte("b"), k)
					case 2:
						require.Equal(t, []byte("c"), k)
					}
					i++
				}
				require.Equal(t, 3, i)
				return nil
			})
			require.NoError(t, err)

			// Verify
			err = db.View(func(tx store.Tx) error {
				bucket, err := tx.Bucket([]byte("test"))
				require.NoError(t, err)
				val, err := bucket.Get([]byte("a"))
				require.Error(t, err)
				require.Nil(t, val)
				val, err = bucket.Get([]byte("b"))
				require.NoError(t, err)
				require.Equal(t, []byte("2"), val)
				val, err = bucket.Get([]byte("c"))
				require.NoError(t, err)
				require.Equal(t, []byte("3"), val)
				return nil
			})
			require.NoError(t, err)
		})
	}
}

// TestEdgeCases tests various edge cases
func TestEdgeCases(t *testing.T) {
	for _, tt := range testedDBs {
		t.Run(tt, func(t *testing.T) {
			db := openDB(t, tt)
			defer closeAndCleanup(t, db)

			// Non-existent bucket
			err := db.View(func(tx store.Tx) error {
				bucket, err := tx.Bucket([]byte("nonexist"))
				require.Error(t, err)
				require.Nil(t, bucket)
				return nil
			})
			require.NoError(t, err)

			// Empty bucket
			err = db.Update(func(tx store.Tx) error {
				_, err := tx.CreateBucket([]byte("empty"))
				require.NoError(t, err)
				return nil
			})
			require.NoError(t, err)

			// Non-existent key
			err = db.View(func(tx store.Tx) error {
				bucket, err := tx.Bucket([]byte("empty"))
				require.NoError(t, err)
				require.NotNil(t, bucket)
				val, err := bucket.Get([]byte("nonexist"))
				require.Nil(t, val)
				require.Error(t, err)
				return nil
			})
			require.NoError(t, err)

			err = db.View(func(tx store.Tx) error {
				bucket, err := tx.Bucket([]byte("empty"))
				require.NoError(t, err)
				for range bucket.Scan(nil) {
					require.False(t, true)
				}
				return nil
			})
			require.NoError(t, err)
		})
	}
}

// TestDB_ConcurrentCloseAndBegin tests that concurrent close and begin operations
// are synchronized correctly, ensuring no deadlocks and proper error handling.
func TestDB_ConcurrentCloseAndBegin(t *testing.T) {
	for _, typ := range testedDBs {
		t.Run(typ, func(t *testing.T) {
			db := openDB(t, typ)

			var wg sync.WaitGroup
			var beginErrors []error
			var mu sync.Mutex

			// Start multiple goroutines attempting to begin read transactions
			for range 10 {
				wg.Go(func() {
					tx, err := db.Begin()
					if err != nil {
						mu.Lock()
						beginErrors = append(beginErrors, err)
						mu.Unlock()
						return
					}
					// Perform minimal operation and rollback
					tx.Rollback()
				})
			}

			// Close the database concurrently
			closeErr := db.Close()
			wg.Wait()

			// Close should succeed as it waits for active transactions
			require.NoError(t, closeErr)

			// Any begin errors should be ErrDatabaseClosed
			for _, err := range beginErrors {
				require.Equal(t, store.ErrDatabaseClosed, err)
			}
		})
	}
}

// TestBucket_SearchGELE tests SearchGE and SearchLE operations
func TestBucket_SearchGELE(t *testing.T) {
	for _, tt := range testedDBs {
		t.Run(tt, func(t *testing.T) {
			db := openDB(t, tt)
			defer closeAndCleanup(t, db)

			// Insert test data
			err := db.Update(func(tx store.Tx) error {
				bucket, err := tx.CreateBucket([]byte("test"))
				require.NoError(t, err)
				require.NoError(t, bucket.Put([]byte("b"), []byte("2")))
				require.NoError(t, bucket.Put([]byte("d"), []byte("4")))
				require.NoError(t, bucket.Put([]byte("f"), []byte("6")))
				return nil
			})
			require.NoError(t, err)

			// Test SearchGE in read-only tx
			err = db.View(func(tx store.Tx) error {
				bucket, err := tx.Bucket([]byte("test"))
				require.NoError(t, err)

				// Exact match
				k, v, err := bucket.SearchGE([]byte("b"))
				require.NoError(t, err)
				require.Equal(t, []byte("b"), k)
				require.Equal(t, []byte("2"), v)

				// Between b and d -> d
				k, v, err = bucket.SearchGE([]byte("c"))
				require.NoError(t, err)
				require.Equal(t, []byte("d"), k)
				require.Equal(t, []byte("4"), v)

				// Larger than all
				_, _, err = bucket.SearchGE([]byte("g"))
				require.ErrorIs(t, err, store.ErrKeyNotFound)

				// Empty key (first)
				k, v, err = bucket.SearchGE(nil)
				require.NoError(t, err)
				require.Equal(t, []byte("b"), k)
				require.Equal(t, []byte("2"), v)

				return nil
			})
			require.NoError(t, err)

			// Test SearchLE in read-only tx
			err = db.View(func(tx store.Tx) error {
				bucket, err := tx.Bucket([]byte("test"))
				require.NoError(t, err)

				// Exact match
				k, v, err := bucket.SearchLE([]byte("d"))
				require.NoError(t, err)
				require.Equal(t, []byte("d"), k)
				require.Equal(t, []byte("4"), v)

				// Between b and d -> b
				k, v, err = bucket.SearchLE([]byte("c"))
				require.NoError(t, err)
				require.Equal(t, []byte("b"), k)
				require.Equal(t, []byte("2"), v)

				// Smaller than all
				_, _, err = bucket.SearchLE([]byte("a"))
				require.ErrorIs(t, err, store.ErrKeyNotFound)

				// Large key (last)
				k, v, err = bucket.SearchLE([]byte("zz"))
				require.NoError(t, err)
				require.Equal(t, []byte("f"), k)
				require.Equal(t, []byte("6"), v)

				return nil
			})
			require.NoError(t, err)
		})
	}
}
