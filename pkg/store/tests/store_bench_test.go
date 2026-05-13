// Copyright (c) 2026 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package store_test

import (
	"fmt"
	"math/rand/v2"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"

	"blockwatch.cc/knoxdb/pkg/store"
	_ "blockwatch.cc/knoxdb/pkg/store/boltdb"
	_ "blockwatch.cc/knoxdb/pkg/store/memdb"
)

func openNativeBoltDB(t testing.TB) *bolt.DB {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.db")
	db, err := bolt.Open(path, 0600, &bolt.Options{NoSync: true})
	require.NoError(t, err)
	return db
}

// BenchmarkPut benchmarks Put operations
func BenchmarkPut(b *testing.B) {
	for _, bm := range testedDBs {
		b.Run(bm, func(b *testing.B) {
			db := openDB(b, bm)
			defer closeAndCleanup(b, db)

			// Setup bucket
			err := db.Update(func(tx store.Tx) error {
				_, err := tx.CreateBucket([]byte("bench"))
				require.NoError(b, err)
				return nil
			})
			require.NoError(b, err)

			b.ResetTimer()
			b.ReportAllocs()
			var i int
			for b.Loop() {
				key := fmt.Appendf(nil, "key%d", i)
				value := fmt.Appendf(nil, "value%d", i)
				err := db.Update(func(tx store.Tx) error {
					bucket, err := tx.Bucket([]byte("bench"))
					require.NoError(b, err)
					require.NotNil(b, bucket)
					return bucket.Put(key, value)
				})
				require.NoError(b, err)
				i++
			}
		})
	}

	// BenchmarkBoltNativePut benchmarks native BoltDB Put operations
	b.Run("bbolt", func(b *testing.B) {
		db := openNativeBoltDB(b)
		defer db.Close()

		// Setup bucket
		err := db.Update(func(tx *bolt.Tx) error {
			_, err := tx.CreateBucket([]byte("bench"))
			require.NoError(b, err)
			return nil
		})
		require.NoError(b, err)

		b.ResetTimer()
		b.ReportAllocs()
		var i int
		for b.Loop() {
			key := fmt.Appendf(nil, "key%d", i)
			value := fmt.Appendf(nil, "value%d", i)
			err := db.Update(func(tx *bolt.Tx) error {
				bucket := tx.Bucket([]byte("bench"))
				require.NotNil(b, bucket)
				return bucket.Put(key, value)
			})
			require.NoError(b, err)
			i++
		}
	})
}

// BenchmarkGet benchmarks Get operations
func BenchmarkGet(b *testing.B) {
	for _, bm := range testedDBs {
		b.Run(bm, func(b *testing.B) {
			db := openDB(b, bm)
			defer closeAndCleanup(b, db)

			// Setup data
			err := db.Update(func(tx store.Tx) error {
				bucket, err := tx.CreateBucket([]byte("bench"))
				require.NoError(b, err)
				for i := range b.N {
					key := fmt.Appendf(nil, "key%d", i)
					value := fmt.Appendf(nil, "value%d", i)
					require.NoError(b, bucket.Put(key, value))
				}
				return nil
			})
			require.NoError(b, err)

			b.ResetTimer()
			b.ReportAllocs()

			i := 0
			n := b.N
			for b.Loop() {
				key := fmt.Appendf(nil, "key%d", i%n)
				err := db.View(func(tx store.Tx) error {
					bucket, err := tx.Bucket([]byte("bench"))
					require.NoError(b, err)
					require.NotNil(b, bucket)
					val, err := bucket.Get(key)
					require.NoError(b, err)
					require.NotNil(b, val)
					return nil
				})
				require.NoError(b, err)
				i++
			}
		})
	}

	// BenchmarkBoltNativeGet benchmarks native BoltDB Get operations
	b.Run("bbolt", func(b *testing.B) {
		db := openNativeBoltDB(b)
		defer db.Close()

		// Setup data
		err := db.Update(func(tx *bolt.Tx) error {
			bucket, err := tx.CreateBucketIfNotExists([]byte("bench"))
			require.NoError(b, err)
			for i := range b.N {
				key := fmt.Appendf(nil, "key%d", i)
				value := fmt.Appendf(nil, "value%d", i)
				require.NoError(b, bucket.Put(key, value))
			}
			return nil
		})
		require.NoError(b, err)

		b.ResetTimer()
		b.ReportAllocs()

		i := 0
		n := b.N
		for b.Loop() {
			key := fmt.Appendf(nil, "key%d", i%n)
			err := db.View(func(tx *bolt.Tx) error {
				bucket := tx.Bucket([]byte("bench"))
				require.NotNil(b, bucket)
				val := bucket.Get(key)
				require.NotNil(b, val)
				return nil
			})
			require.NoError(b, err)
			i++
		}
	})
}

// BenchmarkScan benchmarks sequence iteration
func BenchmarkScan(b *testing.B) {
	for _, bm := range testedDBs {
		b.Run(bm, func(b *testing.B) {
			db := openDB(b, bm)
			defer closeAndCleanup(b, db)

			// Setup data
			err := db.Update(func(tx store.Tx) error {
				bucket, err := tx.CreateBucket([]byte("bench"))
				require.NoError(b, err)
				for i := range 1000 { // fixed size for iteration
					key := fmt.Appendf(nil, "key%04d", i)
					value := fmt.Appendf(nil, "value%d", i)
					require.NoError(b, bucket.Put(key, value))
				}
				return nil
			})
			require.NoError(b, err)

			b.ResetTimer()
			b.ReportAllocs()
			totalKeys := 0
			for b.Loop() {
				err := db.View(func(tx store.Tx) error {
					bucket, err := tx.Bucket([]byte("bench"))
					if err != nil {
						return err
					}
					for k, v := range bucket.Scan(nil) {
						_ = k
						_ = v
						totalKeys++
					}
					return nil
				})
				require.NoError(b, err)
			}
			// Report time per visited key
			if totalKeys > 0 {
				nsPerKey := float64(b.Elapsed().Nanoseconds()) / float64(totalKeys)
				b.ReportMetric(nsPerKey, "ns/key")
			}
		})
	}

	b.Run("bbolt", func(b *testing.B) {
		db := openNativeBoltDB(b)
		defer db.Close()

		// Setup data
		err := db.Update(func(tx *bolt.Tx) error {
			bucket, err := tx.CreateBucketIfNotExists([]byte("bench"))
			require.NoError(b, err)
			for i := range 1000 {
				key := fmt.Appendf(nil, "key%d", i)
				value := fmt.Appendf(nil, "value%d", i)
				require.NoError(b, bucket.Put(key, value))
			}
			return nil
		})
		require.NoError(b, err)

		b.ResetTimer()
		b.ReportAllocs()
		totalKeys := 0
		for b.Loop() {
			err := db.View(func(tx *bolt.Tx) error {
				bucket := tx.Bucket([]byte("bench"))
				require.NotNil(b, bucket)
				cursor := bucket.Cursor()
				for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
					_ = k
					_ = v
					totalKeys++
				}
				return nil
			})
			require.NoError(b, err)
		}
		// Report time per visited key
		if totalKeys > 0 {
			nsPerKey := float64(b.Elapsed().Nanoseconds()) / float64(totalKeys)
			b.ReportMetric(nsPerKey, "ns/key")
		}
	})
}

// BenchmarkPutMany benchmarks Put batch operations
func BenchmarkPutMany(b *testing.B) {
	for _, bm := range testedDBs {
		b.Run(bm, func(b *testing.B) {
			db := openDB(b, bm)
			defer closeAndCleanup(b, db)

			b.ResetTimer()
			b.ReportAllocs()
			totalKeys := 0
			for b.Loop() {
				err := db.Update(func(tx store.Tx) error {
					bucket, err := tx.CreateBucket([]byte("bench"))
					if err != nil {
						return err
					}
					var value [1 << 10]byte
					for range 5_000 {
						i := rand.IntN(1000000)
						key := fmt.Appendf(nil, "key%x", i)
						if err := bucket.Put(key, value[:]); err != nil {
							return err
						}
						totalKeys++
					}
					return nil
				})
				require.NoError(b, err)
			}
			// Report time per visited key
			if totalKeys > 0 {
				nsPerKey := float64(b.Elapsed().Nanoseconds()) / float64(totalKeys)
				b.ReportMetric(nsPerKey, "ns/key")
			}
		})
	}

	// BenchmarkBoltNativePut benchmarks native BoltDB Put operations
	b.Run("bbolt", func(b *testing.B) {
		db := openNativeBoltDB(b)
		defer db.Close()

		b.ResetTimer()
		b.ReportAllocs()
		totalKeys := 0
		for b.Loop() {
			err := db.Update(func(tx *bolt.Tx) error {
				bucket, err := tx.CreateBucketIfNotExists([]byte("bench"))
				if err != nil {
					return err
				}
				var value [1 << 10]byte
				for range 5_000 {
					i := rand.IntN(1000000)
					key := fmt.Appendf(nil, "key%x", i)
					// value := fmt.Appendf(nil, "value%d", i)
					if err := bucket.Put(key, value[:]); err != nil {
						return err
					}
					totalKeys++
				}
				return nil
			})
			require.NoError(b, err)
		}
		// Report time per visited key
		if totalKeys > 0 {
			nsPerKey := float64(b.Elapsed().Nanoseconds()) / float64(totalKeys)
			b.ReportMetric(nsPerKey, "ns/key")
		}
	})
}
