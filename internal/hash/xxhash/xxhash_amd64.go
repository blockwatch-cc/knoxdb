//go:build !appengine && gc && !purego
// +build !appengine,gc,!purego

package xxhash

// Sum64 computes the 64-bit xxHash digest of b.
//
//go:noescape
func Sum64(b []byte) uint64

//go:noescape
func writeBlocks(d *Digest, b []byte) int

// referenced in asm code
var (
	prime2v = prime2
	prime3v = prime3
	prime4v = prime4
	prime5v = prime5
)
