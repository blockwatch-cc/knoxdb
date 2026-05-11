package xroar

var (
	indexNodeSize  = 0
	indexNumKeys   = 1
	indexNodeStart = 2
)

// node stores uint64 keys and the corresponding container offset in the buffer.
// 0th index (indexNodeSize) is used for storing the size of node in bytes.
// 1st index (indexNumKeys) is used for storing the number of keys.
// 2nd index is where we start writing the key-value pairs.
type node []uint64

func keyOffset(i int) int { return indexNodeStart + 2*i }
func valOffset(i int) int { return indexNodeStart + 2*i + 1 }

func (n node) numKeys() int            { return int(n[indexNumKeys]) }
func (n node) size() int               { return int(n[indexNodeSize]) }
func (n node) maxKeys() int            { return (len(n) - indexNodeStart) / 2 }
func (n node) key(i int) uint64        { return n[keyOffset(i)] }
func (n node) val(i int) uint64        { return n[valOffset(i)] }
func (n node) setAt(idx int, k uint64) { n[idx] = k }
func (n node) setNumKeys(num int)      { n[indexNumKeys] = uint64(num) }
func (n node) setNodeSize(sz int)      { n[indexNodeSize] = uint64(sz) }

func (n node) moveRight(lo int) {
	hi := n.numKeys()
	assert(!n.isFull())
	// copy works despite of overlap in src and dst.
	// See https://golang.org/pkg/builtin/#copy
	copy(n[keyOffset(lo+1):keyOffset(hi+1)], n[keyOffset(lo):keyOffset(hi)])
}

// isFull checks that the node is already full.
func (n node) isFull() bool {
	return n.numKeys() == n.maxKeys()
}

// Search returns the index of a smallest key >= k in a node.
func (n node) search(k uint64) int {
	N := n.numKeys()
	lo, hi := 0, N-1
	for lo+16 <= hi {
		mid := lo + (hi-lo)/2
		ki := n.key(mid)
		switch {
		case ki < k:
			lo = mid + 1
		case ki > k:
			hi = mid
			// We should keep it equal, and not -1, because we'll take the first greater entry.
		default:
			return mid
		}
	}
	for ; lo <= hi; lo++ {
		ki := n.key(lo)
		if ki >= k {
			return lo
		}
	}
	return N
	// if N < 4 {
	// simd.Search has a bug which causes this to return index 11 when it should be returning index
	// 9.
	// }
	// return int(simd.Search(n[keyOffset(0):keyOffset(N)], k))
}

// getValue returns the value corresponding to the key if found.
func (n node) getValue(k uint64) (uint64, bool) {
	k &= mask // Ensure k has its lowest bits unset.
	idx := n.search(k)
	// key is not found
	if idx >= n.numKeys() {
		return 0, false
	}
	if ki := n.key(idx); ki == k {
		return n.val(idx), true
	}
	return 0, false
}

// set returns true if it added a new key.
func (n node) set(k, v uint64) bool {
	N := n.numKeys()
	idx := n.search(k)
	if idx == N {
		n.setNumKeys(N + 1)
		n.setAt(keyOffset(idx), k)
		n.setAt(valOffset(idx), v)
		return true
	}

	ki := n.key(idx)
	if N == n.maxKeys() {
		// This happens during split of non-root node, when we are updating the child pointer of
		// right node. Hence, the key should already exist.
		assert(ki == k)
	}
	if ki == k {
		n.setAt(valOffset(idx), v)
		return false
	}
	assert(ki > k)
	// Found the first entry which is greater than k. So, we need to fit k
	// just before it. For that, we should move the rest of the data in the
	// node to the right to make space for k.
	n.moveRight(idx)
	n.setNumKeys(N + 1)
	n.setAt(keyOffset(idx), k)
	n.setAt(valOffset(idx), v)
	return true
}

func (n node) updateOffsets(beyond, by uint64, add bool) {
	for i := 0; i < n.numKeys(); i++ {
		if offset := n.val(i); offset > beyond {
			if add {
				n.setAt(valOffset(i), offset+by)
			} else {
				assert(offset >= by)
				n.setAt(valOffset(i), offset-by)
			}
		}
	}
}
