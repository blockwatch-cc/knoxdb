// Copyright (c) 2025 Blockwatch Data Inc.
// Author: abdul@blockwatch.cc

package fsst

// Symbol that can be put in a queue, ordered on gain
type QSymbol struct {
	symbol *Symbol
	gain   uint64 // mutable because gain value should be ignored in find() on unordered_set of QSymbols
}

func (q QSymbol) Equal(o QSymbol) bool {
	return q.symbol.val.Uint64() == o.symbol.val.Uint64() && q.symbol.Len() == o.symbol.Len()
}

func (q QSymbol) Hash() uint64 {
	k := q.symbol.val.Uint64()
	var m uint64 = 0xc6a4a7935bd1e995
	var r uint64 = 47
	h := 0x8445d61a4e774912 ^ (8 * m)
	k *= m
	k ^= k >> r
	k *= m
	h ^= k
	h *= m
	h ^= h >> r
	h *= m
	h ^= h >> r
	return h
}

type QSymbolPriorityQueue []QSymbol

func (pq QSymbolPriorityQueue) Len() int { return len(pq) }

func (pq QSymbolPriorityQueue) Less(i, j int) bool {
	// insert candidates into priority queue (by gain)
	cmpGn := func(q1, q2 QSymbol) bool {
		return q1.gain > q2.gain || (q1.gain == q2.gain && q1.symbol.val.Uint64() < q2.symbol.val.Uint64())
	}
	return cmpGn(pq[i], pq[j])
}

func (pq QSymbolPriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
}

func (pq *QSymbolPriorityQueue) Push(x any) {
	*pq = append(*pq, x.(QSymbol))
}

func (pq *QSymbolPriorityQueue) Pop() any {
	old := *pq
	n := len(old)
	item := old[n-1]
	*pq = old[0 : n-1]
	return item
}
