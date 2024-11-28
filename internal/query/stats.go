// Copyright (c) 2023 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package query

import (
	"strconv"
	"strings"
	"time"

	"blockwatch.cc/knoxdb/pkg/slicex"
)

// Stats based on implementations e.g.
// compile_time
// analyze_time
// journal_time
// index_time
// scan_time
// total_time
// index_lookups
// packs_scheduled
// packs_scanned
// rows_matched
// rows_scanned

const (
	TOTAL_TIME_KEY   = "total_time"
	SCAN_TIME_KEY    = "scan_time"
	ROWS_SCANNED_KEY = "rows_scanned"
	ROWS_MATCHED_KEY = "rows_matched"
)

type QueryStats struct {
	sections []string
	runtime  map[string]time.Duration
	counts   map[string]int
	last     time.Time
}

func NewQueryStats() QueryStats {
	return QueryStats{
		sections: make([]string, 0),
		runtime:  make(map[string]time.Duration),
		counts:   make(map[string]int),
		last:     time.Now(),
	}
}

func (s *QueryStats) GetCount(key string) int {
	return s.counts[key]
}

func (s *QueryStats) GetRuntime(key string) time.Duration {
	return s.runtime[key]
}

func (s *QueryStats) Tick(key string) {
	now := time.Now()
	_, ok := s.runtime[key]
	if !ok {
		s.sections = append(s.sections, key)
	}
	s.runtime[key] += now.Sub(s.last)
	s.last = now
}

func (s *QueryStats) Count(key string, num int) {
	_, ok := s.counts[key]
	if !ok {
		s.sections = append(s.sections, key)
	}
	s.counts[key] += num
}

func (s *QueryStats) Finalize() {
	var total time.Duration
	if len(s.runtime) > 0 {
		for _, v := range s.runtime {
			total += v
		}
		s.sections = append(s.sections, TOTAL_TIME_KEY)
		s.runtime[TOTAL_TIME_KEY] = total
	} else {
		s.Tick(TOTAL_TIME_KEY)
	}
}

func (s QueryStats) String() string {
	var b strings.Builder
	for i, n := range s.sections {
		if i > 0 {
			b.WriteByte(' ')
		}
		rt, ok := s.runtime[n]
		if ok {
			b.WriteString(n)
			b.WriteByte('=')
			b.WriteString(rt.String())
			continue
		}
		c, ok := s.counts[n]
		if ok {
			b.WriteString(n)
			b.WriteByte('=')
			b.WriteString(strconv.Itoa(c))
		}
	}
	return b.String()
}

func (s *QueryStats) Merge(x *QueryStats) {
	s.sections = slicex.UniqueStringsStable(append(s.sections, x.sections...))
	for n, v := range x.runtime {
		s.runtime[n] += v
	}
	for n, v := range x.counts {
		s.counts[n] += v
	}
}
