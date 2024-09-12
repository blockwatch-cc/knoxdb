// Copyright (c) 2023 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package query

import (
	"strconv"
	"strings"
	"time"
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

func (s *QueryStats) Tick(name string) {
	now := time.Now()
	_, ok := s.runtime[name]
	if !ok {
		s.sections = append(s.sections, name)
	}
	s.runtime[name] += now.Sub(s.last)
	s.last = now
}

func (s *QueryStats) Count(name string, num int) {
	_, ok := s.counts[name]
	if !ok {
		s.sections = append(s.sections, name)
	}
	s.counts[name] += num
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
