// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package series

import (
	"testing"
	"time"
)

type unitTestCase struct {
	s string
	u TimeUnit
	d time.Duration // duration
	e time.Duration // allowed error (for month/quarter)
}

var unitTestCases = []unitTestCase{
	{"m", TimeUnit{1, 'm'}, time.Minute, 0},
	{"h", TimeUnit{1, 'h'}, time.Hour, 0},
	{"d", TimeUnit{1, 'd'}, 24 * time.Hour, 0},
	{"w", TimeUnit{1, 'w'}, 7 * 24 * time.Hour, 0},
	{"M", TimeUnit{1, 'M'}, 730 * time.Hour, time.Hour},
	{"q", TimeUnit{1, 'q'}, 2190 * time.Hour, time.Hour},
	{"y", TimeUnit{1, 'y'}, 365 * 24 * time.Hour, 0},
	{"2m", TimeUnit{2, 'm'}, 2 * time.Minute, 0},
	{"2h", TimeUnit{2, 'h'}, 2 * time.Hour, 0},
	{"2d", TimeUnit{2, 'd'}, 2 * 24 * time.Hour, 0},
	{"2w", TimeUnit{2, 'w'}, 2 * 7 * 24 * time.Hour, 0},
	{"2M", TimeUnit{2, 'M'}, 2 * 730 * time.Hour, time.Hour},
	{"2q", TimeUnit{2, 'q'}, 2 * 2190 * time.Hour, time.Hour},
	{"2y", TimeUnit{2, 'y'}, 2 * 365 * 24 * time.Hour, 0},
}

func TestParse(t *testing.T) {
	for i, v := range unitTestCases {
		u, err := ParseTimeUnit(v.s)
		if err != nil {
			t.Fatalf("%d: parsing %q: %v", i, v.s, err)
		}
		if got, want := u.Value, v.u.Value; got != want {
			t.Errorf("%d: %q value mismatch: got=%d != want=%d", i, v.s, got, want)
		}
		if got, want := u.Unit, v.u.Unit; got != want {
			t.Errorf("%d: %q unit mismatch: got=%v != want=%v", i, v.s, got, want)
		}
		if got, want := u.Duration(), v.d; got-want > v.e {
			t.Errorf("%d: %q duration mismatch: got=%d != want=%d", i, v.s, got, want)
		}
		if got, want := u.String(), v.s; got != want {
			t.Errorf("%d: %q string mismatch: got=%s != want=%s", i, v.s, got, want)
		}
	}
}

func tm(s string) time.Time {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		panic(err)
	}
	return t.UTC()
}

type inOutTestCase struct {
	in  time.Time
	out time.Time
}

var truncateTestCases = map[string][]inOutTestCase{
	"h": []inOutTestCase{
		{tm("2022-12-01T00:00:00Z"), tm("2022-12-01T00:00:00Z")},
		{tm("2022-12-01T00:01:01Z"), tm("2022-12-01T00:00:00Z")},
	},
	"2h": []inOutTestCase{
		{tm("2022-12-01T00:00:00Z"), tm("2022-12-01T00:00:00Z")},
		{tm("2022-12-01T00:01:01Z"), tm("2022-12-01T00:00:00Z")},
		{tm("2022-12-01T01:01:01Z"), tm("2022-12-01T00:00:00Z")},
		{tm("2022-12-01T02:01:01Z"), tm("2022-12-01T02:00:00Z")},
	},
	"3h": []inOutTestCase{
		{tm("2022-12-01T00:00:00Z"), tm("2022-12-01T00:00:00Z")},
		{tm("2022-12-01T00:01:01Z"), tm("2022-12-01T00:00:00Z")},
		{tm("2022-12-01T01:01:01Z"), tm("2022-12-01T00:00:00Z")},
		{tm("2022-12-01T02:01:01Z"), tm("2022-12-01T00:00:00Z")},
		{tm("2022-12-01T03:01:01Z"), tm("2022-12-01T03:00:00Z")},
	},
	"24h": []inOutTestCase{
		{tm("2022-12-01T00:00:00Z"), tm("2022-12-01T00:00:00Z")},
		{tm("2022-12-01T00:01:01Z"), tm("2022-12-01T00:00:00Z")},
		{tm("2022-12-01T01:01:01Z"), tm("2022-12-01T00:00:00Z")},
	},
	"d": []inOutTestCase{
		{tm("2022-12-01T00:00:00Z"), tm("2022-12-01T00:00:00Z")},
		{tm("2022-12-01T00:01:01Z"), tm("2022-12-01T00:00:00Z")},
		{tm("2022-12-02T00:01:01Z"), tm("2022-12-02T00:00:00Z")},
		{tm("2022-12-03T00:01:01Z"), tm("2022-12-03T00:00:00Z")},
	},
	"2d": []inOutTestCase{
		{tm("2022-12-01T00:00:00Z"), tm("2022-12-01T00:00:00Z")},
		{tm("2022-12-01T00:01:01Z"), tm("2022-12-01T00:00:00Z")},
		{tm("2022-12-02T00:01:01Z"), tm("2022-12-01T00:00:00Z")},
		{tm("2022-12-03T00:01:01Z"), tm("2022-12-03T00:00:00Z")},
	},
	"w": []inOutTestCase{
		{tm("2022-12-01T00:00:00Z"), tm("2022-11-27T00:00:00Z")},
		{tm("2022-12-01T00:01:01Z"), tm("2022-11-27T00:00:00Z")},
		{tm("2022-12-04T00:01:01Z"), tm("2022-12-04T00:00:00Z")},
	},
	"2w": []inOutTestCase{
		{tm("2022-01-01T00:00:00Z"), tm("2021-12-26T00:00:00Z")},
		{tm("2022-12-01T00:01:01Z"), tm("2022-11-27T00:00:00Z")},
		{tm("2022-12-01T00:01:01Z"), tm("2022-11-27T00:00:00Z")},
		{tm("2022-12-04T00:01:01Z"), tm("2022-12-04T00:00:00Z")},
	},
	"M": []inOutTestCase{
		{tm("2022-01-01T00:00:00Z"), tm("2022-01-01T00:00:00Z")},
		{tm("2022-12-01T00:01:01Z"), tm("2022-12-01T00:00:00Z")},
		{tm("2022-12-15T00:01:01Z"), tm("2022-12-01T00:00:00Z")},
	},
	"2M": []inOutTestCase{
		{tm("2022-01-01T00:00:00Z"), tm("2022-01-01T00:00:00Z")},
		{tm("2022-11-01T00:01:01Z"), tm("2022-11-01T00:00:00Z")},
		{tm("2022-10-15T00:01:01Z"), tm("2022-09-01T00:00:00Z")},
	},
	"q": []inOutTestCase{
		{tm("2022-01-01T00:00:00Z"), tm("2022-01-01T00:00:00Z")},
		{tm("2022-11-01T00:01:01Z"), tm("2022-10-01T00:00:00Z")},
	},
	"2q": []inOutTestCase{
		{tm("2022-01-01T00:00:00Z"), tm("2022-01-01T00:00:00Z")},
		{tm("2022-11-01T00:01:01Z"), tm("2022-07-01T00:00:00Z")},
	},
	"y": []inOutTestCase{
		{tm("2022-01-01T00:00:00Z"), tm("2022-01-01T00:00:00Z")},
		{tm("2022-11-01T00:01:01Z"), tm("2022-01-01T00:00:00Z")},
	},
	"2y": []inOutTestCase{
		{tm("2022-01-01T00:00:00Z"), tm("2022-01-01T00:00:00Z")},
		{tm("2021-11-01T00:01:01Z"), tm("2020-01-01T00:00:00Z")},
	},
}

var tf = time.RFC3339

func TestTruncate(t *testing.T) {
	for n, cases := range truncateTestCases {
		u, err := ParseTimeUnit(n)
		if err != nil {
			t.Fatalf("parsing %q: %v", n, err)
		}
		for i, v := range cases {
			if got, want := u.Truncate(v.in), v.out; !got.Equal(want) {
				t.Errorf("%s.%d truncate %s[%s] got=%s want=%s",
					n, i, v.in.Format(tf), n, got.Format(tf), want.Format(tf),
				)
			}
		}
	}
}

var nextTestCases = map[string][]inOutTestCase{
	"h": []inOutTestCase{
		{tm("2022-12-01T00:00:00Z"), tm("2022-12-01T01:00:00Z")},
		{tm("2022-12-01T00:01:01Z"), tm("2022-12-01T01:00:00Z")},
	},
	"2h": []inOutTestCase{
		{tm("2022-12-01T00:00:00Z"), tm("2022-12-01T02:00:00Z")},
		{tm("2022-12-01T00:01:01Z"), tm("2022-12-01T02:00:00Z")},
		{tm("2022-12-01T01:01:01Z"), tm("2022-12-01T02:00:00Z")},
		{tm("2022-12-01T02:01:01Z"), tm("2022-12-01T04:00:00Z")},
	},
	"3h": []inOutTestCase{
		{tm("2022-12-01T00:00:00Z"), tm("2022-12-01T03:00:00Z")},
		{tm("2022-12-01T00:01:01Z"), tm("2022-12-01T03:00:00Z")},
		{tm("2022-12-01T01:01:01Z"), tm("2022-12-01T03:00:00Z")},
		{tm("2022-12-01T02:01:01Z"), tm("2022-12-01T03:00:00Z")},
		{tm("2022-12-01T03:01:01Z"), tm("2022-12-01T06:00:00Z")},
	},
	"d": []inOutTestCase{
		{tm("2022-12-01T00:00:00Z"), tm("2022-12-02T00:00:00Z")},
		{tm("2022-12-01T00:01:01Z"), tm("2022-12-02T00:00:00Z")},
		{tm("2022-12-02T00:01:01Z"), tm("2022-12-03T00:00:00Z")},
		{tm("2022-12-03T00:01:01Z"), tm("2022-12-04T00:00:00Z")},
	},
	"2d": []inOutTestCase{
		{tm("2022-12-01T00:00:00Z"), tm("2022-12-03T00:00:00Z")},
		{tm("2022-12-01T00:01:01Z"), tm("2022-12-03T00:00:00Z")},
		{tm("2022-12-02T00:01:01Z"), tm("2022-12-03T00:00:00Z")},
		{tm("2022-12-03T00:01:01Z"), tm("2022-12-05T00:00:00Z")},
	},
	"w": []inOutTestCase{
		{tm("2022-12-01T00:00:00Z"), tm("2022-12-04T00:00:00Z")},
		{tm("2022-12-01T00:01:01Z"), tm("2022-12-04T00:00:00Z")},
		{tm("2022-12-04T00:01:01Z"), tm("2022-12-11T00:00:00Z")},
	},
	"2w": []inOutTestCase{
		{tm("2022-01-01T00:00:00Z"), tm("2022-01-09T00:00:00Z")},
		{tm("2022-12-01T00:01:01Z"), tm("2022-12-11T00:00:00Z")},
		{tm("2022-12-01T00:01:01Z"), tm("2022-12-11T00:00:00Z")},
		{tm("2022-12-04T00:01:01Z"), tm("2022-12-18T00:00:00Z")},
	},
	"M": []inOutTestCase{
		{tm("2022-01-01T00:00:00Z"), tm("2022-02-01T00:00:00Z")},
		{tm("2022-12-01T00:01:01Z"), tm("2023-01-01T00:00:00Z")},
		{tm("2022-12-15T00:01:01Z"), tm("2023-01-01T00:00:00Z")},
	},
	"2M": []inOutTestCase{
		{tm("2022-01-01T00:00:00Z"), tm("2022-03-01T00:00:00Z")},
		{tm("2022-11-01T00:01:01Z"), tm("2023-01-01T00:00:00Z")},
		{tm("2022-10-15T00:01:01Z"), tm("2022-11-01T00:00:00Z")},
	},
	"q": []inOutTestCase{
		{tm("2022-01-01T00:00:00Z"), tm("2022-04-01T00:00:00Z")},
		{tm("2022-11-01T00:01:01Z"), tm("2023-01-01T00:00:00Z")},
	},
	"2q": []inOutTestCase{
		{tm("2022-01-01T00:00:00Z"), tm("2022-07-01T00:00:00Z")},
		{tm("2022-11-01T00:01:01Z"), tm("2023-01-01T00:00:00Z")},
	},
	"y": []inOutTestCase{
		{tm("2022-01-01T00:00:00Z"), tm("2023-01-01T00:00:00Z")},
		{tm("2022-11-01T00:01:01Z"), tm("2023-01-01T00:00:00Z")},
	},
	"2y": []inOutTestCase{
		{tm("2022-01-01T00:00:00Z"), tm("2024-01-01T00:00:00Z")},
		{tm("2021-11-01T00:01:01Z"), tm("2022-01-01T00:00:00Z")},
	},
}

func TestNext(t *testing.T) {
	for n, cases := range nextTestCases {
		u, err := ParseTimeUnit(n)
		if err != nil {
			t.Fatalf("parsing %q: %v", n, err)
		}
		for i, v := range cases {
			if got, want := u.Next(v.in, 1), v.out; !got.Equal(want) {
				t.Errorf("%s.%d next %s[%s] got=%s want=%s",
					n, i, v.in.Format(tf), n, got.Format(tf), want.Format(tf),
				)
			}
		}
	}
}

type fromToTestCase struct {
	from  time.Time
	to    time.Time
	steps int
}

var stepTestCases = map[string][]fromToTestCase{
	"h": []fromToTestCase{
		{tm("2022-12-01T00:00:00Z"), tm("2022-12-01T00:00:00Z"), 0},
		{tm("2022-12-01T00:00:00Z"), tm("2022-12-01T01:00:00Z"), 1},
		{tm("2022-12-01T00:00:00Z"), tm("2022-12-01T02:00:00Z"), 2},
		{tm("2022-12-01T00:00:00Z"), tm("2022-12-01T12:00:00Z"), 10}, // limit
	},
	"2h": []fromToTestCase{
		{tm("2022-12-01T00:00:00Z"), tm("2022-12-01T00:00:00Z"), 0},
		{tm("2022-12-01T00:00:00Z"), tm("2022-12-01T02:00:00Z"), 1},
		{tm("2022-12-01T00:00:00Z"), tm("2022-12-01T06:00:00Z"), 3},
	},
	"d": []fromToTestCase{
		{tm("2022-12-01T00:00:00Z"), tm("2022-12-01T00:00:00Z"), 0},
		{tm("2022-12-01T00:00:00Z"), tm("2022-12-02T00:00:00Z"), 1},
		{tm("2022-12-01T00:00:00Z"), tm("2022-12-03T00:00:00Z"), 2},
	},
	"2d": []fromToTestCase{
		{tm("2022-12-01T00:00:00Z"), tm("2022-12-01T00:00:00Z"), 0},
		{tm("2022-12-01T00:00:00Z"), tm("2022-12-03T00:00:00Z"), 1},
		{tm("2022-12-01T00:00:00Z"), tm("2022-12-07T00:00:00Z"), 3},
	},
	// rest is equivalent, if Next() works, this works too
}

func TestSteps(t *testing.T) {
	for n, cases := range stepTestCases {
		u, err := ParseTimeUnit(n)
		if err != nil {
			t.Fatalf("parsing %q: %v", n, err)
		}
		for i, v := range cases {
			steps := u.Steps(v.from, v.to, 10)
			if got, want := len(steps), v.steps; got != want {
				t.Errorf("%d %s num steps %s--%s got=%d want=%d", i, n, v.from.Format(tf), v.to.Format(tf), got, want)
			}
			last := v.from
			for _, s := range steps {
				if got, want := s.Sub(last), u.Duration(); got != want {
					t.Errorf("%d %s steps dur %s--%s got=%s want=%s", i, n, v.from.Format(tf), v.to.Format(tf), got, want)
				}
				last = s
			}
		}
	}
}
