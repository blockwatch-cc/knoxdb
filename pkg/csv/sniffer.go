// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package csv

import (
	"bufio"
	"bytes"
	"io"
	"math/rand"
	"strconv"
	"strings"
	"time"
	"unicode"

	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/num"
	"blockwatch.cc/knoxdb/pkg/schema"
	"blockwatch.cc/knoxdb/pkg/stringx"
	"blockwatch.cc/knoxdb/pkg/util"
)

const (
	DefaultSampleSize   = 1000
	DefaultSampleBuffer = 64 << 10 // 64k
)

var (
	delims = ",;|\t"
	NULL   = "null"
	null   = []byte(NULL)
	nan1   = []byte("nan")
	nan2   = []byte("NaN")
	inf1   = []byte("inf")
	inf2   = []byte("Inf")
	true1  = []byte("true")
	true2  = []byte("TRUE")
	false1 = []byte("false")
	false2 = []byte("FALSE")
)

type SnifferResult struct {
	NumFields   int    // most likely field count (using the detected separator)
	Sep         rune   // most likely separator character [, ; | \t]
	NeedsTrim   bool   // whitespace around separators
	HasHeader   bool   // header line(s) present
	HasComments bool   // comment line(s) present
	HasQuotes   bool   // quotes present in some fields
	HasEscape   bool   // escaped quotes present in quoted fields
	HasNull     bool   // some fields are nullable
	HasTime     bool   // time field present
	TimeFormat  string // detected time format
	DateFormat  string // detected date format
}

type Sniffer struct {
	rd       io.Reader           // input file (can read, maybe seek)
	sample   *stringx.StringPool // raw sample lines
	n        int                 // max samples (-1: read entire file)
	res      SnifferResult       // result
	buf      []byte              // reusable scan buffer
	cnt      map[rune][2]int     // separator min/max counts
	head     []string            // header/field names (if present)
	fields   []field             // detected field properties
	userTime string              // user-defined time format (optional)
	userDate string              // user-defined date format (optional)
}

// Creates a new sniffer instance to analyze a CSV stream from r. If r is seekable
// sniffer will sample up to n random lines, if not n lines from the start of the
// stream are used. If n is zero a default of 1000 lines are sampled. If n is < 0
// the entire CSV content is sampled (note all sampled lines are loaded into memory
// so using -1 on large files can lead to out of memory situations). Regardless
// of mode, leading comments, CSV header and first body line are always sampled.
func NewSniffer(r io.Reader, n int) *Sniffer {
	if n == 0 {
		n = DefaultSampleSize
	}

	// init separator counting
	counts := make(map[rune][2]int)
	for _, v := range delims {
		counts[v] = [2]int{1<<32 - 1, 0}
	}

	return &Sniffer{
		rd:     r,
		sample: stringx.NewStringPool(max(n, DefaultSampleSize)), // n may be -1
		n:      n,
		buf:    make([]byte, 0, DefaultSampleBuffer),
		cnt:    counts,
	}
}

func (s *Sniffer) WithTimeFormat(f string) *Sniffer {
	s.userTime = f
	return s
}

func (s *Sniffer) WithDateFormat(f string) *Sniffer {
	s.userDate = f
	return s
}

func (s *Sniffer) WithBufferSize(sz int) *Sniffer {
	if sz <= 0 {
		return s
	}
	s.buf = make([]byte, 0, sz)
	return s
}

func (s *Sniffer) Result() SnifferResult {
	return s.res
}

func (s *Sniffer) Schema() *schema.Schema {
	// construct a schema from discovered fields
	b := schema.NewBuilder()
	for i, f := range s.fields {
		b.AddField(s.head[i], f.Type())
		if f.is(fFixed) && (f.Type() == types.FieldTypeBytes || f.Type() == types.FieldTypeString) {
			l := uint16(f.len)
			if f.is(fHex) {
				l /= 2
			}
			b.SetFieldOpts(schema.Fixed(l))
		}
		if f.isDateTime() {
			b.SetFieldOpts(schema.Scale(f.scale))
		}
		if f.isDecimal() {
			b.SetFieldOpts(schema.Scale(f.dot - 1))
		}
	}
	return b.Finalize().Schema()
}

func (s *Sniffer) NewDecoder(r io.Reader) *Decoder {
	return NewDecoder(s.Schema(), r).
		WithSeparator(s.res.Sep).
		WithHeader(s.res.HasHeader).
		WithTrim(s.res.NeedsTrim).
		WithTimeFormat(s.res.TimeFormat).
		WithDateFormat(s.res.DateFormat).
		WithStrictSchema(true).
		WithBuffer(s.buf)
}

func (s *Sniffer) Sniff() error {
	// Stage 1: sample and count each separator's occurences
	if err := s.makeSample(); err != nil {
		return err
	}

	// Stage 2: detect separator & num fields
	s.analyzeSeparator()

	// Stage 3: detect text structure (escape characters, whitespace, quotes, etc)
	s.analyzeLines()

	// Stage 4: analyze header
	s.analyzeHeader()

	// Stage 5: detect field types
	s.analyzeTypes()

	// free sample pool
	s.sample.Close()
	s.sample = stringx.NewStringPool(max(s.n, DefaultSampleSize))

	return nil
}

func (s *Sniffer) analyzeLines() {
	var (
		sep         = []byte{(byte(s.res.Sep))}
		haveComment bool
		haveQuotes  bool
		haveEscape  bool
		haveNull    bool
		haveSpace   bool
	)
	for _, line := range s.sample.Iterator() {
		// skip comment lines
		if line[0] == byte(Comment) {
			haveComment = true
			continue
		}

		// count quotes (only if none were found yet)
		if !haveQuotes {
			haveQuotes = bytes.Count(line, []byte{'"'}) > 0
		}

		// count escaped quotes (note this misdetects on empty quoted fields)
		if haveQuotes && !haveEscape {
			haveEscape = bytes.Count(line, DoubleQuote) > 0
		}

		// split and scan for trimmable whitespace
		var (
			buf = line
			tok []byte
			ok  = true
		)
		if haveQuotes {
			// be careful with quoted fields
			for ok && !haveNull && !haveSpace {
				tok, buf, ok = ParseAndCut(buf, byte(Quote))
				if len(tok) == 0 {
					continue
				}
				if !haveNull {
					haveNull = bytes.Equal(tok, null)
				}
				if !haveSpace {
					haveSpace = unicode.IsSpace(rune(tok[0])) || unicode.IsSpace(rune(tok[len(tok)-1]))
				}
			}
		} else {
			// definitely no quoted fields
			for ok && !haveNull && !haveSpace {
				tok, buf, ok = bytes.Cut(buf, sep)
				if len(tok) == 0 {
					continue
				}
				if !haveNull {
					haveNull = bytes.Equal(tok, null)
				}
				if !haveSpace {
					haveSpace = unicode.IsSpace(rune(tok[0])) || unicode.IsSpace(rune(tok[len(tok)-1]))
				}
			}
		}

		// stop early when all features are true
		if haveQuotes && haveEscape && haveSpace && haveNull {
			break
		}
	}

	s.res.NeedsTrim = haveSpace
	s.res.HasComments = haveComment
	s.res.HasQuotes = haveQuotes
	s.res.HasEscape = haveEscape
	s.res.HasNull = haveNull
}

type fieldFlag int

// string features used to detect type
const (
	// prefix analysis
	fSign   fieldFlag = 1 << iota // 0 leading sign (dash) (int, decimal, float)
	fNum                          // 1 leading number 0-9
	fBool                         // 2 true or false
	fNull                         // 3 null values present
	fEmpty                        // 4 empty fields present
	fZerox                        // 5 0x prefix with hex characters (byte)
	fQuoted                       // 6 quoted string (string)

	// inner characters
	fDecimal // 7 decimal numbers 0-9 (int, uint, decimal)
	fHex     // 8 hex characters [0-9a-fA-F] (byte)
	fOther   // 9 other characters (string, date, time, timestamp)
	fDash    // 10 non-leading dashes (if 36 fixed hex -> [16]byte uuid)
	fDot     // 11 single non-leading decimal dot present 10.23 (decimal or float)
	fExp     // 12 float exponent {e|E}{+|-}, nan, {+|-}inf (float)

	// global features
	fTimestamp // 13 parses as date and time with optional below 1s resolution
	fTime      // 14 parses as time only with optional below 1s resolution
	fDate      // 15 parses as date only
	fFixed     // 16 fixed length across records
)

const defaultFlags = fFixed

var (
	fieldFlagNames = "sign_num_bool_null_empty_0x_quoted_dec_hex_other_dash_dot_exp_ts_time_date_fix"
	fieldFlagOfs   = []int{0, 5, 9, 14, 19, 25, 28, 35, 39, 43, 49, 54, 58, 62, 65, 70, 75, 79}
)

func (f fieldFlag) String() string {
	var b strings.Builder
	var i, n int
	for f > 0 {
		if f&1 > 0 {
			if n > 0 {
				b.WriteByte(',')
			}
			b.WriteString(fieldFlagNames[fieldFlagOfs[i] : fieldFlagOfs[i+1]-1])
			n++
		}
		f >>= 1
		i++
	}
	return b.String()
}

// field features
type field struct {
	len   int       // max field length including quotes but without outer space
	dot   int       // dot position (from right)
	flag  fieldFlag // flags
	tfm   string    // time format
	scale int       // time scale
}

func newField(buf []byte, tfm, dfm string) field {
	f := field{
		flag: defaultFlags,
	}
	f.update(buf, tfm, dfm)
	return f
}

func (f field) Type() types.FieldType {
	switch {
	case f.isBool():
		return types.FieldTypeBoolean
	case f.isSignedInt():
		// i256..i8, big
		switch {
		case f.len > num.MaxInt256Precision:
			return types.FieldTypeBigint // fallback to bigint
		case f.len > num.MaxInt128Precision:
			return types.FieldTypeInt256
		case f.len > num.MaxInt64Precision:
			return types.FieldTypeInt128
		case f.len > num.MaxInt32Precision:
			return types.FieldTypeInt64
		case f.len > num.MaxInt16Precision:
			return types.FieldTypeInt32
		case f.len > num.MaxInt8Precision:
			return types.FieldTypeInt16
		default:
			return types.FieldTypeInt8
		}
	case f.isUnsignedInt():
		// u256..u8
		switch {
		case f.len > num.MaxInt256Precision:
			return types.FieldTypeBigint // fallback to bigint
		case f.len > num.MaxInt128Precision:
			return types.FieldTypeInt256
		case f.len > num.MaxInt64Precision:
			return types.FieldTypeInt128
		case f.len > num.MaxInt32Precision:
			return types.FieldTypeUint64
		case f.len > num.MaxInt16Precision:
			return types.FieldTypeUint32
		case f.len > num.MaxInt8Precision:
			return types.FieldTypeUint16
		default:
			return types.FieldTypeUint8
		}
	case f.isDecimal():
		// d256..d64
		switch {
		case f.len-1 > num.MaxInt256Precision:
			return types.FieldTypeFloat64 // fallback to float
		case f.len-1 > num.MaxInt128Precision:
			return types.FieldTypeDecimal256
		case f.len-1 > num.MaxInt64Precision:
			return types.FieldTypeDecimal128
		case f.len-1 > num.MaxInt32Precision:
			return types.FieldTypeDecimal64
		default:
			return types.FieldTypeDecimal32
		}
	case f.isFloat():
		return types.FieldTypeFloat64
	case f.is(fDate):
		return types.FieldTypeDate
	case f.is(fTime):
		return types.FieldTypeTime
	case f.is(fTimestamp):
		return types.FieldTypeTimestamp
	case f.isBytes():
		return types.FieldTypeBytes
	default:
		// use string as fallback
		return types.FieldTypeString
	}
}

func (f field) is(flag fieldFlag) bool {
	return f.flag&flag > 0
}

func (f field) not(flag fieldFlag) bool {
	return f.flag&flag == 0
}

func (f field) isHeader() bool {
	return f.not(fEmpty) && f.not(fNum) && f.not(fNull)
}

func (f field) isFloat() bool {
	return (f.is(fExp) || (f.is(fDecimal) && f.is(fDot))) &&
		f.not(fQuoted) && f.not(fZerox) && f.not(fHex) && f.not(fOther) && f.not(fDash)
}

func (f field) isDecimal() bool {
	return f.dot > 0 && f.not(fExp) && (f.is(fSign) || f.is(fNum)) && f.is(fDecimal) && f.is(fDot) &&
		f.not(fHex) && f.not(fOther) && f.not(fDash) && f.not(fQuoted)
}

func (f field) isSignedInt() bool {
	return f.is(fSign) && f.is(fDecimal) &&
		f.not(fHex) && f.not(fOther) && f.not(fDash) && f.not(fDot) && f.not(fExp)
}

func (f field) isUnsignedInt() bool {
	return f.not(fSign) && f.is(fNum) &&
		f.not(fHex) && f.not(fOther) && f.not(fDash) && f.not(fDot) && f.not(fExp)
}

func (f field) isBool() bool {
	return f.is(fBool) && f.not(fQuoted) && f.not(fOther) && f.not(fHex) && f.not(fDecimal)
}

// func (f field) isUUID() bool {
// 	return f.len == 36 && f.is(fHex) && f.is(fFixed) && f.is(fDash) &&
// 		f.not(fQuoted) && f.not(fZerox)
// }

func (f field) isBytes() bool {
	return f.len%2 == 0 && f.is(fHex) && f.not(fQuoted) && f.not(fOther) &&
		f.not(fDash) && f.not(fDot) && f.not(fSign)
}

func (f field) maybeDateTime() bool {
	return f.not(fSign) && f.not(fExp) && f.not(fZerox) &&
		(f.is(fQuoted) || f.is(fNum)) && f.is(fDecimal) && f.is(fFixed) &&
		(f.is(fDash) || f.is(fOther))
}

func (f field) isDateTime() bool {
	return f.is(fDate) || f.is(fTime) || f.is(fTimestamp)
}

func (f field) done() bool {
	return f.is(fDate) || f.is(fTime) || f.is(fTimestamp) || f.is(fOther) || f.is(fQuoted)
}

func (f *field) update(buf []byte, tfm, dfm string) {
	// empty and null fields are allowed for fixed length types
	l := len(buf)
	if l == 0 {
		f.flag |= fEmpty
		return
	}
	src := buf

	// analyze first charcter
	switch c := buf[0]; c {
	case '"':
		f.flag |= fQuoted
	case '-':
		f.flag |= fSign
		l-- // don't count optional sign for fixed decimal point numbers
	case '+':
		f.flag |= fSign
		l-- // don't count optional sign for fixed decimal point numbers
	case 'n', 'N':
		if bytes.Equal(buf, null) {
			f.flag |= fNull
			return
		}
		if bytes.Equal(buf, nan1) || bytes.Equal(buf, nan2) {
			f.flag |= fExp
			f.flag &^= fFixed // floats are nof fixed type
			return
		}
		if l == 1 {
			f.flag |= fBool
			f.flag &^= fFixed // bools are nof fixed type
			return
		}
		f.flag |= fOther
	case 'y', 'Y':
		if l == 1 {
			f.flag |= fBool
			f.flag &^= fFixed // bools are nof fixed type
			return
		}
		f.flag |= fOther
	case 't', 'T':
		if bytes.Equal(buf, true1) || bytes.Equal(buf, true2) {
			f.flag |= fBool
			f.flag &^= fFixed // bools are nof fixed type
			return
		}
		f.flag |= fOther
	case 'f', 'F':
		if bytes.Equal(buf, false1) || bytes.Equal(buf, false2) {
			f.flag |= fBool
			f.flag &^= fFixed // bools are nof fixed type
			return
		}
		f.flag |= fHex
	case 'i', 'I':
		if bytes.Equal(buf, inf1) || bytes.Equal(buf, inf2) {
			f.flag |= fExp
			f.flag &^= fFixed // floats are nof fixed type
			return
		}
		f.flag |= fOther
	default:
		switch {
		case c >= '0' && c <= '9':
			if len(buf) > 1 && buf[1] == 'x' {
				f.flag |= fZerox
				buf = buf[1:]
				l -= 2 // don't count 0x prefix into fixed length
			} else {
				f.flag |= fNum
			}
		case (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F'):
			f.flag |= fHex
		default:
			f.flag |= fOther
		}
	}
	buf = buf[1:]

	// analyze remaining characters
	var dotPos, nDots int
	for len(buf) > 0 {
		c := buf[0]
		switch c {
		case '-':
			f.flag |= fDash
		case '.':
			f.flag |= fDot
			dotPos = len(buf)
			nDots++
		case 'e', 'E':
			if len(buf) > 1 && (buf[1] == '+' || buf[1] == '-') {
				f.flag |= fExp
				buf = buf[1:]
			} else {
				f.flag |= fHex
			}
		case 'i', 'I':
			if bytes.Equal(buf, inf1) || bytes.Equal(buf, inf2) {
				f.flag |= fExp
				f.flag &^= fFixed // floats are nof fixed type
				return
			}
			f.flag |= fOther
		default:
			switch {
			case c >= '0' && c <= '9':
				f.flag |= fDecimal
			case (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F'):
				f.flag |= fHex
			default:
				f.flag |= fOther
			}
		}
		buf = buf[1:]
	}

	// constrain hex (even len, no other chars except dash for uuids)
	if f.is(fHex) && (l&1 == 1 || f.is(fOther) || f.is(fDot) || f.is(fQuoted)) {
		f.flag &^= fHex
		f.flag |= fOther
	}

	// accept single dot only
	if f.is(fDot) && nDots > 1 {
		f.flag &^= fDot
		f.flag |= fOther
	}

	// init or update length
	if f.len == 0 {
		f.len = l // init length
		f.dot = dotPos

		// try time parsing (expensive, so only do this for the first non-null non-empty string)
		if f.maybeDateTime() {
			if f.is(fQuoted) {
				src = bytes.Trim(src, `"`)
			}
			flag, format, scale := tryTime(src, tfm, dfm)
			f.flag |= flag
			f.tfm = format
			f.scale = scale
		}
	}
	if f.dot != dotPos {
		f.dot = 0 // reset dotpos when it differs (cannot be fixed decimal type)
	}
	if f.len != l {
		f.flag &^= fFixed     // drop fixed length flag
		f.len = max(f.len, l) // keep max length
	}
}

func tryTime(buf []byte, tfm, dfm string) (fieldFlag, string, int) {
	s := string(buf)
	if tfm != "" {
		if _, err := time.Parse(tfm, s); err == nil {
			return fTimestamp, tfm, 0
		}
	}
	if dfm != "" {
		if _, err := time.Parse(dfm, s); err == nil {
			return fDate, dfm, int(schema.TIME_SCALE_DAY)
		}
	}

	// try knoxdb standard formats
	f, scale, timeOnly, ok := schema.DetectTimeFormat(s)
	if ok {
		if timeOnly {
			return fTime, f, int(scale)
		}
		if scale == schema.TIME_SCALE_DAY {
			return fDate, f, int(scale)
		}
		return fTimestamp, f, int(scale)
	}

	// try other formats
	if f, err := util.DetectTimeFormat(s); err == nil {
		return fTimestamp, f, 0
	}
	return 0, "", 0
}

func (s *Sniffer) analyzeHeader() {
	// analyze the first non comment line (goal: identify header)
	s.head = make([]string, s.res.NumFields)
	for _, line := range s.sample.Iterator() {
		if line[0] == byte(Comment) {
			continue
		}
		// split (handle quotes and trim), extract field names
		s.res.HasHeader = true
		for i, buf := range Split(line, byte(s.res.Sep)) {
			if s.res.NeedsTrim {
				buf = bytes.TrimSpace(buf)
			}
			f := newField(buf, s.userTime, s.userDate)
			if f.isHeader() {
				if f.is(fQuoted) {
					buf = buf[1 : len(buf)-1]
				}
				s.head[i] = SanitizeFieldName(string(buf), i)
			} else {
				// if any field does not fulfil all header requirements
				// we generates field names below
				s.res.HasHeader = false
				break
			}
		}
		break
	}

	// generate field names if we have no header
	if !s.res.HasHeader {
		for i := range s.res.NumFields {
			s.head[i] = "f_" + strconv.Itoa(i)
		}
	}
}

func (s *Sniffer) analyzeTypes() {
	// analyze text properties for each field, aggregate props per column
	skipHeader := s.res.HasHeader
	for _, line := range s.sample.Iterator() {
		// skip comments
		if line[0] == byte(Comment) {
			continue
		}

		// skip header
		if skipHeader {
			skipHeader = false
			continue
		}

		if len(s.fields) == 0 {
			// init fields from first body record
			for _, buf := range Split(line, byte(s.res.Sep)) {
				if s.res.NeedsTrim {
					buf = bytes.TrimSpace(buf)
				}
				s.fields = append(s.fields, newField(buf, s.userTime, s.userDate))
			}
		} else {
			// update with each following record
			for i, buf := range Split(line, byte(s.res.Sep)) {
				if s.fields[i].done() {
					continue
				}
				if s.res.NeedsTrim {
					buf = bytes.TrimSpace(buf)
				}
				s.fields[i].update(buf, s.userTime, s.userDate)
			}
		}
	}

	// edge case when only a header is present
	if len(s.fields) == 0 {
		for range s.res.NumFields {
			s.fields = append(s.fields, newField(nil, "", ""))
		}
	}

	// extract time format
	for _, f := range s.fields {
		if !f.isDateTime() {
			continue
		}
		s.res.HasTime = true
		if f.is(fTime) || f.is(fTimestamp) {
			s.res.TimeFormat = f.tfm
		}
		if f.is(fDate) {
			s.res.DateFormat = f.tfm
		}
		break
	}
}

func (s *Sniffer) analyzeSeparator() {
	// find the most consistently used delimiter and number of fields
	// (min > 0, break ties with max == min)
	var (
		bestSep       rune
		bestNumFields int
	)
	for d, c := range s.cnt {
		if c[0] == 0 {
			continue
		}
		if bestSep == 0 {
			bestSep = d
			bestNumFields = c[1] + 1
			continue
		}
		if c[0] == c[1] {
			bestSep = d
			bestNumFields = c[1] + 1
		}
	}

	s.res.Sep = bestSep
	s.res.NumFields = bestNumFields
}

func (s *Sniffer) makeSample() error {
	rs, isSeeker := s.rd.(io.ReadSeeker)
	if s.n > 0 && isSeeker {
		return s.sampleRandom(rs)
	}
	return s.sampleLinear()
}

func (s *Sniffer) sampleRandom(rs io.ReadSeeker) error {
	// Get file size
	size, err := rs.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}
	if size == 0 {
		return nil
	}
	rs.Seek(0, io.SeekStart)

	rng := rand.New(rand.NewSource(42)) // Use a deterministic seed for reproducibility

	// scan the header lines until we have seen at least three different line starts
	// since there may be comments, empty lines, whitspace lines and a header line
	var (
		n int
		r rune
	)
	scan := bufio.NewScanner(rs)
	scan.Buffer(s.buf[:0], cap(s.buf))
	for scan.Scan() {
		line := scan.Bytes()

		// skip empty lines and lines starting with whitespace
		if len(line) == 0 || line[0] == ' ' {
			continue
		}

		// count unique start characters
		if rune(line[0]) != r {
			n++
			r = rune(line[0])
		}

		// save the line
		s.sample.Append(line)

		// count occurences of each delimiter in non-comment lines
		if line[0] != byte(Comment) {
			for _, v := range delims {
				n := bytes.Count(line, []byte{byte(v)})
				cnt := s.cnt[v]
				cnt[0] = min(cnt[0], n)
				cnt[1] = max(cnt[1], n)
				s.cnt[v] = cnt
			}
		}

		// stop when we've seen enough header variability
		if n > 2 {
			break
		}
	}
	if err := scan.Err(); err != nil {
		return err
	}

	// Sample more lines
	for s.sample.Len() < s.n {
		// Seek to random position
		pos := rng.Int63n(size)
		if _, err := rs.Seek(pos, io.SeekStart); err != nil {
			return err
		}

		// init new scanner
		scan := bufio.NewScanner(rs)
		scan.Buffer(s.buf[:0], cap(s.buf))

		// Move to start of next line (seek may have landed in the middle of a line)
		if !scan.Scan() {
			continue // try again on error
		}

		// Scan the line
		if !scan.Scan() {
			continue // try again on error
		}

		// Store non empty lines
		if line := scan.Bytes(); len(line) > 0 {
			s.sample.Append(line)

			// count occurences of each delimiter in non-comment lines
			if line[0] != byte(Comment) {
				for _, v := range delims {
					n := bytes.Count(line, []byte{byte(v)})
					cnt := s.cnt[v]
					cnt[0] = min(cnt[0], n)
					cnt[1] = max(cnt[1], n)
					s.cnt[v] = cnt
				}
			}
		}
	}

	return nil
}

func (s *Sniffer) sampleLinear() error {
	// init scanner
	scan := bufio.NewScanner(s.rd)
	scan.Buffer(s.buf[:0], cap(s.buf))

	// Read up to n lines, read all if n is -1
	for (s.n < 0 || s.sample.Len() < s.n) && scan.Scan() {
		if line := scan.Bytes(); len(line) > 0 {
			s.sample.Append(line)
			// count occurences of each delimiter in non-comment lines
			if line[0] != byte(Comment) {
				for _, v := range delims {
					n := bytes.Count(line, []byte{byte(v)})
					cnt := s.cnt[v]
					cnt[0] = min(cnt[0], n)
					cnt[1] = max(cnt[1], n)
					s.cnt[v] = cnt
				}
			}
		}
	}

	return scan.Err()
}
