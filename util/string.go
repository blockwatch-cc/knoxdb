// Copyright (c) 2013-2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package util

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"blockwatch.cc/knoxdb/hash"
)

var stringerType = reflect.TypeOf((*fmt.Stringer)(nil)).Elem()

func ToString(t interface{}) string {
	val := reflect.Indirect(reflect.ValueOf(t))
	if !val.IsValid() {
		return ""
	}
	if val.Type().Implements(stringerType) {
		return t.(fmt.Stringer).String()
	}
	if s, err := ToRawString(val.Interface()); err == nil {
		return s
	}
	return fmt.Sprintf("%v", val.Interface())
}

func IsBase64(s string) bool {
	_, err := base64.StdEncoding.DecodeString(s)
	return err == nil
}

func ToRawString(t interface{}) (string, error) {
	val := reflect.Indirect(reflect.ValueOf(t))
	if !val.IsValid() {
		return "", nil
	}
	typ := val.Type()
	switch val.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return strconv.FormatInt(val.Int(), 10), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return strconv.FormatUint(val.Uint(), 10), nil
	case reflect.Float32, reflect.Float64:
		return strconv.FormatFloat(val.Float(), 'g', -1, val.Type().Bits()), nil
	case reflect.String:
		return val.String(), nil
	case reflect.Bool:
		return strconv.FormatBool(val.Bool()), nil
	case reflect.Array:
		if typ.Elem().Kind() != reflect.Uint8 {
			break
		}
		// [...]byte
		var b []byte
		if val.CanAddr() {
			b = val.Slice(0, val.Len()).Bytes()
		} else {
			b = make([]byte, val.Len())
			reflect.Copy(reflect.ValueOf(b), val)
		}
		return hex.EncodeToString(b), nil
	case reflect.Slice:
		if typ.Elem().Kind() != reflect.Uint8 {
			break
		}
		// []byte
		b := val.Bytes()
		return hex.EncodeToString(b), nil
	}
	return "", fmt.Errorf("no method for converting type %s (%v) to string", typ.String(), val.Kind())
}

type StringList []string

func (l StringList) AsInterface() []interface{} {
	il := make([]interface{}, len(l))
	for i, v := range l {
		il[i] = v
	}
	return il
}

func (l StringList) Contains(r string) bool {
	for _, v := range l {
		if v == r {
			return true
		}
	}
	return false
}

func (l *StringList) Add(r string) StringList {
	*l = append(*l, r)
	return *l
}

func (l *StringList) AddFront(r string) StringList {
	*l = append([]string{r}, (*l)...)
	return *l
}

func (l *StringList) AddUnique(r string) StringList {
	if !(*l).Contains(r) {
		l.Add(r)
	}
	return *l
}

func (l *StringList) AddUniqueFront(r string) StringList {
	if !(*l).Contains(r) {
		l.AddFront(r)
	}
	return *l
}

func (l StringList) Index(r string) int {
	for i, v := range l {
		if v == r {
			return i
		}
	}
	return -1
}

func (l StringList) String() string {
	return strings.Join(l, ",")
}

func (l StringList) MarshalText() ([]byte, error) {
	return []byte(l.String()), nil
}

func (l *StringList) UnmarshalText(data []byte) error {
	*l = strings.Split(string(data), ",")
	return nil
}

func NonEmptyString(s ...string) string {
	for _, v := range s {
		if v != "" {
			return v
		}
	}
	return ""
}

// limits the length of string to l UTF8 runes
func LimitString(s string, l int) string {
	c := utf8.RuneCountInString(s)
	if c <= l {
		return s
	}

	c = 0
	var b bytes.Buffer
	for _, runeVal := range s {
		b.WriteRune(runeVal)
		c += 1
		if c >= l {
			break
		}
	}

	return b.String()
}

func LimitStringEllipsis(s string, l int) string {
	c := utf8.RuneCountInString(s)
	if c <= l {
		return s
	}

	c = 0
	var b bytes.Buffer
	for _, runeVal := range s {
		b.WriteRune(runeVal)
		c += 1
		if c >= l-3 {
			break
		}
	}

	return b.String() + "..."
}

func JoinString(sep string, more ...string) string {
	l := make([]string, 0, len(more))
	for _, v := range more {
		if v != "" {
			l = append(l, v)
		}
	}
	return strings.Join(l, sep)
}

func HashString(s string) string {
	h := hash.NewInlineFNV64a()
	h.WriteString(s)
	return hex.EncodeToString(h.Sum())
}

func QuoteString(s string) string {
	return strings.Join([]string{"\"", s, "\""}, "")
}

func JsonString(v interface{}) string {
	b, _ := json.Marshal(v)
	return string(b)
}

func ContainsString(s string, list []string) bool {
	for _, b := range list {
		if b == s {
			return true
		}
	}
	return false
}

func ToCamelCase(src, sep string) string {
	chunks := strings.Split(src, sep)
	for idx, val := range chunks {
		chunks[idx] = strings.Title(val)
	}
	return strings.Join(chunks, "")
}

func FromCamelCase(src, sep string) string {
	var chunks []string
	for idx := 0; idx < len(src); {
		offs := strings.IndexFunc(src[idx+1:], unicode.IsUpper) + 1
		if offs <= 0 {
			offs = len(src) - idx
		}
		chunks = append(chunks, strings.ToLower(src[idx:idx+offs]))
		idx = idx + offs
	}
	return strings.Join(chunks, sep)
}

func EscapeWhitespace(s string) string {
	return strings.Replace(s, " ", "%20", -1)
}

func UnescapeWhitespace(s string) string {
	return strings.Replace(s, "%20", " ", -1)
}

func TrimAllSpace(str string) string {
	return strings.Map(func(r rune) rune {
		if unicode.IsSpace(r) {
			return -1
		}
		return r
	}, str)
}

func UniqueStrings(s []string) []string {
	unique := make(map[string]bool)
	res := make([]string, 0, len(s))
	for _, v := range s {
		h := HashString(v)
		if _, ok := unique[h]; !ok {
			res = append(res, v)
			unique[h] = true
		}
	}
	return res
}

func IsASCII(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] > unicode.MaxASCII {
			return false
		}
	}
	return true
}
