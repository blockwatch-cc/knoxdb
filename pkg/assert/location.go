// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc
//
//go:build with_assert

package assert

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"path"
	"runtime"
	"strings"
	"sync"
)

// stackFrameOffset indicates how many stack frames to unwind
// to read the filename/location/line info for an assertion.
type stackFrameOffset int

// locationInfo represents attributes for each assertion
type locationInfo struct {
	PackageName string `json:"pkg"`
	FuncName    string `json:"func"`
	FileName    string `json:"file"`
	Line        int    `json:"line"`
}

func (l *locationInfo) String() string {
	return fmt.Sprintf("%s/%s %s:%d", l.PackageName, l.FuncName, l.FileName, l.Line)
}

var locationInfoCache sync.Map // map[uint64]*locationInfo

// NewLocationInfo creates a locationInfo from
// the current execution context.
func newLocationInfo(nframes stackFrameOffset) *locationInfo {
	funcname := "*func*"
	pkgname := "*pkg*"
	pc, filename, line, ok := runtime.Caller(int(nframes))
	if !ok {
		filename = "*file*"
		line = 0
	} else {
		if this_func := runtime.FuncForPC(pc); this_func != nil {
			fullname := this_func.Name()
			funcname = path.Ext(fullname)
			pkgname, _ = strings.CutSuffix(fullname, funcname)
			funcname = funcname[1:]
		}
	}
	id := makeLocationId(filename, line)
	if v, ok := locationInfoCache.Load(id); ok {
		return v.(*locationInfo)
	}
	info := &locationInfo{pkgname, funcname, filename, line}
	locationInfoCache.Store(id, info)
	return info
}

func makeLocationId(filename string, line int) uint64 {
	h := fnv.New64()
	h.Write([]byte(filename))
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], uint64(line))
	h.Write(buf[:])
	return h.Sum64()
}
