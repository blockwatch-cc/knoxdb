package vfs

import (
	"fmt"
	"io/fs"
	"strconv"
	"strings"

	"github.com/echa/log"
	experimentalsys "github.com/tetratelabs/wazero/experimental/sys"
	"github.com/tetratelabs/wazero/experimental/sysfs"
	"github.com/tetratelabs/wazero/sys"
)

type dstfs struct {
	internal  experimentalsys.FS // underlying file system to delegate to
	log       log.Logger
	openFiles map[int]string
}

var _ experimentalsys.FS = (*dstfs)(nil)

func New(dir, logfile string) experimentalsys.FS {
	l := log.Disabled
	if logfile != "" {
		l = log.New(&log.Config{
			Level:    log.LevelInfo,
			Flags:    log.ParseFlags(""),
			Backend:  "file",
			Filename: logfile,
			FileMode: 0644,
		})
	}
	return &dstfs{
		internal: sysfs.DirFS(dir),
		log:      l,
	}
}

var oFlags = []string{
	"O_RDONLY",
	"O_RDWR",
	"O_WRONLY",
	"O_APPEND",
	"O_CREAT",
	"O_DIRECTORY",
	"O_DSYNC",
	"O_EXCL",
	"O_NOFOLLOW",
	"O_NONBLOCK",
	"O_RSYNC",
	"O_SYNC",
	"O_TRUNC",
}

func printOflag(f experimentalsys.Oflag) string {
	var b strings.Builder
	for i := 0; i < 13; i++ {
		if f&experimentalsys.Oflag(i) > 0 {
			if b.Len() > 0 {
				b.WriteString("|")
			}
			b.WriteString(oFlags[i])
		}
	}
	s := b.String()
	if s == "" {
		s = "0"
	}
	return s
}

func extractFd(val any) int {
	// &{/ 0 0 0x14000058000 4 false false <nil>}
	s := fmt.Sprintf("%v", val)
	var fd int
	if split := strings.Split(s, " "); len(split) > 4 {
		fd, _ = strconv.Atoi(split[4])
	}
	return fd
}

func (d *dstfs) OpenFile(path string, flag experimentalsys.Oflag, perm fs.FileMode) (experimentalsys.File, experimentalsys.Errno) {
	f, err := d.internal.OpenFile(path, flag, perm)
	if err != 0 {
		d.log.Infof("open(%q, %s, 0%03o) = %d %v", path, printOflag(flag), perm, err, err)
		return nil, err
	}
	fd := extractFd(f)
	d.log.Infof("open(%q, %s, 0%03o) = %d", path, printOflag(flag), perm, fd)
	return newFile(f, fd, path, d.log), 0
}

func (d *dstfs) Lstat(path string) (sys.Stat_t, experimentalsys.Errno) {
	st, errno := d.internal.Lstat(path)
	d.log.Infof("fstatat(AT_FDCWD, %q, %v) = %d", path, st, errno)
	return st, errno
}

func (d *dstfs) Stat(path string) (sys.Stat_t, experimentalsys.Errno) {
	st, errno := d.internal.Stat(path)
	d.log.Infof("fstat(%q, %v) = %d", path, st, errno)
	return st, errno
}

func (d *dstfs) Mkdir(path string, perm fs.FileMode) experimentalsys.Errno {
	return d.internal.Mkdir(path, perm)
}

func (d *dstfs) Chmod(path string, perm fs.FileMode) experimentalsys.Errno {
	return d.internal.Chmod(path, perm)
}

func (d *dstfs) Rename(from, to string) experimentalsys.Errno {
	return d.internal.Rename(from, to)
}

func (d *dstfs) Rmdir(path string) experimentalsys.Errno {
	return d.internal.Rmdir(path)
}

func (d *dstfs) Unlink(path string) experimentalsys.Errno {
	return d.internal.Unlink(path)
}

func (d *dstfs) Link(oldPath, newPath string) experimentalsys.Errno {
	return d.internal.Link(oldPath, newPath)
}

func (d *dstfs) Symlink(oldPath, linkName string) experimentalsys.Errno {
	return d.internal.Symlink(oldPath, linkName)
}

func (d *dstfs) Readlink(path string) (string, experimentalsys.Errno) {
	return d.internal.Readlink(path)
}

func (d *dstfs) Utimens(path string, atim, mtim int64) experimentalsys.Errno {
	return d.internal.Utimens(path, atim, mtim)
}
