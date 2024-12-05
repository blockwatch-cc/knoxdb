package vfs

import (
	"io/fs"
	"time"

	"github.com/echa/log"
	experimentalsys "github.com/tetratelabs/wazero/experimental/sys"
	"github.com/tetratelabs/wazero/experimental/sysfs"
	"github.com/tetratelabs/wazero/sys"
)

type dstfs struct {
	internal experimentalsys.FS // underlying file system to delegate to
	log      log.Logger
	// openFiles map[int]string
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

func (d *dstfs) OpenFile(path string, flag experimentalsys.Oflag, perm fs.FileMode) (experimentalsys.File, experimentalsys.Errno) {
	f, err := d.internal.OpenFile(path, flag, perm)
	if err != 0 {
		d.log.Infof("openat(AT_FDCWD, %q, %s, 0%03o) = %d %v", path, printOflag(flag), perm, err, err)
		return nil, err
	}
	fd := extractFd(f)
	d.log.Infof("openat(AT_FDCWD, %q, %s, 0%03o) = %d", path, printOflag(flag), perm, fd)
	return newFile(f, fd, path, d.log), 0
}

func (d *dstfs) Lstat(path string) (sys.Stat_t, experimentalsys.Errno) {
	st, errno := d.internal.Lstat(path)
	d.log.Infof("fstatat(AT_FDCWD, %q, %s) = %d %s", path, printFstat(st), -int(errno), errnos[errno])
	return st, errno
}

func (d *dstfs) Stat(path string) (sys.Stat_t, experimentalsys.Errno) {
	st, errno := d.internal.Stat(path)
	d.log.Infof("fstatat(AT_FDCWD, %s, %v) = %d %s", path, printFstat(st), -int(errno), errnos[errno])
	return st, errno
}

func (d *dstfs) Mkdir(path string, perm fs.FileMode) experimentalsys.Errno {
	errno := d.internal.Mkdir(path, perm)
	d.log.Infof("mkdirat(AT_FDCWD, %q, 0%03o) = %d %s", path, perm, -int(errno), errnos[errno])
	return errno
}

func (d *dstfs) Chmod(path string, perm fs.FileMode) experimentalsys.Errno {
	errno := d.internal.Chmod(path, perm)
	d.log.Infof("fchmodat(AT_FDCWD, %q, 0%03o) = %d %s", path, perm, -int(errno), errnos[errno])
	return errno
}

func (d *dstfs) Rename(from, to string) experimentalsys.Errno {
	errno := d.internal.Rename(from, to)
	d.log.Infof("renameat(AT_FDCWD %q, AT_FDCWD, %q) = %d %s", from, to, -int(errno), errnos[errno])
	return errno
}

func (d *dstfs) Rmdir(path string) experimentalsys.Errno {
	errno := d.internal.Rmdir(path)
	d.log.Infof("unlinkat(AT_FDCWD, %q, AT_REMOVEDIR) = %d %s", path, -int(errno), errnos[errno])
	return errno
}

func (d *dstfs) Unlink(path string) experimentalsys.Errno {
	errno := d.internal.Unlink(path)
	d.log.Infof("unlinkat(AT_FDCWD, %q, 0) = %d %s", path, -int(errno), errnos[errno])
	return errno
}

func (d *dstfs) Link(oldPath, newPath string) experimentalsys.Errno {
	errno := d.internal.Link(oldPath, newPath)
	d.log.Infof("linkat(AT_FDCWD, %q, AT_FDCWD, %q, 0) = %d %s",
		oldPath, newPath, -int(errno), errnos[errno])
	return errno
}

func (d *dstfs) Symlink(oldPath, linkName string) experimentalsys.Errno {
	errno := d.internal.Symlink(oldPath, linkName)
	d.log.Infof("symlinkat(%q, AT_FDCWD, %q, 0) = %d %s",
		oldPath, linkName, -int(errno), errnos[errno])
	return errno
}

func (d *dstfs) Readlink(path string) (name string, errno experimentalsys.Errno) {
	name, errno = d.internal.Readlink(path)
	if errno == 0 {
		d.log.Infof("readlinkat(AT_FDCWD, %q, %s, 128) = %d %s", path, name, len(name))
	} else {
		d.log.Infof("readlinkat(AT_FDCWD, %q, %s, 128) = %d %s", path, name, -int(errno), errnos[errno])
	}
	return
}

func (d *dstfs) Utimens(path string, atim, mtim int64) experimentalsys.Errno {
	errno := d.internal.Utimens(path, atim, mtim)
	d.log.Infof("utimesat(AT_FDCWD, [[%d, %d],[%d, %d]], 0) = %d %s",
		atim/int64(time.Second), atim%int64(time.Second),
		mtim/int64(time.Second), mtim%int64(time.Second),
		path, -int(errno), errnos[errno],
	)
	return errno
}
