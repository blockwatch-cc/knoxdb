package vfs

import (
	"time"

	"github.com/echa/log"
	experimentalsys "github.com/tetratelabs/wazero/experimental/sys"
	"github.com/tetratelabs/wazero/sys"
)

type file struct {
	// internal is the underlying file system to delegate to. This is
	// purposefully not embedded so that any new methods need to be explicitly
	// added.
	internal experimentalsys.File
	log      log.Logger
	fd       int
	name     string
}

var _ experimentalsys.File = (*file)(nil)

func newFile(f experimentalsys.File, fd int, name string, l log.Logger) *file {
	return &file{internal: f, fd: fd, name: name, log: l}
}

func (f *file) Dev() (uint64, experimentalsys.Errno) {
	return f.internal.Dev()
}

func (f *file) Ino() (sys.Inode, experimentalsys.Errno) {
	return f.internal.Ino()
}

func (f *file) IsDir() (bool, experimentalsys.Errno) {
	return f.internal.IsDir()
}

func (f *file) IsAppend() bool {
	return f.internal.IsAppend()
}

func (f *file) SetAppend(enable bool) experimentalsys.Errno {
	return f.internal.SetAppend(enable)
}

func (f *file) Stat() (sys.Stat_t, experimentalsys.Errno) {
	st, errno := f.internal.Stat()
	f.log.Infof("fstat(%d, %s) = %d %s", f.fd, printFstat(st), -int(errno), errnos[errno])
	return st, errno
}

func (f *file) Read(buf []byte) (n int, errno experimentalsys.Errno) {
	n, errno = f.internal.Read(buf)
	if errno == 0 {
		f.log.Infof("read(%d, %s, %d) = %d", f.fd, preview(buf[:n]), len(buf), n)
	} else {
		f.log.Infof("read(%d, \"\", %d) = %d %s", f.fd, len(buf), -int(errno), errnos[errno])
	}
	return
}

func (f *file) Pread(buf []byte, off int64) (n int, errno experimentalsys.Errno) {
	n, errno = f.internal.Pread(buf, off)
	if errno == 0 {
		f.log.Infof("read(%d, %s, %d, %d) = %d", f.fd, preview(buf[:n]), len(buf), off, n)
	} else {
		f.log.Infof("read(%d, \"\", %d, %d) = %d %s", f.fd, len(buf), off, -int(errno), errnos[errno])
	}
	return
}

//golint:nolint
func (f *file) Seek(offset int64, whence int) (newOffset int64, errno experimentalsys.Errno) {
	newOffset, errno = f.internal.Seek(offset, whence)
	if errno == 0 {
		f.log.Infof("lseek(%d, %d, %s) = %d", f.fd, offset, seekFlags[whence], newOffset)
	} else {
		f.log.Infof("lseek(%d, %d, %s) = %d %s", f.fd, offset, seekFlags[whence], -int(errno), errnos[errno])
	}
	return
}

func (f *file) Readdir(n int) (dirents []experimentalsys.Dirent, errno experimentalsys.Errno) {
	return f.internal.Readdir(n)
}

func (f *file) Write(buf []byte) (n int, errno experimentalsys.Errno) {
	n, errno = f.internal.Write(buf)
	if errno == 0 {
		f.log.Infof("write(%d, %s, %d) = %d", f.fd, preview(buf), len(buf), len(buf))
	} else {
		f.log.Infof("write(%d, \"\", %d) = %d %s", f.fd, len(buf), -int(errno), errnos[errno])
	}
	return
}

func (f *file) Pwrite(buf []byte, off int64) (n int, errno experimentalsys.Errno) {
	n, errno = f.internal.Pwrite(buf, off)
	if errno == 0 {
		f.log.Infof("pwrite64(%d, %s, %d, %d) = %d", f.fd, preview(buf), len(buf), off, len(buf))
	} else {
		f.log.Infof("pwrite64(%d, \"\", %d, %d) = %d %s", f.fd, len(buf), off, -int(errno), errnos[errno])
	}
	return
}

func (f *file) Truncate(size int64) experimentalsys.Errno {
	errno := f.internal.Truncate(size)
	f.log.Infof("ftruncate(%d, %d) = %d %s", f.fd, size, -int(errno), errnos[errno])
	return errno
}

func (f *file) Sync() experimentalsys.Errno {
	errno := f.internal.Sync()
	f.log.Infof("fsync(%d) = %d %s", f.fd, -int(errno), errnos[errno])
	return errno
}

func (f *file) Datasync() experimentalsys.Errno {
	errno := f.internal.Datasync()
	f.log.Infof("fdatasync(%d) = %d %s", f.fd, -int(errno), errnos[errno])
	return errno
}

func (f *file) Utimens(atim, mtim int64) experimentalsys.Errno {
	errno := f.internal.Utimens(atim, mtim)
	f.log.Infof("futimes(%d, [[%d, %d],[%d, %d]], 0) = %d %s",
		f.fd,
		atim/int64(time.Second), atim%int64(time.Second),
		mtim/int64(time.Second), mtim%int64(time.Second),
		-int(errno), errno,
	)
	return errno
}

func (f *file) Close() experimentalsys.Errno {
	errno := f.internal.Close()
	f.log.Infof("close(%d) = %d %s", f.fd, -int(errno), errnos[errno])
	return errno

}
