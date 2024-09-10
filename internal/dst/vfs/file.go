package vfs

import (
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
	f.log.Infof("fstat(%d, %v) = %d", f.fd, st, errno)
	return st, errno
}

func (f *file) Read(buf []byte) (n int, errno experimentalsys.Errno) {
	return f.internal.Read(buf)
}

func (f *file) Pread(buf []byte, off int64) (n int, errno experimentalsys.Errno) {
	return f.internal.Pread(buf, off)
}

func (f *file) Seek(offset int64, whence int) (newOffset int64, errno experimentalsys.Errno) {
	return f.internal.Seek(offset, whence)
}

func (f *file) Readdir(n int) (dirents []experimentalsys.Dirent, errno experimentalsys.Errno) {
	return f.internal.Readdir(n)
}

func (f *file) Write(buf []byte) (n int, errno experimentalsys.Errno) {
	return f.internal.Write(buf)
}

func (f *file) Pwrite(buf []byte, off int64) (n int, errno experimentalsys.Errno) {
	return f.internal.Pwrite(buf, off)
}

func (f *file) Truncate(size int64) experimentalsys.Errno {
	errno := f.internal.Truncate(size)
	f.log.Infof("trunc(%d, %d) = %d", f.fd, size, errno)
	return errno
}

func (f *file) Sync() experimentalsys.Errno {
	errno := f.internal.Sync()
	f.log.Infof("fsync(%d) = %d", f.fd, errno)
	return errno
}

func (f *file) Datasync() experimentalsys.Errno {
	errno := f.internal.Datasync()
	f.log.Infof("fdatasync(%d) = %d", f.fd, errno)
	return errno
}

func (f *file) Utimens(atim, mtim int64) experimentalsys.Errno {
	return f.internal.Utimens(atim, mtim)
}

func (f *file) Close() experimentalsys.Errno {
	errno := f.internal.Close()
	f.log.Infof("close(%d) = %d", f.fd, errno)
	return errno

}
