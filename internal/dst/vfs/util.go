package vfs

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	experimentalsys "github.com/tetratelabs/wazero/experimental/sys"
	"github.com/tetratelabs/wazero/sys"
)

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

// wasip1 error definitions, not syscall errors
var errnos = []string{
	"",
	"EACCES (permission denied)",
	"EAGAIN (resource unavailable, try again)",
	"EBADF (bad file descriptor)",
	"EEXIST (file exists)",
	"EFAULT (bad address)",
	"EINTR (interrupted function)",
	"EINVAL (invalid argument)",
	"EIO (input/output error)",
	"EISDIR (is a directory)",
	"ELOOP (too many levels of symbolic links)",
	"ENAMETOOLONG (filename too long)",
	"ENOENT (no such file or directory)",
	"ENOSYS (functionality not supported)",
	"ENOTDIR (not a directory or a symbolic link to a directory)",
	"ERANGE (result too large)",
	"ENOTEMPTY (directory not empty)",
	"ENOTSOCK (not a socket)",
	"ENOTSUP (not supported (may be the same value as [EOPNOTSUPP]))",
	"EPERM (operation not permitted)",
	"EROFS (read-only file system)",
}

var seekFlags = []string{"SEEK_SET", "SEEK_CUR", "SEEK_END"}

func preview(buf []byte) string {
	n := len(buf)
	if n > 16 {
		return fmt.Sprintf("%q...", buf[:16])
	}
	return fmt.Sprintf("%q", buf)
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
	if b.Len() > 0 {
		b.WriteString("|")
	}
	// Go always adds CLOEXEC and O_LARGEFILE flags on unix
	b.WriteString("O_CLOEXEC|O_LARGEFILE")
	s := b.String()
	if s == "" {
		s = "0"
	}
	return s
}

func printFstat(s sys.Stat_t) string {
	return fmt.Sprintf("{st_mode=%s, st_size=%d, st_nlink=%d, st_atime=%s, st_mtime=%s, st_ctime=%s}",
		printFileMode(s.Mode), s.Size, s.Nlink, time.Unix(s.Atim, 0), time.Unix(s.Mtim, 0), time.Unix(s.Ctim, 0))
}

func printFileMode(m os.FileMode) string {
	var b strings.Builder

	// type
	switch m & os.ModeType {
	case os.ModeDir:
		b.WriteString("S_IFDIR|")
	case os.ModeSymlink:
		b.WriteString("S_IFLNK|")
	case os.ModeNamedPipe:
		b.WriteString("S_IFIFO|")
	case os.ModeSocket:
		b.WriteString("S_IFSOCK|")
	case os.ModeDevice:
		b.WriteString("S_IFBLK|")
	case os.ModeCharDevice:
		b.WriteString("S_IFCHR|")
	case os.ModeIrregular:
		b.WriteString("IRREGULAR|")
	case 0:
		b.WriteString("S_IFREG|")
	}

	// flags
	for _, flag := range []struct {
		mode os.FileMode
		name string
	}{
		{os.ModeSticky, "S_ISVTX|"},
		{os.ModeSetuid, "S_ISUID|"},
		{os.ModeSetgid, "S_ISGID|"},
		{os.ModeAppend, "DMAPPEND|"},  // plan 9 only
		{os.ModeExclusive, "DMEXCL|"}, // plan 9 only
		{os.ModeTemporary, "DMTMP|"},  // plan 9 only
	} {
		if m&flag.mode > 0 {
			b.WriteString(flag.name)
		}
	}

	// perm
	b.WriteString("0" + strconv.FormatInt(int64(m.Perm()), 8))

	return b.String()
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
