// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

// Buffered reader/writer with buffer alignment for direct I/O.
// Copy of https://github.com/golang/go/blob/master/src/bufio/bufio.go
// with unused features stripped

package wal

import (
	"errors"
	"io"
)

var (
	ErrInvalidUnreadByte = errors.New("bufio: invalid use of UnreadByte")
	ErrInvalidUnreadRune = errors.New("bufio: invalid use of UnreadRune")
	ErrBufferFull        = errors.New("bufio: buffer full")
	ErrNegativeCount     = errors.New("bufio: negative count")
)

// Buffered input.

// BufioReader implements buffering for an io.Reader object.
// A new BufioReader is created by calling [NewBufioReader] or [NewBufioReaderSize];
// alternatively the zero value of a BufioReader may be used after calling [Reset]
// on it.
type BufioReader struct {
	buf  []byte
	rd   io.Reader // reader provided by the client
	r, w int       // buf read and write positions
	err  error
}

// NewReaderSize returns a new [Reader] whose buffer has at least the specified
// size. If the argument io.Reader is already a [Reader] with large enough
// size, it returns the underlying [Reader].
func NewBufioReaderSize(rd io.Reader, size int) *BufioReader {
	// Is it already a Reader?
	b, ok := rd.(*BufioReader)
	if ok && len(b.buf) >= size {
		return b
	}
	r := new(BufioReader)
	r.reset(makeAligned(max(size, WAL_BUFFER_SIZE)), rd)
	return r
}

// NewBufioReader returns a new [Reader] whose buffer has the default size.
func NewBufioReader(rd io.Reader) *BufioReader {
	return NewBufioReaderSize(rd, WAL_BUFFER_SIZE)
}

// Size returns the size of the underlying buffer in bytes.
func (b *BufioReader) Size() int { return len(b.buf) }

// Reset discards any buffered data, resets all state, and switches
// the buffered reader to read from r.
// Calling Reset on the zero value of [BufioReader] initializes the internal buffer
// to the default size.
// Calling b.Reset(b) (that is, resetting a [BufioReader] to itself) does nothing.
func (b *BufioReader) Reset(r io.Reader) {
	// If a BufioReader r is passed to NewBufioReader, NewBufioReader will return r.
	// Different layers of code may do that, and then later pass r
	// to Reset. Avoid infinite recursion in that case.
	if b == r {
		return
	}
	if b.buf == nil {
		b.buf = makeAligned(WAL_BUFFER_SIZE)
	}
	b.reset(b.buf, r)
}

func (b *BufioReader) reset(buf []byte, r io.Reader) {
	*b = BufioReader{
		buf: buf,
		rd:  r,
	}
}

var errNegativeRead = errors.New("bufio: reader returned negative count from Read")

func (b *BufioReader) readErr() error {
	err := b.err
	b.err = nil
	return err
}

// Read reads data into p.
// It returns the number of bytes read into p.
// The bytes are taken from at most one Read on the underlying [Reader],
// hence n may be less than len(p).
// To read exactly len(p) bytes, use io.ReadFull(b, p).
// If the underlying [BufioReader] can return a non-zero count with io.EOF,
// then this Read method can do so as well; see the [io.Reader] docs.
func (b *BufioReader) Read(p []byte) (n int, err error) {
	n = len(p)
	if n == 0 {
		if b.Buffered() > 0 {
			return 0, nil
		}
		return 0, b.readErr()
	}
	if b.r == b.w {
		if b.err != nil {
			return 0, b.readErr()
		}
		if len(p) >= len(b.buf) {
			// Large read, empty buffer.
			// Read directly into p to avoid copy.
			n, b.err = b.rd.Read(p)
			if n < 0 {
				panic(errNegativeRead)
			}
			return n, b.readErr()
		}
		// One read.
		// Do not use b.fill, which will loop.
		b.r = 0
		b.w = 0
		n, b.err = b.rd.Read(b.buf)
		if n < 0 {
			panic(errNegativeRead)
		}
		if n == 0 {
			return 0, b.readErr()
		}
		b.w += n
	}

	// copy as much as we can
	// Note: if the slice panics here, it is probably because
	// the underlying reader returned a bad count. See issue 49795.
	n = copy(p, b.buf[b.r:b.w])
	b.r += n
	return n, nil
}

// Buffered returns the number of bytes that can be read from the current buffer.
func (b *BufioReader) Buffered() int { return b.w - b.r }

// BufioWriter implements buffering for an [io.Writer] object.
// If an error occurs writing to a [BufioWriter], no more data will be
// accepted and all subsequent writes, and [BufioWriter.Flush], will return the error.
// After all data has been written, the client should call the
// [BufioWriter.Flush] method to guarantee all data has been forwarded to
// the underlying [io.Writer].
type BufioWriter struct {
	err error
	buf []byte
	n   int
	wr  io.Writer
}

// NewBufioWriterSize returns a new [BufioWriter] whose buffer has at least the specified
// size. If the argument io.Writer is already a [BufioWriter] with large enough
// size, it returns the underlying [BufioWriter].
func NewBufioWriterSize(w io.Writer, size int) *BufioWriter {
	// Is it already a Writer?
	b, ok := w.(*BufioWriter)
	if ok && len(b.buf) >= size {
		return b
	}
	if size <= 0 {
		size = WAL_BUFFER_SIZE
	}
	return &BufioWriter{
		buf: makeAligned(size),
		wr:  w,
	}
}

// NewBufioWriter returns a new [BufioWriter] whose buffer has the default size.
// If the argument io.Writer is already a [BufioWriter] with large enough buffer size,
// it returns the underlying [BufioWriter].
func NewBufioWriter(w io.Writer) *BufioWriter {
	return NewBufioWriterSize(w, WAL_BUFFER_SIZE)
}

// Size returns the size of the underlying buffer in bytes.
func (b *BufioWriter) Size() int { return len(b.buf) }

// Reset discards any unflushed buffered data, clears any error, and
// resets b to write its output to w.
// Calling Reset on the zero value of [BufioWriter] initializes the internal buffer
// to the default size.
// Calling w.Reset(w) (that is, resetting a [BufioWriter] to itself) does nothing.
func (b *BufioWriter) Reset(w io.Writer) {
	// If a BufioWriter w is passed to NewBufioWriter, NewBufioWriter will return w.
	// Different layers of code may do that, and then later pass w
	// to Reset. Avoid infinite recursion in that case.
	if b == w {
		return
	}
	if b.buf == nil {
		b.buf = makeAligned(WAL_BUFFER_SIZE)
	}
	b.err = nil
	b.n = 0
	b.wr = w
}

// Flush writes any buffered data to the underlying [io.Writer].
func (b *BufioWriter) Flush() error {
	if b.err != nil {
		return b.err
	}
	if b.n == 0 {
		return nil
	}
	n, err := b.wr.Write(b.buf[0:b.n])
	if n < b.n && err == nil {
		err = io.ErrShortWrite
	}
	if err != nil {
		if n > 0 && n < b.n {
			copy(b.buf[0:b.n-n], b.buf[n:b.n])
		}
		b.n -= n
		b.err = err
		return err
	}
	b.n = 0
	return nil
}

// Available returns how many bytes are unused in the buffer.
func (b *BufioWriter) Available() int { return len(b.buf) - b.n }

// AvailableBuffer returns an empty buffer with b.Available() capacity.
// This buffer is intended to be appended to and
// passed to an immediately succeeding [BufioWriter.Write] call.
// The buffer is only valid until the next write operation on b.
func (b *BufioWriter) AvailableBuffer() []byte {
	return b.buf[b.n:][:0]
}

// Buffered returns the number of bytes that have been written into the current buffer.
func (b *BufioWriter) Buffered() int { return b.n }

// Write writes the contents of p into the buffer.
// It returns the number of bytes written.
// If nn < len(p), it also returns an error explaining
// why the write is short.
func (b *BufioWriter) Write(p []byte) (nn int, err error) {
	for len(p) > b.Available() && b.err == nil {
		var n int
		if b.Buffered() == 0 {
			// Large write, empty buffer.
			// Write directly from p to avoid copy.
			n, b.err = b.wr.Write(p)
		} else {
			n = copy(b.buf[b.n:], p)
			b.n += n
			b.Flush()
		}
		nn += n
		p = p[n:]
	}
	if b.err != nil {
		return nn, b.err
	}
	n := copy(b.buf[b.n:], p)
	b.n += n
	nn += n
	return nn, nil
}
