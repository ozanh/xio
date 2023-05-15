package xio

import (
	"errors"
	"io"
	"sync"
)

var ErrNoSpaceLeft = errors.New("no space left")

type ByteSliceReadWriterAt struct {
	rwmu     sync.RWMutex
	buf      []byte
	autoGrow bool
}

func NewByteSliceReadWriterAt(buf []byte, autoGrow bool) *ByteSliceReadWriterAt {
	return &ByteSliceReadWriterAt{
		buf:      buf,
		autoGrow: autoGrow,
	}
}

func (brw *ByteSliceReadWriterAt) ReadAt(p []byte, off int64) (int, error) {
	brw.rwmu.RLock()
	defer brw.rwmu.RUnlock()

	if off >= int64(len(brw.buf)) {
		return 0, io.EOF
	}
	n := copy(p, brw.buf[off:])
	l := len(p)
	if n < l && l > 0 {
		return n, io.EOF
	}
	return n, nil
}

func (brw *ByteSliceReadWriterAt) WriteAt(p []byte, off int64) (int, error) {
	brw.rwmu.Lock()
	defer brw.rwmu.Unlock()

	if brw.autoGrow {
		explen := off + int64(len(p))
		brw.tryGrow(explen)
	}
	n := copy(brw.buf[off:], p)
	if n < len(p) {
		return n, ErrNoSpaceLeft
	}
	return n, nil
}

func (brw *ByteSliceReadWriterAt) Bytes() []byte {
	brw.rwmu.RLock()
	defer brw.rwmu.RUnlock()
	return brw.buf
}

func (brw *ByteSliceReadWriterAt) Len() int {
	brw.rwmu.RLock()
	defer brw.rwmu.RUnlock()
	return len(brw.buf)
}

func (brw *ByteSliceReadWriterAt) tryGrow(explen int64) {
	size := len(brw.buf)
	if int64(size) < explen {
		if int64(cap(brw.buf)) < explen {
			buf := make([]byte, explen)
			copy(buf, brw.buf)
			brw.buf = buf
		}
		brw.buf = brw.buf[:explen]
	}
}
