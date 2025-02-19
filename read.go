package xio

import (
	"io"
)

// ReadFill reads from r into buf until it is full or an error occurs.
// It returns the number of bytes read into buf and the error, if any.
// ReadFill returns io.ErrShortBuffer if len(buf) < 1.
// ReadFill returns io.ErrNoProgress if the reader returns 0 bytes for
// maxConsecutiveEmptyReads times.
// ReadFill panics if the reader returns a negative count from Read.
func ReadFill[T io.Reader](r T, buf []byte) (n int, err error) {
	if len(buf) < 1 {
		return 0, io.ErrShortBuffer
	}

	var emptyReads int

	for n < len(buf) && err == nil {
		var nn int
		nn, err = r.Read(buf[n:])
		if nn < 0 {
			panic("xio: reader returned negative count from Read")
		}

		if nn != 0 {
			n += nn
			emptyReads = 0
			continue
		}

		if err == nil {
			emptyReads++

			if emptyReads > maxConsecutiveEmptyReads {
				err = io.ErrNoProgress
			}
		}
	}

	return
}
