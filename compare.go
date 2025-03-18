package xio

import (
	"bytes"
	"errors"
	"fmt"
	"io"
)

var (
	ErrCmpReadError         = errors.New("xio: compare: read error")
	ErrCmpByteCountMismatch = errors.New("xio: compare: byte count mismatch")
	ErrCmpDataMismatch      = errors.New("xio: compare: data mismatch")
)

// ReadersDataCmpError is an error returned by CmpReadersData when
// the data read from the readers is not equal.
type ReadersDataCmpError struct {
	Err        error
	ErrLeft    error
	ErrRight   error
	BytesLeft  []byte
	BytesRight []byte
	Offset     int64
}

func (e *ReadersDataCmpError) Error() string {
	return fmt.Sprintf("%v: err-left: %v, err-right: %v, offset: %d",
		e.Err, e.ErrLeft, e.ErrRight, e.Offset)
}

func (e *ReadersDataCmpError) Unwrap() error {
	return e.Err
}

// CmpReadersData compares the data read from two readers.
// It returns an error if the data read from the readers is not equal.
// CmpReadersData reads data from the readers in chunks and compares the
// data in the chunks. If the data read from the readers is not equal, it
// returns an error with the data read from the readers. The error is of type
// *ReadersDataCmpError.
func CmpReadersData[T1, T2 io.Reader](left T1, right T2) error {
	return CmpReadersDataWithBuffer(left, right, nil)
}

// CmpReadersDataWithBuffer is like CmpReadersData but accepts a buffer
// to use for reading data from the readers. If the buffer is not provided, a
// new buffer of size 64KB is used. Given buffer is split into two halves and
// used for reading data from the readers.
func CmpReadersDataWithBuffer[T1, T2 io.Reader](left T1, right T2, buffer []byte) error {
	if len(buffer) > 0 && len(buffer)%2 != 0 {
		buffer = buffer[:len(buffer)-1]
	}

	if len(buffer) == 0 {
		buffer = make([]byte, 64*1024)
	}

	half := len(buffer) / 2

	var offset int64
	for {

		n1, err1 := ReadFill(left, buffer[:half])
		if err1 == io.EOF {
			err1 = nil
		}

		end := half + n1
		if n1 <= 0 {
			end = 2 * half
		}

		n2, err2 := ReadFill(right, buffer[half:end])
		if err2 == io.EOF {
			err2 = nil
		}

		var err error

		if err1 != nil || err2 != nil {
			err = ErrCmpReadError
		} else if n1 != n2 {
			err = ErrCmpByteCountMismatch
		} else if !bytes.Equal(
			buffer[:n1],
			buffer[half:half+n2],
		) {
			err = ErrCmpDataMismatch
		}

		if err != nil {
			return &ReadersDataCmpError{
				Err:        err,
				ErrLeft:    err1,
				ErrRight:   err2,
				BytesLeft:  clipBytes(buffer[:n1]),
				BytesRight: clipBytes(buffer[half : half+n2]),
				Offset:     offset,
			}
		}

		if n1 == 0 {
			break
		}

		offset += int64(n1)
	}

	return nil
}

func clipBytes(b []byte) []byte {
	return b[:len(b):len(b)]
}
