package xio

import (
	"bytes"
	"errors"
	"io"
)

var (
	ErrComparisonReadError         = errors.New("read error")
	ErrComparisonByteCountMismatch = errors.New("byte count mismatch")
	ErrComparisonDataMismatch      = errors.New("data mismatch")
)

// ReadersDataComparisonError is an error returned by CompareReadersData when
// the data read from the readers is not equal.
type ReadersDataComparisonError struct {
	Err        error
	ErrLeft    error
	ErrRight   error
	BytesLeft  []byte
	BytesRight []byte
	Offset     int64
}

func (e *ReadersDataComparisonError) Error() string {
	return e.Err.Error()
}

func (e *ReadersDataComparisonError) Unwrap() error {
	return e.Err
}

// CompareReadersData compares the data read from two readers.
// It returns an error if the data read from the readers is not equal.
// CompareReadersData reads data from the readers in chunks and compares the
// data in the chunks. If the data read from the readers is not equal, it
// returns an error with the data read from the readers. The error is of type
// *ReadersDataComparisonError.
func CompareReadersData[T io.Reader](left, right T) error {
	return CompareReadersDataWithBuffer(left, right, nil)
}

// CompareReadersDataWithBuffer is like CompareReadersData but accepts a buffer
// to use for reading data from the readers. If the buffer is not provided, a
// new buffer of size 64KB is used. Given buffer is split into two halves and
// used for reading data from the readers.
func CompareReadersDataWithBuffer[T io.Reader](left, right T, buffer []byte) error {
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
			err = ErrComparisonReadError
		} else if n1 != n2 {
			err = ErrComparisonByteCountMismatch
		} else if !bytes.Equal(
			buffer[:n1],
			buffer[half:half+n2],
		) {
			err = ErrComparisonDataMismatch
		}

		if err != nil {
			return &ReadersDataComparisonError{
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
