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

func CompareReadersData[T io.Reader](left, right T) error {
	return CompareReadersDataWithBuffer(left, right, nil)
}

func CompareReadersDataWithBuffer[T io.Reader](left, right T, buf []byte) error {
	if len(buf) > 0 && len(buf)%2 != 0 {
		buf = buf[:len(buf)-1]
	}

	if len(buf) == 0 {
		buf = make([]byte, 2*64*bytes.MinRead)
	} else if len(buf) < 2*bytes.MinRead {
		buf = make([]byte, 2*bytes.MinRead)
	}

	half := len(buf) / 2

	var offset int64
	for {

		n1, err1 := ReadFill(left, buf[:half])
		if err1 == io.EOF {
			err1 = nil
		}

		end := half + n1
		if n1 <= 0 {
			end = 2 * half
		}

		n2, err2 := ReadFill(right, buf[half:end])
		if err2 == io.EOF {
			err2 = nil
		}

		var err error

		if err1 != nil || err2 != nil {
			err = ErrComparisonReadError
		} else if n1 != n2 {
			err = ErrComparisonByteCountMismatch
		} else if !bytes.Equal(
			buf[:n1],
			buf[half:half+n2],
		) {
			err = ErrComparisonDataMismatch
		}

		if err != nil {
			return &ReadersDataComparisonError{
				Err:        err,
				ErrLeft:    err1,
				ErrRight:   err2,
				BytesLeft:  clipBytes(buf[:n1]),
				BytesRight: clipBytes(buf[half : half+n2]),
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
