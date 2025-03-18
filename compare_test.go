package xio_test

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"strings"
	"testing"

	"github.com/ozanh/xio"
)

func TestCmpReadersDataWithBuffer(t *testing.T) {
	newStringsReader := func(s string) func() io.Reader {
		return func() io.Reader {
			return strings.NewReader(s)
		}
	}

	newAnyReader := func(r io.Reader) func() io.Reader {
		return func() io.Reader {
			return r
		}
	}

	type testCase struct {
		name      string
		leftFn    func() io.Reader
		rightFn   func() io.Reader
		wantErrIs error
	}

	tests := []testCase{
		{
			name:      "equal empty readers",
			leftFn:    newStringsReader(""),
			rightFn:   newStringsReader(""),
			wantErrIs: nil,
		},
		{
			name:      "equal data",
			leftFn:    newStringsReader("hello world"),
			rightFn:   newStringsReader("hello world"),
			wantErrIs: nil,
		},
		{
			name: "equal data random",
			leftFn: func() io.Reader {
				r := rand.New(rand.NewSource(1))
				return io.LimitReader(r, 1000*1000)
			},
			rightFn: func() io.Reader {
				r := rand.New(rand.NewSource(1))
				return io.LimitReader(r, 1000*1000)
			},
			wantErrIs: nil,
		},
		{
			name:      "different length",
			leftFn:    newStringsReader("hello"),
			rightFn:   newStringsReader("hello world"),
			wantErrIs: xio.ErrCmpByteCountMismatch,
		},
		{
			name:      "different content same length",
			leftFn:    newStringsReader("hello world"),
			rightFn:   newStringsReader("hello earth"),
			wantErrIs: xio.ErrCmpDataMismatch,
		},
		{
			name: "left reader error",
			leftFn: newAnyReader(
				&xio.ErrOrEofReader{Err: errors.New("left reader error")},
			),
			rightFn:   newStringsReader("data"),
			wantErrIs: xio.ErrCmpReadError,
		},
		{
			name:   "right reader error",
			leftFn: newStringsReader("data"),
			rightFn: newAnyReader(
				&xio.ErrOrEofReader{Err: errors.New("right reader error")},
			),
			wantErrIs: xio.ErrCmpReadError,
		},
	}

	bufSizes := []int{
		0,                   // nil buffer
		1,                   // odd size
		2*bytes.MinRead - 1, // small size
		2048,                // typical size
		4095,                // odd large size
	}

	for _, tt := range tests {
		for _, bsize := range bufSizes {
			name := tt.name
			if bsize > 0 {
				name = fmt.Sprintf("%s with %d buffer size", tt.name, bsize)
			}

			t.Run(name, func(t *testing.T) {
				var buf []byte
				if bsize > 0 {
					buf = make([]byte, bsize)
				}

				leftrc := xio.NewReadDataCounter(tt.leftFn())
				rightrc := xio.NewReadDataCounter(tt.rightFn())

				err := xio.CmpReadersDataWithBuffer(leftrc, rightrc, buf)
				if !errors.Is(err, tt.wantErrIs) {
					t.Errorf("CmpReadersDataWithBuffer() error = %v, wantErr %v", err, tt.wantErrIs)
				}

				leftCount, rightCount := leftrc.Count(), rightrc.Count()

				if err == nil {
					if leftCount != rightCount {
						t.Errorf("CmpReadersDataWithBuffer() byte count mismatch = %d, want %d", leftCount, rightCount)
					}
					// Check if the readers are consumed completely.
					n, err := leftrc.Read([]byte{0})
					if n > 0 || err != io.EOF {
						t.Errorf("CmpReadersDataWithBuffer() left reader not consumed completely. n=%d, err=%v", n, err)
					}

					n, err = rightrc.Read([]byte{0})
					if n > 0 || err != io.EOF {
						t.Errorf("CmpReadersDataWithBuffer() right reader not consumed completely. n=%d, err=%v", n, err)
					}

					return
				}

				cerr := err.(*xio.ReadersDataCmpError)

				expectedMaxOffset := minU64(leftCount, rightCount)

				if cerr.Offset < 0 || uint64(cerr.Offset) > expectedMaxOffset {
					t.Errorf("CmpReadersDataWithBuffer() error offset = %d, want between 0 and %d", cerr.Offset, expectedMaxOffset)
				}
			})
		}
	}
}

func TestCmpReadersData(t *testing.T) {
	tests := []struct {
		name    string
		left    io.Reader
		right   io.Reader
		wantErr error
	}{
		{
			name:    "equal data",
			left:    strings.NewReader("test data"),
			right:   strings.NewReader("test data"),
			wantErr: nil,
		},
		{
			name:    "different data",
			left:    strings.NewReader("test data"),
			right:   strings.NewReader("other data"),
			wantErr: xio.ErrCmpDataMismatch,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := xio.CmpReadersData(tt.left, tt.right)
			if !errors.Is(err, tt.wantErr) {
				t.Errorf("CmpReadersData() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func minU64(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}
