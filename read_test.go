package xio_test

import (
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/ozanh/xio"
)

func TestReadFill(t *testing.T) {
	type testCase struct {
		name      string
		bufSize   int
		newReader func() io.Reader
		wantN     int
		wantErr   error
		wantPanic bool
	}

	tests := []testCase{
		{
			name:    "successful read",
			bufSize: 5,
			newReader: func() io.Reader {
				return &mockReader{
					data: []byte("hello"),
				}
			},
			wantN:   5,
			wantErr: nil,
		},
		{
			name:    "empty buffer",
			bufSize: 0,
			newReader: func() io.Reader {
				return &mockReader{
					data: []byte("hello"),
				}
			},
			wantN:   0,
			wantErr: io.ErrShortBuffer,
		},
		{
			name:    "reader error",
			bufSize: 5,
			newReader: func() io.Reader {
				return &mockReader{
					data: []byte("he"),
					err:  io.ErrUnexpectedEOF,
				}
			},
			wantN:   2,
			wantErr: io.ErrUnexpectedEOF,
		},
		{
			name:    "too many empty reads",
			bufSize: 5,
			newReader: func() io.Reader {
				return &mockReader{
					data:       []byte("h"),
					emptyCount: 101, // maxConsecutiveEmptyReads + 1
				}
			},
			wantN:   1,
			wantErr: io.ErrNoProgress,
		},
		{
			name:    "negative read count",
			bufSize: 6,
			newReader: func() io.Reader {
				return &negativeReader{
					initReader: strings.NewReader("hello"),
				}
			},
			wantN:     0,
			wantErr:   nil,
			wantPanic: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					if !tc.wantPanic {
						t.Errorf("unexpected panic: %v", r)
					}
				} else if tc.wantPanic {
					t.Error("expected panic, but did not get one")
				}
			}()

			reader := tc.newReader()

			buf := make([]byte, tc.bufSize)
			n, err := xio.ReadFill(reader, buf)

			if n != tc.wantN {
				t.Errorf("got n = %v, want %v", n, tc.wantN)
			}

			if !errors.Is(err, tc.wantErr) {
				t.Errorf("got error = %v, want %v", err, tc.wantErr)
			}
		})
	}
}

type mockReader struct {
	data       []byte
	pos        int
	emptyCount int
	err        error
}

func (r *mockReader) Read(p []byte) (n int, err error) {
	if r.pos >= len(r.data) {
		if r.err != nil {
			return 0, r.err
		}
		if r.emptyCount > 0 {
			r.emptyCount--
			return 0, nil
		}
		return 0, io.EOF
	}

	n = copy(p, r.data[r.pos:])
	r.pos += n
	return n, nil
}

type negativeReader struct {
	initReader io.Reader
}

func (r *negativeReader) Read(p []byte) (int, error) {
	n, err := r.initReader.Read(p)
	if n > 0 {
		return n, nil
	}
	return -1, err
}
