package xio_test

import (
	"io"
	"testing"

	"github.com/ozanh/xio"
)

func TestErrOrEofReader(t *testing.T) {
	testCases := []struct {
		name    string
		r       *xio.ErrOrEofReader
		wantErr error
	}{
		{
			name:    "EOF",
			r:       &xio.ErrOrEofReader{},
			wantErr: io.EOF,
		},
		{
			name:    "Error",
			r:       &xio.ErrOrEofReader{Err: io.ErrUnexpectedEOF},
			wantErr: io.ErrUnexpectedEOF,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			n, err := tt.r.Read(make([]byte, 1))
			if err != tt.wantErr {
				t.Errorf("got %v, want %v", err, tt.wantErr)
			}
			if n != 0 {
				t.Errorf("got %d, want 0", n)
			}
		})
	}
}
