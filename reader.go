package xio

import "io"

type ErrOrEofReader struct {
	Err error
}

func (r *ErrOrEofReader) Read(p []byte) (int, error) {
	if r.Err != nil {
		return 0, r.Err
	}
	return 0, io.EOF
}
