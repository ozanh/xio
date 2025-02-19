package xio

import "io"

// ErrOrEofReader is an io.Reader implementation that always returns an error
// set in Err field or io.EOF if Err is nil.
type ErrOrEofReader struct {
	Err error
}

// Read implements the io.Reader interface. It returns the error set in Err
// field or io.EOF if Err is nil.
func (r *ErrOrEofReader) Read(p []byte) (int, error) {
	if r.Err != nil {
		return 0, r.Err
	}
	return 0, io.EOF
}
