package xio

import (
	"io"
	"sync/atomic"
)

// ReadCounter is an interface that extends io.Reader with a Count method.
// The Count method returns the total number of bytes read from the reader.
type ReadCounter interface {
	io.Reader
	Count() uint64
}

// ReadDataCounter is a wrapper for an io.Reader that counts the number of bytes
// read. The Count method returns the total number of bytes read from the
// reader. Call Count to get total number of bytes read.
// Note: Count() is not goroutine-safe. Use ReadDataAtomicCounter[T] for
// goroutine-safe counting.
type ReadDataCounter[T io.Reader] struct {
	C uint64
	R T
}

var (
	_ ReadCounter = (*ReadDataCounter[io.Reader])(nil)
)

// NewReadDataCounter returns a new ReadDataCounter that wraps the given reader.
func NewReadDataCounter[T io.Reader](r T) *ReadDataCounter[T] {
	return &ReadDataCounter[T]{R: r}
}

// Read implements the io.Reader interface.
func (r *ReadDataCounter[T]) Read(p []byte) (int, error) {
	n, err := r.R.Read(p)
	if n > 0 {
		r.C += uint64(n)
	}
	return n, err
}

// Count returns the total number of bytes read from the reader. It is not
// goroutine-safe.
func (r *ReadDataCounter[T]) Count() uint64 {
	return r.C
}

// ReadDataAtomicCounter is a wrapper for an io.Reader that counts the number of
// bytes read. The Count method returns the total number of bytes read from the
// reader. Call Count to get total number of bytes read.
// Note: Count() is goroutine-safe.
type ReadDataAtomicCounter[T io.Reader] struct {
	C atomic.Uint64
	R T
}

var (
	_ ReadCounter = (*ReadDataAtomicCounter[io.Reader])(nil)
)

func NewReadDataAtomicCounter[T io.Reader](r T) *ReadDataAtomicCounter[T] {
	return &ReadDataAtomicCounter[T]{R: r}
}

// Read implements the io.Reader interface.
func (r *ReadDataAtomicCounter[T]) Read(p []byte) (int, error) {
	n, err := r.R.Read(p)
	if n > 0 {
		r.C.Add(uint64(n))
	}
	return n, err
}

// Count returns the total number of bytes read from the reader. It is
// goroutine-safe.
func (r *ReadDataAtomicCounter[T]) Count() uint64 {
	return r.C.Load()
}
