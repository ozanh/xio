package xio

import (
	"io"
	"sync/atomic"
)

type ReadCounter interface {
	io.Reader
	Count() uint64
}

type ReadDataCounter[T io.Reader] struct {
	C uint64
	R T
}

var (
	_ ReadCounter = (*ReadDataCounter[io.Reader])(nil)
)

func NewReadDataCounter[T io.Reader](r T) *ReadDataCounter[T] {
	return &ReadDataCounter[T]{R: r}
}

func (r *ReadDataCounter[T]) Read(p []byte) (int, error) {
	n, err := r.R.Read(p)
	if n > 0 {
		r.C += uint64(n)
	}
	return n, err
}

func (r *ReadDataCounter[T]) Count() uint64 {
	return r.C
}

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

func (r *ReadDataAtomicCounter[T]) Read(p []byte) (int, error) {
	n, err := r.R.Read(p)
	if n > 0 {
		r.C.Add(uint64(n))
	}
	return n, err
}

func (r *ReadDataAtomicCounter[T]) Count() uint64 {
	return r.C.Load()
}
