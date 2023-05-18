package xio

import (
	"errors"
	"io"
)

// DefaultBlockSize is the default block size used when block size is not specified.
const DefaultBlockSize = 1 * 1024 * 1024

const debug = false

// ErrNoSpaceLeft is returned when there is no space left in the underlying storage.
var ErrNoSpaceLeft = errors.New("no space left")

// ErrInvalidOffset is returned when the offset is invalid.
var ErrInvalidOffset = errors.New("invalid offset")

// Storage is the interface that wraps the basic io.ReaderAt and io.WriterAt interfaces.
type Storage interface {
	io.ReaderAt
	io.WriterAt
}
