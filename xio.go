package xio

import (
	"errors"
	"io"
)

// DefaultBlockSize is the default block size used when block size is not specified.
const DefaultBlockSize = 1 * 1024 * 1024

const debug = false

const maxConsecutiveEmptyReads = 100

// ErrNoSpaceLeft is returned when there is no space left in the underlying storage.
var ErrNoSpaceLeft = errors.New("no space left")

// ErrShortRead is returned when the read operation could not read the requested
// number of bytes for the ReadAt operations.
var ErrShortRead = errors.New("short read")

// Storage is the interface that wraps the basic io.ReaderAt and io.WriterAt interfaces.
type Storage interface {
	io.ReaderAt
	io.WriterAt
}
