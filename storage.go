package xio

import (
	"errors"
	"io"
	"sync"
)

// BlockStorageBuffer implements the Storage interface using a slice of byte
// slices. It is suitable for large data storage instead of using a single byte
// slice. BlockStorageBuffer methods are goroutine-safe.
type BlockStorageBuffer struct {
	rwmu        sync.RWMutex // Guards blocks
	blocks      [][]byte
	storageSize int
	blockSize   int
}

// NewBlockStorageBuffer creates a new BlockStorageBuffer with the given block
// size and storage size.
func NewBlockStorageBuffer(blockSize, storageSize int) *BlockStorageBuffer {
	if blockSize <= 0 {
		blockSize = DefaultBlockSize
	}

	if storageSize < blockSize {
		storageSize = blockSize
	} else if rem := storageSize % blockSize; rem != 0 {
		storageSize = storageSize + blockSize - rem
	}

	return &BlockStorageBuffer{
		blocks:      make([][]byte, storageSize/blockSize),
		storageSize: storageSize,
		blockSize:   blockSize,
	}
}

// ReadAt reads len(p) bytes from the storage starting at byte offset off.
// It implements io.ReaderAt interface.
func (bs *BlockStorageBuffer) ReadAt(p []byte, off int64) (n int, err error) {
	bs.rwmu.RLock()
	defer bs.rwmu.RUnlock()

	if off >= int64(bs.storageSize) {
		err = io.EOF
		return
	}
	if off < 0 {
		err = errors.New("xio: BlockStorageBuffer: negative offset")
		return
	}

	for {
		var nr int

		index, offset := bs.translate(off)
		if index >= len(bs.blocks) {
			err = io.EOF
			break
		}

		if bs.blocks[index] == nil {
			pp := p[n:]
			nr = len(pp)
			if v := bs.blockSize - offset; nr > v {
				nr = v
			}
			for i := 0; i < nr; i++ {
				pp[i] = 0
			}
		} else {
			nr = copy(p[n:], bs.blocks[index][offset:])
		}
		n += nr
		off += int64(nr)

		if n >= len(p) || nr == 0 {
			break
		}
	}
	return
}

// translate translates the offset to index and offset in the block.
func (bs *BlockStorageBuffer) translate(off int64) (index, offset int) {
	index = int(off / int64(bs.blockSize))
	offset = int(off % int64(bs.blockSize))
	return
}

// WriteAt writes len(p) bytes to the storage starting at byte offset off.
// It implements io.WriterAt interface.
func (bs *BlockStorageBuffer) WriteAt(p []byte, off int64) (n int, err error) {
	bs.rwmu.Lock()
	defer bs.rwmu.Unlock()

	if off >= int64(bs.storageSize) {
		err = ErrNoSpaceLeft
		return
	}
	if off < 0 {
		err = errors.New("xio: BlockStorageBuffer: negative offset")
		return
	}

	for {
		index, offset := bs.translate(off)
		if index >= len(bs.blocks) {
			err = ErrNoSpaceLeft
			break
		}

		if bs.blocks[index] == nil {
			bs.blocks[index] = make([]byte, bs.blockSize)
		}

		nw := copy(bs.blocks[index][offset:], p[n:])
		n += nw
		off += int64(nw)
		if n >= len(p) || nw == 0 {
			break
		}
	}
	if n != len(p) && err == nil {
		err = io.ErrShortWrite
	}
	return
}

// BlockSize returns the block size.
func (bs *BlockStorageBuffer) BlockSize() int {
	return bs.blockSize
}

// StorageSize returns the storage size.
func (bs *BlockStorageBuffer) StorageSize() int {
	return bs.storageSize
}

// StorageBuffer implements the Storage interface using a single byte slice.
// It is for testing, debug or small data storage. StorageBuffer methods are
// goroutine-safe.
type StorageBuffer struct {
	rwmu     sync.RWMutex // Guards buf
	buf      []byte
	autoGrow bool
}

// NewStorageBuffer creates a new StorageBuffer with the given byte slice and
// auto grow flag. StorageBuffer is suitable for small data storage and testing,
// for large data storage, use BlockStorageBuffer.
func NewStorageBuffer(buf []byte, autoGrow bool) *StorageBuffer {
	return &StorageBuffer{
		buf:      buf,
		autoGrow: autoGrow,
	}
}

// ReadAt reads len(p) bytes from the storage starting at byte offset off.
func (s *StorageBuffer) ReadAt(p []byte, off int64) (int, error) {
	s.rwmu.RLock()
	defer s.rwmu.RUnlock()

	if off < 0 {
		return 0, errors.New("xio: StorageBuffer: negative offset")
	}

	if off >= int64(len(s.buf)) {
		return 0, io.EOF
	}

	n := copy(p, s.buf[off:])

	l := len(p)
	if n < l && l > 0 {
		return n, io.EOF
	}
	return n, nil
}

// WriteAt writes len(p) bytes to the storage starting at byte offset off.
func (s *StorageBuffer) WriteAt(p []byte, off int64) (int, error) {
	s.rwmu.Lock()
	defer s.rwmu.Unlock()

	if off < 0 {
		return 0, errors.New("xio: StorageBuffer: negative offset")
	}

	if s.autoGrow {
		explen := off + int64(len(p))
		s.tryGrow(explen)
	}

	n := copy(s.buf[off:], p)
	if n < len(p) {
		return n, ErrNoSpaceLeft
	}
	return n, nil
}

// Bytes returns the underlying byte slice.
func (s *StorageBuffer) Bytes() []byte {
	s.rwmu.RLock()
	defer s.rwmu.RUnlock()

	return s.buf
}

// Len returns the length of the underlying byte slice.
func (s *StorageBuffer) Len() int {
	s.rwmu.RLock()
	defer s.rwmu.RUnlock()

	return len(s.buf)
}

// AutoGrow reports whether the storage buffer is auto growing, which is set
// when creating the storage buffer.
func (s *StorageBuffer) AutoGrow() bool {
	s.rwmu.RLock()
	defer s.rwmu.RUnlock()

	return s.autoGrow
}

func (s *StorageBuffer) tryGrow(explen int64) {
	size := len(s.buf)
	if int64(size) < explen {
		if int64(cap(s.buf)) < explen {
			buf := make([]byte, explen)
			copy(buf, s.buf)
			s.buf = buf
		}
		s.buf = s.buf[:explen]
	}
}
