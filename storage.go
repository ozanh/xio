package xio

import (
	"io"
	"sync"
)

// BlockStorageBuffer implements the Storage interface using a slice of byte slices.
type BlockStorageBuffer struct {
	rwmu        sync.RWMutex // Guards blocks
	blocks      [][]byte
	storageSize int
	blockSize   int
}

// NewBlockStorageBuffer creates a new BlockStorageBuffer with the given block size and storage size.
func NewBlockStorageBuffer(blockSize, storageSize int) *BlockStorageBuffer {
	if blockSize <= 0 {
		blockSize = DefaultBlockSize
	}

	if storageSize < blockSize {
		storageSize = blockSize
	}

	if storageSize%blockSize != 0 {
		storageSize = (storageSize/blockSize + 1) * blockSize
	}

	return &BlockStorageBuffer{
		blocks:      make([][]byte, storageSize/blockSize),
		storageSize: storageSize,
		blockSize:   blockSize,
	}
}

// ReadAt reads len(p) bytes from the storage starting at byte offset off.
func (bs *BlockStorageBuffer) ReadAt(p []byte, off int64) (n int, err error) {
	bs.rwmu.RLock()
	defer bs.rwmu.RUnlock()

	if off >= int64(bs.storageSize) {
		err = io.EOF
		return
	}
	ioff := int(off)

	for {
		var nr int

		index, offset := bs.translate(ioff)
		if index >= len(bs.blocks) {
			err = io.EOF
			break
		}
		if index < 0 {
			err = ErrInvalidOffset
			break
		}
		if bs.blocks[index] == nil {
			pp := p[n:]
			nr := len(pp)
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
		ioff += nr

		if n >= len(p) || nr == 0 {
			break
		}
	}
	return
}

// translate translates the offset to index and offset in the block.
func (bs *BlockStorageBuffer) translate(off int) (index, offset int) {
	index = off / bs.blockSize
	offset = off % bs.blockSize
	return
}

// WriteAt writes len(p) bytes to the storage starting at byte offset off.
func (bs *BlockStorageBuffer) WriteAt(p []byte, off int64) (n int, err error) {
	bs.rwmu.Lock()
	defer bs.rwmu.Unlock()

	if off >= int64(bs.storageSize) {
		err = ErrNoSpaceLeft
		return
	}
	ioff := int(off)

	for {
		index, offset := bs.translate(ioff)
		if index >= len(bs.blocks) {
			err = ErrNoSpaceLeft
			break
		}
		if index < 0 {
			err = ErrInvalidOffset
			break
		}

		if bs.blocks[index] == nil {
			bs.blocks[index] = make([]byte, bs.blockSize)
		}

		nw := copy(bs.blocks[index][offset:], p[n:])
		n += nw
		ioff += nw
		if n >= len(p) || nw == 0 {
			break
		}
	}
	if n != len(p) {
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

// Cap returns the total capacity of the storage. It is for debug purpose only.
func (bs *BlockStorageBuffer) Cap() int {
	bs.rwmu.RLock()
	defer bs.rwmu.RUnlock()

	var n int
	for _, b := range bs.blocks {
		n += cap(b)
	}
	return n
}

// StorageBuffer implements the Storage interface using a byte slice. It is for testing and debug purpose only.
type StorageBuffer struct {
	rwmu     sync.RWMutex // Guards buf
	buf      []byte
	autoGrow bool
}

// NewStorageBuffer creates a new StorageBuffer with the given byte slice and auto grow flag.
func NewStorageBuffer(buf []byte, autoGrow bool) *StorageBuffer {
	return &StorageBuffer{
		buf:      buf,
		autoGrow: autoGrow,
	}
}

// ReadAt reads len(p) bytes from the storage starting at byte offset off.
func (bs *StorageBuffer) ReadAt(p []byte, off int64) (int, error) {
	bs.rwmu.RLock()
	defer bs.rwmu.RUnlock()

	if off >= int64(len(bs.buf)) {
		return 0, io.EOF
	}
	n := copy(p, bs.buf[off:])
	l := len(p)
	if n < l && l > 0 {
		return n, io.EOF
	}
	return n, nil
}

// WriteAt writes len(p) bytes to the storage starting at byte offset off.
func (bs *StorageBuffer) WriteAt(p []byte, off int64) (int, error) {
	bs.rwmu.Lock()
	defer bs.rwmu.Unlock()

	if bs.autoGrow {
		explen := off + int64(len(p))
		bs.tryGrow(explen)
	}
	n := copy(bs.buf[off:], p)
	if n < len(p) {
		return n, ErrNoSpaceLeft
	}
	return n, nil
}

// Bytes returns the underlying byte slice.
func (bs *StorageBuffer) Bytes() []byte {
	bs.rwmu.RLock()
	defer bs.rwmu.RUnlock()
	return bs.buf
}

// Len returns the length of the underlying byte slice.
func (bs *StorageBuffer) Len() int {
	bs.rwmu.RLock()
	defer bs.rwmu.RUnlock()
	return len(bs.buf)
}

func (bs *StorageBuffer) tryGrow(explen int64) {
	size := len(bs.buf)
	if int64(size) < explen {
		if int64(cap(bs.buf)) < explen {
			buf := make([]byte, explen)
			copy(buf, bs.buf)
			bs.buf = buf
		}
		bs.buf = bs.buf[:explen]
	}
}
