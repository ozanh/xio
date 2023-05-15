package xio

import (
	"bytes"
	"io"
	"sync"
)

const DefaultBufPipeBlockSize = 1 * 1024 * 1024

const MaxBufPipeQueueCapacity = 2048

// BufPipeReader is the read half of a buffered pipe.
type BufPipeReader struct {
	p *bufPipe
}

// Read implements the io.Reader interface: it reads data from the buffered pipe, blocking until a writer arrives or
// the write end is closed.
func (br *BufPipeReader) Read(p []byte) (n int, err error) {
	return br.p.Read(p)
}

// Close closes the reader; subsequent writes to the write half of the buffered pipe will return the error
// ErrClosedPipe.
func (br *BufPipeReader) Close() error {
	return br.CloseWithError(nil)
}

// CloseWithError closes the reader; subsequent writes to the write half of the buffered pipe will return the error err.
func (br *BufPipeReader) CloseWithError(err error) error {
	return br.p.closeRead(err)
}

// BufPipeWriter is the write half of a buffered pipe.
type BufPipeWriter struct {
	p *bufPipe
}

// Write implements the io.Writer interface: it writes data to the underlying buffered storage.
func (bw *BufPipeWriter) Write(p []byte) (n int, err error) {
	return bw.p.Write(p)
}

// Close closes the writer; subsequent reads from the read half of the buffered pipe will return no bytes and EOF until
// all the buffered data is read.
func (bw *BufPipeWriter) Close() error {
	return bw.CloseWithError(nil)
}

// CloseWithError closes the writer; subsequent reads from the read half of the buffered pipe will return no bytes and
// the error err, or EOF.
func (bw *BufPipeWriter) CloseWithError(err error) error {
	if err == nil {
		return bw.p.closeWrite()
	}
	return bw.p.closeWriteErr(err, false)
}

// Storager is the interface that wraps the basic ReadAt and WriteAt methods.
type Storager interface {
	io.ReaderAt
	io.WriterAt
}

// aBlock represents a block of data in the storage.
type aBlock struct {
	offset int64
	size   int64
}

// bufPipe is a buffered pipe that uses a storage as a backing store.
type bufPipe struct {
	stor Storager
	// blockch channel holds blocks available for reading. It is critical that this channel is buffered and cap is
	// multiple of the block size. It is closed when the writer is closed.
	blockch chan aBlock
	// donech channel is closed when the reader or writer is closed.
	donech chan struct{}
	// rewindch channel is used to signal that the reader has rewound the storage to the beginning.
	rewindch chan struct{}

	rmu     sync.Mutex // Guards buf
	buf     *bytes.Buffer
	wmu     sync.Mutex // Guards wrblock, blockch, and serializes writes
	wrblock aBlock

	rerr      onceError
	werr      onceError
	storSize  int64
	blockSize int64
	doneOnce  sync.Once // Protects closing donech
	blockOnce sync.Once // Protects closing blockch
}

// BufPipe creates a buffered pipe with a given block size and storage size.
// If blockSize is zero, a default value is used. If blockSize is less than bytes.MinRead, it is set to bytes.MinRead.
// The storageSize should be a multiple of the block size.
// If storageSize is not positive, it panics.
func BufPipe(blockSize int, storageSize int64, storager Storager) (*BufPipeReader, *BufPipeWriter) {
	if blockSize <= 0 {
		blockSize = DefaultBufPipeBlockSize
	} else if blockSize < bytes.MinRead {
		blockSize = bytes.MinRead
	}
	if storageSize <= 0 {
		panic("storageSize must be positive")
	}
	if storageSize < int64(blockSize) {
		blockSize = int(storageSize)
	}
	blockChCap := storageSize / int64(blockSize)
	if blockChCap > MaxBufPipeQueueCapacity {
		blockChCap = MaxBufPipeQueueCapacity
	}
	bp := &bufPipe{
		stor:      storager,
		blockch:   make(chan aBlock, blockChCap),
		donech:    make(chan struct{}),
		rewindch:  make(chan struct{}, 1),
		buf:       bytes.NewBuffer(nil),
		storSize:  storageSize,
		blockSize: int64(blockSize),
	}
	return &BufPipeReader{p: bp}, &BufPipeWriter{p: bp}
}

// Read implements io.Reader. BufPipeReader should call this method.
func (bp *bufPipe) Read(p []byte) (n int, err error) {
	bp.rmu.Lock()
	defer bp.rmu.Unlock()

	if bp.buf.Len() > 0 {
		return bp.buf.Read(p)
	}
	return bp.slowRead(p)
}

func (bp *bufPipe) slowRead(p []byte) (n int, err error) {
	block := <-bp.blockch
	if block.size <= 0 {
		if bp.buf.Cap() > 0 {
			bp.buf = &bytes.Buffer{} // remove reference to let GC collect the buffer sooner.
		}
		var err error
		if e := bp.readCloseError(); e != nil {
			err = e
		} else {
			err = io.EOF
		}
		return 0, err
	}

	src := io.NewSectionReader(bp.stor, block.offset, block.size)
	bp.buf.Reset()
	_, err = bp.buf.ReadFrom(src)
	if err != nil {
		_ = bp.closeRead(err)
		bp.buf.Reset()
		return 0, err
	}
	if block.offset == 0 {
		select {
		case bp.rewindch <- struct{}{}:
		default:
		}
	}
	return bp.buf.Read(p)
}

// Write implements io.Writer. BufPipeWriter should call this method.
func (bp *bufPipe) Write(p []byte) (n int, err error) {
	select {
	case <-bp.donech:
		return 0, bp.writeCloseError()
	default:
	}
	bp.wmu.Lock()
	defer bp.wmu.Unlock()

	for once := true; once || len(p) > 0; once = false {
		var nw int
		nw, err = bp.writeBlock(p)
		n += nw
		if err != nil {
			_ = bp.closeWriteErr(err, true)
			return
		}
		select {
		case <-bp.donech:
			err = bp.writeCloseError()
			return
		default:
		}
		p = p[nw:]
	}
	err = bp.sendBlock(true)
	return
}

func (bp *bufPipe) writeBlock(p []byte) (n int, err error) {
	if err := bp.sendBlock(true); err != nil {
		return 0, err
	}

	if int64(len(p)) > bp.blockSize {
		p = p[:bp.blockSize]
	}
	if v := bp.wrblock.offset + bp.wrblock.size; v+int64(len(p)) > bp.storSize {
		p = p[:bp.storSize-v]
	}
	n, err = bp.writeAtFull(p, bp.wrblock.offset+bp.wrblock.size)
	bp.wrblock.size += int64(n)
	return n, err
}

func (bp *bufPipe) sendBlock(noclosing bool) error {
	if noclosing &&
		bp.wrblock.size < bp.blockSize &&
		bp.wrblock.offset+bp.wrblock.size < bp.storSize {
		return nil
	}

	prevBlock := bp.wrblock
	select {
	case <-bp.donech:
		return bp.writeCloseError()
	case bp.blockch <- prevBlock:
	}

	bp.wrblock = aBlock{offset: prevBlock.offset + prevBlock.size}
	if bp.wrblock.offset >= bp.storSize {
		if noclosing {
			select {
			case <-bp.donech:
				return bp.writeCloseError()
			case <-bp.rewindch:
			}
			bp.wrblock.offset = 0
		}
	}
	return nil
}

func (bp *bufPipe) writeAtFull(p []byte, offset int64) (n int, err error) {
	var nw int
	for len(p) > 0 {
		nw, err = bp.stor.WriteAt(p, offset)
		n += nw
		if err != nil {
			return
		}
		p = p[nw:]
		offset += int64(nw)
	}
	return
}

func (bp *bufPipe) closeRead(err error) error {
	iserr := true
	if err == nil {
		iserr = false
		err = io.ErrClosedPipe
	}
	bp.rerr.Store(err)
	bp.doneOnce.Do(func() { close(bp.donech) })
	if iserr {
		bp.wmu.Lock()
		defer bp.wmu.Unlock()
		bp.blockOnce.Do(func() { close(bp.blockch) })
	}
	return nil
}

func (bp *bufPipe) closeWrite() error {
	var locked bool
	if bp.rerr.Load() == nil {
		locked = true
		bp.wmu.Lock()
		defer bp.wmu.Unlock()

		_ = bp.sendBlock(false)

	}
	return bp.closeWriteErr(nil, locked)
}

func (bp *bufPipe) closeWriteErr(err error, locked bool) error {
	if err == nil {
		err = io.EOF
	}
	bp.werr.Store(err)
	bp.doneOnce.Do(func() { close(bp.donech) })
	if !locked {
		bp.wmu.Lock()
		defer bp.wmu.Unlock()
	}
	bp.blockOnce.Do(func() { close(bp.blockch) })
	return nil
}

func (bp *bufPipe) readCloseError() error {
	rerr := bp.rerr.Load()
	if werr := bp.werr.Load(); rerr == nil && werr != nil {
		return werr
	}
	return io.ErrClosedPipe
}

func (bp *bufPipe) writeCloseError() error {
	werr := bp.werr.Load()
	if rerr := bp.rerr.Load(); werr == nil && rerr != nil {
		return rerr
	}
	return io.ErrClosedPipe
}

// onceError is an object that will only store an error once.
type onceError struct {
	sync.Mutex // guards following
	err        error
}

// Store stores the error if it has not been stored before.
func (a *onceError) Store(err error) {
	a.Lock()
	defer a.Unlock()
	if a.err != nil {
		return
	}
	a.err = err
}

// Load returns the stored error.
func (a *onceError) Load() error {
	a.Lock()
	defer a.Unlock()
	return a.err
}
