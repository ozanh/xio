package xio

import (
	"bytes"
	"io"
	"sync"
)

const maxBufPipeReadQueueCap = 2048

// BufPipeReader is the read half of a buffered pipe.
type BufPipeReader struct {
	bp *bufPipe
}

// Read implements the io.Reader interface.
// It reads data from the buffered pipe, blocking until a writer arrives or
// the write end is closed.
func (br *BufPipeReader) Read(p []byte) (n int, err error) {
	return br.bp.Read(p)
}

// Close closes the reader; subsequent writes to the write half of the buffered
// pipe will return the error ErrClosedPipe.
func (br *BufPipeReader) Close() error {
	return br.CloseWithError(nil)
}

// CloseWithError closes the reader; subsequent writes to the write half of
// the buffered pipe will return the error err.
func (br *BufPipeReader) CloseWithError(err error) error {
	return br.bp.closeRead(err)
}

// BufPipeWriter is the write half of a buffered pipe.
type BufPipeWriter struct {
	bp *bufPipe
}

// Write implements the io.Writer interface: it writes data to the underlying
// buffered storage.
func (bw *BufPipeWriter) Write(p []byte) (n int, err error) {
	return bw.bp.Write(p)
}

// Close closes the writer; subsequent reads from the read half of the buffered
// / pipe will return no bytes and EOF until all the buffered data is read, if
// read end is not closed.
func (bw *BufPipeWriter) Close() error {
	return bw.bp.closeWriteErr(nil)
}

// CloseWithError closes the writer; subsequent reads from the read half of the
// buffered pipe will return no bytes and the error err, or EOF.
func (bw *BufPipeWriter) CloseWithError(err error) error {
	return bw.bp.closeWriteErr(err)
}

// aBlock represents a block of data in the storage.
type aBlock struct {
	startOffset int64
	written     int64
}

func (a *aBlock) endOffset() int64 {
	return a.startOffset + a.written
}

// bufPipe is a buffered pipe that uses a storage as a backing store.
type bufPipe struct {
	stor Storage
	// rdQueue channel holds blocks available for reading.
	rdQueue chan aBlock
	// wrQueue channel holds offsets available for writing.
	wrQueue chan int64
	// done channel is closed when the reader or writer is closed.
	done chan struct{}

	rmu     sync.Mutex // Serializes reads.
	buf     bytes.Buffer
	wmu     sync.Mutex // Serializes writes
	wrblock aBlock     // The block currently being written

	rerr      onceError
	werr      onceError
	storSize  int64
	blockSize int64
	doneOnce  sync.Once // Protects closing done
}

// BufPipe creates a buffered pipe with a given block size and storage size.
// If blockSize is zero, a default value is used. If blockSize is less than bytes.MinRead, it is set to bytes.MinRead.
// The storageSize should be a multiple of the block size.
// If storageSize is not positive, it panics.
func BufPipe(blockSize int, storageSize int64, storager Storage) (*BufPipeReader, *BufPipeWriter) {
	bsize := int64(blockSize)
	if bsize <= 0 {
		bsize = DefaultBlockSize
	} else if blockSize < bytes.MinRead {
		bsize = bytes.MinRead
	}
	if storageSize < bsize {
		bsize = storageSize
	}
	qcap := storageSize / bsize
	if storageSize%bsize != 0 {
		qcap++
	}
	wrQueue := make(chan int64, qcap)
	if qcap > maxBufPipeReadQueueCap {
		qcap = maxBufPipeReadQueueCap
	}
	for offset := bsize; offset < storageSize; offset += bsize {
		wrQueue <- offset
	}

	bp := &bufPipe{
		stor:      storager,
		rdQueue:   make(chan aBlock, qcap),
		wrQueue:   wrQueue,
		done:      make(chan struct{}),
		storSize:  storageSize,
		blockSize: bsize,
	}
	return &BufPipeReader{bp: bp}, &BufPipeWriter{bp: bp}
}

// Read implements io.Reader. BufPipeReader should call this method.
func (bp *bufPipe) Read(p []byte) (int, error) {
	bp.rmu.Lock()
	defer bp.rmu.Unlock()

	if bp.buf.Len() > 0 {
		return bp.buf.Read(p)
	}
	return bp.slowRead(p)
}

func (bp *bufPipe) slowRead(p []byte) (int, error) {
	var block aBlock
	select {
	case block = <-bp.rdQueue:
	case <-bp.done:
		if err := bp.rerr.Load(); err != nil {
			return 0, err
		}
		// Try one more time to read from the queue,
		// in case a write happened between the last read and the done.
		select {
		case block = <-bp.rdQueue:
		default:
		}
	}

	if block.written > 0 {
		if debug {
			println("reading block:", block.startOffset)
		}
		n, err := bp.readAndFill(block.startOffset, block.written, p)
		if debug {
			println("read block:", block.startOffset, "n:", n)
		}
		if err != nil {
			_ = bp.closeRead(err)
		} else {
			bp.enqueueRead(block.startOffset)
		}
		return n, err
	}
	if debug {
		println("read empty block:", block.startOffset)
	}
	bp.buf.Reset()
	var err error
	if e := bp.readCloseError(); e != nil {
		err = e
	} else {
		err = io.EOF
	}
	return 0, err
}

func (bp *bufPipe) enqueueRead(offset int64) {
	select {
	case <-bp.done:
	case bp.wrQueue <- offset:
		if debug {
			println("enqueued read block:", offset)
		}
	}
}

func (bp *bufPipe) readAndFill(offset, size int64, p []byte) (int, error) {
	bp.buf.Reset()

	n := len(p)
	if int64(n) > size {
		n = int(size)
	}
	n, err := bp.stor.ReadAt(p[:n], offset)
	if err != nil {
		return n, err
	}
	size -= int64(n)
	if size > 0 {
		src := io.NewSectionReader(bp.stor, offset+int64(n), size)
		_, err = bp.buf.ReadFrom(src)
	}
	return n, err
}

// Write implements io.Writer. BufPipeWriter should call this method.
func (bp *bufPipe) Write(p []byte) (n int, err error) {
	select {
	case <-bp.done:
		return 0, bp.writeCloseError()
	default:
	}
	bp.wmu.Lock()
	defer bp.wmu.Unlock()
	if debug {
		println("current write block:", bp.wrblock.startOffset, "written:", bp.wrblock.written)
	}

	for once := true; once || len(p) > 0; once = false {
		var nw int
		nw, err = bp.writeBlock(p)
		n += nw
		if err != nil {
			_ = bp.closeWriteErr(err)
			return
		}
		select {
		case <-bp.done:
			err = bp.writeCloseError()
			return
		default:
		}
		p = p[nw:]
	}
	err = bp.sendBlock(true)
	return
}

func (bp *bufPipe) writeBlock(p []byte) (int, error) {
	if err := bp.sendBlock(true); err != nil {
		return 0, err
	}

	wsize := int64(len(p))
	if rem := (bp.blockSize - bp.wrblock.written); wsize > rem {
		wsize = rem
	}
	offset := bp.wrblock.endOffset()
	if offset+wsize > bp.storSize {
		wsize = bp.storSize - offset
	}
	n, err := bp.stor.WriteAt(p[:wsize], offset)
	bp.wrblock.written += int64(n)
	return n, err
}

func (bp *bufPipe) sendBlock(noclosing bool) error {
	if noclosing &&
		bp.wrblock.written < bp.blockSize &&
		bp.wrblock.endOffset() < bp.storSize {
		return nil
	}
	if debug {
		println("send block:", bp.wrblock.startOffset, "written:", bp.wrblock.written)
	}
	select {
	case bp.rdQueue <- bp.wrblock:
	case <-bp.done:
		return bp.writeCloseError()
	}
	if !noclosing {
		return nil
	}

	select {
	case <-bp.done:
		return bp.writeCloseError()
	case offset := <-bp.wrQueue:
		bp.wrblock = aBlock{startOffset: offset}
	}
	if debug {
		println("new write block:", bp.wrblock.startOffset, "written:", bp.wrblock.written)
	}
	return nil
}

func (bp *bufPipe) closeRead(err error) error {
	if err == nil {
		err = io.ErrClosedPipe
	}
	bp.rerr.Store(err)
	bp.doneOnce.Do(func() { close(bp.done) })
	return nil
}

func (bp *bufPipe) closeWriteErr(err error) error {
	if err == nil && bp.rerr.Load() == nil {
		bp.wmu.Lock()
		_ = bp.sendBlock(false)
		bp.wmu.Unlock()
	}
	if err == nil {
		err = io.EOF
	}
	bp.werr.Store(err)
	bp.doneOnce.Do(func() { close(bp.done) })
	return nil
}

func (bp *bufPipe) readCloseError() error {
	rerr := bp.rerr.Load()
	if rerr == nil {
		if werr := bp.werr.Load(); werr != nil {
			return werr
		}
		return nil
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
