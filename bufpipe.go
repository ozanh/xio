package xio

import (
	"bytes"
	"io"
	"sync"
)

// BufPipeReader is the read half of a buffered pipe.
type BufPipeReader struct {
	bp *bufPipe
}

// Read implements the io.Reader interface.
// It reads data from the buffered pipe, blocking until a writer arrives or
// the write end is closed.
func (br *BufPipeReader) Read(p []byte) (n int, err error) {
	return br.bp.read(p)
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
	return bw.bp.write(p)
}

// Close closes the writer; subsequent reads from the read half of the buffered
// / pipe will return no bytes and EOF until all the buffered data is read, if
// read end is not closed.
func (bw *BufPipeWriter) Close() error {
	return bw.bp.closeWrite(nil)
}

// CloseWithError closes the writer; subsequent reads from the read half of the
// buffered pipe will return no bytes and the error err, or EOF.
func (bw *BufPipeWriter) CloseWithError(err error) error {
	return bw.bp.closeWrite(err)
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
	stor *SyncStorage

	queue *blockQueue

	// doneChan channel is closed when the reader or writer is closed.
	doneChan chan struct{}

	rmu     sync.Mutex // Serializes reads.
	buf     *bytes.Buffer
	wmu     sync.Mutex // Serializes writes
	wrblock aBlock     // The block currently being written

	rerr        onceError
	werr        onceError
	storageSize int64
	blockSize   int64
	doneOnce    sync.Once // Protects closing done
}

// BufPipe creates a buffered pipe with a given block size and storage size.
// If blockSize is zero, a default value is used. If blockSize is less than bytes.MinRead, it is set to bytes.MinRead.
// The storageSize should be a multiple of the block size.
// If storageSize is not positive, it panics.
func BufPipe(blockSize int, storageSize int64, storage Storage) (*BufPipeReader, *BufPipeWriter) {
	bsize := int64(blockSize)
	if bsize <= 0 {
		bsize = DefaultBlockSize
	} else if blockSize < bytes.MinRead {
		bsize = bytes.MinRead
	}

	if storageSize < bsize {
		bsize = storageSize
	}

	syncStorage, _ := storage.(*SyncStorage)
	if syncStorage == nil {
		syncStorage = NewSyncStorage(storage)
	}

	bp := &bufPipe{
		stor: syncStorage,
		queue: &blockQueue{
			readSignal:  make(chan struct{}, 1),
			writeSignal: make(chan struct{}, 1),
			writable:    newSegmentedSlice[int64](16),
			readable:    newSegmentedSlice[aBlock](16),
		},
		buf:         bytes.NewBuffer(nil),
		doneChan:    make(chan struct{}),
		storageSize: storageSize,
		blockSize:   bsize,
	}

	for offset := bsize; offset < storageSize; offset += bsize {
		bp.queue.writable.append(offset)
	}

	return &BufPipeReader{bp: bp}, &BufPipeWriter{bp: bp}
}

func (bp *bufPipe) read(p []byte) (int, error) {
	bp.rmu.Lock()
	defer bp.rmu.Unlock()

	select {
	case <-bp.doneChan:
		return 0, bp.readCloseError()
	default:
	}

	if bp.buf.Len() > 0 {
		return bp.buf.Read(p)
	} else if len(p) == 0 {
		return 0, nil
	}

	return bp.slowRead(p)
}

func (bp *bufPipe) slowRead(p []byte) (int, error) {
	block, err := bp.getReadable()
	if err != nil {
		return 0, err
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
			bp.enqueueWritable(block.startOffset)
		}
		return n, err
	}

	if debug {
		println("read empty block:", block.startOffset)
	}

	bp.buf.Reset()

	if e := bp.readCloseError(); e != nil {
		err = e
	} else {
		err = io.EOF
	}
	return 0, err
}

func (bp *bufPipe) getReadable() (aBlock, error) {

	for {
		block, ok := bp.queue.popReadable()
		if ok {
			return block, nil
		}
		select {
		case <-bp.doneChan:
			return aBlock{}, bp.readCloseError()
		case <-bp.queue.readSignal:
			continue
		}
	}
}

func (bp *bufPipe) enqueueWritable(offset int64) {
	bp.queue.pushWritable(offset)
	if debug {
		println("enqueued read block:", offset)
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
		if err == io.EOF {
			err = nil
		}
		return n, err
	}

	size -= int64(n)
	if size > 0 {
		src := io.NewSectionReader(bp.stor, offset+int64(n), size)
		_, err = bp.buf.ReadFrom(src)
	}
	return n, err
}

func (bp *bufPipe) write(p []byte) (n int, err error) {
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
			_ = bp.closeWrite(err)
			return
		}
		p = p[nw:]
	}

	err = bp.flushBlock()

	if debug {
		println("end of write block:", bp.wrblock.startOffset, "written:", bp.wrblock.written)
	}
	return
}

func (bp *bufPipe) writeBlock(p []byte) (int, error) {
	if err := bp.flushBlock(); err != nil {
		return 0, err
	}

	wsize := int64(len(p))
	if rem := (bp.blockSize - bp.wrblock.written); wsize > rem {
		wsize = rem
	}

	offset := bp.wrblock.endOffset()
	if offset+wsize > bp.storageSize {
		wsize = bp.storageSize - offset
	}

	n, err := bp.stor.WriteAt(p[:wsize], offset)
	bp.wrblock.written += int64(n)
	return n, err
}

func (bp *bufPipe) flushBlock() error {
	select {
	case <-bp.doneChan:
		return bp.writeCloseError()
	default:
	}

	if bp.wrblock.written < bp.blockSize &&
		bp.wrblock.endOffset() < bp.storageSize {
		return nil
	}

	if debug {
		println("send block:", bp.wrblock.startOffset, "written:", bp.wrblock.written)
	}

	bp.queue.pushReadable(bp.wrblock)

	for {
		offset, ok := bp.queue.popWritable()
		if ok {
			bp.wrblock = aBlock{startOffset: offset}
			if debug {
				println("new write block:", bp.wrblock.startOffset, "written:", bp.wrblock.written)
			}
			break
		}
		select {
		case <-bp.doneChan:
			return bp.writeCloseError()
		case <-bp.queue.writeSignal:
		}
	}

	return nil
}

func (bp *bufPipe) closeRead(err error) error {
	if err == nil {
		err = io.ErrClosedPipe
	}
	bp.rerr.store(err)
	bp.doneOnce.Do(func() { close(bp.doneChan) })
	return nil
}

func (bp *bufPipe) closeWrite(err error) error {
	if err == nil {
		bp.wmu.Lock()
		defer bp.wmu.Unlock()

		bp.queue.pushReadable(bp.wrblock)
		if bp.wrblock != (aBlock{}) {
			bp.queue.pushReadable(aBlock{})
		}
		bp.werr.store(io.EOF)
		return nil
	}
	bp.werr.store(err)
	bp.doneOnce.Do(func() { close(bp.doneChan) })
	return nil
}

func (bp *bufPipe) readCloseError() error {
	rerr := bp.rerr.load()
	if werr := bp.werr.load(); rerr == nil && werr != nil {
		return werr
	}
	return io.ErrClosedPipe
}

func (bp *bufPipe) writeCloseError() error {
	werr := bp.werr.load()
	if rerr := bp.rerr.load(); werr == nil && rerr != nil {
		return rerr
	}
	return io.ErrClosedPipe
}

// onceError is an object that will only store an error once.
type onceError struct {
	sync.Mutex // guards following
	err        error
}

// store stores the error if it has not been stored before.
func (a *onceError) store(err error) {
	a.Lock()
	defer a.Unlock()

	if a.err != nil {
		return
	}
	a.err = err
}

// load returns the stored error.
func (a *onceError) load() error {
	a.Lock()
	defer a.Unlock()

	return a.err
}

type blockQueue struct {
	readSignal  chan struct{}
	writeSignal chan struct{}
	mu          sync.Mutex
	writable    *segmentedSlice[int64]
	readable    *segmentedSlice[aBlock]
}

func (b *blockQueue) popReadable() (aBlock, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.readable.popFirst()
}

func (b *blockQueue) popWritable() (int64, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.writable.popFirst()
}

func (b *blockQueue) pushWritable(offset int64) {
	b.mu.Lock()
	b.writable.append(offset)
	b.mu.Unlock()

	select {
	case b.writeSignal <- struct{}{}:
	default:
	}
}

func (b *blockQueue) pushReadable(block aBlock) {
	b.mu.Lock()
	b.readable.append(block)
	b.mu.Unlock()

	select {
	case b.readSignal <- struct{}{}:
	default:
	}
}

type segmentedSlice[T any] struct {
	segments [][]T
	segSize  int
}

func newSegmentedSlice[T any](segSize int) *segmentedSlice[T] {
	return &segmentedSlice[T]{segSize: segSize}
}

func (s *segmentedSlice[T]) append(item T) {
	last := len(s.segments) - 1
	if last < 0 || len(s.segments[last]) >= s.segSize {
		s.segments = append(s.segments, make([]T, 0, s.segSize))
		last++
	}
	s.segments[last] = append(s.segments[last], item)
}

func (s *segmentedSlice[T]) popFirst() (T, bool) {
	if len(s.segments) == 0 {
		var zero T
		return zero, false
	}

	seg := s.segments[0]
	if len(seg) == 0 {
		s.segments = append(s.segments[:0], s.segments[1:]...)
		return s.popFirst()
	}

	item := seg[0]
	s.segments[0] = seg[1:]
	return item, true
}
