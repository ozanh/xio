package xio

import (
	"bytes"
	"io"
	"sync"
)

// BufPipeReader is the read side of a buffered pipe.
// Close() or CloseWithError() should be called when the reader is no longer
// needed.
type BufPipeReader struct {
	bp *bufPipe
}

// Read implements the io.Reader interface.
// It reads written data from the buffer, and blocks until a written block
// arrives from the write side, or the read/write close happens.
// If write side closes normally (without an error), read side will receive
// io.EOF after reading the buffered data.
func (br *BufPipeReader) Read(p []byte) (n int, err error) {
	return br.bp.read(p)
}

// Close closes the reader; subsequent writes to the write side of the buffered
// pipe will return the error io.ErrClosedPipe.
//
// Normally, receiving io.ErrClosedPipe error in the write side means that
// read side is closed without an error, maybe not interested in reading
// anymore.
//
// Close() always returns nil error.
// See CloseWithError() for closing with an error, and an example.
func (br *BufPipeReader) Close() error {
	return br.CloseWithError(nil)
}

// CloseWithError closes the reader; subsequent writes to the write side of
// the buffered pipe will receive the provided err.
//
// Calling CloseWithError(nil) is the same as calling Close().
// CloseWithError() always returns nil error.
//
// Example:
//
//		pr, pw := BufPipe(/*...*/)
//
//	 	// ...
//
//		n, err := io.Copy(dest, pr)
//	 	pipeReader.CloseWithError(err)
//
// CloseWithError(err) is more ergonomic over Close() with io.Copy().
//
// Subsequest Read() after Close() or CloseWithError() will receive
// io.ErrClosedPipe.
func (br *BufPipeReader) CloseWithError(err error) error {
	return br.bp.closeRead(err)
}

// BufPipeWriter is the write side of a buffered pipe.
type BufPipeWriter struct {
	bp *bufPipe
}

// Write implements the io.Writer interface: it writes data to the underlying
// storage. After filling the current block, it enqueues the block for the read
// side, and tries to continue writing to the next available block, which is
// provided by the read side.
//
// Returning n (number of bytes written) means that the data is written to the
// underlying storage, read side reads the data from the storage asynchronously.
// To flush the last block to the storage, call Close() otherwise the last block
// will not be seen by the read side.
func (bw *BufPipeWriter) Write(p []byte) (n int, err error) {
	return bw.bp.write(p)
}

// Close closes the writer and flushes the last block; subsequent reads from the
// read side of the buffered pipe will receive io.EOF until all the buffered
// data is read, if read side is not closed in between.
//
// Close() must be called when the writer is no longer needed because it flushes
// the last block to the storage, and read side cannot stop reading until a
// close happens in the write or read side.
//
// Close must happen after all the writes are done. Subsequent Write() after
// Close() will receive io.ErrClosedPipe error.
//
// Close() always returns nil error.
//
// Calling CloseWithError(nil) is the same as calling Close().
// See CloseWithError() for closing with an error, and an example.
func (bw *BufPipeWriter) Close() error {
	return bw.bp.closeWrite(nil)
}

// CloseWithError closes the writer; subsequent reads from the read side of the
// buffered pipe will receive this error err.
// If err is nil, io.EOF is used, which is the same as calling Close().
//
// CloseWithError() with a non-nil error can be called anytime to stop read and
// write operations. Note that it does not flush the last block for the read
// side. Closing is only taken into account from the Write() before actual write
// to the underlying storage.
//
// CloseWithError() always returns nil error.
//
// Example:
//
//		pr, pw := BufPipe(/*...*/)
//
//	 	// ...
//
//		n, err := io.Copy(pw, source)
//	 	pw.CloseWithError(err)
//
// Subsequent Write() after Close() or CloseWithError() will receive
// io.ErrClosedPipe.
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

// bufPipe is a buffered pipe implementation that uses a storage as a backing
// store. It is inspired by the io.Pipe implementation from the Go standard
// library to improve I/O performance for large data transfers.
// It uses the underlying storage to buffer the data between the reader and
// writer.
type bufPipe struct {
	stor Storage

	// queue is a queue of blocks that are readable and writable.
	queue *blockQueue

	// doneChan channel is closed when the reader or writer is closed.
	doneChan chan struct{}

	rmu     sync.Mutex  // Serializes reads.
	sbuf    *sectionBuf // Buffer for reads.
	wmu     sync.Mutex  // Serializes writes.
	wrblock aBlock      // The block currently being written.

	rerr        onceError
	werr        onceError
	storageSize int64
	blockSize   int64
	doneOnce    sync.Once // Protects closing done.
}

// BufPipe creates a buffered pipe with a given block size and storage size.
// It was inspired by the io.Pipe function from the Go standard library.
//
// The blockSize should be at least 512 bytes and storageSize should be at least
// blockSize.
// If blockSize is <= 0, a default value is used, DefaultBlockSize 1MiB.
//
// If storageSize is <= 0, it is set to blockSize.
// If storageSize is less than blockSize then blockSize is capped to
// storageSize for simplicity but in terms of performance and correctness, you
// should provide appropriate values.
func BufPipe(blockSize int, storageSize int64, storage Storage) (*BufPipeReader, *BufPipeWriter) {
	bsize := int64(blockSize)
	if bsize <= 0 {
		bsize = DefaultBlockSize
	}

	if storageSize <= 0 {
		storageSize = bsize
	}

	if storageSize < bsize {
		bsize = storageSize
	}

	bp := &bufPipe{
		stor:        storage,
		queue:       newDefaultBlockQueue(),
		sbuf:        newSectionBuf(int(blockSize), storage),
		doneChan:    make(chan struct{}),
		storageSize: storageSize,
		blockSize:   bsize,
	}

	for offset := bsize; offset < storageSize; offset += bsize {
		bp.queue.writable.append(offset)
	}

	return &BufPipeReader{bp: bp}, &BufPipeWriter{bp: bp}
}

func (bp *bufPipe) read(p []byte) (n int, err error) {
	bp.rmu.Lock()
	defer bp.rmu.Unlock()

	for once := true; once || len(p) > 0; once = false {
		var nr int
		nr, err = bp.readBlock(p)
		n += nr
		if err != nil {
			break
		}
		p = p[nr:]
	}
	return
}

func (bp *bufPipe) readBlock(p []byte) (int, error) {
	select {
	case <-bp.doneChan:
		return 0, bp.readCloseError()
	default:
	}

	if bp.sbuf.Len() > 0 {
		return bp.sbuf.Read(p)
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
		n, err := bp.sbuf.ReadFillAt(p, block.startOffset, block.written)
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

	bp.sbuf.Reset()

	if e := bp.readCloseError(); e != nil {
		err = e
	} else {
		err = io.EOF
	}
	return 0, err
}

func (bp *bufPipe) getReadable() (aBlock, error) {

	for {
		block, ok := bp.queue.PopReadable()
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
	bp.queue.PushWritable(offset)
	if debug {
		println("enqueued read block:", offset)
	}
}

func (bp *bufPipe) write(p []byte) (n int, err error) {
	bp.wmu.Lock()
	defer bp.wmu.Unlock()

	if debug {
		println("current write block:", bp.wrblock.startOffset, "written:", bp.wrblock.written)
	}

	// If the write is closed with io.EOF, this write request is invalid,
	// return error.
	// Note that closing with io.EOF does not close the done channel, so the
	// read end can still read the data.
	if bp.werr.load() != nil {
		return 0, io.ErrClosedPipe
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

	bp.queue.PushReadable(bp.wrblock)

	for {
		offset, ok := bp.queue.PopWritable()
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
	var eof bool

	if err == nil {
		eof = true
		err = io.EOF
	}

	stored := bp.werr.store(err)
	if !stored {
		// Another error is already stored.
		return nil
	}
	if !eof {
		bp.doneOnce.Do(func() { close(bp.doneChan) })
		return nil
	}

	bp.wmu.Lock()
	defer bp.wmu.Unlock()

	defer bp.queue.CloseRead()

	bp.queue.PushReadable(bp.wrblock)
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

type sectionBuf struct {
	b *bytes.Buffer
	s *io.SectionReader
	l io.LimitedReader
}

func newSectionBuf(blockSize int, r io.ReaderAt) *sectionBuf {
	const maxInt64 = 1<<63 - 1
	const maxInt32 = 1<<31 - 1

	bufSize := blockSize
	if bufSize+bytes.MinRead <= maxInt32 {
		if bufSize <= bytes.MinRead {
			bufSize = bytes.MinRead
		} else if rem := bufSize % bytes.MinRead; rem != 0 {
			bufSize += bytes.MinRead - rem
		}
	}

	return &sectionBuf{
		b: bytes.NewBuffer(make([]byte, 0, bufSize)),
		s: io.NewSectionReader(r, 0, maxInt64),
	}
}
func (s *sectionBuf) Reset() {
	s.b.Reset()
}

func (s *sectionBuf) Len() int {
	return s.b.Len()
}

func (s *sectionBuf) Read(p []byte) (int, error) {
	return s.b.Read(p)
}

func (s *sectionBuf) ReadFillAt(p []byte, off, size int64) (int, error) {
	s.b.Reset()

	n := len(p)
	if int64(n) > size {
		n = int(size)
	}

	n, err := s.s.ReadAt(p[:n], off)
	if err != nil {
		if err == io.EOF {
			err = nil
		}
		return n, err
	}

	remaining := size - int64(n)
	if remaining > 0 {
		_, err = s.s.Seek(off+int64(n), io.SeekStart)
		if err != nil {
			return n, err
		}

		s.l.R = s.s
		s.l.N = remaining

		_, err = s.b.ReadFrom(&s.l)
	}

	return n, err
}

// onceError is an object that will only store an error once.
type onceError struct {
	sync.Mutex // guards following
	err        error
}

// store stores the error if it has not been stored before, and reports whether
// the error was stored.
func (a *onceError) store(err error) bool {
	a.Lock()
	defer a.Unlock()

	if a.err != nil {
		return false
	}
	a.err = err
	return true
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
	readClosed  bool
}

func newDefaultBlockQueue() *blockQueue {
	return &blockQueue{
		readSignal:  make(chan struct{}, 1),
		writeSignal: make(chan struct{}, 1),
		writable:    newSegmentedSlice[int64](16),
		readable:    newSegmentedSlice[aBlock](16),
	}
}

func (b *blockQueue) PopReadable() (aBlock, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()

	block, ok := b.readable.popFirst()
	ok = ok || b.readClosed
	return block, ok
}

func (b *blockQueue) PopWritable() (int64, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.writable.popFirst()
}

func (b *blockQueue) PushWritable(offset int64) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.writable.append(offset)

	select {
	case b.writeSignal <- struct{}{}:
	default:
	}
}

func (b *blockQueue) PushReadable(block aBlock) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.readClosed {
		return
	}

	b.readable.append(block)

	select {
	case b.readSignal <- struct{}{}:
	default:
	}
}

func (b *blockQueue) CloseRead() {
	b.mu.Lock()
	defer b.mu.Unlock()

	if !b.readClosed {
		b.readClosed = true
		close(b.readSignal)
	}
}

type segmentedSlice[T any] struct {
	segments []*segment[T]
	segSize  int
}

func newSegmentedSlice[T any](segSize int) *segmentedSlice[T] {
	return &segmentedSlice[T]{
		segments: make([]*segment[T], 0, segSize),
		segSize:  segSize,
	}
}

func (s *segmentedSlice[T]) grow() {
	if cap(s.segments) > len(s.segments) {
		s.segments = s.segments[:len(s.segments)+1]

		if s.segments[len(s.segments)-1] != nil {
			return
		}
		s.segments[len(s.segments)-1] = newSegment[T](s.segSize)
		return
	}

	s.segments = append(s.segments, newSegment[T](s.segSize))
}

func (s *segmentedSlice[T]) append(item T) {
	last := len(s.segments) - 1
	if last < 0 || s.segments[last].count() >= s.segSize {
		s.grow()
		last++
	}
	s.segments[last].append(item)
}

func (s *segmentedSlice[T]) popFirst() (T, bool) {
	if len(s.segments) == 0 {
		var zero T
		return zero, false
	}

	seg := s.segments[0]
	if seg.notEmpty() {
		return seg.popHead(), true
	}

	copy(s.segments, s.segments[1:])

	if cap(s.segments) > s.segSize {
		seg = nil
	} else {
		seg.reset()
	}

	s.segments[len(s.segments)-1] = seg
	s.segments = s.segments[:len(s.segments)-1]

	if seg == nil && len(s.segments) < cap(s.segments)/2 {
		s.segments = append(s.segments[:0:0], s.segments...)
	}
	return s.popFirst()
}

type segment[T any] struct {
	slice []T
	head  int
}

func newSegment[T any](capacity int) *segment[T] {
	return &segment[T]{
		slice: make([]T, 0, capacity),
	}
}

func (s *segment[T]) count() int {
	return len(s.slice) - s.head
}

func (s *segment[T]) notEmpty() bool {
	return s.head < len(s.slice)
}

func (s *segment[T]) popHead() (item T) {
	item = s.slice[s.head]
	s.head++

	if s.head == len(s.slice) {
		s.reset()
	}
	return
}

func (s *segment[T]) append(item T) {
	if s.head != 0 && s.count() == 0 {
		s.reset()
	}
	s.slice = append(s.slice, item)
}

func (s *segment[T]) reset() {
	s.slice = s.slice[:0]
	s.head = 0
}
