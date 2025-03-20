package xio

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/bits"
	"sync"

	"github.com/elastic/go-freelru"
)

var errNegativeReadAt = errors.New("xio: reader at returned negative count")

// LruReaderAt wraps an io.ReaderAt and caches its data in an LRU cache.
// It is designed for reading random offsets efficiently, making it suitable
// for scenarios where repeated reads from non-contiguous regions occur.
// Use NewLruReaderAt to create an instance for your underlying reader.
// Underlying reader should not be modified while the LruReaderAt is in use to
// avoid data inconsistency.
//
// It is safe for concurrent use, but method calls are synchronized with a
// mutex.
type LruReaderAt[T io.ReaderAt] struct {
	reader    T
	cache     *freelru.LRU[uint64, []byte]
	pool      *sync.Pool
	blockSize int
	cacheSize int
	shift     uint32
	mask      int64
	mu        sync.Mutex
	metrics   lruReaderAtMetrics
	eofIndex  uint64
	eofSeen   bool
}

// LruReaderAtMetrics contains the metrics of an LruReaderAt.
type LruReaderAtMetrics struct {
	CacheInserts    uint64
	CacheCollisions uint64
	CacheEvictions  uint64
	CacheRemovals   uint64
	CacheHits       uint64
	CacheMisses     uint64
	CacheHitBytes   uint64
	PoolAllocs      uint64
	CachedCount     uint64
}

type lruReaderAtMetrics struct {
	cacheHitBytes uint64
	poolAllocs    uint64
}

// NewLruReaderAt creates a new CachingReaderAt with the given reader, blockSize, and cacheSize.
// blockSize and cacheSize should be a power of 2 for better performance.
// blockSize and cacheSize must be greater than 0.
// blockSize and cacheSize must be less than MaxUint32.
func NewLruReaderAt[T io.ReaderAt](reader T, blockSize, cacheSize int) (*LruReaderAt[T], error) {
	const maxUint32 = 1<<32 - 1

	if blockSize <= 0 {
		return nil, errors.New("xio: LruReaderAt: blockSize must be greater than 0")
	}
	if uint64(blockSize) >= maxUint32 {
		return nil, errors.New("xio: LruReaderAt: blockSize must be less than MaxUint32")
	}

	if cacheSize <= 0 {
		return nil, errors.New("xio: LruReaderAt: cacheSize must be greater than 0")
	}
	if uint64(cacheSize) >= maxUint32 {
		return nil, errors.New("xio: LruReaderAt: cacheSize must be less than MaxUint32")
	}

	cache, err := freelru.New[uint64, []byte](uint32(cacheSize), lruHash)
	if err != nil {
		return nil, fmt.Errorf("xio: LruReaderAt: lru error: %w", err)
	}

	lra := &LruReaderAt[T]{
		reader:    reader,
		cache:     cache,
		pool:      &sync.Pool{},
		blockSize: blockSize,
		cacheSize: cacheSize,
	}

	lra.pool.New = func() any {
		// Mutex is already held by the caller.
		lra.metrics.poolAllocs++
		b := make([]byte, blockSize)
		return &b
	}

	cache.SetOnEvict(func(k uint64, v []byte) {
		// Mutex is already held by the caller.
		if lra.eofSeen && k == lra.eofIndex {
			lra.eofIndex = 0
			lra.eofSeen = false
		}
		lra.putBuffer(v)
	})

	if bits.OnesCount32(uint32(blockSize)) == 1 {
		lra.shift = uint32(bits.TrailingZeros32(uint32(blockSize)))
		lra.mask = int64(blockSize) - 1
	}

	return lra, nil
}

// Reset resets the LruReaderAt with a new reader and purges the cache for
// reuse.
func (lra *LruReaderAt[T]) Reset(reader T) {
	lra.mu.Lock()
	defer lra.mu.Unlock()

	lra.reader = reader
	lra.purge()
}

// Purge purges the underlying lru cache, and resets the metrics.
func (lra *LruReaderAt[T]) Purge() {
	lra.mu.Lock()
	defer lra.mu.Unlock()

	lra.purge()
}

func (lra *LruReaderAt[T]) purge() {
	lra.cache.Purge()
	lra.metrics = lruReaderAtMetrics{}
	lra.eofIndex = 0
	lra.eofSeen = false
}

// ReadAt implements the io.ReaderAt and reads len(p) bytes into p starting at
// offset, using the cache where possible.
// If a cached block doesn’t fully satisfy the request, it reads the remainder
// from the underlying reader.
//
// It returns ErrShortRead if the read operation could not read the requested
// number of bytes and the underlying reader did not return io.EOF or other
// error.
//
// If number of read bytes is equal to the len(p), it always returns nil error
// if EOF was reached.
func (lra *LruReaderAt[T]) ReadAt(p []byte, offset int64) (n int, err error) {
	if offset < 0 {
		return 0, errors.New("xio: LruReaderAt: negative offset")
	}
	if len(p) == 0 {
		return 0, nil
	}

	lra.mu.Lock()
	defer lra.mu.Unlock()

	n, err = lra.readAt(p, offset)

	if len(p) == n && err == io.EOF {
		err = nil
	}

	return
}

func (lra *LruReaderAt[T]) readAt(p []byte, offset int64) (int, error) {

	var blockIndex uint64
	var blockOffset int
	var blockStart int64

	if lra.mask != 0 {
		blockIndex = uint64(offset >> lra.shift)
		blockOffset = int(offset & lra.mask)
		blockStart = offset &^ lra.mask
	} else {
		blockIndex = uint64(offset / int64(lra.blockSize))
		blockOffset = int(offset % int64(lra.blockSize))
		blockStart = offset - int64(blockOffset)
	}

	if debug {
		println(
			"lra:", lra, ", offset:", offset, ", blockIndex:", blockIndex,
			", blockOffset:", blockOffset, ", blockStart:", blockStart,
			", blockSize:", lra.blockSize, ", len(p):", len(p),
		)
	}

	totalRead := 0
	remaining := p

	twoBlocks := 2 * int64(lra.blockSize)

	for totalRead < len(p) {

		if blockBuf, ok := lra.cache.Get(blockIndex); ok &&
			blockOffset <= len(blockBuf) {

			n := copy(remaining, blockBuf[blockOffset:])
			totalRead += n
			remaining = remaining[n:]

			lra.metrics.cacheHitBytes += uint64(n)

			if totalRead == len(p) {
				return totalRead, nil
			}

			if lra.eofSeen &&
				blockIndex == lra.eofIndex &&
				n == len(blockBuf)-blockOffset {

				return totalRead, io.EOF
			}

			blockIndex++
			blockOffset = 0
			blockStart += int64(lra.blockSize)
			continue
		}

		var readBuf []byte
		var direct bool

		if blockOffset == 0 && int64(len(remaining)) >= twoBlocks {

			count := lra.countBlocksBeforeCacheHit(len(remaining), blockIndex+1)

			if count > 1 {
				readBuf = remaining[:count*lra.blockSize]
				direct = true
			}
		}
		if !direct {
			readBuf = lra.getBuffer()
		}

		nRead, err := lra.reader.ReadAt(readBuf, blockStart)

		if err == nil && len(readBuf) != nRead {
			err = ErrShortRead
		}

		if debug {
			println(
				"lra:", lra, ", blockIndex:", blockIndex, ", blockStart:",
				blockStart, ", nRead:", nRead, ", readErr:", errString(err),
				", readBufSize:", len(readBuf), ", totalRead:", totalRead,
				", remainingSize:", len(remaining),
			)
		}
		if nRead < 0 {
			panic(errNegativeReadAt)
		}

		if nRead <= blockOffset {
			if direct || nRead <= 0 {
				totalRead += nRead
				return totalRead, err
			}

			if err == io.EOF {
				lra.eofIndex = blockIndex
				lra.eofSeen = true
				lra.cache.Add(blockIndex, readBuf[:nRead])
			} else {
				lra.putBuffer(readBuf)
			}

			return totalRead, err
		}

		if direct {
			totalRead += nRead
			remaining = remaining[nRead:]

			for nRead > lra.blockSize {
				lra.removeIfFull()

				blockBuf := lra.getBuffer()

				copy(blockBuf, readBuf[:lra.blockSize])

				lra.cache.Add(blockIndex, blockBuf)
				readBuf = readBuf[lra.blockSize:]

				nRead -= lra.blockSize

				blockIndex++
				blockStart += int64(lra.blockSize)
			}

			if nRead > 0 {
				if nRead == lra.blockSize || err == io.EOF {
					lra.removeIfFull()

					blockBuf := lra.getBuffer()

					n := copy(blockBuf, readBuf[:nRead])
					lra.cache.Add(blockIndex, blockBuf[:n])

					if err == io.EOF {
						lra.eofIndex = blockIndex
						lra.eofSeen = true
					}
				}
			}

		} else {
			n := copy(remaining, readBuf[blockOffset:nRead])
			totalRead += n
			remaining = remaining[n:]

			if err == io.EOF {
				lra.cache.Add(blockIndex, readBuf[:nRead])

				lra.eofIndex = blockIndex
				lra.eofSeen = true

				if n < (nRead - blockOffset) {
					err = nil
				}
			} else if nRead == lra.blockSize {
				lra.cache.Add(blockIndex, readBuf)
			} else {
				lra.putBuffer(readBuf)
			}
		}

		if err != nil || nRead == 0 || len(remaining) == 0 {
			return totalRead, err
		}

		blockIndex++
		blockOffset = 0
		blockStart += int64(lra.blockSize)
	}

	return totalRead, nil
}

func (lra *LruReaderAt[T]) countBlocksBeforeCacheHit(bufSize int, nextIndex uint64) int {
	var count int
	for {
		if lra.cache.Contains(nextIndex) {
			break
		}

		bufSize -= lra.blockSize
		if bufSize < 0 {
			break
		}

		count++
		if bufSize == 0 {
			break
		}
		nextIndex++
	}
	return count
}

// Metrics returns the current metrics of the LruReaderAt.
// Purge resets these metrics.
func (lra *LruReaderAt[T]) Metrics() LruReaderAtMetrics {
	lra.mu.Lock()
	defer lra.mu.Unlock()

	lruMetrics := lra.cache.Metrics()

	return LruReaderAtMetrics{
		CacheInserts:    lruMetrics.Inserts,
		CacheCollisions: lruMetrics.Collisions,
		CacheEvictions:  lruMetrics.Evictions,
		CacheRemovals:   lruMetrics.Removals,
		CacheHits:       lruMetrics.Hits,
		CacheMisses:     lruMetrics.Misses,
		CacheHitBytes:   lra.metrics.cacheHitBytes,
		PoolAllocs:      lra.metrics.poolAllocs,
		CachedCount:     uint64(lra.cache.Len()),
	}
}

func (lra *LruReaderAt[T]) removeIfFull() {
	if lra.cache.Len() >= lra.cacheSize {
		lra.cache.RemoveOldest()
	}
}

func (lra *LruReaderAt[T]) getBuffer() []byte {
	b := *lra.pool.Get().(*[]byte)
	return b[:lra.blockSize]
}

func (lra *LruReaderAt[T]) putBuffer(b []byte) {
	if cap(b) == lra.blockSize {
		lra.pool.Put(&b)
	}
}

func lruHash(key uint64) uint32 {
	// FNV-1a hash
	const prime32 = 16777619
	const offset32 = 2166136261

	b := [8]byte{}
	binary.LittleEndian.PutUint64(b[:], key)

	h := uint32(offset32)
	h = (h ^ uint32(b[0])) * prime32
	h = (h ^ uint32(b[1])) * prime32
	h = (h ^ uint32(b[2])) * prime32
	h = (h ^ uint32(b[3])) * prime32
	h = (h ^ uint32(b[4])) * prime32
	h = (h ^ uint32(b[5])) * prime32
	h = (h ^ uint32(b[6])) * prime32
	h = (h ^ uint32(b[7])) * prime32
	return h
}

func errString(err error) string {
	if err == nil {
		return "<nil>"
	}
	return err.Error()
}
