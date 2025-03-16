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
// avoid data corruption.
//
// It is safe for concurrent use.
type LruReaderAt[T io.ReaderAt] struct {
	reader    T
	cache     *freelru.LRU[uint64, []byte]
	pool      *sync.Pool
	blockSize int
	shift     uint32
	mask      int64
	mu        sync.Mutex
	eofIndex  uint64
	metrics   lruReaderAtMetrics
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
	}

	lra.pool.New = func() any {
		// Mutex is already held by the caller.
		lra.metrics.poolAllocs++
		b := make([]byte, blockSize)
		return &b
	}

	cache.SetOnEvict(func(_ uint64, v []byte) { lra.putBuffer(v) })

	if bits.OnesCount32(uint32(blockSize)) == 1 {
		lra.shift = uint32(bits.TrailingZeros32(uint32(blockSize)))
		lra.mask = int64(blockSize) - 1
	}

	return lra, nil
}

// Purge purges the underlying lru cache, and resets the metrics.
func (lra *LruReaderAt[T]) Purge() {
	lra.mu.Lock()
	defer lra.mu.Unlock()

	lra.cache.Purge()
	lra.eofIndex = 0
	lra.metrics = lruReaderAtMetrics{}
}

// ReadAt reads len(p) bytes into p starting at offset, using the cache where possible.
// If a cached block doesn’t fully satisfy the request, it reads the remainder
// from the underlying reader.
func (lra *LruReaderAt[T]) ReadAt(p []byte, offset int64) (int, error) {
	if offset < 0 {
		return 0, io.EOF
	}
	if len(p) == 0 {
		return 0, nil
	}

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
			lra, ": offset: ", offset, ", blockIndex: ", blockIndex,
			", blockOffset: ", blockOffset, ", blockStart: ", blockStart,
			" blockSize: ", lra.blockSize,
		)
	}

	totalRead := 0
	remaining := p

	lra.mu.Lock()
	defer lra.mu.Unlock()

	for totalRead < len(p) {

		if blockBuf, ok := lra.cache.Get(blockIndex); ok && blockOffset <= len(blockBuf) {
			n := copy(remaining, blockBuf[blockOffset:])
			totalRead += n
			remaining = remaining[n:]
			lra.metrics.cacheHitBytes += uint64(n)

			if totalRead == len(p) {
				return totalRead, nil
			}

			if blockIndex == lra.eofIndex &&
				len(blockBuf) != lra.blockSize {

				return totalRead, io.EOF
			}

			blockIndex++
			blockOffset = 0
			blockStart += int64(lra.blockSize)
			continue
		}

		var readBuf []byte
		var direct bool

		if blockOffset == 0 && len(remaining) >= 2*lra.blockSize {
			count := lra.countBlocksBeforeCacheHit(len(remaining), blockIndex+1)
			if count > 1 {
				readBuf = remaining[:count*lra.blockSize]
				direct = true
			}
		}
		if readBuf == nil {
			readBuf = lra.getBuffer()
		}

		nRead, err := lra.reader.ReadAt(readBuf, blockStart)

		if debug {
			println(
				lra, ": blockIndex: ", blockIndex, ", blockStart: ",
				blockStart, ", nRead: ", nRead, ", readErr: ", err,
				" readBufSize: ", len(readBuf),
			)
		}
		if nRead < 0 {
			panic(errNegativeReadAt)
		}

		if nRead <= blockOffset {
			if direct || nRead <= 0 {
				totalRead += nRead
				return readAtResult(len(p), totalRead, err)
			}

			if err == io.EOF {
				lra.eofIndex = blockIndex
				lra.cache.Add(blockIndex, readBuf[:nRead])
			} else {
				lra.putBuffer(readBuf)
			}

			return readAtResult(len(p), totalRead, err)
		}

		if direct {
			totalRead += nRead
			remaining = remaining[nRead:]

			for nRead > lra.blockSize {
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
					blockBuf := lra.getBuffer()

					n := copy(blockBuf, readBuf[:nRead])
					lra.cache.Add(blockIndex, blockBuf[:n])

					if err == io.EOF {
						lra.eofIndex = blockIndex
					}
				}
			}

		} else {
			n := copy(remaining, readBuf[blockOffset:nRead])
			totalRead += n
			remaining = remaining[n:]

			if err == io.EOF {
				lra.eofIndex = blockIndex
				lra.cache.Add(blockIndex, readBuf[:nRead])
			} else if nRead == lra.blockSize {
				lra.cache.Add(blockIndex, readBuf)
			} else {
				lra.putBuffer(readBuf)
			}
		}

		if err != nil || nRead == 0 || len(remaining) == 0 {
			return readAtResult(len(p), totalRead, err)
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

func readAtResult(expectedRead, totalRead int, err error) (int, error) {
	if err == nil && expectedRead != totalRead {
		err = ErrShortRead
	}
	return totalRead, err
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
