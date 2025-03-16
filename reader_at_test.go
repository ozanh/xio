package xio_test

import (
	"bytes"
	"crypto/rand"
	"errors"
	"io"
	"math"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ozanh/xio"
)

var errIfEofNil = errors.New("eof or nil")

func TestLruReaderAt(t *testing.T) {
	testCases := []struct {
		name        string
		readerFunc  func() io.ReaderAt
		blockSize   int
		cacheSize   int
		reads       []readersAtCase
		wantErrFunc func(*testing.T, error)
	}{
		{
			name: "empty reader",
			readerFunc: func() io.ReaderAt {
				autoGrow := false
				return xio.NewStorageBuffer([]byte{}, autoGrow)
			},
			blockSize: 32,
			cacheSize: 100,
			reads: []readersAtCase{
				{sz: 10, off: 0, el: io.EOF, er: io.EOF},
				{sz: 10, off: 10, el: io.EOF, er: io.EOF},
			},
		},
		{
			name: "small reader",
			readerFunc: func() io.ReaderAt {
				autoGrow := false
				sb := xio.NewStorageBuffer(make([]byte, 100), autoGrow)
				mustRandFillWriterAt(sb, sb.Len())
				return sb
			},
			blockSize: 32,
			cacheSize: 100,
			reads: []readersAtCase{
				{sz: 10, off: 0},
				{sz: 10, off: 10},
				{sz: 10, off: 20},
				{sz: 10, off: 30},
				{sz: 10, off: 40},
				{sz: 10, off: 50},
				{sz: 10, off: 60},
				{sz: 10, off: 70},
				{sz: 10, off: 80},
				{sz: 10, off: 90, el: errIfEofNil, er: errIfEofNil},
				{sz: 10, off: 100, el: io.EOF, er: io.EOF},
				{sz: 10, off: 101, el: io.EOF, er: io.EOF},
			},
		},
		{
			name: "small reader odd sizes",
			readerFunc: func() io.ReaderAt {
				autoGrow := false
				sb := xio.NewStorageBuffer(make([]byte, 100), autoGrow)
				mustRandFillWriterAt(sb, sb.Len())
				return sb
			},
			blockSize: 33,
			cacheSize: 100,
			reads: []readersAtCase{
				{sz: 11, off: 0},
				{sz: 22, off: 11},
				{sz: 66, off: 33},
				{sz: 33, off: 99, el: errIfEofNil, er: errIfEofNil},
				{sz: 33, off: 100, el: io.EOF, er: io.EOF},
				{sz: 33, off: 101, el: io.EOF, er: io.EOF},
			},
		},
		{
			name: "small reader odd sizes 2",
			readerFunc: func() io.ReaderAt {
				autoGrow := false
				sb := xio.NewStorageBuffer(make([]byte, 100), autoGrow)
				mustRandFillWriterAt(sb, sb.Len())
				return sb
			},
			blockSize: 33,
			cacheSize: 3,
			reads: []readersAtCase{
				{sz: 11, off: 0},
				{sz: 22, off: 11},
				{sz: 66, off: 33},
				{sz: 3, off: 99, el: errIfEofNil, er: errIfEofNil},
				{sz: 1, off: 100, el: io.EOF, er: io.EOF},
				{sz: 1, off: 101, el: io.EOF, er: io.EOF},
			},
		},
		{
			name: "big read",
			readerFunc: func() io.ReaderAt {
				autoGrow := false
				sb := xio.NewStorageBuffer(make([]byte, 1024), autoGrow)
				mustRandFillWriterAt(sb, sb.Len())
				return sb
			},
			blockSize: 33,
			cacheSize: 3,
			reads: []readersAtCase{
				{sz: 256, off: 0},
				{sz: 256, off: 256},
				{sz: 256, off: 512},
				{sz: 256, off: 768, el: errIfEofNil, er: errIfEofNil},
				{sz: 256, off: 1024, el: io.EOF, er: io.EOF},
				{sz: 256, off: 1025, el: io.EOF, er: io.EOF},
			},
		},
		{
			name: "bigger read",
			readerFunc: func() io.ReaderAt {
				autoGrow := false
				sb := xio.NewStorageBuffer(make([]byte, 1024), autoGrow)
				mustRandFillWriterAt(sb, sb.Len())
				return sb
			},
			blockSize: 13,
			cacheSize: 3,
			reads: []readersAtCase{
				{sz: 1025, off: 0, el: io.EOF, er: io.EOF},
			},
		},
		{
			name: "read back and forth",
			readerFunc: func() io.ReaderAt {
				autoGrow := false
				sb := xio.NewStorageBuffer(make([]byte, 200), autoGrow)
				mustRandFillWriterAt(sb, sb.Len())
				return sb
			},
			blockSize: 15,
			cacheSize: 3,
			reads: []readersAtCase{
				{sz: 30, off: 100},
				{sz: 50, off: 1},
				{sz: 30, off: 100},
				{sz: 50, off: 1},
				{sz: 50, off: 51},
				{sz: 50, off: 0},
			},
		},
		{
			name: "eof cache",
			readerFunc: func() io.ReaderAt {
				autoGrow := false
				sb := xio.NewStorageBuffer(make([]byte, 100), autoGrow)
				mustRandFillWriterAt(sb, sb.Len())
				return sb
			},
			blockSize: 15,
			cacheSize: 3,
			reads: []readersAtCase{
				{sz: 10, off: 91, el: errIfEofNil, er: errIfEofNil},
				{sz: 10, off: 91, el: errIfEofNil, er: errIfEofNil},
			},
		},
		{
			name: "eof cache 2",
			readerFunc: func() io.ReaderAt {
				autoGrow := false
				sb := xio.NewStorageBuffer(make([]byte, 100), autoGrow)
				mustRandFillWriterAt(sb, sb.Len())
				return sb
			},
			blockSize: 15,
			cacheSize: 3,
			reads: []readersAtCase{
				{sz: 10, off: 99, el: errIfEofNil, er: errIfEofNil},
				{sz: 10, off: 99, el: errIfEofNil, er: errIfEofNil},
			},
		},
		{
			name: "eof cache 3",
			readerFunc: func() io.ReaderAt {
				autoGrow := false
				sb := xio.NewStorageBuffer(make([]byte, 100), autoGrow)
				mustRandFillWriterAt(sb, sb.Len())
				return sb
			},
			blockSize: 15,
			cacheSize: 3,
			reads: []readersAtCase{
				{sz: 15, off: 90, el: errIfEofNil, er: errIfEofNil},
				{sz: 15, off: 90, el: errIfEofNil, er: errIfEofNil},
			},
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			left := tt.readerFunc()
			lra, err := xio.NewLruReaderAt(left, tt.blockSize, tt.cacheSize)
			if err != nil {
				if tt.wantErrFunc != nil {
					tt.wantErrFunc(t, err)
				} else {
					t.Fatal(err)
				}
				return
			}

			right := lra

			testReadersAt(t, left, right, tt.reads)
		})
	}

}

type readersAtCase struct {
	sz  int
	off int64
	el  error
	er  error
}

func testReadersAt(t *testing.T, left, right io.ReaderAt, cases []readersAtCase) {
	t.Helper()

	for i, c := range cases {
		bufLeft := make([]byte, c.sz)
		bufRight := make([]byte, c.sz)

		n1, errLeft := left.ReadAt(bufLeft, c.off)
		n2, errRight := right.ReadAt(bufRight, c.off)

		if c.el == errIfEofNil && errLeft == io.EOF {
			errLeft = nil
		}
		if c.er == errIfEofNil && errRight == io.EOF {
			errRight = nil
		}

		require.Equal(t, n1, n2, "n: case %d", i)
		require.Equal(t, bufLeft, bufRight, "buf: case %d", i)

		require.Equal(t, errLeft, errRight, "err: case %d", i)
	}

	err := xio.CompareReadersData(
		io.NewSectionReader(left, 0, math.MaxInt64),
		io.NewSectionReader(right, 0, math.MaxInt64),
	)
	require.NoError(t, err, "CompareReadersData")
}

func mustRandFillWriterAt(w io.WriterAt, size int) {
	ow := io.NewOffsetWriter(w, 0)

	_, err := io.CopyN(ow, rand.Reader, int64(size))
	if err != nil {
		panic(err)
	}
}

func TestLruReaderAt_edge_cases(t *testing.T) {
	t.Run("invalid blockSize", func(t *testing.T) {
		_, err := xio.NewLruReaderAt(io.ReaderAt(nil), 0, 1)
		require.Error(t, err)

		_, err = xio.NewLruReaderAt(io.ReaderAt(nil), -1, 1)
		require.Error(t, err)
	})

	t.Run("invalid cacheSize", func(t *testing.T) {
		_, err := xio.NewLruReaderAt(io.ReaderAt(nil), 1, 0)
		require.Error(t, err)

		_, err = xio.NewLruReaderAt(io.ReaderAt(nil), 1, -1)
		require.Error(t, err)
	})

	t.Run("negative offset", func(t *testing.T) {
		lra, err := xio.NewLruReaderAt(io.ReaderAt(nil), 1, 1)
		require.NoError(t, err)

		buf := make([]byte, 1)
		n, err := lra.ReadAt(buf, -1)
		require.Equal(t, 0, n)
		require.NotNil(t, err)
		require.Contains(t, err.Error(), "negative offset")

		m := lra.Metrics()
		require.Zero(t, m)
	})

	t.Run("empty buffer is EOF", func(t *testing.T) {
		lra, err := xio.NewLruReaderAt(io.ReaderAt(nil), 1, 1)
		require.NoError(t, err)

		buf := make([]byte, 0)
		n, err := lra.ReadAt(buf, 0)
		require.Equal(t, 0, n)
		require.Nil(t, err)
	})

	t.Run("no data", func(t *testing.T) {
		r := xio.NewStorageBuffer([]byte{}, false)
		lra, err := xio.NewLruReaderAt(r, 64*1024, 1024)
		require.NoError(t, err)

		buf := make([]byte, 1)
		n, err := lra.ReadAt(buf, 0)
		require.Equal(t, 0, n)
		require.Equal(t, io.EOF, err)
	})

	t.Run("read beyond is EOF", func(t *testing.T) {
		r := xio.NewStorageBuffer(make([]byte, 1000), false)

		lra, err := xio.NewLruReaderAt(r, 1024, 10)
		require.NoError(t, err)

		buf := make([]byte, 1024)
		n, err := lra.ReadAt(buf, 1000)
		require.Equal(t, 0, n)
		require.Equal(t, io.EOF, err)
	})

	t.Run("read fully cached data with EOF", func(t *testing.T) {
		r := xio.NewStorageBuffer(make([]byte, 1000), false)

		lra, err := xio.NewLruReaderAt(r, 1024, 10)
		require.NoError(t, err)

		buf := make([]byte, 1024)
		n, err := lra.ReadAt(buf, 0)
		require.Equal(t, r.Len(), n)
		require.Equal(t, io.EOF, err)

		n, err = lra.ReadAt(buf, 1000)
		require.Equal(t, 0, n)
		require.Equal(t, io.EOF, err)
	})

	t.Run("read fully cached data with EOF 2", func(t *testing.T) {
		r := xio.NewStorageBuffer(make([]byte, 1000), false)

		lra, err := xio.NewLruReaderAt(r, 512, 10)
		require.NoError(t, err)

		buf := make([]byte, 1024)
		n, err := lra.ReadAt(buf, 0)
		require.Equal(t, r.Len(), n)
		require.Equal(t, io.EOF, err)

		n, err = lra.ReadAt(buf, 1000)
		require.Equal(t, 0, n)
		require.Equal(t, io.EOF, err)
	})

	t.Run("read partially cached data with EOF", func(t *testing.T) {
		r := xio.NewStorageBuffer(make([]byte, 1000), false)

		lra, err := xio.NewLruReaderAt(r, 1000, 10)
		require.NoError(t, err)

		buf := make([]byte, 1000)
		n, err := lra.ReadAt(buf, 0)
		require.Equal(t, r.Len(), n)
		require.Nil(t, err)

		n, err = lra.ReadAt(buf, 800)
		require.Equal(t, r.Len()-800, n)
		require.Equal(t, io.EOF, err)
	})

	t.Run("partial block read at block boundary", func(t *testing.T) {
		r := xio.NewStorageBuffer(make([]byte, 800), false)

		lra, err := xio.NewLruReaderAt(r, 500, 10)
		require.NoError(t, err)

		buf := make([]byte, 800)
		n, err := lra.ReadAt(buf, 0)
		require.Equal(t, r.Len(), n)
		require.Equal(t, io.EOF, err)

		n, err = lra.ReadAt(buf[:100], 500)
		require.Equal(t, 100, n)
		require.Equal(t, nil, err)
	})

	t.Run("read from offset exactly at the last byte", func(t *testing.T) {
		r := xio.NewStorageBuffer(make([]byte, 1), false)

		lra, err := xio.NewLruReaderAt(r, 1, 2)
		require.NoError(t, err)

		buf := make([]byte, 1)
		n, err := lra.ReadAt(buf, 0)
		require.Equal(t, 1, n)
		require.Nil(t, err)

		n, err = lra.ReadAt(buf, 0)
		require.Equal(t, 1, n)
		require.Nil(t, err)
	})

	t.Run("read small block crossing boundary multiple times", func(t *testing.T) {
		r := xio.NewStorageBuffer(make([]byte, 256), false)

		lra, err := xio.NewLruReaderAt(r, 64, 2)
		require.NoError(t, err)

		buf := make([]byte, 40)
		// Repeated reads at offsets that cross block boundaries
		for off := int64(20); off < int64(r.Len()); off += 30 {
			n, err := lra.ReadAt(buf, off)
			if off+40 < int64(r.Len()) {
				require.Equal(t, 40, n)
				require.Nil(t, err)
			} else {
				// Past end
				require.Less(t, n, 40)
				require.Equal(t, io.EOF, err)
			}
		}
	})

	t.Run("negative read panic", func(t *testing.T) {
		lra, err := xio.NewLruReaderAt(negativeReaderAt{}, 64, 2)
		require.NoError(t, err)

		defer func() {
			if r := recover(); r == nil {
				t.Error("expected panic")
			}
		}()

		_, _ = lra.ReadAt([]byte{0}, 0)

		t.Error("unreachable")
	})
}

type negativeReaderAt struct{}

func (negativeReaderAt) ReadAt(p []byte, off int64) (int, error) {
	return -1, io.EOF
}

func TestLruReaderAt_caching(t *testing.T) {
	t.Run("purge", func(t *testing.T) {
		r := xio.NewStorageBuffer(make([]byte, 1000), false)
		mustRandFillWriterAt(r, r.Len())

		lra, err := xio.NewLruReaderAt(r, 500, 2)
		require.NoError(t, err)

		metrics := lra.Metrics()
		require.Empty(t, metrics)

		buf := make([]byte, 1000)
		n, err := lra.ReadAt(buf, 0)
		require.Equal(t, r.Len(), n)
		require.Nil(t, err)

		metrics = lra.Metrics()
		require.NotEmpty(t, metrics)

		lra.Purge()

		metrics = lra.Metrics()
		require.Empty(t, metrics)

		mustRandFillWriterAt(r, r.Len())

		buf2 := make([]byte, 1000)
		n, err = lra.ReadAt(buf2, 0)
		require.Equal(t, r.Len(), n)
		require.Nil(t, err)

		require.False(t, bytes.Equal(buf, buf2), "cache not purged")
	})

	t.Run("metrics", func(t *testing.T) {
		r := xio.NewStorageBuffer(make([]byte, 1000), false)
		mustRandFillWriterAt(r, r.Len())

		lra, err := xio.NewLruReaderAt(r, 500, 2)
		require.NoError(t, err)

		m := lra.Metrics()
		require.Zero(t, m)

		buf := make([]byte, 1000)
		n, err := lra.ReadAt(buf, 0)
		require.Equal(t, r.Len(), n)
		require.Nil(t, err)

		m = lra.Metrics()
		require.NotZero(t, m)
		require.Equal(t, uint64(0), m.CacheHitBytes)
		require.Equal(t, uint64(2), m.PoolAllocs)

		n, err = lra.ReadAt(buf, 0)
		require.Equal(t, r.Len(), n)
		require.Nil(t, err)

		m = lra.Metrics()
		require.NotZero(t, m)
		require.Equal(t, uint64(1000), m.CacheHitBytes)
		require.Equal(t, uint64(2), m.PoolAllocs)

		lra.Purge()

		m = lra.Metrics()
		require.Zero(t, m)
	})
}

func TestLruReaderAt_concurrency(t *testing.T) {
	r := xio.NewStorageBuffer(make([]byte, 1000), false)

	lra, err := xio.NewLruReaderAt(r, 10, 20)
	require.NoError(t, err)

	const concurrency = 10
	const iterations = 100

	var wg sync.WaitGroup
	wg.Add(concurrency)

	for i := 0; i < concurrency; i++ {
		go func() {
			defer wg.Done()

			for j := 0; j < iterations; j++ {
				buf := make([]byte, 1000)
				n, err := lra.ReadAt(buf, 0)
				assert.Equal(t, r.Len(), n)
				assert.Nil(t, err)
			}
		}()
	}

	wg.Wait()
}
