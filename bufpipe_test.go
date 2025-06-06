package xio_test

import (
	"bytes"
	"crypto/md5"
	cryptorand "crypto/rand"
	"crypto/sha256"
	"errors"
	"io"
	mathrand "math/rand"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ozanh/xio"
)

type bufPipeTestCase struct {
	name         string
	blockSize    int
	storsize     int64
	randFileSize int64
	storageFn    func(blockSize int, storsize int64) xio.Storage
	readerFn     func(r io.Reader) io.Reader
}

func TestBufPipe(t *testing.T) {
	testCases := []bufPipeTestCase{
		{
			name:         "blockSize=3 storSize=1024 randFileSize=1025",
			blockSize:    3,
			storsize:     1024,
			randFileSize: 1025,
		},
		{
			name:         "blockSize=1 storSize=10KiB randFileSize=10KiB",
			blockSize:    1,
			storsize:     10 * 1024,
			randFileSize: 10 * 1024,
		},
		{
			name:         "blockSize=1 storSize=100KiB randFileSize=100KiB blockStorage",
			blockSize:    1,
			storsize:     100 * 1024,
			randFileSize: 100 * 1024,
			storageFn: func(blockSize int, storsize int64) xio.Storage {
				return xio.NewBlockStorageBuffer(blockSize, int(storsize))
			},
		},
		{
			name:         "blockSize=10 storSize=1MiB+1 randFileSize=1MiB-1 blockStorage",
			blockSize:    10,
			storsize:     1*1024*1024 + 1,
			randFileSize: 1*1024*1024 - 1,
			storageFn: func(blockSize int, storsize int64) xio.Storage {
				return xio.NewBlockStorageBuffer(blockSize, int(storsize))
			},
		},
		{
			name:         "blockSize=512 storSize=1MiB+1 randFileSize=1MiB-1",
			blockSize:    512,
			storsize:     1*1024*1024 + 1,
			randFileSize: 1*1024*1024 - 1,
		},
		{
			name:         "blockSize=512 storSize=1MiB+1 randFileSize=1MiB-1 slowReader",
			blockSize:    512,
			storsize:     1*1024*1024 + 1,
			randFileSize: 1*1024*1024 - 1,
			readerFn:     makeSlowReader,
		},
		{
			name:         "blockSize=512 storSize=1MiB-1 randFileSize=1MiB+1",
			blockSize:    512,
			storsize:     1*1024*1024 - 1,
			randFileSize: 1*1024*1024 + 1,
		},
		{
			name:         "blockSize=512 storSize=1MiB-1 randFileSize=1MiB+1 slowReader",
			blockSize:    512,
			storsize:     1*1024*1024 - 1,
			randFileSize: 1*1024*1024 + 1,
			readerFn:     makeSlowReader,
		},
		{
			name:         "blockSize=1MiB storSize=1MiB randFileSize=1MiB",
			blockSize:    1 * 1024 * 1024,
			storsize:     1 * 1024 * 1024,
			randFileSize: 1 * 1024 * 1024,
		},
		{
			name:         "blockSize=1MiB storSize=1MiB randFileSize=1MiB slowReader",
			blockSize:    1 * 1024 * 1024,
			storsize:     1 * 1024 * 1024,
			randFileSize: 1 * 1024 * 1024,
			readerFn:     makeSlowReader,
		},
		{
			name:         "blockSize=1MiB storsize=1MiB randFileSize=1MiB+1",
			blockSize:    1 * 1024 * 1024,
			storsize:     1 * 1024 * 1024,
			randFileSize: 1*1024*1024 + 1,
		},
		{
			name:         "blockSize=1MiB storsize=1MiB randFileSize=1MiB+1 slowReader",
			blockSize:    1 * 1024 * 1024,
			storsize:     1 * 1024 * 1024,
			randFileSize: 1*1024*1024 + 1,
			readerFn:     makeSlowReader,
		},
		{
			name:         "blockSize=1MiB storsize=1MiB randFileSize=1MiB-1",
			blockSize:    1 * 1024 * 1024,
			storsize:     1 * 1024 * 1024,
			randFileSize: 1*1024*1024 - 1,
		},
		{
			name:         "blockSize=1MiB storsize=1MiB randFileSize=1MiB-1 slowReader",
			blockSize:    1 * 1024 * 1024,
			storsize:     1 * 1024 * 1024,
			randFileSize: 1*1024*1024 - 1,
			readerFn:     makeSlowReader,
		},
		{
			name:         "blockSize=1MiB storsize=1MiB-1 randFileSize=1MiB-1",
			blockSize:    1 * 1024 * 1024,
			storsize:     1*1024*1024 - 1,
			randFileSize: 1*1024*1024 - 1,
		},
		{
			name:         "blockSize=1MiB storsize=1MiB-1 randFileSize=1MiB+1",
			blockSize:    1 * 1024 * 1024,
			storsize:     1*1024*1024 - 1,
			randFileSize: 1*1024*1024 + 1,
		},
		{
			name:         "blockSize=4MiB storsize=8MiB randFileSize=9MiB",
			blockSize:    4 * 1024 * 1024,
			storsize:     8 * 1024 * 1024,
			randFileSize: 9 * 1024 * 1024,
		},
		{
			name:         "blockSize=4MiB storsize=8MiB randFileSize=9MiB blockStorage",
			blockSize:    4 * 1024 * 1024,
			storsize:     8 * 1024 * 1024,
			randFileSize: 9 * 1024 * 1024,
			storageFn: func(blockSize int, storsize int64) xio.Storage {
				return xio.NewBlockStorageBuffer(blockSize, int(storsize))
			},
		},
		{
			name:         "blockSize=4MiB storsize=8MiB randFileSize=7MiB",
			blockSize:    4 * 1024 * 1024,
			storsize:     8 * 1024 * 1024,
			randFileSize: 7 * 1024 * 1024,
		},
		{
			name:         "blockSize=4MiB storsize=8MiB randFileSize=8MiB",
			blockSize:    4 * 1024 * 1024,
			storsize:     8 * 1024 * 1024,
			randFileSize: 8 * 1024 * 1024,
		},
		{
			name:         "blockSize=4MiB storsize=8MiB randFileSize=8MiB slowReader",
			blockSize:    4 * 1024 * 1024,
			storsize:     8 * 1024 * 1024,
			randFileSize: 8 * 1024 * 1024,
			readerFn:     makeSlowReader,
		},
		{
			name:         "blockSize=4MiB storsize=8MiB randFileSize=8MiB slowReader blockStorage",
			blockSize:    4 * 1024 * 1024,
			storsize:     8 * 1024 * 1024,
			randFileSize: 8 * 1024 * 1024,
			storageFn: func(blockSize int, storsize int64) xio.Storage {
				return xio.NewBlockStorageBuffer(blockSize, int(storsize))
			},
			readerFn: makeSlowReader,
		},
		{
			name:         "blockSize=4MiB storsize=8MiB randFileSize=8MiB+100",
			blockSize:    4 * 1024 * 1024,
			storsize:     8 * 1024 * 1024,
			randFileSize: 8*1024*1024 + 100,
		},
		{
			name:         "blockSize=3MiB storsize=7MiB randFileSize=8MiB-100",
			blockSize:    3 * 1024 * 1024,
			storsize:     7 * 1024 * 1024,
			randFileSize: 8*1024*1024 - 100,
		},
		{
			name:         "blockSize=3MB storsize=7MB randFileSize=8MB+100",
			blockSize:    3 * 1000 * 1000,
			storsize:     7 * 1000 * 1000,
			randFileSize: 8*1000*1000 + 100,
		},
		{
			name:         "blockSize=3MB storsize=7MB randFileSize=8MB+100 slowReader",
			blockSize:    3 * 1000 * 1000,
			storsize:     7 * 1000 * 1000,
			randFileSize: 8*1000*1000 + 100,
			readerFn:     makeSlowReader,
		},
		{
			name:         "blockSize=3MB storsize=7MB randFileSize=8MB-100",
			blockSize:    3 * 1000 * 1000,
			storsize:     7 * 1000 * 1000,
			randFileSize: 8*1000*1000 - 100,
		},
		{
			name:         "blockSize=1MB storsize=1MB randFileSize=8MB",
			blockSize:    1 * 1000 * 1000,
			storsize:     1 * 1000 * 1000,
			randFileSize: 8 * 1000 * 1000,
		},
		{
			name:         "blockSize=1MB storsize=1MB randFileSize=8MB-1",
			blockSize:    1 * 1000 * 1000,
			storsize:     1 * 1000 * 1000,
			randFileSize: 8*1000*1000 - 1,
		},
		{
			name:         "blockSize=1MB storsize=1MB randFileSize=8MB+1",
			blockSize:    1 * 1000 * 1000,
			storsize:     1 * 1000 * 1000,
			randFileSize: 8*1000*1000 + 1,
		},
		{
			name:         "blockSize=1MB storsize=1MB+1 randFileSize=1MB",
			blockSize:    1 * 1000 * 1000,
			storsize:     1*1000*1000 + 1,
			randFileSize: 1 * 1000 * 1000,
		},
		{
			name:         "blockSize=1MB storsize=1MB+1 randFileSize=1MB-1",
			blockSize:    1 * 1000 * 1000,
			storsize:     1*1000*1000 + 1,
			randFileSize: 1*1000*1000 - 1,
		},
		{
			name:         "blockSize=1MB storsize=1MB+1 randFileSize=1MB+1",
			blockSize:    1 * 1000 * 1000,
			storsize:     1*1000*1000 + 1,
			randFileSize: 1*1000*1000 + 1,
		},
		{
			name:         "blockSize=1MB storsize=1MB-1 randFileSize=1MB",
			blockSize:    1 * 1000 * 1000,
			storsize:     1*1000*1000 - 1,
			randFileSize: 1 * 1000 * 1000,
		},
		{
			name:         "blockSize=1MB storsize=1MB-1 randFileSize=1MB-1",
			blockSize:    1 * 1000 * 1000,
			storsize:     1*1000*1000 - 1,
			randFileSize: 1*1000*1000 - 1,
		},
		{
			name:         "blockSize=1MB storsize=1MB-1 randFileSize=1MB+1",
			blockSize:    1 * 1000 * 1000,
			storsize:     1*1000*1000 - 1,
			randFileSize: 1*1000*1000 + 1,
		},
		{
			name:         "blockSize=1MB storsize=1MB-1 randFileSize=1MB+1 slowReader",
			blockSize:    1 * 1000 * 1000,
			storsize:     1*1000*1000 - 1,
			randFileSize: 1*1000*1000 + 1,
			readerFn:     makeSlowReader,
		},
		{
			name:         "blockSize=4MB storsize=10MB randFileSize=20MB+1",
			blockSize:    4 * 1000 * 1000,
			storsize:     10 * 1000 * 1000,
			randFileSize: 20*1000*1000 + 1,
		},
		{
			name:         "blockSize=4MB storsize=10MB randFileSize=20MB+1 slowReader",
			blockSize:    4 * 1000 * 1000,
			storsize:     10 * 1000 * 1000,
			randFileSize: 20*1000*1000 + 1,
			readerFn:     makeSlowReader,
		},
		{
			name:         "blockSize=4MB storsize=13MB randFileSize=20MB+1",
			blockSize:    4 * 1000 * 1000,
			storsize:     13 * 1000 * 1000,
			randFileSize: 20*1000*1000 + 1,
		},
		{
			name:         "blockSize=4MB storsize=13MB randFileSize=20MB+1 blockStorage",
			blockSize:    4 * 1000 * 1000,
			storsize:     13 * 1000 * 1000,
			randFileSize: 20*1000*1000 + 1,
			storageFn: func(blockSize int, storsize int64) xio.Storage {
				return xio.NewBlockStorageBuffer(blockSize, int(storsize))
			},
		},
		{
			name:         "blockSize=4MB storsize=13MB randFileSize=20MB+1 slowReader",
			blockSize:    4 * 1000 * 1000,
			storsize:     13 * 1000 * 1000,
			randFileSize: 20*1000*1000 + 1,
			readerFn:     makeSlowReader,
		},
		{
			name:         "blockSize=4MB storsize=13MB randFileSize=20MB+1 blockStorage slowReader",
			blockSize:    4 * 1000 * 1000,
			storsize:     13 * 1000 * 1000,
			randFileSize: 20*1000*1000 + 1,
			storageFn: func(blockSize int, storsize int64) xio.Storage {
				return xio.NewBlockStorageBuffer(blockSize, int(storsize))
			},
			readerFn: makeSlowReader,
		},
	}

	for _, tt := range testCases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			testBufPipeHash(t, tt)
		})
	}
}

func testBufPipeHash(t *testing.T, tc bufPipeTestCase) {
	t.Helper()

	tempdir := t.TempDir()

	var storage xio.Storage
	if tc.storageFn == nil {
		s, err := os.CreateTemp(tempdir, "storage")
		require.NoError(t, err)
		defer s.Close()
		require.NoError(t, s.Truncate(tc.storsize))
		storage = s
	} else {
		storage = tc.storageFn(tc.blockSize, tc.storsize)
	}

	pr, pw := xio.BufPipe(tc.blockSize, tc.storsize, storage)

	srcfile, err := os.CreateTemp(tempdir, "randfile")
	require.NoError(t, err)
	defer srcfile.Close()

	_, err = io.Copy(srcfile, io.LimitReader(cryptorand.Reader, tc.randFileSize))
	require.NoError(t, err)

	_, err = srcfile.Seek(0, io.SeekStart)
	require.NoError(t, err)

	wrs256 := sha256.New()
	wrmd5 := md5.New()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		n, err := io.Copy(io.MultiWriter(wrs256, wrmd5, pw), srcfile)
		assert.NoError(t, err)
		assert.Equal(t, tc.randFileSize, n)
		assert.NoError(t, pw.Close())
	}()

	rds256 := sha256.New()
	rdmd5 := md5.New()

	var src io.Reader = pr
	if tc.readerFn != nil {
		src = tc.readerFn(src)
	}

	n, err := io.Copy(io.MultiWriter(rds256, rdmd5), src)
	require.NoError(t, err)
	require.Equal(t, tc.randFileSize, n)
	require.NoError(t, pr.Close())
	wg.Wait()

	// test storage overflow
	if f, ok := storage.(interface{ Stat() (os.FileInfo, error) }); ok {

		info, err := f.Stat()
		require.NoError(t, err)
		require.Equal(t, tc.storsize, info.Size())

	} else if s, ok := storage.(*xio.BlockStorageBuffer); ok {

		require.GreaterOrEqual(t, int64(s.StorageSize()), tc.storsize)

	} else if s, ok := storage.(*xio.StorageBuffer); ok {

		size := tc.storsize
		if s.AutoGrow() {
			if tc.randFileSize < tc.storsize {
				size = tc.randFileSize
			}
			require.GreaterOrEqual(t, int64(s.Len()), size)
		} else {
			require.Equal(t, size, int64(s.Len()))
		}

	} else {
		t.Fatalf("unknown storage type %T", storage)
	}

	require.Equal(t, wrs256.Sum(nil), rds256.Sum(nil))
	require.Equal(t, wrmd5.Sum(nil), rdmd5.Sum(nil))
}

func TestBufPipe_storage_buffer_auto_grow(t *testing.T) {
	testCases := []bufPipeTestCase{
		{
			name:         "blockSize=1 storSize=100KiB randFileSize=100KiB",
			blockSize:    1,
			storsize:     100 * 1024,
			randFileSize: 100 * 1024,
			storageFn: func(blockSize int, storsize int64) xio.Storage {
				autoGrow := true
				return xio.NewStorageBuffer(nil, autoGrow)
			},
		},
		{
			name:         "blockSize=2 storSize=100KiB randFileSize=100KiB",
			blockSize:    2,
			storsize:     100 * 1024,
			randFileSize: 100 * 1024,
			storageFn: func(blockSize int, storsize int64) xio.Storage {
				autoGrow := true
				return xio.NewStorageBuffer(nil, autoGrow)
			},
		},
		{
			name:         "blockSize=3 storSize=1024 randFileSize=1025",
			blockSize:    3,
			storsize:     1024,
			randFileSize: 1025,
			storageFn: func(blockSize int, storsize int64) xio.Storage {
				autoGrow := true
				return xio.NewStorageBuffer(nil, autoGrow)
			},
		},
		{
			name:         "blockSize=10 storSize=1KiB+1 randFileSize=1KiB-1",
			blockSize:    10,
			storsize:     1*1024 + 1,
			randFileSize: 1*1024 - 1,
			storageFn: func(_ int, _ int64) xio.Storage {
				autoGrow := true
				return xio.NewStorageBuffer(nil, autoGrow)
			},
		},
	}

	for _, tt := range testCases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			testBufPipeHash(t, tt)
		})
	}
}

func makeSlowReader(r io.Reader) io.Reader {
	return &slowReader{
		reader: r,
		rand:   mathrand.New(mathrand.NewSource(time.Now().UnixNano())),
	}
}

type slowReader struct {
	reader io.Reader
	rand   *mathrand.Rand
}

func (r *slowReader) Read(p []byte) (int, error) {
	i := r.rand.Intn(10)
	d := time.Duration(i) * time.Millisecond / 10
	time.Sleep(d)
	return r.reader.Read(p)
}

func TestBufPipe_single_block(t *testing.T) {
	storage := xio.NewStorageBuffer(make([]byte, 1000), true)
	pr, pw := xio.BufPipe(1024, 1024*1024, storage)

	data := bytes.Repeat([]byte("a"), 1024)
	n, err := pw.Write(data)
	require.NoError(t, err)
	require.Equal(t, 1024, n)

	p := make([]byte, 1024)
	n, err = pr.Read(p)
	require.NoError(t, err)
	require.Equal(t, 1024, n)
	require.Equal(t, data, p)

	err = pw.Close()
	require.NoError(t, err)

	n, err = pr.Read(p)
	require.Equal(t, io.EOF, err)
	require.Equal(t, 0, n)

	require.Equal(t, 1024, storage.Len())
}

func TestBufPipe_no_space(t *testing.T) {
	storage := xio.NewStorageBuffer(make([]byte, 1000), false)
	_, pw := xio.BufPipe(1024, 1024*1024, storage)

	data := make([]byte, 1024)
	_, err := io.ReadFull(cryptorand.Reader, data)
	require.NoError(t, err)

	n, err := pw.Write(data)
	require.Equal(t, xio.ErrNoSpaceLeft, err)
	require.Equal(t, 1000, n)
	require.Equal(t, 1000, storage.Len())
	require.Equal(t, data[:1000], storage.Bytes())
}

func TestBufPipe_write_close_error(t *testing.T) {
	storage := xio.NewStorageBuffer(make([]byte, 1024), false)
	pr, pw := xio.BufPipe(1024, 1024*1024, storage)

	data := make([]byte, 1024)
	_, err := io.ReadFull(cryptorand.Reader, data)
	require.NoError(t, err)

	n, err := pw.Write(data)
	require.NoError(t, err)
	require.Equal(t, 1024, n)
	require.Equal(t, 1024, storage.Len())
	require.Equal(t, data, storage.Bytes())

	err = pw.Close()
	require.NoError(t, err)

	p := make([]byte, 1024)
	n, err = pr.Read(p)
	require.NoError(t, err)
	require.Equal(t, 1024, n)
	require.Equal(t, data, p)

	n, err = pr.Read(p)
	require.Equal(t, io.EOF, err)
	require.Equal(t, 0, n)
}

func TestBufPipe_write_close_error_async(t *testing.T) {
	storage := xio.NewStorageBuffer(make([]byte, 1024), false)
	pr, pw := xio.BufPipe(1024, 1024*1024, storage)

	data := make([]byte, 1024)
	_, err := io.ReadFull(cryptorand.Reader, data)
	require.NoError(t, err)

	n, err := pw.Write(data)
	require.NoError(t, err)
	require.Equal(t, 1024, n)
	require.Equal(t, 1024, storage.Len())
	require.Equal(t, data, storage.Bytes())

	p := make([]byte, 1024)
	n, err = pr.Read(p)
	require.NoError(t, err)
	require.Equal(t, 1024, n)
	require.Equal(t, data, p)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(100 * time.Millisecond)
		err := pw.Close()
		assert.NoError(t, err)
	}()

	errTest := errors.New("test")
	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(100 * time.Millisecond)
		err := pw.CloseWithError(errTest)
		assert.NoError(t, err)
	}()

	n, err = pr.Read(p)
	if err != io.EOF && err != errTest {
		t.Fatalf("expected EOF or errTest, got %v", err)
	}
	require.Equal(t, 0, n)
	wg.Wait()
}

func TestBufPipe_read_close_error_async(t *testing.T) {
	storage := xio.NewStorageBuffer(make([]byte, 1024), false)
	pr, pw := xio.BufPipe(1024, 1024*1024, storage)

	data := make([]byte, 1024)
	_, err := io.ReadFull(cryptorand.Reader, data)
	require.NoError(t, err)

	n, err := pw.Write(data)
	require.NoError(t, err)
	require.Equal(t, 1024, n)
	require.Equal(t, 1024, storage.Len())
	require.Equal(t, data, storage.Bytes())

	p := make([]byte, 1024)
	n, err = pr.Read(p)
	require.NoError(t, err)
	require.Equal(t, 1024, n)
	require.Equal(t, data, p)

	errTest := errors.New("test")
	done := make(chan struct{})
	go func() {
		defer close(done)
		time.Sleep(100 * time.Millisecond)
		err := pr.CloseWithError(errTest)
		assert.NoError(t, err)
	}()

	n, err = pr.Read(p)
	require.Equal(t, io.ErrClosedPipe, err)
	require.Equal(t, 0, n)
	<-done

	n, err = pw.Write(data)
	require.Equal(t, errTest, err)
	require.Equal(t, 0, n)
}

func Test_blockQueue(t *testing.T) {
	// Most of the tests are generated by Cursor.
	// I've added some tests to test the edge cases.

	testCases := []struct {
		name     string
		testFunc func(t *testing.T)
	}{
		{
			name: "empty_queue_operations",
			testFunc: func(t *testing.T) {
				bq := xio.NewDefaultBlockQueue()

				// Test PopReadable on empty queue
				block, ok := bq.PopReadable()
				require.False(t, ok)
				require.Equal(t, int64(0), xio.GetABlockStartOffset(block))
				require.Equal(t, int64(0), xio.GetABlockWritten(block))

				// Test PopWritable on empty queue
				offset, ok := bq.PopWritable()
				require.False(t, ok)
				require.Equal(t, int64(0), offset)
			},
		},
		{
			name: "push_pop_writable_single",
			testFunc: func(t *testing.T) {
				bq := xio.NewDefaultBlockQueue()

				// Push a single writable offset
				bq.PushWritable(1024)

				// Pop it back
				offset, ok := bq.PopWritable()
				require.True(t, ok)
				require.Equal(t, int64(1024), offset)

				// Queue should be empty now
				offset, ok = bq.PopWritable()
				require.False(t, ok)
				require.Equal(t, int64(0), offset)
			},
		},
		{
			name: "push_pop_writable_multiple",
			testFunc: func(t *testing.T) {
				bq := xio.NewDefaultBlockQueue()

				// Push multiple writable offsets
				offsets := []int64{1024, 2048, 4096, 8192}
				for _, offset := range offsets {
					bq.PushWritable(offset)
				}

				// Pop them back in FIFO order
				for _, expectedOffset := range offsets {
					offset, ok := bq.PopWritable()
					require.True(t, ok)
					require.Equal(t, expectedOffset, offset)
				}

				// Queue should be empty now
				offset, ok := bq.PopWritable()
				require.False(t, ok)
				require.Equal(t, int64(0), offset)
			},
		},
		{
			name: "push_pop_readable_single",
			testFunc: func(t *testing.T) {
				bq := xio.NewDefaultBlockQueue()

				// Create a test block
				testBlock := xio.NewABlock(1024, 512)

				// Push a single readable block
				bq.PushReadable(testBlock)

				// Pop it back
				block, ok := bq.PopReadable()
				require.True(t, ok)
				require.Equal(t, xio.GetABlockStartOffset(testBlock), xio.GetABlockStartOffset(block))
				require.Equal(t, xio.GetABlockWritten(testBlock), xio.GetABlockWritten(block))

				// Queue should be empty now
				block, ok = bq.PopReadable()
				require.False(t, ok)
				require.Equal(t, int64(0), xio.GetABlockStartOffset(block))
				require.Equal(t, int64(0), xio.GetABlockWritten(block))
			},
		},
		{
			name: "push_pop_readable_multiple",
			testFunc: func(t *testing.T) {
				bq := xio.NewDefaultBlockQueue()

				// Create test blocks
				testBlocks := []xio.ABlock{
					xio.NewABlock(0, 1024),
					xio.NewABlock(1024, 2048),
					xio.NewABlock(3072, 512),
					xio.NewABlock(3584, 1536),
				}

				// Push multiple readable blocks
				for _, block := range testBlocks {
					bq.PushReadable(block)
				}

				// Pop them back in FIFO order
				for _, expectedBlock := range testBlocks {
					block, ok := bq.PopReadable()
					require.True(t, ok)
					require.Equal(t, xio.GetABlockStartOffset(expectedBlock), xio.GetABlockStartOffset(block))
					require.Equal(t, xio.GetABlockWritten(expectedBlock), xio.GetABlockWritten(block))
				}

				// Queue should be empty now
				block, ok := bq.PopReadable()
				require.False(t, ok)
				require.Equal(t, int64(0), xio.GetABlockStartOffset(block))
				require.Equal(t, int64(0), xio.GetABlockWritten(block))
			},
		},
		{
			name: "mixed_operations",
			testFunc: func(t *testing.T) {
				bq := xio.NewDefaultBlockQueue()

				// Mix writable and readable operations
				bq.PushWritable(1024)
				bq.PushReadable(xio.NewABlock(0, 512))
				bq.PushWritable(2048)
				bq.PushReadable(xio.NewABlock(512, 1024))

				// Pop writables
				offset, ok := bq.PopWritable()
				require.True(t, ok)
				require.Equal(t, int64(1024), offset)

				offset, ok = bq.PopWritable()
				require.True(t, ok)
				require.Equal(t, int64(2048), offset)

				// Pop readables
				block, ok := bq.PopReadable()
				require.True(t, ok)
				require.Equal(t, int64(0), xio.GetABlockStartOffset(block))
				require.Equal(t, int64(512), xio.GetABlockWritten(block))

				block, ok = bq.PopReadable()
				require.True(t, ok)
				require.Equal(t, int64(512), xio.GetABlockStartOffset(block))
				require.Equal(t, int64(1024), xio.GetABlockWritten(block))

				// Both queues should be empty
				_, ok = bq.PopWritable()
				require.False(t, ok)

				_, ok = bq.PopReadable()
				require.False(t, ok)
			},
		},
		{
			name: "close_read_empty_queue",
			testFunc: func(t *testing.T) {
				bq := xio.NewDefaultBlockQueue()

				// Close read on empty queue
				bq.CloseRead()

				// PopReadable should return true (indicating closed state)
				block, ok := bq.PopReadable()
				require.True(t, ok)
				require.Equal(t, int64(0), xio.GetABlockStartOffset(block))
				require.Equal(t, int64(0), xio.GetABlockWritten(block))
			},
		},
		{
			name: "close_read_with_data",
			testFunc: func(t *testing.T) {
				bq := xio.NewDefaultBlockQueue()

				// Add some readable blocks
				testBlocks := []xio.ABlock{
					xio.NewABlock(0, 1024),
					xio.NewABlock(1024, 512),
				}

				for _, block := range testBlocks {
					bq.PushReadable(block)
				}

				// Close read
				bq.CloseRead()

				// Should still be able to pop existing blocks
				for _, expectedBlock := range testBlocks {
					block, ok := bq.PopReadable()
					require.True(t, ok)
					require.Equal(t, xio.GetABlockStartOffset(expectedBlock), xio.GetABlockStartOffset(block))
					require.Equal(t, xio.GetABlockWritten(expectedBlock), xio.GetABlockWritten(block))
				}

				// After emptying, should still return true (closed state)
				block, ok := bq.PopReadable()
				require.True(t, ok)
				require.Equal(t, int64(0), xio.GetABlockStartOffset(block))
				require.Equal(t, int64(0), xio.GetABlockWritten(block))
			},
		},
		{
			name: "large_number_operations",
			testFunc: func(t *testing.T) {
				bq := xio.NewDefaultBlockQueue()

				const numOps = 1000

				// Push many writable offsets
				for i := 0; i < numOps; i++ {
					bq.PushWritable(int64(i * 1024))
				}

				// Push many readable blocks
				for i := 0; i < numOps; i++ {
					bq.PushReadable(xio.NewABlock(int64(i*2048), int64(512+i)))
				}

				// Pop all writables
				for i := 0; i < numOps; i++ {
					offset, ok := bq.PopWritable()
					require.True(t, ok)
					require.Equal(t, int64(i*1024), offset)
				}

				// Pop all readables
				for i := 0; i < numOps; i++ {
					block, ok := bq.PopReadable()
					require.True(t, ok)
					require.Equal(t, int64(i*2048), xio.GetABlockStartOffset(block))
					require.Equal(t, int64(512+i), xio.GetABlockWritten(block))
				}

				// Both queues should be empty
				_, ok := bq.PopWritable()
				require.False(t, ok)

				_, ok = bq.PopReadable()
				require.False(t, ok)
			},
		},
		{
			name: "zero_values",
			testFunc: func(t *testing.T) {
				bq := xio.NewDefaultBlockQueue()

				// Test with zero offset
				bq.PushWritable(0)
				offset, ok := bq.PopWritable()
				require.True(t, ok)
				require.Equal(t, int64(0), offset)

				// Test with zero block
				zeroBlock := xio.NewABlock(0, 0)
				bq.PushReadable(zeroBlock)
				block, ok := bq.PopReadable()
				require.True(t, ok)
				require.Equal(t, xio.GetABlockStartOffset(zeroBlock), xio.GetABlockStartOffset(block))
				require.Equal(t, xio.GetABlockWritten(zeroBlock), xio.GetABlockWritten(block))
			},
		},
		{
			name: "negative_values",
			testFunc: func(t *testing.T) {
				bq := xio.NewDefaultBlockQueue()

				// Test with negative offset (should work as it's just int64)
				bq.PushWritable(-1024)
				offset, ok := bq.PopWritable()
				require.True(t, ok)
				require.Equal(t, int64(-1024), offset)

				// Test with negative block values
				negBlock := xio.NewABlock(-512, -256)
				bq.PushReadable(negBlock)
				block, ok := bq.PopReadable()
				require.True(t, ok)
				require.Equal(t, xio.GetABlockStartOffset(negBlock), xio.GetABlockStartOffset(block))
				require.Equal(t, xio.GetABlockWritten(negBlock), xio.GetABlockWritten(block))
			},
		},
		{
			name: "interleaved_push_pop",
			testFunc: func(t *testing.T) {
				bq := xio.NewDefaultBlockQueue()

				// Interleave push and pop operations
				bq.PushWritable(1024)
				offset, ok := bq.PopWritable()
				require.True(t, ok)
				require.Equal(t, int64(1024), offset)

				bq.PushReadable(xio.NewABlock(0, 512))
				block, ok := bq.PopReadable()
				require.True(t, ok)
				require.Equal(t, int64(0), xio.GetABlockStartOffset(block))
				require.Equal(t, int64(512), xio.GetABlockWritten(block))

				// Push multiple, pop one, push more, pop all
				bq.PushWritable(2048)
				bq.PushWritable(4096)

				offset, ok = bq.PopWritable()
				require.True(t, ok)
				require.Equal(t, int64(2048), offset)

				bq.PushWritable(8192)

				offset, ok = bq.PopWritable()
				require.True(t, ok)
				require.Equal(t, int64(4096), offset)

				offset, ok = bq.PopWritable()
				require.True(t, ok)
				require.Equal(t, int64(8192), offset)
			},
		},
		{
			name: "bufpipe_deadlock_issue",
			testFunc: func(t *testing.T) {
				// I found a deadlock issue in the BufPipe implementation.
				// To resolve it, I've added a new method to the blockQueue: CloseRead,
				// and enhanced the blockQueue implementation to handle the case where
				// the read side awaits a signal that never comes.
				// It's hard to reproduce the issue with BufPipe, so I've added a test case
				// for the blockQueue.

				bq := xio.NewDefaultBlockQueue()

				bq.PushReadable(xio.NewABlock(0, 1024))
				block, ok := bq.PopReadable()
				require.True(t, ok)
				require.Equal(t, int64(0), xio.GetABlockStartOffset(block))
				require.Equal(t, int64(1024), xio.GetABlockWritten(block))

				// PushReadable put an item in the readSignal channel.
				_, ok = <-bq.ReadSignal()
				if !ok {
					t.Fatal("read signal closed")
				}

				bq.CloseRead()

				block, ok = bq.PopReadable()
				require.True(t, ok) // should always be true because CloseRead is called.
				require.Equal(t, int64(0), xio.GetABlockStartOffset(block))
				require.Equal(t, int64(0), xio.GetABlockWritten(block))

				// PushReadable put an item in the readSignal channel.
				_, ok = <-bq.ReadSignal()
				if ok {
					t.Fatal("read signal is not closed")
				}

				// Subsequent PushReadable() should not cause a panic, but it is no-op.
				bq.PushReadable(xio.NewABlock(1024, 2048))
				block, ok = bq.PopReadable()
				require.True(t, ok)
				require.Equal(t, int64(0), xio.GetABlockStartOffset(block))
				require.Equal(t, int64(0), xio.GetABlockWritten(block))

				// Subsequent CloseRead() should not cause a panic.
				bq.CloseRead()
			},
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			tt.testFunc(t)
		})
	}
}
