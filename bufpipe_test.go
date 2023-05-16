package xio_test

import (
	"bytes"
	"crypto/md5"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"io"
	"math/big"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ozanh/xio"
)

type testCase struct {
	name         string
	blockSize    int
	storsize     int64
	randFileSize int64
	slowReader   bool
}

func TestBufPipe(t *testing.T) {
	testCases := []testCase{
		{
			name:         "blockSize=1 storSize=1MiB randFileSize=1MiB",
			blockSize:    1,
			storsize:     1 * 1024 * 1024,
			randFileSize: 1 * 1024 * 1024,
		},
		{
			name:         "blockSize=10 storSize=1MiB+1 randFileSize=1MiB-1",
			blockSize:    10,
			storsize:     1*1024*1024 + 1,
			randFileSize: 1*1024*1024 - 1,
		},
		{
			name:         "blockSize=512 storSize=1MiB+1 randFileSize=1MiB-1",
			blockSize:    512,
			storsize:     1*1024*1024 + 1,
			randFileSize: 1*1024*1024 - 1,
			slowReader:   true,
		},
		{
			name:         "blockSize=512 storSize=1MiB-1 randFileSize=1MiB+1",
			blockSize:    512,
			storsize:     1*1024*1024 - 1,
			randFileSize: 1*1024*1024 + 1,
			slowReader:   true,
		},
		{
			name:         "blockSize=1MiB storSize=1MiB randFileSize=1MiB",
			blockSize:    1 * 1024 * 1024,
			storsize:     1 * 1024 * 1024,
			randFileSize: 1 * 1024 * 1024,
			slowReader:   true,
		},
		{
			name:         "blockSize=1MiB storsize=1MiB randFileSize=1MiB+1",
			blockSize:    1 * 1024 * 1024,
			storsize:     1 * 1024 * 1024,
			randFileSize: 1*1024*1024 + 1,
			slowReader:   true,
		},
		{
			name:         "blockSize=1MiB storsize=1MiB randFileSize=1MiB-1",
			blockSize:    1 * 1024 * 1024,
			storsize:     1 * 1024 * 1024,
			randFileSize: 1*1024*1024 - 1,
			slowReader:   true,
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
			slowReader:   true,
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
			slowReader:   true,
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
			slowReader:   true,
		},
		{
			name:         "blockSize=4MB storsize=10MB randFileSize=20MB+1",
			blockSize:    4 * 1000 * 1000,
			storsize:     10 * 1000 * 1000,
			randFileSize: 20*1000*1000 + 1,
			slowReader:   true,
		},
		{
			name:         "blockSize=4MB storsize=13MB randFileSize=20MB+1",
			blockSize:    4 * 1000 * 1000,
			storsize:     13 * 1000 * 1000,
			randFileSize: 20*1000*1000 + 1,
			slowReader:   true,
		},
	}

	for _, tc := range testCases {
		tc := tc
		if tc.slowReader {
			tcs := tc
			tc.slowReader = false
			t.Run(tc.name+"-slowReader", func(t *testing.T) {
				t.Parallel()
				testBufPipeHash(t, tcs)

			})
		}
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testBufPipeHash(t, tc)
		})
	}
}

func testBufPipeHash(t *testing.T, tc testCase) {
	t.Helper()

	tempdir := t.TempDir()

	storage, err := os.CreateTemp(tempdir, "storage")
	require.NoError(t, err)
	defer storage.Close()
	//storage := xio.NewByteSliceReadWriterAt(nil, true)

	err = storage.Truncate(tc.storsize)
	require.NoError(t, err)

	pr, pw := xio.BufPipe(tc.blockSize, tc.storsize, storage)

	srcfile, err := os.CreateTemp(tempdir, "randfile")
	require.NoError(t, err)
	defer srcfile.Close()

	_, err = io.Copy(srcfile, io.LimitReader(rand.Reader, tc.randFileSize))
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
	if tc.slowReader {
		src = &slowReader{r: pr}
	}

	n, err := io.Copy(io.MultiWriter(rds256, rdmd5), src)
	require.NoError(t, err)
	require.Equal(t, tc.randFileSize, n)
	require.NoError(t, pr.Close())
	wg.Wait()

	// test overflow
	info, err := storage.Stat()
	require.NoError(t, err)
	require.Equal(t, tc.storsize, info.Size())

	require.Equal(t, wrs256.Sum(nil), rds256.Sum(nil))
	require.Equal(t, wrmd5.Sum(nil), rdmd5.Sum(nil))
}

type slowReader struct {
	r io.Reader
}

func (r *slowReader) Read(p []byte) (int, error) {
	b, err := rand.Int(rand.Reader, big.NewInt(120))
	if err == nil {
		d := time.Duration(b.Int64()) * time.Millisecond / 10
		time.Sleep(d)
	}
	return r.r.Read(p)
}

func TestBufPipe_single_block(t *testing.T) {
	storage := xio.NewByteSliceReadWriterAt(make([]byte, 1000), true)
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
	storage := xio.NewByteSliceReadWriterAt(make([]byte, 1000), false)
	_, pw := xio.BufPipe(1024, 1024*1024, storage)

	data := make([]byte, 1024)
	_, err := io.ReadFull(rand.Reader, data)
	require.NoError(t, err)

	n, err := pw.Write(data)
	require.Equal(t, xio.ErrNoSpaceLeft, err)
	require.Equal(t, 1000, n)
	require.Equal(t, 1000, storage.Len())
	require.Equal(t, data[:1000], storage.Bytes())
}

func TestBufPipe_write_close_error(t *testing.T) {
	storage := xio.NewByteSliceReadWriterAt(make([]byte, 1024), false)
	pr, pw := xio.BufPipe(1024, 1024*1024, storage)

	data := make([]byte, 1024)
	_, err := io.ReadFull(rand.Reader, data)
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
	storage := xio.NewByteSliceReadWriterAt(make([]byte, 1024), false)
	pr, pw := xio.BufPipe(1024, 1024*1024, storage)

	data := make([]byte, 1024)
	_, err := io.ReadFull(rand.Reader, data)
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

	done := make(chan struct{})
	go func() {
		defer close(done)
		time.Sleep(100 * time.Millisecond)
		err := pw.Close()
		assert.NoError(t, err)
	}()
	n, err = pr.Read(p)
	require.Equal(t, io.EOF, err)
	require.Equal(t, 0, n)
	<-done
}

func TestBufPipe_read_close_error_async(t *testing.T) {
	storage := xio.NewByteSliceReadWriterAt(make([]byte, 1024), false)
	pr, pw := xio.BufPipe(1024, 1024*1024, storage)

	data := make([]byte, 1024)
	_, err := io.ReadFull(rand.Reader, data)
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
