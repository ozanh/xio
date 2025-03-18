package xio_test

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/ozanh/xio"
)

func ExampleBufPipe_example1() {
	const blockSize = 512
	const storageSize = 1024 * 1024

	storage := xio.NewBlockStorageBuffer(blockSize, storageSize)
	pr, pw := xio.BufPipe(blockSize, storageSize, storage)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()

		_, err := pw.Write([]byte("hello"))
		if err != nil {
			panic(err)
		}
		_ = pw.Close()
	}()

	buf := bytes.NewBuffer(nil)

	_, err := io.Copy(buf, pr)
	_ = pr.CloseWithError(err)

	wg.Wait()

	fmt.Printf("%s", buf.Bytes())

	// Output: hello
}

func ExampleBufPipe_example2() {
	const blockSize = 1024 * 1024
	const storageSize = 10 * 1024 * 1024

	storage := xio.NewBlockStorageBuffer(blockSize, storageSize)
	pr, pw := xio.BufPipe(blockSize, storageSize, storage)

	go func() {
		src := io.LimitReader(rand.Reader, storageSize)

		_, err := io.Copy(pw, src)
		_ = pw.CloseWithError(err)
	}()

	n, err := io.Copy(io.Discard, pr)
	_ = pr.CloseWithError(err)

	fmt.Printf("%d", n)

	// Output: 10485760
}

func ExampleBufPipe_example3() {
	const blockSize = 1024 * 1024
	const storageSize = 10 * 1024 * 1024

	storage := xio.NewBlockStorageBuffer(blockSize, storageSize)
	pr, pw := xio.BufPipe(blockSize, storageSize, storage)

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()

		src := io.LimitReader(rand.Reader, storageSize)

		_, err := io.Copy(pw, src)
		_ = pw.CloseWithError(err)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	done := make(chan struct{})

	go func() {
		select {
		case <-ctx.Done():
			_ = pw.CloseWithError(ctx.Err())
		case <-done:
		}
	}()

	n, err := io.Copy(io.Discard, pr)
	_ = pr.CloseWithError(err)

	close(done)

	wg.Wait()

	fmt.Printf("%d", n)

	// Output: 10485760
}

func ExampleBufPipe_example4() {
	const blockSize = 1024 * 1024
	const storageSize = 10 * 1024 * 1024

	storage, err := os.CreateTemp("", "storage")
	if err != nil {
		panic(err)
	}
	defer os.Remove(storage.Name())
	defer storage.Close()

	err = storage.Truncate(storageSize)
	if err != nil {
		panic(err)
	}

	pr, pw := xio.BufPipe(blockSize, storageSize, storage)

	go func() {
		src := io.LimitReader(rand.Reader, 40_000_000)

		_, err := io.Copy(pw, src)
		_ = pw.CloseWithError(err)
	}()

	n, err := io.Copy(io.Discard, pr)
	if err != nil {
		panic(err)
	}
	fmt.Printf("%d", n)

	// Output: 40000000
}

func ExampleLruReaderAt_simple() {
	// Simple example of using LruReaderAt with xio.StorageBuffer.
	// Any io.ReaderAt can be used as the underlying reader.

	const helloWorld = "Hello, World!"

	rw := xio.NewStorageBuffer(make([]byte, 1024), false)
	n, err := rw.WriteAt([]byte(helloWorld), 0)
	if err != nil {
		panic(err)
	}
	if n != len(helloWorld) {
		panic("write failed")
	}

	const blockSize = 512
	const cacheSize = 10

	lruReader, err := xio.NewLruReaderAt(rw, blockSize, cacheSize)
	if err != nil {
		panic(err)
	}

	buf := make([]byte, len(helloWorld))
	n, err = lruReader.ReadAt(buf, 0)
	if err != nil {
		panic(err)
	}
	if n != len(helloWorld) {
		panic("read failed")
	}

	fmt.Printf("%s\n", buf)

	// Output: Hello, World!
}

func ExampleCmpReadersData() {
	b := make([]byte, 1000*1000)

	_, err := xio.ReadFill(rand.Reader, b)
	if err != nil {
		panic(err)
	}

	r1 := bytes.NewReader(b)
	r2 := bufio.NewReader(bytes.NewReader(b))

	err = xio.CmpReadersData(r1, r2)
	if err != nil {
		panic(err)
	}

	fmt.Println("Readers have equal data")

	// Output: Readers have equal data
}
