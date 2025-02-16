package xio_test

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/ozanh/xio"
)

func ExampleBufPipe_example1() {
	const blockSize = 512
	const storageSize = 1024 * 1024

	storage := xio.NewBlockStorageBuffer(blockSize, storageSize)
	pr, pw := xio.BufPipe(blockSize, storageSize, storage)

	go func() {
		pw.Write([]byte("hello")) //nolint:errcheck // for simplicity
		pw.Close()                //nolint:errcheck // for simplicity
	}()

	buf := make([]byte, 1024)
	n, err := pr.Read(buf)
	if err != nil {
		panic(err)
	}
	fmt.Printf("%s", buf[:n])

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
	if err != nil {
		panic(err)
	}
	fmt.Printf("%d", n)

	// Output: 10485760
}

func ExampleBufPipe_example3() {
	const blockSize = 1024 * 1024
	const storageSize = 10 * 1024 * 1024

	storage := xio.NewBlockStorageBuffer(blockSize, storageSize)
	pr, pw := xio.BufPipe(blockSize, storageSize, storage)

	go func() {
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

	close(done)

	if err != nil {
		panic(err)
	}
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
