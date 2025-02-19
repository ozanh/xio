package xio_test

import (
	"bytes"
	"io"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/ozanh/xio"
)

func BenchmarkCompareReadersDataWithBuffer(b *testing.B) {
	seed := time.Now().UnixNano()

	leftData, err := io.ReadAll(
		io.LimitReader(rand.New(rand.NewSource(seed)), 1000*1000),
	)
	if err != nil {
		b.Fatal(err)
	}

	rightData, err := io.ReadAll(
		io.LimitReader(rand.New(rand.NewSource(seed)), 1000*1000),
	)
	if err != nil {
		b.Fatal(err)
	}

	left := bytes.NewReader(leftData)
	right := bytes.NewReader(rightData)

	buf := make([]byte, 8*64*bytes.MinRead)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		err := xio.CompareReadersDataWithBuffer(left, right, buf)
		if err != nil {
			b.Fatal(err)
		}

		left.Reset(leftData)
		right.Reset(rightData)
	}
}

func BenchmarkBufPipe_with_StorageBuffer(b *testing.B) {
	const payloadSize = 10 * 1024 * 1024
	const blockSize = 1 * 1024 * 1024
	const storageSize = payloadSize

	// Allocate all the memory we need before the benchmark starts.
	bsb := xio.NewStorageBuffer(make([]byte, storageSize), false)

	payload := make([]byte, payloadSize)
	reader := bytes.NewReader(payload)
	copyBufr := make([]byte, 32*1024)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		reader.Reset(payload)

		pr, pw := xio.BufPipe(blockSize, storageSize, bsb)

		var wg sync.WaitGroup

		wg.Add(1)
		go func() {
			defer wg.Done()

			n, err := reader.WriteTo(pw)

			_ = pw.CloseWithError(err)

			if err != nil {
				b.Error(err)
			}

			if n != payloadSize {
				b.Errorf("unexpected write size, got: %d", n)
			}
		}()

		n, err := io.CopyBuffer(io.Discard, pr, copyBufr)

		_ = pr.CloseWithError(err)

		if err != nil {
			b.Fatal(err)
		}

		if n != payloadSize {
			b.Fatalf("unexpected read size, got: %d", n)
		}

		wg.Wait()
	}
}

func BenchmarkIoPipe(b *testing.B) {
	const payloadSize = 10 * 1024 * 1024

	payload := make([]byte, payloadSize)
	reader := bytes.NewReader(payload)
	copyBufr := make([]byte, 32*1024)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		reader.Reset(payload)

		pr, pw := io.Pipe()

		var wg sync.WaitGroup

		wg.Add(1)
		go func() {
			defer wg.Done()

			n, err := reader.WriteTo(pw)

			_ = pw.CloseWithError(err)

			if err != nil {
				b.Error(err)
			}

			if n != payloadSize {
				b.Errorf("unexpected write size, got: %d", n)
			}
		}()

		n, err := io.CopyBuffer(io.Discard, pr, copyBufr)

		_ = pr.CloseWithError(err)

		if err != nil {
			b.Fatal(err)
		}

		if n != payloadSize {
			b.Fatalf("unexpected read size, got: %d", n)
		}

		wg.Wait()
	}
}
