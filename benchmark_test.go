package xio_test

import (
	"bytes"
	"io"
	"math/rand"
	"testing"

	"github.com/ozanh/xio"
)

func BenchmarkCompareReadersDataWithBuffer(b *testing.B) {
	leftData, err := io.ReadAll(
		io.LimitReader(rand.New(rand.NewSource(1)), 1000*1000),
	)
	if err != nil {
		b.Fatal(err)
	}

	rightData, err := io.ReadAll(
		io.LimitReader(rand.New(rand.NewSource(1)), 1000*1000),
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
