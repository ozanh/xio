package xio_test

import (
	"bytes"
	"io"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/ozanh/xio"
)

func TestBlocksStorageBuffer_ReadAt(t *testing.T) {
	// Test case 1: Read within storage bounds
	blockSize := 256
	storageSize := 1024
	storage := xio.NewBlockStorageBuffer(blockSize, storageSize)
	data := []byte("Hello, World!")
	expected := make([]byte, len(data))
	copy(expected, data)
	off := int64(0)

	// Write data to the storage
	n, err := storage.WriteAt(data, off)
	if err != nil {
		t.Fatalf("Failed to write data to the storage: %v", err)
	}
	if n != len(data) {
		t.Fatalf("Expected to write %d bytes, but wrote %d", len(data), n)
	}

	// Read data from the storage
	readData := make([]byte, len(data))
	n, err = storage.ReadAt(readData, off)
	if err != nil {
		t.Fatalf("Failed to read data from the storage: %v", err)
	}
	if n != len(data) {
		t.Fatalf("Expected to read %d bytes, but got %d", len(data), n)
	}
	if string(readData) != string(expected) {
		t.Fatalf("Read data does not match expected data")
	}

	// Test case 2: Read beyond storage bounds
	readData = make([]byte, len(data))
	off = int64(storageSize + 1)
	n, err = storage.ReadAt(readData, off)
	if err != io.EOF {
		t.Fatalf("Expected io.EOF error, but got %v", err)
	}
	if n != 0 {
		t.Fatalf("Expected to read 0 bytes, but got %d", n)
	}
}

func TestBlocksStorageBuffer_WriteAt(t *testing.T) {
	// Test case 1: Write within storage bounds
	blockSize := 256
	storageSize := 1024
	storage := xio.NewBlockStorageBuffer(blockSize, storageSize)
	data := []byte("Hello, World!")
	off := int64(0)

	// Write data to the storage
	n, err := storage.WriteAt(data, off)
	if err != nil {
		t.Fatalf("Failed to write data to the storage: %v", err)
	}
	if n != len(data) {
		t.Fatalf("Expected to write %d bytes, but wrote %d", len(data), n)
	}

	// Read data from the storage to verify
	readData := make([]byte, len(data))
	_, err = storage.ReadAt(readData, off)
	if err != nil {
		t.Fatalf("Failed to read data from the storage: %v", err)
	}
	if string(readData) != string(data) {
		t.Fatalf("Written data does not match expected data")
	}

	// Test case 2: Write beyond storage bounds
	data = []byte("This is a longer string that exceeds the storage size")
	off = int64(storageSize + 1)
	n, err = storage.WriteAt(data, off)
	if err != xio.ErrNoSpaceLeft {
		t.Fatalf("Expected ErrNoSpaceLeft error, but got %v", err)
	}
	if n != 0 {
		t.Fatalf("Expected to write 0 bytes, but wrote %d", n)
	}
}

func TestBlocksStorageBuffer_read_not_written(t *testing.T) {
	blockSize := 256
	storageSize := 1024
	storage := xio.NewBlockStorageBuffer(blockSize, storageSize)
	data := []byte("Hello, World!")
	off := int64(0)

	// Write data to the storage
	n, err := storage.WriteAt(data, off)
	if err != nil {
		t.Fatalf("Failed to write data to the storage: %v", err)
	}
	if n != len(data) {
		t.Fatalf("Expected to write %d bytes, but wrote %d", len(data), n)
	}

	off = int64(len(data)) + 1
	readData := make([]byte, len(data))
	n, err = storage.ReadAt(readData, off)
	if err != nil {
		t.Fatalf("Failed to read data from the storage: %v", err)
	}
	if n != len(data) {
		t.Fatalf("Expected to read %d bytes, but got %d", len(data), n)
	}
	if string(readData) != string(make([]byte, len(data))) {
		t.Fatalf("Read data does not match expected data")
	}
}

func TestNewBlocksStorageBuffer_size_getters(t *testing.T) {
	blockSize := 256
	storageSize := 1024
	storage := xio.NewBlockStorageBuffer(blockSize, storageSize)

	if storage.BlockSize() != blockSize {
		t.Fatalf("Expected block size %d, but got %d", blockSize, storage.BlockSize())
	}

	if storage.StorageSize() != storageSize {
		t.Fatalf("Expected storage size %d, but got %d", storageSize, storage.StorageSize())
	}
}

func TestBlockStorageBuffer_read_all(t *testing.T) {
	const storageSize = 1024

	bs := xio.NewBlockStorageBuffer(256, storageSize)

	if bs.StorageSize() != storageSize {
		t.Fatalf("Storage size mismatch, expected %d, got %d", storageSize, bs.StorageSize())
	}

	srd := io.NewSectionReader(bs, 0, int64(bs.StorageSize()))

	data, err := io.ReadAll(srd)
	if err != nil {
		t.Fatalf("Failed to read all data: %v", err)
	}

	for i := 0; i < len(data); i++ {
		if data[i] != 0 {
			t.Fatalf("non-zero byte found at index %d", i)
		}
	}

	// Fill the storage with random data

	randData, err := io.ReadAll(
		io.LimitReader(
			rand.New(rand.NewSource(time.Now().UnixNano())),
			storageSize,
		),
	)
	if err != nil {
		t.Fatalf("Failed to read random data: %v", err)
	}

	owr := io.NewOffsetWriter(bs, 0)
	n, err := owr.Write(randData)
	if err != nil {
		t.Fatalf("Failed to write random data: %v", err)
	}

	if n != len(randData) {
		t.Fatalf("Data write mismatch %d != %d", n, len(randData))
	}

	srd = io.NewSectionReader(bs, 0, int64(bs.StorageSize()))

	data, err = io.ReadAll(srd)
	if err != nil {
		t.Fatalf("Failed to read all data: %v", err)
	}

	if len(data) != storageSize {
		t.Fatalf("Data size mismatch")
	}

	if string(data) != string(randData) {
		t.Fatalf("Data mismatch")
	}
}

func TestBlockStorageBuffer_odd_numbers(t *testing.T) {
	const storageSize = 1255

	bs := xio.NewBlockStorageBuffer(251, 1024)

	if bs.StorageSize() != storageSize {
		t.Fatalf("Storage size mismatch, expected %d, got %d", storageSize, bs.StorageSize())
	}

	srd := io.NewSectionReader(bs, 0, int64(bs.StorageSize()))

	buf := bytes.NewBuffer(nil)

	n, err := io.CopyBuffer(&writerOnly{buf}, &readerOnly{srd}, make([]byte, 5))
	if err != nil {
		t.Fatalf("Failed to read all data: %v", err)
	}

	if n != storageSize {
		t.Fatalf("Written size mismatch, expected %d, got %d", storageSize, n)
	}

	data := buf.Bytes()

	for i := 0; i < len(data); i++ {
		if data[i] != 0 {
			t.Fatalf("non-zero byte found at index %d", i)
		}
	}

	// Fill the storage with random data

	randData, err := io.ReadAll(
		io.LimitReader(
			rand.New(rand.NewSource(time.Now().UnixNano())),
			storageSize,
		),
	)
	if err != nil {
		t.Fatalf("Failed to read random data: %v", err)
	}
	for i := 0; i < 111; i++ {
		randData[i] = 0
	}

	owr := io.NewOffsetWriter(bs, 111)
	nn, err := owr.Write(randData[111:])
	if err != nil {
		t.Fatalf("Failed to write random data: %v", err)
	}
	nn += 111

	if nn != len(randData) {
		t.Fatalf("Data write mismatch %d != %d", n, len(randData))
	}

	srd = io.NewSectionReader(bs, 0, int64(bs.StorageSize()))

	buf = bytes.NewBuffer(nil)

	n, err = io.CopyBuffer(&writerOnly{buf}, &readerOnly{srd}, make([]byte, 7))
	if err != nil {
		t.Fatalf("Failed to read all data: %v", err)
	}

	if n != storageSize {
		t.Fatalf("Written size mismatch, expected %d, got %d", storageSize, n)
	}

	data = buf.Bytes()

	if len(data) != storageSize {
		t.Fatalf("Data size mismatch")
	}

	if string(data) != string(randData) {
		t.Fatalf("Data mismatch")
	}
}

func TestBlockStorageBuffer_errors(t *testing.T) {
	t.Run("read invalid offset", func(t *testing.T) {
		bs := xio.NewBlockStorageBuffer(256, 1024)

		n, err := bs.ReadAt(make([]byte, 1), -1)
		if err == nil {
			t.Fatalf("Expected error, but got nil")
		}

		if n != 0 {
			t.Fatalf("Expected to read 0 bytes, but read %d", n)
		}

		if !strings.Contains(err.Error(), "invalid offset") {
			t.Fatalf("Expected invalid offset error, but got %v", err)
		}
	})

	t.Run("write invalid offset", func(t *testing.T) {
		bs := xio.NewBlockStorageBuffer(256, 1024)

		n, err := bs.WriteAt(make([]byte, 1), -1)
		if err == nil {
			t.Fatalf("Expected error, but got nil")
		}

		if n != 0 {
			t.Fatalf("Expected to write 0 bytes, but wrote %d", n)
		}

		if !strings.Contains(err.Error(), "invalid offset") {
			t.Fatalf("Expected invalid offset error, but got %v", err)
		}
	})

	t.Run("read more than storage size", func(t *testing.T) {
		bs := xio.NewBlockStorageBuffer(256, 1024)

		n, err := bs.ReadAt(make([]byte, 1), 1025)
		if err == nil {
			t.Fatalf("Expected error, but got nil")
		}

		if n != 0 {
			t.Fatalf("Expected to read 0 bytes, but read %d", n)
		}

		if err != io.EOF {
			t.Fatalf("Expected EOF error, but got %v", err)
		}
	})

	t.Run("write more than storage size", func(t *testing.T) {
		bs := xio.NewBlockStorageBuffer(256, 1024)

		n, err := bs.WriteAt(make([]byte, 1), 1025)
		if err == nil {
			t.Fatalf("Expected error, but got nil")
		}

		if n != 0 {
			t.Fatalf("Expected to write 0 bytes, but wrote %d", n)
		}

		if err != xio.ErrNoSpaceLeft {
			t.Fatalf("Expected ErrNoSpaceLeft error, but got %v", err)
		}
	})
}

type writerOnly struct {
	io.Writer
}

type readerOnly struct {
	io.Reader
}
