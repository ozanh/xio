package xio_test

import (
	"io"
	"testing"

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
