package xio_test

import (
	"bytes"
	"io"
	"strings"
	"sync"
	"testing"

	"github.com/ozanh/xio"
)

type counterTestCase struct {
	name        string
	reader      io.Reader
	wantData    []byte
	wantCount   uint64
	wantErr     error
	concurrency int
}

func TestReadCounter(t *testing.T) {
	var iface any = new(xio.ReadDataCounter[io.Reader])

	if v, _ := iface.(xio.ReadCounter); v == nil {
		t.Fatal("ReadDataCounter does not implement ReadCounter")
	}

	iface = new(xio.ReadDataAtomicCounter[io.Reader])

	if v, _ := iface.(xio.ReadCounter); v == nil {
		t.Fatal("ReadDataAtomicCounter does not implement ReadCounter")
	}
}

func TestReadDataCounter(t *testing.T) {
	tests := []counterTestCase{
		{
			name:      "normal string read",
			reader:    strings.NewReader("hello"),
			wantData:  []byte("hello"),
			wantCount: 5,
		},
		{
			name:      "empty string read",
			reader:    strings.NewReader(""),
			wantData:  []byte(""),
			wantCount: 0,
		},
		{
			name:      "multiple reads",
			reader:    bytes.NewReader([]byte("hello world")),
			wantData:  []byte("hello world"),
			wantCount: 11,
		},
		{
			name:    "error case",
			reader:  &xio.ErrOrEofReader{Err: xio.ErrComparisonReadError},
			wantErr: xio.ErrComparisonReadError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			counter := xio.NewReadDataCounter(tt.reader)

			data, err := io.ReadAll(counter)

			if tt.wantData != nil && !bytes.Equal(data, tt.wantData) {
				t.Errorf("expected data '%s', got '%s'", tt.wantData, data)
			}

			if tt.wantErr != nil && err != tt.wantErr {
				t.Errorf("expected error %v, got %v", tt.wantErr, err)
			}

			if counter.Count() != tt.wantCount {
				t.Errorf("expected count %d, got %d", tt.wantCount, counter.Count())
			}
		})
	}
}

func TestReadDataAtomicCounter(t *testing.T) {
	tests := []counterTestCase{
		{
			name:      "normal string read",
			reader:    strings.NewReader("hello"),
			wantCount: 5,
		},
		{
			name:      "empty string read",
			reader:    strings.NewReader(""),
			wantCount: 0,
		},
		{
			name:        "concurrent reads",
			reader:      strings.NewReader("hello world"),
			wantCount:   11,
			concurrency: 10,
		},
		{
			name:    "error case",
			reader:  &xio.ErrOrEofReader{Err: xio.ErrComparisonReadError},
			wantErr: xio.ErrComparisonReadError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			counter := xio.NewReadDataAtomicCounter(&syncReader{r: tt.reader})

			if tt.concurrency > 0 {
				var wg sync.WaitGroup
				for i := 0; i < tt.concurrency; i++ {
					wg.Add(1)
					go func() {
						defer wg.Done()

						_, _ = counter.Read(make([]byte, 2))
					}()
				}
				wg.Wait()
			} else {
				data, err := io.ReadAll(counter)

				if tt.wantData != nil && !bytes.Equal(data, tt.wantData) {
					t.Errorf("expected data '%s', got '%s'", tt.wantData, data)
				}

				if tt.wantErr != nil && err.Error() != tt.wantErr.Error() {
					t.Errorf("expected error %v, got %v", tt.wantErr, err)
				}
			}

			if counter.Count() != tt.wantCount {
				t.Errorf("expected count %d, got %d", tt.wantCount, counter.Count())
			}
		})
	}
}

type syncReader struct {
	mu sync.Mutex
	r  io.Reader
}

func (r *syncReader) Read(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.r.Read(p)
}
