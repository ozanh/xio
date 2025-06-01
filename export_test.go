package xio

// Exported for testing purposes.

// NewDefaultBlockQueue creates a new default block queue for testing purposes.
func NewDefaultBlockQueue() *blockQueue {
	return newDefaultBlockQueue()
}

// ReadSignal returns the read signal channel for testing purposes.
func (b *blockQueue) ReadSignal() <-chan struct{} {
	return b.readSignal
}

// WriteSignal returns the write signal channel for testing purposes.
func (b *blockQueue) WriteSignal() <-chan struct{} {
	return b.writeSignal
}

// ABlock is exported for testing purposes.
type ABlock = aBlock

// NewABlock creates a new ABlock for testing purposes.
func NewABlock(startOffset, written int64) ABlock {
	return ABlock{
		startOffset: startOffset,
		written:     written,
	}
}

// GetABlockStartOffset returns the startOffset field of ABlock for testing purposes.
func GetABlockStartOffset(block ABlock) int64 {
	return block.startOffset
}

// GetABlockWritten returns the written field of ABlock for testing purposes.
func GetABlockWritten(block ABlock) int64 {
	return block.written
}
