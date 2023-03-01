package buffer

import "errors"

type Buffer interface {
	// Read from buffer, this method extends io.Reader
	Read([]byte) (int, error)
	// ReadBytes read from the current read index until the delim byte
	ReadBytes(delim byte) ([]byte, error)
	// Write bytes to buffer, io.Writer
	Write([]byte) (int, error)

	Len() int
	Cap() int
	// MarkReadIndex 记录当前read指针位置，可以通过reset回溯
	MarkReadIndex()
	// ResetReadIndex 重置read指针到上次mark的位置
	ResetReadIndex()
	// Reset 清空整个buffer
	Reset()
	Bytes() []byte
	ReadIndex() int
	WriteIndex() int
}

const MaxBufferSize = 16 * 1024

var (
	ErrBufferOverflow  = errors.New("buffer overflows")
	ErrBufferSizeLimit = errors.New("buffer overrun size limit")
	ErrUnexpectedEOF   = errors.New("unexpected EOF")
)
