package buffer

type Buffer interface {
	// Read from buffer, this method extends io.Reader
	Read([]byte) (int, error)
	// ReadUntil read from the current read index until the delim byte
	ReadUntil(delim byte) ([]byte, error)
	// Next returns a slice of N bytes starting from current read index
	Next(n int) ([]byte, error)
	// Skip n bytes
	Skip(n int) error
	// ReadByte reads only one byte from read index
	ReadByte() (byte, error)

	// Write bytes to buffer, io.Writer
	Write([]byte) (int, error)
	// WriteString writes a string to buffer
	WriteString(s string) error
	// WriteByte writes a single byte to buffer
	WriteByte(b byte) error

	Len() int
	Cap() int
	// Available get the available space in buffer
	Available() int
}

const EmptyBufferSize = 256
const MaxBufferSize = 16 * 1024
