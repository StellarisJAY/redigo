package tcp

type Connection interface {
	ReadLoop() error
	WriteLoop() error
	Close()
	Write([]byte)
}
