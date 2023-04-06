package tcp

type Server interface {
	Start() error
	Close()
}
