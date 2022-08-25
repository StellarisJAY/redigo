package tcp

type Poller interface {
	Listen(address string) error
	Accept() error
	Handle() error
}
