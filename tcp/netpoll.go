package tcp

import "context"

type Poller interface {
	Listen(address string) error
	Accept() error
	Handle(ctx context.Context) error
}
