package tcp

import (
	"context"
	"redigo/pkg/redis"
)

// EventLoop 主事件循环，负责Accept和DispatchIO
type EventLoop interface {
	Listen(address string) error
	Accept() error
	Handle(ctx context.Context) error
	DispatchIO(conn redis.Connection, flag byte)
}

const (
	Read byte = iota
	Write
)

type IOTask struct {
	conn redis.Connection
	flag byte
}
