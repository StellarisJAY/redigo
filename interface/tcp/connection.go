package tcp

import "redigo/redis/protocol"

type Connection interface {
	ReadLoop() error
	WriteLoop() error
	Close()
	SendReply(*protocol.Reply)

	SelectDB(index int)
	DBIndex() int
}
