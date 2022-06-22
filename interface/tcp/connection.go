package tcp

import "redigo/redis/protocol"

type Connection interface {
	ReadLoop() error
	WriteLoop() error
	Close()
	Write([]byte)
	SendReply(*protocol.Reply)
}
