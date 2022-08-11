package redis

import (
	"redigo/redis/protocol"
)

type Connection interface {
	ReadLoop() error
	WriteLoop() error
	Close()
	SendReply(*protocol.Reply)

	SelectDB(index int)
	DBIndex() int

	SetMulti(bool)
	IsMulti() bool
	EnqueueCommand(command Command)
	GetQueuedCommands() []Command
	AddWatching(key string, version int64)
	GetWatching() map[string]int64
	UnWatch()

	Active() bool
	RemoteAddr() string
}
