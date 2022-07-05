package database

import (
	"redigo/redis"
	"redigo/redis/protocol"
	"time"
)

type DB interface {
	// SubmitCommand submit a command to execution channel
	SubmitCommand(command redis.Command)
	// Close DB
	Close()
	// ExecuteLoop continuously execute commands in serialized way
	ExecuteLoop() error
	Execute(command redis.Command) *protocol.Reply
	ForEach(dbIdx int, fun func(key string, entry *Entry, expire *time.Time) bool)
	Len(dbIdx int) int
}

/*
	Entry holds a data of a key
*/
type Entry struct {
	Data interface{}
}
