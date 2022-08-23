package database

import (
	"redigo/redis"
	"time"
)

type DB interface {
	// SubmitCommand submit a command to execution channel
	SubmitCommand(command redis.Command)
	// Close DB
	Close()
	// ExecuteLoop continuously execute commands in serialized way
	ExecuteLoop() error
	Execute(command redis.Command) *redis.RespCommand
	ForEach(dbIdx int, fun func(key string, entry *Entry, expire *time.Time) bool)
	Len(dbIdx int) int

	OnConnectionClosed(conn redis.Connection)
}

/*
	Entry holds a data of a key
*/
type Entry struct {
	Key          string
	Data         interface{}
	NextLRUEntry *Entry
	PrevLRUEntry *Entry
	DataSize     int64
	AccessCount  int
}
