package database

import (
	"redigo/redis"
)

type DB interface {
	// SubmitCommand submit a command to execution channel
	SubmitCommand(command *redis.Command)
	// Close DB
	Close()
	// ExecuteLoop continuously execute commands in serialized way
	ExecuteLoop() error
}
