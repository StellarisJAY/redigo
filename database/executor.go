package database

import (
	"redigo/redis"
	"redigo/redis/protocol"
	"strings"
)

// ExecFunc executes a command using target database, returns a Reply
type ExecFunc func(db *SingleDB, command redis.Command) *protocol.Reply

type CommandExecutor struct {
	execFunc ExecFunc
}

var executors = make(map[string]*CommandExecutor)

func RegisterCommandExecutor(cmdName string, exec ExecFunc) {
	cmdName = strings.ToLower(cmdName)
	executor := &CommandExecutor{execFunc: exec}
	executors[cmdName] = executor
}
