package database

import (
	"redigo/redis"
	"strings"
)

type ExecFunc func(db *SingleDB, command *redis.Command)

type CommandExecutor struct {
	execFunc ExecFunc
}

var executors = make(map[string]*CommandExecutor)

func RegisterCommandExecutor(cmdName string, exec ExecFunc) {
	cmdName = strings.ToLower(cmdName)
	executor := &CommandExecutor{execFunc: exec}
	executors[cmdName] = executor
}
