package database

import (
	"redigo/interface/redis"
	"redigo/redis/protocol"
	"strings"
)

// ExecFunc executes a command using target database, returns a Reply
type ExecFunc func(db *SingleDB, command redis.Command) *protocol.Reply

type CommandExecutor struct {
	execFunc ExecFunc
	argCount int // nums of args needed, if argCount < 0: len(args) >= -argCount
}

var executors = make(map[string]*CommandExecutor)

func RegisterCommandExecutor(cmdName string, exec ExecFunc, argCount int) {
	cmdName = strings.ToLower(cmdName)
	executor := &CommandExecutor{execFunc: exec, argCount: argCount}
	executors[cmdName] = executor
}

func ValidateArgCount(name string, count int) bool {
	executor := executors[name]
	if executor.argCount < 0 {
		return count >= -executor.argCount
	} else {
		return count == executor.argCount
	}
}
func (e *CommandExecutor) validateArgCount(count int) bool {
	if e.argCount < 0 {
		return count >= -e.argCount
	} else {
		return count == e.argCount
	}
}
