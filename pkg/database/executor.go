package database

import (
	"redigo/pkg/redis"
	"strings"
)

// ExecFunc 定义命令处理函数，参数：数据库实例、命令，返回：redis命令
type ExecFunc func(db *SingleDB, command redis.Command) *redis.RespCommand

type CommandExecutor struct {
	execFunc ExecFunc
	argCount int // 命令需要的参数数量，argCount>0, 则参数必须等于argCount，小于0则大于等于argCount
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
