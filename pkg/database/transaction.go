package database

import (
	"fmt"
	"redigo/pkg/redis"
	"strconv"
	"strings"
)

var forbiddenCmds = map[string]bool{"flushdb": true, "watch": true, "unwatch": true}

func Watch(db *SingleDB, conn redis.Connection, keys []string) *redis.RespCommand {
	for _, key := range keys {
		version := db.getVersion(key)
		conn.AddWatching(fmt.Sprintf("%d_%s", db.idx, key), version)
	}
	return redis.OKCommand
}

func UnWatch(conn redis.Connection) *redis.RespCommand {
	conn.UnWatch()
	return redis.OKCommand
}

func StartMulti(conn redis.Connection) *redis.RespCommand {
	if conn.IsMulti() {
		return redis.NewErrorCommand(redis.NestedMultiCallError)
	}
	conn.SetMulti(true)
	return redis.OKCommand
}

func EnqueueCommand(conn redis.Connection, command redis.Command) *redis.RespCommand {
	name := command.Name()
	cmdExecutor, ok := executors[name]
	if name == "multi" {
		return redis.NewErrorCommand(redis.NestedMultiCallError)
	}
	if name == "select" {
		conn.EnqueueCommand(command.(*redis.RespCommand))
		return redis.QueuedCommand
	}
	if !ok {
		return redis.NewErrorCommand(redis.CreateUnknownCommandError(name))
	}
	if _, ok = forbiddenCmds[name]; ok {
		return redis.NewErrorCommand(redis.CommandCannotUseInMultiError)
	}
	if !cmdExecutor.validateArgCount(len(command.Args())) {
		return redis.NewErrorCommand(redis.CreateWrongArgumentNumberError(name))
	}
	conn.EnqueueCommand(command.(*redis.RespCommand))
	return redis.QueuedCommand
}

func Exec(db *MultiDB, conn redis.Connection) *redis.RespCommand {
	defer conn.SetMulti(false)
	// check the watched keys' versions
	watching := conn.GetWatching()
	for watch, version := range watching {
		if isWatchingChanged(db, watch, version) {
			return redis.NilCommand
		}
	}
	commands := conn.GetQueuedCommands()
	replies := make([][]byte, len(commands))
	for i, command := range commands {
		reply := db.executeCommand(command)
		replies[i] = redis.Encode(reply)
	}
	return redis.NewNestedArrayCommand(replies)
}

func Discard(conn redis.Connection) *redis.RespCommand {
	if !conn.IsMulti() {
		return redis.NewErrorCommand(redis.DiscardWithoutMultiError)
	}
	conn.SetMulti(false)
	return redis.OKCommand
}

// check the version of the watched key. The string arg watch is dbIndex and key combined
func isWatchingChanged(db *MultiDB, watch string, version int64) bool {
	split := strings.Index(watch, "_")
	dbIndex, err := strconv.Atoi(watch[0:split])
	if err != nil {
		return true
	}
	key := watch[split+1:]
	return version != db.getVersion(dbIndex, key)
}
