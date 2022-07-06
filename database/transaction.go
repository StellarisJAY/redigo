package database

import (
	"fmt"
	"redigo/interface/redis"
	"redigo/redis/protocol"
	"strconv"
	"strings"
)

var forbiddenCmds = map[string]bool{"flushdb": true}

func Watch(db *SingleDB, conn redis.Connection, keys []string) *protocol.Reply {
	for _, key := range keys {
		version := db.getVersion(key)
		conn.AddWatching(fmt.Sprintf("%d_%s", db.idx, key), version)
	}
	return protocol.OKReply
}

func StartMulti(conn redis.Connection) *protocol.Reply {
	if conn.IsMulti() {
		return protocol.NewErrorReply(protocol.NestedMultiCallError)
	}
	conn.SetMulti(true)
	return protocol.OKReply
}

func EnqueueCommand(conn redis.Connection, command redis.Command) *protocol.Reply {
	name := command.Name()
	cmdExecutor, ok := executors[name]
	if name == "multi" {
		return protocol.NewErrorReply(protocol.NestedMultiCallError)
	}
	if !ok {
		return protocol.NewErrorReply(protocol.CreateUnknownCommandError(name))
	}
	if _, ok = forbiddenCmds[name]; ok {
		return protocol.NewErrorReply(protocol.CommandCannotUseInMultiError)
	}
	if !cmdExecutor.validateArgCount(len(command.Args())) {
		return protocol.NewErrorReply(protocol.CreateWrongArgumentNumberError(name))
	}
	conn.EnqueueCommand(command)
	return protocol.QueuedReply
}

func Exec(db *MultiDB, conn redis.Connection) *protocol.Reply {
	// check the watched keys' versions
	watching := conn.GetWatching()
	for watch, version := range watching {
		if isWatchingChanged(db, watch, version) {
			return protocol.NilReply
		}
	}
	commands := conn.GetQueuedCommands()
	replies := make([][]byte, len(commands))
	for i, command := range commands {
		reply := db.executeCommand(command)
		replies[i] = reply.ToBytes()
	}
	return protocol.NewArrayReply(replies)
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