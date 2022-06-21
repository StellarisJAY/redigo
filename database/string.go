package database

import (
	"redigo/redis"
	"redigo/redis/protocol"
)

func init() {
	RegisterCommandExecutor("set", executeSet)
	RegisterCommandExecutor("get", executeGet)
	RegisterCommandExecutor("setnx", executeSetNX)
}

func executeSet(db *SingleDB, command *redis.Command) {
	args := command.Args()
	conn := command.Connection()
	if len(args) < 2 {
		conn.Write(protocol.CreateWrongArgumentNumberError("set"))
		return
	}
	key := string(args[0])
	value := args[1]
	entry := &Entry{Data: value}
	res := db.data.Put(key, entry)
	conn.Write(protocol.CreateNumberReply(res))
}

func executeGet(db *SingleDB, command *redis.Command) {
	args := command.Args()
	conn := command.Connection()
	if args == nil || len(args) == 0 {
		conn.Write(protocol.CreateWrongArgumentNumberError("get"))
		return
	}
	key := string(args[0])
	v, exists := db.data.Get(key)
	if exists {
		entry := v.(*Entry)
		value := entry.Data.([]byte)
		conn.Write(protocol.CreateSingleStringReply(string(value)))
	} else {
		conn.Write(protocol.CreateSingleStringReply("(nil)"))
	}
}

func executeSetNX(db *SingleDB, command *redis.Command) {
	args := command.Args()
	conn := command.Connection()
	if len(args) < 2 {
		conn.Write(protocol.CreateWrongArgumentNumberError("setnx"))
		return
	}
	key := string(args[0])
	value := args[1]
	entry := &Entry{Data: value}
	exists := db.data.PutIfAbsent(key, entry)
	conn.Write(protocol.CreateNumberReply(exists))
}
