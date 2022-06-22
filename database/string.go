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

func executeSet(db *SingleDB, command *redis.Command) *protocol.Reply {
	args := command.Args()
	if len(args) < 2 {
		return protocol.NewErrorReply(protocol.CreateWrongArgumentNumberError("set"))
	}
	key := string(args[0])
	value := args[1]
	entry := &Entry{Data: value}
	res := db.data.Put(key, entry)
	return protocol.NewNumberReply(res)
}

func executeGet(db *SingleDB, command *redis.Command) *protocol.Reply {
	args := command.Args()
	if args == nil || len(args) == 0 {
		return protocol.NewErrorReply(protocol.CreateWrongArgumentNumberError("get"))
	}
	key := string(args[0])
	v, exists := db.data.Get(key)
	if exists {
		entry := v.(*Entry)
		value := entry.Data.([]byte)
		return protocol.NewSingleValueReply(value)
	} else {
		return protocol.NewSingleStringReply("(nil)")
	}
}

func executeSetNX(db *SingleDB, command *redis.Command) *protocol.Reply {
	args := command.Args()
	if len(args) < 2 {
		return protocol.NewErrorReply(protocol.CreateWrongArgumentNumberError("setnx"))
	}
	key := string(args[0])
	value := args[1]
	entry := &Entry{Data: value}
	exists := db.data.PutIfAbsent(key, entry)
	return protocol.NewNumberReply(exists)
}
