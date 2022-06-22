package database

import (
	"redigo/redis"
	"redigo/redis/protocol"
)

func init() {
	RegisterCommandExecutor("keys", execKeys)
	RegisterCommandExecutor("ttl", execTTL)
	RegisterCommandExecutor("pttl", execPTTL)
}

func execKeys(db *SingleDB, command redis.Command) *protocol.Reply {
	args := command.Args()
	if len(args) == 0 {
		return protocol.NewErrorReply(protocol.CreateWrongArgumentNumberError("keys"))
	}
	keys := make([]string, db.data.Len())
	i := 0
	// todo add pattern matching here
	db.data.ForEach(func(key string, value interface{}) bool {
		keys[i] = key
		i++
		return true
	})
	return protocol.NewStringArrayReply(keys)
}

func execTTL(db *SingleDB, command redis.Command) *protocol.Reply {
	args := command.Args()
	if len(args) != 1 {
		return protocol.NewErrorReply(protocol.CreateWrongArgumentNumberError("ttl"))
	}
	key := string(args[0])
	_, exists := db.data.Get(key)
	if !exists {
		return protocol.NewNumberReply(-2)
	}
	ttl := db.TTL(key)
	if ttl == -1 {
		return protocol.NewNumberReply(-1)
	}
	return protocol.NewNumberReply(int(ttl.Seconds()))
}

func execPTTL(db *SingleDB, command redis.Command) *protocol.Reply {
	args := command.Args()
	if len(args) != 1 {
		return protocol.NewErrorReply(protocol.CreateWrongArgumentNumberError("ttl"))
	}
	key := string(args[0])
	_, exists := db.data.Get(key)
	if !exists {
		return protocol.NewNumberReply(-2)
	}
	ttl := db.TTL(key)
	if ttl == -1 {
		return protocol.NewNumberReply(-1)
	}
	return protocol.NewNumberReply(int(ttl.Milliseconds()))
}
