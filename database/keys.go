package database

import (
	"redigo/redis"
	"redigo/redis/protocol"
)

func init() {
	RegisterCommandExecutor("keys", execKeys)
}

func execKeys(db *SingleDB, command *redis.Command) *protocol.Reply {
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
