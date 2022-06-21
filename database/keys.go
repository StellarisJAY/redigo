package database

import (
	"redigo/redis"
	"redigo/redis/protocol"
)

func init() {
	RegisterCommandExecutor("keys", execKeys)
}

func execKeys(db *SingleDB, command *redis.Command) {
	args := command.Args()
	conn := command.Connection()
	if len(args) == 0 {
		conn.Write(protocol.CreateWrongArgumentNumberError("keys"))
		return
	}
	keys := make([]string, db.data.Len())
	i := 0
	// todo add pattern matching here
	db.data.ForEach(func(key string, value interface{}) bool {
		keys[i] = key
		i++
		return true
	})
	conn.Write(protocol.CreateBulkStringArrayReply(keys))
}
