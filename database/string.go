package database

import (
	"redigo/redis"
	"redigo/redis/protocol"
)

const (
	defaultPolicy = 0
	insertPolicy  = 1
	updatePolicy  = 2
)

func init() {
	RegisterCommandExecutor("set", executeSet)
	RegisterCommandExecutor("get", executeGet)
	RegisterCommandExecutor("setnx", executeSetNX)
	RegisterCommandExecutor("append", executeAppend)
}

func executeSet(db *SingleDB, command *redis.Command) *protocol.Reply {
	args := command.Args()
	if len(args) < 2 {
		return protocol.NewErrorReply(protocol.CreateWrongArgumentNumberError("set"))
	}
	key := string(args[0])
	value := args[1]
	// parse args, determine 'SET' Policy: NX or XX or default
	policy := defaultPolicy
	for _, a := range args {
		arg := string(a)
		if arg == "NX" {
			policy = insertPolicy
		} else if arg == "XX" {
			policy = updatePolicy
		}
	}

	entry := &Entry{Data: value}
	var result int
	switch policy {
	case defaultPolicy:
		result = db.data.Put(key, entry)
	case insertPolicy:
		result = db.data.PutIfAbsent(key, entry)
	case updatePolicy:
		result = db.data.PutIfExists(key, entry)
	}
	if result == 0 {
		return protocol.NewSingleStringReply("(nil)")
	} else {
		return protocol.NewSingleStringReply("OK")
	}
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
		return protocol.NewBulkValueReply(value)
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

func executeAppend(db *SingleDB, command *redis.Command) *protocol.Reply {
	args := command.Args()
	if len(args) < 2 {
		return protocol.NewErrorReply(protocol.CreateWrongArgumentNumberError("append"))
	}
	key := string(args[0])
	appendValue := args[1]
	v, exists := db.data.Get(key)
	var length int
	if exists {
		entry := v.(*Entry)
		originalValue := entry.Data.([]byte)
		value := make([]byte, len(originalValue)+len(appendValue))
		copy(value[0:len(originalValue)], originalValue)
		copy(value[len(originalValue):], appendValue)
		entry.Data = value
		_ = db.data.Put(key, entry)
		length = len(value)
	} else {
		entry := &Entry{Data: appendValue}
		_ = db.data.Put(key, entry)
		length = len(appendValue)
	}
	return protocol.NewNumberReply(length)
}
