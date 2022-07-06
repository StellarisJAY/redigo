package database

import (
	"redigo/datastruct/dict"
	"redigo/datastruct/list"
	"redigo/datastruct/set"
	"redigo/datastruct/zset"
	"redigo/interface/database"
	"redigo/interface/redis"
	"redigo/redis/protocol"
	"redigo/util/pattern"
	"strconv"
	"time"
)

func init() {
	RegisterCommandExecutor("ttl", execTTL, 1)
	RegisterCommandExecutor("pttl", execPTTL, 1)
	RegisterCommandExecutor("del", execDel, -1)
	RegisterCommandExecutor("exists", execExists, -1)
	RegisterCommandExecutor("persist", execPersist, 1)
	RegisterCommandExecutor("expire", execExpire, 2)
	RegisterCommandExecutor("type", execType, 1)
	RegisterCommandExecutor("pexpireat", execPExpireAt, 2)
}

func execKeys(db *SingleDB, command redis.Command, keys []string) *protocol.Reply {
	args := command.Args()
	if len(args) != 1 {
		return protocol.NewErrorReply(protocol.CreateWrongArgumentNumberError("KEYS"))
	}
	if string(args[0]) == "*" {
		return protocol.NewStringArrayReply(keys)
	}
	p := pattern.ParsePattern(string(args[0]))
	i := 0
	for _, key := range keys {
		if p.Matches(key) && db.TTL(key) != -2 {
			keys[i] = key
			i++
		}
	}
	return protocol.NewStringArrayReply(keys[:i])
}

func execTTL(db *SingleDB, command redis.Command) *protocol.Reply {
	args := command.Args()
	if !ValidateArgCount(command.Name(), len(args)) {
		return protocol.NewErrorReply(protocol.CreateWrongArgumentNumberError("ttl"))
	}
	key := string(args[0])
	_, exists := db.getEntry(string(args[0]))
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
	if !ValidateArgCount(command.Name(), len(args)) {
		return protocol.NewErrorReply(protocol.CreateWrongArgumentNumberError("ttl"))
	}
	key := string(args[0])
	_, exists := db.getEntry(key)
	if !exists {
		return protocol.NewNumberReply(-2)
	}
	ttl := db.TTL(key)
	if ttl == -1 {
		return protocol.NewNumberReply(-1)
	}
	return protocol.NewNumberReply(int(ttl.Milliseconds()))
}

func execDel(db *SingleDB, command redis.Command) *protocol.Reply {
	args := command.Args()
	if !ValidateArgCount(command.Name(), len(args)) {
		return protocol.NewErrorReply(protocol.CreateWrongArgumentNumberError("del"))
	}
	result := 0
	for _, arg := range args {
		key := string(arg)
		deleted := db.data.Remove(key)
		if deleted == 1 {
			db.CancelTTL(key)
			db.addAof(command.Parts())
		}
		result += deleted
	}
	return protocol.NewNumberReply(result)
}

func execExists(db *SingleDB, command redis.Command) *protocol.Reply {
	args := command.Args()
	if !ValidateArgCount(command.Name(), len(args)) {
		return protocol.NewErrorReply(protocol.CreateWrongArgumentNumberError("exists"))
	}
	existCount := 0
	for _, arg := range args {
		key := string(arg)
		_, exist := db.data.Get(key)
		if exist {
			existCount++
		}
	}
	return protocol.NewNumberReply(existCount)
}

func execPersist(db *SingleDB, command redis.Command) *protocol.Reply {
	args := command.Args()
	if !ValidateArgCount(command.Name(), len(args)) {
		return protocol.NewErrorReply(protocol.CreateWrongArgumentNumberError("persist"))
	}
	key := string(args[0])
	_, exists := db.getEntry(key)
	if !exists {
		return protocol.NewNumberReply(0)
	}
	removed := db.CancelTTL(key)
	if removed == 1 {
		db.addAof(command.Parts())
	}
	return protocol.NewNumberReply(removed)
}

func execExpire(db *SingleDB, command redis.Command) *protocol.Reply {
	args := command.Args()
	if !ValidateArgCount(command.Name(), len(args)) {
		return protocol.NewErrorReply(protocol.CreateWrongArgumentNumberError("persist"))
	}
	key := string(args[0])
	_, exists := db.getEntry(key)
	if !exists {
		return protocol.NewNumberReply(0)
	}
	// parse ttl number
	if num, err := strconv.Atoi(string(args[1])); err != nil {
		return protocol.NewErrorReply(protocol.HashValueNotIntegerError)
	} else {
		if num > 0 {
			// cancel old ttl, set new expire time
			db.CancelTTL(key)
			db.Expire(key, time.Duration(num)*time.Second)
		} else {
			// remove key and cancel ttl
			db.data.Remove(key)
			db.CancelTTL(key)
		}
		return protocol.NewNumberReply(1)
	}
}

func execType(db *SingleDB, command redis.Command) *protocol.Reply {
	args := command.Args()
	if !ValidateArgCount(command.Name(), len(args)) {
		return protocol.NewErrorReply(protocol.CreateWrongArgumentNumberError("TYPE"))
	}
	entry, exists := db.getEntry(string(args[0]))
	var result string
	if !exists {
		result = "none"
	} else {
		result = typeOf(*entry)
	}
	return protocol.NewSingleStringReply(result)
}

func execPExpireAt(db *SingleDB, command redis.Command) *protocol.Reply {
	args := command.Args()
	if !ValidateArgCount(command.Name(), len(args)) {
		return protocol.NewErrorReply(protocol.CreateWrongArgumentNumberError("PEXPIREAT"))
	}
	expireAt, err := strconv.ParseInt(string(args[2]), 0, 64)
	if err != nil {
		return protocol.NewErrorReply(protocol.HashValueNotIntegerError)
	}
	_, exists := db.getEntry(string(args[0]))
	if exists {
		expireTime := time.UnixMilli(expireAt)
		db.ExpireAt(string(args[0]), &expireTime)
		return protocol.NewNumberReply(1)
	} else {
		return protocol.NewNumberReply(0)
	}
}

func typeOf(entry database.Entry) string {
	switch entry.Data.(type) {
	case dict.Dict:
		return "hash"
	case []byte:
		return "string"
	case *list.LinkedList:
		return "list"
	case *zset.SortedSet:
		return "zset"
	case *set.Set:
		return "set"
	}
	return "none"
}
