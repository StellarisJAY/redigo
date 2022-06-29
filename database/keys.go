package database

import (
	"redigo/datastruct/dict"
	"redigo/datastruct/list"
	"redigo/datastruct/set"
	"redigo/datastruct/zset"
	"redigo/redis"
	"redigo/redis/protocol"
	"strconv"
	"time"
)

func init() {
	RegisterCommandExecutor("keys", execKeys)
	RegisterCommandExecutor("ttl", execTTL)
	RegisterCommandExecutor("pttl", execPTTL)
	RegisterCommandExecutor("del", execDel)
	RegisterCommandExecutor("exists", execExists)
	RegisterCommandExecutor("persist", execPersist)
	RegisterCommandExecutor("expire", execExpire)
	RegisterCommandExecutor("type", execType)
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

func execDel(db *SingleDB, command redis.Command) *protocol.Reply {
	args := command.Args()
	if len(args) < 1 {
		return protocol.NewErrorReply(protocol.CreateWrongArgumentNumberError("del"))
	}
	result := 0
	for _, arg := range args {
		key := string(arg)
		deleted := db.data.Remove(key)
		if deleted == 1 {
			db.CancelTTL(key)
		}
		result += deleted
	}
	return protocol.NewNumberReply(result)
}

func execExists(db *SingleDB, command redis.Command) *protocol.Reply {
	args := command.Args()
	if len(args) < 1 {
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
	if len(args) != 1 {
		return protocol.NewErrorReply(protocol.CreateWrongArgumentNumberError("persist"))
	}
	key := string(args[0])
	removed := db.ttlMap.Remove(key)
	if removed == 1 {
		db.CancelTTL(key)
	}
	return protocol.NewNumberReply(removed)
}

func execExpire(db *SingleDB, command redis.Command) *protocol.Reply {
	args := command.Args()
	if len(args) != 2 {
		return protocol.NewErrorReply(protocol.CreateWrongArgumentNumberError("persist"))
	}
	key := string(args[0])
	_, exists := db.data.Get(key)
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
	if len(args) != 1 {
		return protocol.NewErrorReply(protocol.CreateWrongArgumentNumberError("TYPE"))
	}
	v, exists := db.data.Get(string(args[0]))
	var result string
	if !exists {
		result = "none"
	} else {
		result = typeOf(*(v.(*Entry)))
	}
	return protocol.NewSingleStringReply(result)
}

func typeOf(entry Entry) string {
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
