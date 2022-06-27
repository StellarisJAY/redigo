package database

import (
	"redigo/datastruct/dict"
	"redigo/redis"
	"redigo/redis/protocol"
	"strconv"
)

func init() {
	RegisterCommandExecutor("hset", execHSet)
	RegisterCommandExecutor("hget", execHGet)
	RegisterCommandExecutor("hdel", execHDel)
	RegisterCommandExecutor("hexists", execHExists)
	RegisterCommandExecutor("hgetall", execHGetAll)
	RegisterCommandExecutor("hkeys", execHKeys)
	RegisterCommandExecutor("hlen", execHLen)
	RegisterCommandExecutor("hmget", execHMGet)
	RegisterCommandExecutor("hsetnx", execHSetNX)
	RegisterCommandExecutor("hincrby", execHIncrBy)
	RegisterCommandExecutor("hstrlen", execHStrLen)
	RegisterCommandExecutor("hvals", execHVals)
}

func execHSet(db *SingleDB, command redis.Command) *protocol.Reply {
	args := command.Args()
	if len(args) < 3 {
		return protocol.NewErrorReply(protocol.CreateWrongArgumentNumberError("HSET"))
	}
	hash, err := getOrInitHash(db, string(args[0]))
	if err != nil {
		return protocol.NewErrorReply(err)
	}
	// get key-value pairs from args; put them into hash structure
	kvs := args[1:]
	i := 0
	for i < len(kvs) {
		k := string(kvs[i])
		if i >= len(kvs)-1 {
			break
		}
		val := kvs[i+1]
		hash.Put(k, val)
		i += 2
	}
	// return how many key-value pairs has been put
	return protocol.NewNumberReply(i / 2)
}

func execHGet(db *SingleDB, command redis.Command) *protocol.Reply {
	args := command.Args()
	if len(args) != 2 {
		return protocol.NewErrorReply(protocol.CreateWrongArgumentNumberError("HGET"))
	}
	hash, exists, err := getHash(db, string(args[0]))
	if err != nil {
		return protocol.NewErrorReply(err)
	}
	if exists {
		value, ok := hash.Get(string(args[1]))
		if ok {
			return protocol.NewBulkValueReply(value.([]byte))
		}
	}
	// return (nil) if hash not exists or key not exists
	return protocol.NilReply
}

func execHDel(db *SingleDB, command redis.Command) *protocol.Reply {
	args := command.Args()
	if len(args) < 2 {
		return protocol.NewErrorReply(protocol.CreateWrongArgumentNumberError("HDEL"))
	}
	hash, exists, err := getHash(db, string(args[0]))
	if err != nil {
		return protocol.NewErrorReply(err)
	}
	if exists {
		delKeys := args[1:]
		count := 0
		for _, del := range delKeys {
			count += hash.Remove(string(del))
		}
		return protocol.NewNumberReply(count)
	}
	return protocol.NewNumberReply(0)
}

func execHExists(db *SingleDB, command redis.Command) *protocol.Reply {
	args := command.Args()
	if len(args) != 2 {
		return protocol.NewErrorReply(protocol.CreateWrongArgumentNumberError("HEXISTS"))
	}
	hash, exists, err := getHash(db, string(args[0]))
	if err != nil {
		return protocol.NewErrorReply(err)
	}
	if exists {
		if _, exists = hash.Get(string(args[1])); exists {
			return protocol.NewNumberReply(1)
		}
	}
	return protocol.NewNumberReply(0)
}

func execHGetAll(db *SingleDB, command redis.Command) *protocol.Reply {
	args := command.Args()
	if len(args) != 1 {
		return protocol.NewErrorReply(protocol.CreateWrongArgumentNumberError("HGETALL"))
	}
	hash, exists, err := getHash(db, string(args[0]))
	if err != nil {
		return protocol.NewErrorReply(err)
	}
	if exists && hash.Len() > 0 {
		result := make([][]byte, hash.Len()*2)
		i := 0
		// store key-value pairs in slice
		hash.ForEach(func(key string, value interface{}) bool {
			result[i] = []byte(key)
			result[i+1] = value.([]byte)
			i += 2
			return true
		})
		return protocol.NewArrayReply(result)
	}
	return protocol.EmptyListReply
}

func execHKeys(db *SingleDB, command redis.Command) *protocol.Reply {
	args := command.Args()
	if len(args) != 1 {
		return protocol.NewErrorReply(protocol.CreateWrongArgumentNumberError("HKEYS"))
	}
	hash, exists, err := getHash(db, string(args[0]))
	if err != nil {
		return protocol.NewErrorReply(err)
	}
	if exists {
		keys := hash.Keys()
		return protocol.NewStringArrayReply(keys)
	}
	return protocol.EmptyListReply
}

func execHLen(db *SingleDB, command redis.Command) *protocol.Reply {
	args := command.Args()
	if len(args) != 1 {
		return protocol.NewErrorReply(protocol.CreateWrongArgumentNumberError("HKEYS"))
	}
	hash, exists, err := getHash(db, string(args[0]))
	if err != nil {
		return protocol.NewErrorReply(err)
	}
	if exists {
		return protocol.NewNumberReply(hash.Len())
	}
	return protocol.NewNumberReply(0)
}

func execHMGet(db *SingleDB, command redis.Command) *protocol.Reply {
	args := command.Args()
	if len(args) < 2 {
		return protocol.NewErrorReply(protocol.CreateWrongArgumentNumberError("HKEYS"))
	}
	hash, exists, err := getHash(db, string(args[0]))
	if err != nil {
		return protocol.NewErrorReply(err)
	}
	if exists {
		keys := args[1:]
		result := make([][]byte, len(keys))
		for i, k := range keys {
			value, exists := hash.Get(string(k))
			if exists {
				result[i] = value.([]byte)
			} else {
				result[i] = nil
			}
		}
		return protocol.NewArrayReply(result)
	}
	return protocol.EmptyListReply
}

func execHSetNX(db *SingleDB, command redis.Command) *protocol.Reply {
	args := command.Args()
	if len(args) != 3 {
		return protocol.NewErrorReply(protocol.CreateWrongArgumentNumberError("HSETNX"))
	}
	hash, err := getOrInitHash(db, string(args[0]))
	if err != nil {
		return protocol.NewErrorReply(err)
	}
	return protocol.NewNumberReply(hash.PutIfAbsent(string(args[1]), args[2]))
}

func execHIncrBy(db *SingleDB, command redis.Command) *protocol.Reply {
	args := command.Args()
	if len(args) != 3 {
		return protocol.NewErrorReply(protocol.CreateWrongArgumentNumberError("HINCRBY"))
	}
	// parse delta value
	delta, err := strconv.Atoi(string(args[2]))
	if err != nil {
		return protocol.NewErrorReply(protocol.HashValueNotIntegerError)
	}
	// get or init a new hash structure
	hash, err := getOrInitHash(db, string(args[0]))
	if err != nil {
		return protocol.NewErrorReply(err)
	}

	val, exists := hash.Get(string(args[1]))
	var result int
	if exists {
		// value type must be integer
		result, err = strconv.Atoi(string(val.([]byte)))
		if err != nil {
			return protocol.NewErrorReply(protocol.HashValueNotIntegerError)
		}
	} else {
		result = 0
	}
	result += delta
	hash.Put(string(args[1]), []byte(strconv.Itoa(result)))
	return protocol.NewNumberReply(result)
}

func execHStrLen(db *SingleDB, command redis.Command) *protocol.Reply {
	args := command.Args()
	if len(args) != 2 {
		return protocol.NewErrorReply(protocol.CreateWrongArgumentNumberError("HSTRLEN"))
	}
	hash, exists, err := getHash(db, string(args[0]))
	if err != nil {
		return protocol.NewErrorReply(err)
	}
	if exists {
		value, exists := hash.Get(string(args[1]))
		if exists {
			return protocol.NewNumberReply(len(value.([]byte)))
		}
	}
	return protocol.NewNumberReply(0)
}

func execHVals(db *SingleDB, command redis.Command) *protocol.Reply {
	args := command.Args()
	if len(args) != 1 {
		return protocol.NewErrorReply(protocol.CreateWrongArgumentNumberError("HVALS"))
	}
	hash, exists, err := getHash(db, string(args[0]))
	if err != nil {
		return protocol.NewErrorReply(err)
	}
	if exists {
		if length := hash.Len(); length == 0 {
			return protocol.EmptyListReply
		} else {
			vals := make([][]byte, length)
			i := 0
			hash.ForEach(func(key string, value interface{}) bool {
				vals[i] = value.([]byte)
				i++
				return true
			})
			return protocol.NewArrayReply(vals)
		}
	}
	return protocol.EmptyListReply
}

func isHash(entry *Entry) bool {
	switch entry.Data.(type) {
	case *dict.SimpleDict:
		return true
	case *dict.SafeDict:
		return true
	}
	return false
}

func getOrInitHash(db *SingleDB, key string) (dict.Dict, error) {
	v, exists := db.data.Get(key)
	if exists {
		entry := v.(*Entry)
		if !isHash(entry) {
			return nil, protocol.WrongTypeOperationError
		}
		return entry.Data.(dict.Dict), nil
	} else {
		hash := dict.NewSimpleDict()
		db.data.Put(key, &Entry{Data: hash})
		return hash, nil
	}
}

func getHash(db *SingleDB, key string) (dict.Dict, bool, error) {
	v, exists := db.data.Get(key)
	if exists {
		entry := v.(*Entry)
		if !isHash(entry) {
			return nil, false, protocol.WrongTypeOperationError
		}
		return entry.Data.(dict.Dict), true, nil
	}
	return nil, false, nil
}
