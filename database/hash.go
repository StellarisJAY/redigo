package database

import (
	"redigo/datastruct/dict"
	"redigo/interface/database"
	"redigo/redis"
	"strconv"
)

func init() {
	RegisterCommandExecutor("hset", execHSet, -3)
	RegisterCommandExecutor("hget", execHGet, 2)
	RegisterCommandExecutor("hdel", execHDel, 2)
	RegisterCommandExecutor("hexists", execHExists, 2)
	RegisterCommandExecutor("hgetall", execHGetAll, 1)
	RegisterCommandExecutor("hkeys", execHKeys, 1)
	RegisterCommandExecutor("hlen", execHLen, 1)
	RegisterCommandExecutor("hmget", execHMGet, -2)
	RegisterCommandExecutor("hsetnx", execHSetNX, 3)
	RegisterCommandExecutor("hincrby", execHIncrBy, 3)
	RegisterCommandExecutor("hstrlen", execHStrLen, 2)
	RegisterCommandExecutor("hvals", execHVals, 1)
}

func execHSet(db *SingleDB, command redis.Command) *redis.RespCommand {
	args := command.Args()
	if !ValidateArgCount(command.Name(), len(args)) {
		return redis.NewErrorCommand(redis.CreateWrongArgumentNumberError("HSET"))
	}
	hash, err := getOrInitHash(db, string(args[0]))
	if err != nil {
		return redis.NewErrorCommand(err)
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
	db.addAof(command.Parts())
	// return how many key-value pairs has been put
	return redis.NewNumberCommand(i / 2)
}

func execHGet(db *SingleDB, command redis.Command) *redis.RespCommand {
	args := command.Args()
	if !ValidateArgCount(command.Name(), len(args)) {
		return redis.NewErrorCommand(redis.CreateWrongArgumentNumberError("HGET"))
	}
	hash, exists, err := getHash(db, string(args[0]))
	if err != nil {
		return redis.NewErrorCommand(err)
	}
	if exists {
		value, ok := hash.Get(string(args[1]))
		if ok {
			return redis.NewBulkStringCommand(value.([]byte))
		}
	}
	// return (nil) if hash not exists or key not exists
	return redis.NilCommand
}

func execHDel(db *SingleDB, command redis.Command) *redis.RespCommand {
	args := command.Args()
	if !ValidateArgCount(command.Name(), len(args)) {
		return redis.NewErrorCommand(redis.CreateWrongArgumentNumberError("HDEL"))
	}
	hash, exists, err := getHash(db, string(args[0]))
	if err != nil {
		return redis.NewErrorCommand(err)
	}
	if exists {
		delKeys := args[1:]
		count := 0
		for _, del := range delKeys {
			count += hash.Remove(string(del))
		}
		db.addAof(command.Parts())
		return redis.NewNumberCommand(count)
	}
	return redis.NewNumberCommand(0)
}

func execHExists(db *SingleDB, command redis.Command) *redis.RespCommand {
	args := command.Args()
	if !ValidateArgCount(command.Name(), len(args)) {
		return redis.NewErrorCommand(redis.CreateWrongArgumentNumberError("HEXISTS"))
	}
	hash, exists, err := getHash(db, string(args[0]))
	if err != nil {
		return redis.NewErrorCommand(err)
	}
	if exists {
		if _, exists = hash.Get(string(args[1])); exists {
			return redis.NewNumberCommand(1)
		}
	}
	return redis.NewNumberCommand(0)
}

func execHGetAll(db *SingleDB, command redis.Command) *redis.RespCommand {
	args := command.Args()
	if !ValidateArgCount(command.Name(), len(args)) {
		return redis.NewErrorCommand(redis.CreateWrongArgumentNumberError("HGETALL"))
	}
	hash, exists, err := getHash(db, string(args[0]))
	if err != nil {
		return redis.NewErrorCommand(err)
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
		return redis.NewArrayCommand(result)
	}
	return redis.EmptyListCommand
}

func execHKeys(db *SingleDB, command redis.Command) *redis.RespCommand {
	args := command.Args()
	if !ValidateArgCount(command.Name(), len(args)) {
		return redis.NewErrorCommand(redis.CreateWrongArgumentNumberError("HKEYS"))
	}
	hash, exists, err := getHash(db, string(args[0]))
	if err != nil {
		return redis.NewErrorCommand(err)
	}
	if exists {
		keys := hash.Keys()
		return redis.NewStringArrayCommand(keys)
	}
	return redis.EmptyListCommand
}

func execHLen(db *SingleDB, command redis.Command) *redis.RespCommand {
	args := command.Args()
	if !ValidateArgCount(command.Name(), len(args)) {
		return redis.NewErrorCommand(redis.CreateWrongArgumentNumberError("HKEYS"))
	}
	hash, exists, err := getHash(db, string(args[0]))
	if err != nil {
		return redis.NewErrorCommand(err)
	}
	if exists {
		return redis.NewNumberCommand(hash.Len())
	}
	return redis.NewNumberCommand(0)
}

func execHMGet(db *SingleDB, command redis.Command) *redis.RespCommand {
	args := command.Args()
	if !ValidateArgCount(command.Name(), len(args)) {
		return redis.NewErrorCommand(redis.CreateWrongArgumentNumberError("HKEYS"))
	}
	hash, exists, err := getHash(db, string(args[0]))
	if err != nil {
		return redis.NewErrorCommand(err)
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
		return redis.NewArrayCommand(result)
	}
	return redis.EmptyListCommand
}

func execHSetNX(db *SingleDB, command redis.Command) *redis.RespCommand {
	args := command.Args()
	if !ValidateArgCount(command.Name(), len(args)) {
		return redis.NewErrorCommand(redis.CreateWrongArgumentNumberError("HSETNX"))
	}
	hash, err := getOrInitHash(db, string(args[0]))
	if err != nil {
		return redis.NewErrorCommand(err)
	}
	absent := hash.PutIfAbsent(string(args[1]), args[2])
	if absent == 1 {
		db.addAof(command.Parts())
	}
	return redis.NewNumberCommand(absent)
}

func execHIncrBy(db *SingleDB, command redis.Command) *redis.RespCommand {
	args := command.Args()
	if !ValidateArgCount(command.Name(), len(args)) {
		return redis.NewErrorCommand(redis.CreateWrongArgumentNumberError("HINCRBY"))
	}
	// parse delta value
	delta, err := strconv.Atoi(string(args[2]))
	if err != nil {
		return redis.NewErrorCommand(redis.HashValueNotIntegerError)
	}
	// get or init a new hash structure
	hash, err := getOrInitHash(db, string(args[0]))
	if err != nil {
		return redis.NewErrorCommand(err)
	}

	val, exists := hash.Get(string(args[1]))
	var result int
	if exists {
		// value type must be integer
		result, err = strconv.Atoi(string(val.([]byte)))
		if err != nil {
			return redis.NewErrorCommand(redis.HashValueNotIntegerError)
		}
	} else {
		result = 0
	}
	result += delta
	hash.Put(string(args[1]), []byte(strconv.Itoa(result)))
	db.addAof(command.Parts())
	return redis.NewNumberCommand(result)
}

func execHStrLen(db *SingleDB, command redis.Command) *redis.RespCommand {
	args := command.Args()
	if !ValidateArgCount(command.Name(), len(args)) {
		return redis.NewErrorCommand(redis.CreateWrongArgumentNumberError("HSTRLEN"))
	}
	hash, exists, err := getHash(db, string(args[0]))
	if err != nil {
		return redis.NewErrorCommand(err)
	}
	if exists {
		value, exists := hash.Get(string(args[1]))
		if exists {
			return redis.NewNumberCommand(len(value.([]byte)))
		}
	}
	return redis.NewNumberCommand(0)
}

func execHVals(db *SingleDB, command redis.Command) *redis.RespCommand {
	args := command.Args()
	if !ValidateArgCount(command.Name(), len(args)) {
		return redis.NewErrorCommand(redis.CreateWrongArgumentNumberError("HVALS"))
	}
	hash, exists, err := getHash(db, string(args[0]))
	if err != nil {
		return redis.NewErrorCommand(err)
	}
	if exists {
		if length := hash.Len(); length == 0 {
			return redis.EmptyListCommand
		} else {
			vals := make([][]byte, length)
			i := 0
			hash.ForEach(func(key string, value interface{}) bool {
				vals[i] = value.([]byte)
				i++
				return true
			})
			return redis.NewArrayCommand(vals)
		}
	}
	return redis.EmptyListCommand
}

func isHash(entry *database.Entry) bool {
	switch entry.Data.(type) {
	case *dict.SimpleDict:
		return true
	case *dict.SafeDict:
		return true
	}
	return false
}

func getOrInitHash(db *SingleDB, key string) (dict.Dict, error) {
	entry, exists := db.getEntry(key)
	if exists {
		if !isHash(entry) {
			return nil, redis.WrongTypeOperationError
		}
		return entry.Data.(dict.Dict), nil
	} else {
		hash := dict.NewSimpleDict()
		db.data.Put(key, &database.Entry{Data: hash})
		return hash, nil
	}
}

func getHash(db *SingleDB, key string) (dict.Dict, bool, error) {
	entry, exists := db.getEntry(key)
	if exists {
		if !isHash(entry) {
			return nil, false, redis.WrongTypeOperationError
		}
		return entry.Data.(dict.Dict), true, nil
	}
	return nil, false, nil
}
