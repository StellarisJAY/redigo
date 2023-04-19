package database

import (
	"redigo/pkg/datastruct/dict"
	"redigo/pkg/datastruct/list"
	"redigo/pkg/datastruct/set"
	"redigo/pkg/datastruct/zset"
	"redigo/pkg/interface/database"
	"redigo/pkg/redis"
	"redigo/pkg/util/pattern"
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
	RegisterCommandExecutor("rename", execRename, 2)
	RegisterCommandExecutor("renamenx", execRenameNX, 2)
	RegisterCommandExecutor("randomkey", execRandomKey, 0)
}

func execKeys(db *SingleDB, command redis.Command, keys []string) *redis.RespCommand {
	args := command.Args()
	if len(args) != 1 {
		return redis.NewErrorCommand(redis.CreateWrongArgumentNumberError("KEYS"))
	}
	if string(args[0]) == "*" {
		return redis.NewStringArrayCommand(keys)
	}
	p := pattern.ParsePattern(string(args[0]))
	i := 0
	for _, key := range keys {
		if p.Matches(key) && db.TTL(key) != -2 {
			keys[i] = key
			i++
		}
	}
	return redis.NewStringArrayCommand(keys[:i])
}

func execTTL(db *SingleDB, command redis.Command) *redis.RespCommand {
	args := command.Args()
	if !ValidateArgCount(command.Name(), len(args)) {
		return redis.NewErrorCommand(redis.CreateWrongArgumentNumberError("ttl"))
	}
	key := string(args[0])
	_, exists := db.GetEntry(string(args[0]))
	if !exists {
		return redis.NewNumberCommand(-2)
	}
	ttl := db.TTL(key)
	if ttl == -1 {
		return redis.NewNumberCommand(-1)
	}
	return redis.NewNumberCommand(int(ttl.Seconds()))
}

func execPTTL(db *SingleDB, command redis.Command) *redis.RespCommand {
	args := command.Args()
	if !ValidateArgCount(command.Name(), len(args)) {
		return redis.NewErrorCommand(redis.CreateWrongArgumentNumberError("ttl"))
	}
	key := string(args[0])
	_, exists := db.GetEntry(key)
	if !exists {
		return redis.NewNumberCommand(-2)
	}
	ttl := db.TTL(key)
	if ttl == -1 {
		return redis.NewNumberCommand(-1)
	}
	return redis.NewNumberCommand(int(ttl.Milliseconds()))
}

func execDel(db *SingleDB, command redis.Command) *redis.RespCommand {
	args := command.Args()
	if !ValidateArgCount(command.Name(), len(args)) {
		return redis.NewErrorCommand(redis.CreateWrongArgumentNumberError("del"))
	}
	result := 0
	for _, arg := range args {
		key := string(arg)
		deleted := db.data.Remove(key)
		if deleted == 1 {
			db.DeleteEntry(key)
		}
		result += deleted
	}
	db.addAof(command.Parts())
	return redis.NewNumberCommand(result)
}

func execExists(db *SingleDB, command redis.Command) *redis.RespCommand {
	args := command.Args()
	if !ValidateArgCount(command.Name(), len(args)) {
		return redis.NewErrorCommand(redis.CreateWrongArgumentNumberError("exists"))
	}
	existCount := 0
	for _, arg := range args {
		key := string(arg)
		_, exist := db.data.Get(key)
		if exist {
			existCount++
		}
	}
	return redis.NewNumberCommand(existCount)
}

func execPersist(db *SingleDB, command redis.Command) *redis.RespCommand {
	args := command.Args()
	if !ValidateArgCount(command.Name(), len(args)) {
		return redis.NewErrorCommand(redis.CreateWrongArgumentNumberError("persist"))
	}
	key := string(args[0])
	_, exists := db.GetEntry(key)
	if !exists {
		return redis.NewNumberCommand(0)
	}
	removed := db.CancelTTL(key)
	if removed == 1 {
		db.addAof(command.Parts())
	}
	return redis.NewNumberCommand(removed)
}

func execExpire(db *SingleDB, command redis.Command) *redis.RespCommand {
	args := command.Args()
	if !ValidateArgCount(command.Name(), len(args)) {
		return redis.NewErrorCommand(redis.CreateWrongArgumentNumberError("persist"))
	}
	key := string(args[0])
	_, exists := db.GetEntry(key)
	if !exists {
		return redis.NewNumberCommand(0)
	}
	// parse ttl number
	if num, err := strconv.Atoi(string(args[1])); err != nil {
		return redis.NewErrorCommand(redis.HashValueNotIntegerError)
	} else {
		if num > 0 {
			// cancel old ttl, set new expire time
			db.CancelTTL(key)
			delay := time.Duration(num)
			db.Expire(key, time.Duration(num)*time.Second)
			// add expireAt to aof
			db.addAof([][]byte{[]byte("pexpireat"), args[0], []byte(strconv.FormatInt(time.Now().Add(delay).UnixMilli(), 10))})
		} else {
			// remove key and cancel ttl
			db.data.Remove(key)
			db.CancelTTL(key)
			// key already expired, add del to aof
			db.addAof([][]byte{[]byte("del"), args[0]})
		}
		return redis.NewNumberCommand(1)
	}
}

func execType(db *SingleDB, command redis.Command) *redis.RespCommand {
	args := command.Args()
	if !ValidateArgCount(command.Name(), len(args)) {
		return redis.NewErrorCommand(redis.CreateWrongArgumentNumberError("TYPE"))
	}
	entry, exists := db.GetEntry(string(args[0]))
	var result string
	if !exists {
		result = "none"
	} else {
		result = typeOf(*entry)
	}
	return redis.NewSingleLineCommand([]byte(result))
}

func execPExpireAt(db *SingleDB, command redis.Command) *redis.RespCommand {
	args := command.Args()
	if !ValidateArgCount(command.Name(), len(args)) {
		return redis.NewErrorCommand(redis.CreateWrongArgumentNumberError("PEXPIREAT"))
	}
	expireAt, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return redis.NewErrorCommand(redis.HashValueNotIntegerError)
	}
	_, exists := db.GetEntry(string(args[0]))
	if exists {
		expireTime := time.UnixMilli(expireAt)
		if expireTime.Before(time.Now()) {
			db.data.Remove(string(args[0]))
			db.addAof([][]byte{[]byte("del"), args[0]})
		} else {
			db.ExpireAt(string(args[0]), &expireTime)
			db.addAof(command.Parts())
		}
		return redis.NewNumberCommand(1)
	} else {
		return redis.NewNumberCommand(0)
	}
}

func execRename(db *SingleDB, command redis.Command) *redis.RespCommand {
	args := command.Args()
	if !ValidateArgCount(command.Name(), len(args)) {
		return redis.NewErrorCommand(redis.CreateWrongArgumentNumberError("rename"))
	}
	oldKey := string(args[0])
	newKey := string(args[1])
	if oldKey == newKey {
		return redis.OKCommand
	}
	if err := db.Rename(oldKey, newKey); err != nil {
		return redis.NewErrorCommand(err)
	}
	return redis.OKCommand
}

func execRenameNX(db *SingleDB, command redis.Command) *redis.RespCommand {
	args := command.Args()
	if !ValidateArgCount(command.Name(), len(args)) {
		return redis.NewErrorCommand(redis.CreateWrongArgumentNumberError("RENAMENX"))
	}
	oldKey := string(args[0])
	newKey := string(args[1])
	if newKey == oldKey {
		return redis.NewNumberCommand(0)
	}
	if res, err := db.RenameNX(oldKey, newKey); err != nil {
		return redis.NewErrorCommand(err)
	} else {
		return redis.NewNumberCommand(res)
	}
}

func execRandomKey(db *SingleDB, command redis.Command) *redis.RespCommand {
	if db.Len(0) == 0 {
		return redis.NilCommand
	}
	keys := db.randomKeys(1)
	return redis.NewBulkStringCommand([]byte(keys[0]))
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

func buildExpireCommand(key string, ttl time.Duration) [][]byte {
	expireAt := time.Now().Add(ttl).UnixMilli()
	return [][]byte{[]byte("pexpireat"), []byte(key), []byte(strconv.FormatInt(expireAt, 10))}
}
