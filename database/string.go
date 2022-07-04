package database

import (
	"log"
	"redigo/datastruct/bitmap"
	"redigo/interface/database"
	"redigo/redis"
	"redigo/redis/protocol"
	"strconv"
	"time"
)

const (
	defaultPolicy = 0
	insertPolicy  = 1
	updatePolicy  = 2

	infiniteExpireTime = 0
)

func init() {
	RegisterCommandExecutor("set", executeSet)
	RegisterCommandExecutor("get", executeGet)
	RegisterCommandExecutor("setnx", executeSetNX)
	RegisterCommandExecutor("append", executeAppend)
	RegisterCommandExecutor("incr", executeIncr)
	RegisterCommandExecutor("decr", executeDecr)
	RegisterCommandExecutor("incrby", executeIncrby)
	RegisterCommandExecutor("decrby", executeDecrby)
	RegisterCommandExecutor("setbit", execSetBit)
	RegisterCommandExecutor("getbit", execGetBit)
}

func executeSet(db *SingleDB, command redis.Command) *protocol.Reply {
	args := command.Args()
	if len(args) < 2 {
		return protocol.NewErrorReply(protocol.CreateWrongArgumentNumberError("set"))
	}
	key := string(args[0])
	value := args[1]
	// parse args, determine 'SET' Policy: NX or XX or default
	policy := defaultPolicy
	expireTime := infiniteExpireTime
	var delay time.Duration
	for i, a := range args {
		arg := string(a)
		if arg == "NX" {
			policy = insertPolicy
		} else if arg == "XX" {
			policy = updatePolicy
		} else if arg == "EX" || arg == "PX" {
			if expireTime != infiniteExpireTime || i == len(args)-1 {
				return protocol.NewErrorReply(protocol.CreateWrongArgumentNumberError("set "))
			}
			if num, err := strconv.Atoi(string(args[i+1])); err != nil {
				log.Println("Error arg: ", arg)
				return protocol.NewErrorReply(protocol.HashValueNotIntegerError)
			} else {
				expireTime = num
				switch arg {
				case "EX":
					delay = time.Duration(expireTime) * time.Second
				case "PX":
					delay = time.Duration(expireTime) * time.Millisecond
				}

			}
		}
	}

	entry := &database.Entry{Data: value}
	var result int
	switch policy {
	case defaultPolicy:
		result = db.data.Put(key, entry)
	case insertPolicy:
		result = db.data.PutIfAbsent(key, entry)
	case updatePolicy:
		result = db.data.PutIfExists(key, entry)
	}
	// set ttl
	if expireTime != infiniteExpireTime {
		db.Expire(key, delay)
	} else {
		// cancel old ttl
		db.CancelTTL(key)
	}
	if result == 0 {
		return protocol.NilReply
	} else {
		return protocol.OKReply
	}
}

func executeGet(db *SingleDB, command redis.Command) *protocol.Reply {
	args := command.Args()
	if args == nil || len(args) == 0 {
		return protocol.NewErrorReply(protocol.CreateWrongArgumentNumberError("get"))
	}
	result, exists, err := getString(db, string(args[0]))
	if err != nil {
		return protocol.NewErrorReply(err)
	}
	if exists {
		return protocol.NewBulkValueReply(result)
	}
	return protocol.NilReply
}

func executeSetNX(db *SingleDB, command redis.Command) *protocol.Reply {
	args := command.Args()
	if len(args) < 2 {
		return protocol.NewErrorReply(protocol.CreateWrongArgumentNumberError("setnx"))
	}
	key := string(args[0])
	value := args[1]
	entry := &database.Entry{Data: value}
	exists := db.data.PutIfAbsent(key, entry)
	if exists != 0 {
		db.CancelTTL(key)
	}
	return protocol.NewNumberReply(exists)
}

func executeAppend(db *SingleDB, command redis.Command) *protocol.Reply {
	args := command.Args()
	if len(args) < 2 {
		return protocol.NewErrorReply(protocol.CreateWrongArgumentNumberError("append"))
	}
	key := string(args[0])
	appendValue := args[1]
	v, exists := db.data.Get(key)
	var length int
	if exists {
		entry := v.(*database.Entry)
		// check if entry is string type
		if !isString(*entry) {
			return protocol.NewErrorReply(protocol.WrongTypeOperationError)
		}
		// append new value to original string
		originalValue := entry.Data.([]byte)
		value := make([]byte, len(originalValue)+len(appendValue))
		copy(value[0:len(originalValue)], originalValue)
		copy(value[len(originalValue):], appendValue)
		entry.Data = value
		_ = db.data.Put(key, entry)
		length = len(value)
	} else {
		// key doesn't exist.
		entry := &database.Entry{Data: appendValue}
		_ = db.data.Put(key, entry)
		length = len(appendValue)
	}
	return protocol.NewNumberReply(length)
}

func executeIncr(db *SingleDB, command redis.Command) *protocol.Reply {
	args := command.Args()
	if len(args) != 1 {
		return protocol.NewErrorReply(protocol.CreateWrongArgumentNumberError("incr"))
	}
	key := string(args[0])
	return add(db, key, 1)
}

func executeDecr(db *SingleDB, command redis.Command) *protocol.Reply {
	args := command.Args()
	if len(args) != 1 {
		return protocol.NewErrorReply(protocol.CreateWrongArgumentNumberError("decr"))
	}
	key := string(args[0])
	return add(db, key, -1)
}

func executeIncrby(db *SingleDB, command redis.Command) *protocol.Reply {
	args := command.Args()
	if len(args) != 2 {
		return protocol.NewErrorReply(protocol.CreateWrongArgumentNumberError("incrby"))
	}
	key := string(args[0])
	deltaStr := string(args[1])
	if delta, err := strconv.Atoi(deltaStr); err != nil {
		return protocol.NewErrorReply(protocol.HashValueNotIntegerError)
	} else {
		return add(db, key, delta)
	}
}

func executeDecrby(db *SingleDB, command redis.Command) *protocol.Reply {
	args := command.Args()
	if len(args) != 2 {
		return protocol.NewErrorReply(protocol.CreateWrongArgumentNumberError("decrby"))
	}
	key := string(args[0])
	deltaStr := string(args[1])
	if delta, err := strconv.Atoi(deltaStr); err != nil {
		return protocol.NewErrorReply(protocol.HashValueNotIntegerError)
	} else {
		return add(db, key, -delta)
	}
}

// add : add a delta value to the key's value
func add(db *SingleDB, key string, delta int) *protocol.Reply {
	v, exists := db.data.Get(key)
	if exists {
		entry := v.(*database.Entry)
		// check entry type
		if !isString(*entry) {
			return protocol.NewErrorReply(protocol.WrongTypeOperationError)
		}
		s := string(entry.Data.([]byte))
		if val, err := strconv.Atoi(s); err != nil {
			return protocol.NewErrorReply(protocol.HashValueNotIntegerError)
		} else {
			val = val + delta
			value := []byte(strconv.Itoa(val))
			entry.Data = value
			db.data.Put(key, entry)
			return protocol.NewNumberReply(val)
		}
	} else {
		entry := &database.Entry{Data: []byte(strconv.Itoa(delta))}
		db.data.Put(key, entry)
		return protocol.NewNumberReply(delta)
	}
}

func execSetBit(db *SingleDB, command redis.Command) *protocol.Reply {
	args := command.Args()
	if len(args) != 3 {
		return protocol.NewErrorReply(protocol.CreateWrongArgumentNumberError("SETBIT"))
	}
	// parse offset and bit number
	offset, err := strconv.ParseInt(string(args[1]), 0, 64)
	bit, err := strconv.ParseInt(string(args[2]), 0, 8)
	if err != nil {
		return protocol.NewErrorReply(protocol.HashValueNotIntegerError)
	}
	// get bitmap struct
	bm, exists, err := getBitMap(db, string(args[0]))
	if err != nil {
		return protocol.NewErrorReply(err)
	}
	if !exists {
		// create a new bitmap if not exist
		bm = bitmap.New()
		entry := &database.Entry{Data: bm}
		db.data.Put(string(args[0]), entry)
	}
	original := bm.SetBit(offset, byte(bit))
	return protocol.NewNumberReply(int(original))
}

func execGetBit(db *SingleDB, command redis.Command) *protocol.Reply {
	args := command.Args()
	if len(args) != 2 {
		return protocol.NewErrorReply(protocol.CreateWrongArgumentNumberError("GETBIT"))
	}
	// check offset number
	offset, err := strconv.ParseInt(string(args[1]), 0, 64)
	if err != nil {
		return protocol.NewErrorReply(protocol.HashValueNotIntegerError)
	}
	// get bitmap data structure
	bitMap, exists, err := getBitMap(db, string(args[0]))
	if err != nil {
		return protocol.NewErrorReply(err)
	}
	if exists {
		return protocol.NewNumberReply(int(bitMap.GetBit(offset)))
	}
	return protocol.NewNumberReply(0)
}

// getString get the value of this key, if not string returns an error
func getString(db *SingleDB, key string) ([]byte, bool, error) {
	entry, exists := db.getEntry(key)
	// check key's existence
	if !exists {
		return nil, false, nil
	}
	if !isString(*entry) {
		return nil, true, protocol.WrongTypeOperationError
	}
	return entry.Data.([]byte), true, nil
}

func getBitMap(db *SingleDB, key string) (*bitmap.BitMap, bool, error) {
	entry, exists := db.getEntry(key)
	if !exists {
		return nil, false, nil
	}
	if !isBitMap(*entry) {
		return nil, true, protocol.WrongTypeOperationError
	}
	return entry.Data.(*bitmap.BitMap), true, nil
}

func isString(entry database.Entry) bool {
	switch entry.Data.(type) {
	case []byte:
		return true
	}
	return false
}

func isBitMap(entry database.Entry) bool {
	switch entry.Data.(type) {
	case *bitmap.BitMap:
		return true
	}
	return false
}
