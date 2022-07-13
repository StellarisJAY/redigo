package database

import (
	"log"
	"redigo/datastruct/bitmap"
	"redigo/interface/database"
	"redigo/interface/redis"
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
	RegisterCommandExecutor("set", executeSet, -2)
	RegisterCommandExecutor("get", executeGet, 1)
	RegisterCommandExecutor("setnx", executeSetNX, 2)
	RegisterCommandExecutor("append", executeAppend, 2)
	RegisterCommandExecutor("incr", executeIncr, 1)
	RegisterCommandExecutor("decr", executeDecr, 1)
	RegisterCommandExecutor("incrby", executeIncrby, 2)
	RegisterCommandExecutor("decrby", executeDecrby, 2)
	RegisterCommandExecutor("strlen", execStrLen, 1)
	RegisterCommandExecutor("setbit", execSetBit, 3)
	RegisterCommandExecutor("getbit", execGetBit, 2)
}

func executeSet(db *SingleDB, command redis.Command) *protocol.Reply {
	args := command.Args()
	if !ValidateArgCount(command.Name(), len(args)) {
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

	db.addAof(command.Parts())
	// set ttl
	if expireTime != infiniteExpireTime {
		db.Expire(key, delay)
		db.addAof([][]byte{[]byte("pexpireat"), args[0], []byte(strconv.FormatInt(time.Now().Add(delay).UnixMilli(), 10))})
	} else {
		// cancel old ttl
		cancelled := db.CancelTTL(key)
		if cancelled == 1 {
			// add PERSIST command to aof
			db.addAof([][]byte{[]byte("persist"), args[0]})
		}
	}
	if result == 0 {
		return protocol.NilReply
	} else {
		db.addVersion(key)
		return protocol.OKReply
	}
}

func executeGet(db *SingleDB, command redis.Command) *protocol.Reply {
	args := command.Args()
	if !ValidateArgCount(command.Name(), len(args)) {
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
	if !ValidateArgCount(command.Name(), len(args)) {
		return protocol.NewErrorReply(protocol.CreateWrongArgumentNumberError("setnx"))
	}
	key := string(args[0])
	value := args[1]
	entry := &database.Entry{Data: value}
	exists := db.data.PutIfAbsent(key, entry)
	if exists != 0 {
		// add command to AOF
		db.addAof(command.Parts())
		canceled := db.CancelTTL(key)
		if canceled == 1 {
			// add PERSIST command to aof
			db.addAof([][]byte{[]byte("persist"), args[0]})
		}
	}
	return protocol.NewNumberReply(exists)
}

func executeAppend(db *SingleDB, command redis.Command) *protocol.Reply {
	args := command.Args()
	if !ValidateArgCount(command.Name(), len(args)) {
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
		db.addAof(command.Parts())
	} else {
		// key doesn't exist.
		entry := &database.Entry{Data: appendValue}
		_ = db.data.Put(key, entry)
		length = len(appendValue)
		db.addAof([][]byte{[]byte("SET"), args[0], args[1]})
	}
	return protocol.NewNumberReply(length)
}

func executeIncr(db *SingleDB, command redis.Command) *protocol.Reply {
	args := command.Args()
	if !ValidateArgCount(command.Name(), len(args)) {
		return protocol.NewErrorReply(protocol.CreateWrongArgumentNumberError("incr"))
	}
	key := string(args[0])
	return add(db, key, 1)
}

func executeDecr(db *SingleDB, command redis.Command) *protocol.Reply {
	args := command.Args()
	if !ValidateArgCount(command.Name(), len(args)) {
		return protocol.NewErrorReply(protocol.CreateWrongArgumentNumberError("decr"))
	}
	key := string(args[0])
	return add(db, key, -1)
}

func executeIncrby(db *SingleDB, command redis.Command) *protocol.Reply {
	args := command.Args()
	if !ValidateArgCount(command.Name(), len(args)) {
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
	if !ValidateArgCount(command.Name(), len(args)) {
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

func execStrLen(db *SingleDB, command redis.Command) *protocol.Reply {
	args := command.Args()
	if !ValidateArgCount(command.Name(), len(args)) {
		return protocol.NewErrorReply(protocol.CreateWrongArgumentNumberError("STRLEN"))
	}
	value, exists, err := getString(db, string(args[0]))
	if err != nil {
		return protocol.NewErrorReply(err)
	}
	if exists {
		return protocol.NewNumberReply(len(value))
	}
	return protocol.NewNumberReply(0)
}

func execSetBit(db *SingleDB, command redis.Command) *protocol.Reply {
	args := command.Args()
	if !ValidateArgCount(command.Name(), len(args)) {
		return protocol.NewErrorReply(protocol.CreateWrongArgumentNumberError("SETBIT"))
	}
	// parse offset and bit number
	offset, err := strconv.ParseInt(string(args[1]), 10, 64)
	bit, err := strconv.ParseInt(string(args[2]), 10, 8)
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
	if !ValidateArgCount(command.Name(), len(args)) {
		return protocol.NewErrorReply(protocol.CreateWrongArgumentNumberError("GETBIT"))
	}
	// check offset number
	offset, err := strconv.ParseInt(string(args[1]), 10, 64)
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
