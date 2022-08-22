package database

import (
	"log"
	"redigo/datastruct/bitmap"
	"redigo/interface/database"
	"redigo/redis"
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
	RegisterCommandExecutor("bitcount", execBitCount, 3)
}

func executeSet(db *SingleDB, command redis.Command) *redis.RespCommand {
	args := command.Args()
	if !ValidateArgCount(command.Name(), len(args)) {
		return redis.NewErrorCommand(redis.CreateWrongArgumentNumberError("set"))
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
				return redis.NewErrorCommand(redis.CreateWrongArgumentNumberError("set "))
			}
			if num, err := strconv.Atoi(string(args[i+1])); err != nil {
				log.Println("Error arg: ", arg)
				return redis.NewErrorCommand(redis.HashValueNotIntegerError)
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

	entry := &database.Entry{Key: key, Data: value, DataSize: int64(len(value))}
	var result int
	switch policy {
	case defaultPolicy:
		result = db.putOrUpdateEntry(entry)
	case insertPolicy:
		result = db.putIfAbsent(entry)
	case updatePolicy:
		result = db.putIfExists(key, value)
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
		return redis.NilCommand
	} else {
		db.addVersion(key)
		return redis.OKCommand
	}
}

func executeGet(db *SingleDB, command redis.Command) *redis.RespCommand {
	args := command.Args()
	if !ValidateArgCount(command.Name(), len(args)) {
		return redis.NewErrorCommand(redis.CreateWrongArgumentNumberError("get"))
	}
	result, exists, err := getString(db, string(args[0]))
	if err != nil {
		return redis.NewErrorCommand(err)
	}
	if exists {
		return redis.NewBulkStringCommand(result)
	}
	return redis.NilCommand
}

func executeSetNX(db *SingleDB, command redis.Command) *redis.RespCommand {
	args := command.Args()
	if !ValidateArgCount(command.Name(), len(args)) {
		return redis.NewErrorCommand(redis.CreateWrongArgumentNumberError("setnx"))
	}
	key := string(args[0])
	value := args[1]
	entry := &database.Entry{Data: value}
	result := db.putIfAbsent(entry)
	if result != 0 {
		// add command to AOF
		db.addAof(command.Parts())
		canceled := db.CancelTTL(key)
		if canceled == 1 {
			// add PERSIST command to aof
			db.addAof([][]byte{[]byte("persist"), args[0]})
		}
	}
	return redis.NewNumberCommand(result)
}

func executeAppend(db *SingleDB, command redis.Command) *redis.RespCommand {
	args := command.Args()
	if !ValidateArgCount(command.Name(), len(args)) {
		return redis.NewErrorCommand(redis.CreateWrongArgumentNumberError("append"))
	}
	key := string(args[0])
	appendValue := args[1]
	v, exists := db.data.Get(key)
	var length int
	if exists {
		entry := v.(*database.Entry)
		// check if entry is string type
		if !isString(*entry) {
			return redis.NewErrorCommand(redis.WrongTypeOperationError)
		}
		// append new value to original string
		originalValue := entry.Data.([]byte)
		value := make([]byte, len(originalValue)+len(appendValue))
		copy(value[0:len(originalValue)], originalValue)
		copy(value[len(originalValue):], appendValue)
		db.updateEntry(entry, value)
		length = len(value)
		db.addAof(command.Parts())
	} else {
		// key doesn't exist.
		entry := &database.Entry{Data: appendValue}
		_ = db.putEntry(entry)
		length = len(appendValue)
		db.addAof([][]byte{[]byte("SET"), args[0], args[1]})
	}
	return redis.NewNumberCommand(length)
}

func executeIncr(db *SingleDB, command redis.Command) *redis.RespCommand {
	args := command.Args()
	if !ValidateArgCount(command.Name(), len(args)) {
		return redis.NewErrorCommand(redis.CreateWrongArgumentNumberError("incr"))
	}
	key := string(args[0])
	return add(db, key, 1)
}

func executeDecr(db *SingleDB, command redis.Command) *redis.RespCommand {
	args := command.Args()
	if !ValidateArgCount(command.Name(), len(args)) {
		return redis.NewErrorCommand(redis.CreateWrongArgumentNumberError("decr"))
	}
	key := string(args[0])
	return add(db, key, -1)
}

func executeIncrby(db *SingleDB, command redis.Command) *redis.RespCommand {
	args := command.Args()
	if !ValidateArgCount(command.Name(), len(args)) {
		return redis.NewErrorCommand(redis.CreateWrongArgumentNumberError("incrby"))
	}
	key := string(args[0])
	deltaStr := string(args[1])
	if delta, err := strconv.Atoi(deltaStr); err != nil {
		return redis.NewErrorCommand(redis.HashValueNotIntegerError)
	} else {
		return add(db, key, delta)
	}
}

func executeDecrby(db *SingleDB, command redis.Command) *redis.RespCommand {
	args := command.Args()
	if !ValidateArgCount(command.Name(), len(args)) {
		return redis.NewErrorCommand(redis.CreateWrongArgumentNumberError("decrby"))
	}
	key := string(args[0])
	deltaStr := string(args[1])
	if delta, err := strconv.Atoi(deltaStr); err != nil {
		return redis.NewErrorCommand(redis.HashValueNotIntegerError)
	} else {
		return add(db, key, -delta)
	}
}

// add : add a delta value to the key's value
func add(db *SingleDB, key string, delta int) *redis.RespCommand {
	v, exists := db.data.Get(key)
	if exists {
		entry := v.(*database.Entry)
		// check entry type
		if !isString(*entry) {
			return redis.NewErrorCommand(redis.WrongTypeOperationError)
		}
		s := string(entry.Data.([]byte))
		if val, err := strconv.Atoi(s); err != nil {
			return redis.NewErrorCommand(redis.HashValueNotIntegerError)
		} else {
			val = val + delta
			value := []byte(strconv.Itoa(val))
			entry.Data = value
			db.data.Put(key, entry)
			return redis.NewNumberCommand(val)
		}
	} else {
		entry := &database.Entry{Data: []byte(strconv.Itoa(delta))}
		db.data.Put(key, entry)
		return redis.NewNumberCommand(delta)
	}
}

func execStrLen(db *SingleDB, command redis.Command) *redis.RespCommand {
	args := command.Args()
	if !ValidateArgCount(command.Name(), len(args)) {
		return redis.NewErrorCommand(redis.CreateWrongArgumentNumberError("STRLEN"))
	}
	value, exists, err := getString(db, string(args[0]))
	if err != nil {
		return redis.NewErrorCommand(err)
	}
	if exists {
		return redis.NewNumberCommand(len(value))
	}
	return redis.NewNumberCommand(0)
}

func execSetBit(db *SingleDB, command redis.Command) *redis.RespCommand {
	args := command.Args()
	if !ValidateArgCount(command.Name(), len(args)) {
		return redis.NewErrorCommand(redis.CreateWrongArgumentNumberError("SETBIT"))
	}
	// parse offset and bit number
	offset, err := strconv.ParseInt(string(args[1]), 10, 64)
	bit, err := strconv.ParseInt(string(args[2]), 10, 8)
	if err != nil {
		return redis.NewErrorCommand(redis.HashValueNotIntegerError)
	}
	// get bitmap struct
	bm, exists, err := getBitMap(db, string(args[0]))
	if err != nil {
		return redis.NewErrorCommand(err)
	}
	if !exists {
		// create a new bitmap if not exist
		bm = bitmap.New()
		entry := &database.Entry{Data: bm}
		db.data.Put(string(args[0]), entry)
	}
	original := bm.SetBit(offset, byte(bit))
	db.addAof(command.Parts())
	return redis.NewNumberCommand(int(original))
}

func execGetBit(db *SingleDB, command redis.Command) *redis.RespCommand {
	args := command.Args()
	if !ValidateArgCount(command.Name(), len(args)) {
		return redis.NewErrorCommand(redis.CreateWrongArgumentNumberError("GETBIT"))
	}
	// check offset number
	offset, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return redis.NewErrorCommand(redis.HashValueNotIntegerError)
	}
	// get bitmap data structure
	bitMap, exists, err := getBitMap(db, string(args[0]))
	if err != nil {
		return redis.NewErrorCommand(err)
	}
	if exists {
		return redis.NewNumberCommand(int(bitMap.GetBit(offset)))
	}
	return redis.NewNumberCommand(0)
}
func execBitCount(db *SingleDB, command redis.Command) *redis.RespCommand {
	args := command.Args()
	if !ValidateArgCount(command.Name(), len(args)) {
		return redis.NewErrorCommand(redis.CreateWrongArgumentNumberError("BITCOUNT"))
	}

	start, err := strconv.Atoi(string(args[1]))
	end, err := strconv.Atoi(string(args[2]))
	if err != nil {
		return redis.NewErrorCommand(redis.ValueNotIntegerOrOutOfRangeError)
	}
	bitMap, exists, err := getBitMap(db, string(args[0]))
	if err != nil {
		return redis.NewErrorCommand(err)
	}
	if !exists {
		return redis.NewNumberCommand(0)
	}
	return redis.NewNumberCommand(int(bitMap.BitCount(int64(start), int64(end))))
}

// getString get the value of this key, if not string returns an error
func getString(db *SingleDB, key string) ([]byte, bool, error) {
	entry, exists := db.GetEntry(key)
	// check key's existence
	if !exists {
		return nil, false, nil
	}
	if isString(*entry) {
		return entry.Data.([]byte), true, nil
	}
	if isBitMap(*entry) {
		var bitMap *bitmap.BitMap = entry.Data.(*bitmap.BitMap)
		return *bitMap, true, nil
	}
	return nil, true, redis.WrongTypeOperationError
}

func getBitMap(db *SingleDB, key string) (*bitmap.BitMap, bool, error) {
	entry, exists := db.GetEntry(key)
	if !exists {
		return nil, false, nil
	}
	if isString(*entry) {
		var bitMap bitmap.BitMap = entry.Data.([]byte)
		return &bitMap, true, nil
	}
	if isBitMap(*entry) {
		return entry.Data.(*bitmap.BitMap), true, nil
	}

	return nil, true, redis.WrongTypeOperationError
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
