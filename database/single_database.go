package database

import (
	"errors"
	"log"
	"redigo/datastruct/dict"
	"redigo/datastruct/lock"
	"redigo/redis"
	"redigo/redis/protocol"
	"redigo/util/timewheel"
	"time"
)

type SingleDB struct {
	data   dict.Dict
	ttlMap dict.Dict
	lock   *lock.Locker
	idx    int
}

func NewSingleDB(idx int) *SingleDB {
	return &SingleDB{
		data:   dict.NewSimpleDict(),
		ttlMap: dict.NewSimpleDict(),
		lock:   lock.NewLock(1024),
		idx:    idx,
	}
}

func (db *SingleDB) ExecuteLoop() error {
	panic(errors.New("unsupported operation"))
}

func (db *SingleDB) SubmitCommand(command redis.Command) {
	panic(errors.New("unsupported operation"))
}

/*
	Execute a command
	Finds the executor in executor map, then call execFunc to handle it
*/
func (db *SingleDB) Execute(command redis.Command) *protocol.Reply {
	cmd := command.Name()
	if cmd == "keys" {
		// get all keys from db, but don't match pattern now
		keys := db.data.Keys()
		// start a new goroutine to do pattern matching
		go func(command redis.Command, keys []string) {
			reply := execKeys(command, keys)
			command.Connection().SendReply(reply)
		}(command, keys)
		return nil
	} else {
		exec, exists := executors[cmd]
		if exists {
			reply := exec.execFunc(db, command)
			return reply
		} else {
			// command executor doesn't exist, send unknown command to client
			return protocol.NewErrorReply(protocol.CreateUnknownCommandError(cmd))
		}
	}
}

// Expire set a key's expire time
func (db *SingleDB) Expire(key string, ttl time.Duration) {
	expireTime := time.Now().Add(ttl)
	db.ttlMap.Put(key, expireTime)
	// schedule auto remove in time wheel
	timewheel.ScheduleDelayed(ttl, "expire_"+key, func() {
		_, exists := db.ttlMap.Get(key)
		if !exists {
			return
		}
		db.ttlMap.Remove(key)
		db.data.Remove(key)
		log.Println("Expired Key removed: ", key)
	})
}

func (db *SingleDB) TTL(key string) time.Duration {
	v, exists := db.ttlMap.Get(key)
	if exists {
		expireTime := v.(time.Time)
		ttl := expireTime.Sub(time.Now())
		if ttl < 0 {
			return 0
		}
		return ttl
	}
	return -1
}

func (db *SingleDB) CancelTTL(key string) {
	_, exists := db.ttlMap.Get(key)
	if exists {
		db.ttlMap.Remove(key)
		timewheel.Cancel("expire_" + key)
	}
}

func (db *SingleDB) Close() {
	//TODO implement me
	panic("implement me")
}
