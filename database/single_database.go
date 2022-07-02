package database

import (
	"errors"
	"log"
	"redigo/config"
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
			reply := execKeys(db, command, keys)
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
	// if server enabled scheduler for expiring
	if config.Properties.UseScheduleExpire {
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

func (db *SingleDB) CancelTTL(key string) int {
	_, exists := db.ttlMap.Get(key)
	if exists {
		db.ttlMap.Remove(key)
		if config.Properties.UseScheduleExpire {
			timewheel.Cancel("expire_" + key)
		}
		return 1
	}
	return 0
}

// Check if key expired, remove key if necessary
func (db *SingleDB) expireIfNeeded(key string) bool {
	v, ok := db.ttlMap.Get(key)
	if !ok {
		return false
	}
	expireAt := v.(time.Time)
	if expireAt.Before(time.Now()) {
		// remove key
		db.data.Remove(key)
		// remove the scheduler task for key's ttl
		if config.Properties.UseScheduleExpire {
			db.CancelTTL(key)
		}
		log.Println("Lazy expire key: ", key)
		return true
	}
	return false
}

// get the data entry holding the key's value. Checking key's existence and expire time
func (db *SingleDB) getEntry(key string) (entry *Entry, exists bool) {
	v, ok := db.data.Get(key)
	if !ok || db.expireIfNeeded(key) {
		return nil, false
	}
	return v.(*Entry), true
}

func (db *SingleDB) Close() {
	//TODO implement me
	panic("implement me")
}
