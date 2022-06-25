package database

import (
	"log"
	"redigo/datastruct/dict"
	"redigo/datastruct/lock"
	"redigo/redis"
	"redigo/redis/protocol"
	"redigo/util/timewheel"
	"time"
)

type SingleDB struct {
	data        dict.Dict
	ttlMap      dict.Dict
	lock        *lock.Locker
	idx         int
	commandChan chan redis.Command
}

func NewSingleDB(idx int, commandChanSize int) *SingleDB {
	return &SingleDB{
		data:        dict.NewSimpleDict(),
		ttlMap:      dict.NewSimpleDict(),
		lock:        lock.NewLock(1024),
		idx:         idx,
		commandChan: make(chan redis.Command, commandChanSize),
	}
}

func (db *SingleDB) ExecuteLoop() error {
	for {
		// wait for command
		cmd := <-db.commandChan
		// execute command
		reply := db.Execute(cmd)
		// send reply
		cmd.Connection().SendReply(reply)
	}
}

func (db *SingleDB) SubmitCommand(command redis.Command) {
	// submit a command to command channel
	db.commandChan <- command
}

/*
	Execute a command
	Finds the executor in executor map, then call execFunc to handle it
*/
func (db *SingleDB) Execute(command redis.Command) *protocol.Reply {
	cmd := command.Name()
	exec, exists := executors[cmd]
	if exists {
		reply := exec.execFunc(db, command)
		return reply
	} else {
		log.Println("Unknown command: ", cmd)
		// command executor doesn't exist, send unknown command to client
		return protocol.NewErrorReply(protocol.CreateUnknownCommandError(cmd))
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
