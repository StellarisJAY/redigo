package database

import (
	"log"
	"redigo/datastruct/dict"
	"redigo/datastruct/lock"
	"redigo/redis"
	"redigo/redis/protocol"
	"redigo/util/timewheel"
	"strings"
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
		db.execute(cmd)
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
func (db *SingleDB) execute(command redis.Command) {
	cmd := strings.ToLower(command.Get(0))
	conn := command.Connection()
	if cmd == "command" {
		conn.SendReply(protocol.OKReply)
		return
	}
	// loop for command executor
	exec, exists := executors[cmd]
	if exists {
		reply := exec.execFunc(db, command)
		conn.SendReply(reply)
	} else {
		log.Println("Unknown command: ", cmd)
		// command executor doesn't exist, send unknown command to client
		conn.SendReply(protocol.NewErrorReply(protocol.CreateUnknownCommandError(cmd)))
	}
}

// Expire set a key's expire time
func (db *SingleDB) Expire(key string, ttl time.Duration) {
	expireTime := time.Now().Add(ttl)
	db.ttlMap.Put(key, expireTime)
	// schedule auto remove in time wheel
	timewheel.ScheduleDelayed(ttl, "expire_"+key, func() {
		ttl, exists := db.ttlMap.Get(key)
		if !exists {
			return
		}
		expireAt := ttl.(time.Time)
		// check if expire time before now
		if expired := time.Now().After(expireAt); expired {
			db.ttlMap.Remove(key)
			db.data.Remove(key)
			log.Println("Expired Key removed: ", key)
		}
	})
}

func (db *SingleDB) Close() {
	//TODO implement me
	panic("implement me")
}
