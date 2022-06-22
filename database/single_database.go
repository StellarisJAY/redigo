package database

import (
	"log"
	"redigo/datastruct/dict"
	"redigo/datastruct/lock"
	"redigo/redis"
	"redigo/redis/protocol"
	"strings"
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

func (db *SingleDB) Close() {
	//TODO implement me
	panic("implement me")
}
