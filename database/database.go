package database

import (
	"redigo/interface/database"
	"redigo/redis"
	"redigo/redis/protocol"
	"strconv"
)

/*
	Entry holds a data of a key
*/
type Entry struct {
	Data     interface{}
	expireAt int64
}
type MultiDB struct {
	dbSet     []database.DB
	cmdChan   chan redis.Command
	executors map[string]func(redis.Command) *protocol.Reply
}

func NewMultiDB(dbSize, cmdChanSize int) *MultiDB {
	db := &MultiDB{
		dbSet:     make([]database.DB, dbSize),
		cmdChan:   make(chan redis.Command, cmdChanSize),
		executors: make(map[string]func(redis.Command) *protocol.Reply),
	}
	db.initCommandExecutors()
	// initialize single databases in db set
	for i := 0; i < dbSize; i++ {
		db.dbSet[i] = NewSingleDB(i)
	}
	return db
}

func (m *MultiDB) initCommandExecutors() {
	m.executors["command"] = func(command redis.Command) *protocol.Reply {
		return protocol.OKReply
	}
	m.executors["select"] = m.execSelectDB
	m.executors["ping"] = m.execPing
}

func (m *MultiDB) SubmitCommand(command redis.Command) {
	m.cmdChan <- command
}

func (m *MultiDB) Close() {
	//TODO implement me
	panic("implement me")
}

// ExecuteLoop of Multi Database
func (m *MultiDB) ExecuteLoop() error {
	for {
		cmd := <-m.cmdChan
		// execute command and get a reply if command is not dispatched to single database
		reply := m.Execute(cmd)
		if reply != nil {
			cmd.Connection().SendReply(reply)
		}
	}
}

func (m *MultiDB) Execute(command redis.Command) *protocol.Reply {
	cmdName := command.Name()
	if exec, ok := m.executors[cmdName]; ok {
		return exec(command)
	} else {
		// dispatch command to a single database
		index := command.Connection().DBIndex()
		// submit command to target database, target database will send reply
		return m.dbSet[index].Execute(command)
	}
}

func (m *MultiDB) execSelectDB(command redis.Command) *protocol.Reply {
	args := command.Args()
	if len(args) != 1 {
		return protocol.NewErrorReply(protocol.CreateWrongArgumentNumberError("select"))
	}
	index, err := strconv.Atoi(string(args[0]))
	// check database index
	if err != nil {
		return protocol.NewErrorReply(protocol.InvalidDBIndexError)
	} else if index >= len(m.dbSet) {
		return protocol.NewErrorReply(protocol.DBIndexOutOfRangeError)
	} else {
		connection := command.Connection()
		connection.SelectDB(index)
		return protocol.OKReply
	}
}

func (m *MultiDB) execPing(command redis.Command) *protocol.Reply {
	args := command.Args()
	var message string
	if len(args) < 1 {
		message = "PONG"
	} else {
		message = string(args[0])
	}
	return protocol.NewSingleStringReply(message)
}
