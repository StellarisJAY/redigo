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
	dbSet   []database.DB
	cmdChan chan redis.Command
}

func NewMultiDB(dbSize, cmdChanSize int) *MultiDB {
	db := &MultiDB{
		dbSet:   make([]database.DB, dbSize),
		cmdChan: make(chan redis.Command, cmdChanSize),
	}
	// initialize single databases in db set
	for i := 0; i < dbSize; i++ {
		db.dbSet[i] = NewSingleDB(i, cmdChanSize)
		// start single database's execute loop
		go func(idx int) {
			err := db.dbSet[idx].ExecuteLoop()
			if err != nil {
				panic(err)
			}
		}(i)
	}
	return db
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
	// select command, switches database
	if cmdName == "select" {
		return m.execSelectDB(command)
	} else if cmdName == "command" {
		return protocol.OKReply
	} else {
		// dispatch command to a single database
		index := command.Connection().DBIndex()
		// submit command to target database, target database will send reply
		m.dbSet[index].SubmitCommand(command)
		return nil
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
