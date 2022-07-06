package database

import (
	"redigo/aof"
	"redigo/config"
	"redigo/interface/database"
	"redigo/interface/redis"
	"redigo/redis/protocol"
	"strconv"
	"time"
)

type MultiDB struct {
	dbSet      []database.DB
	cmdChan    chan redis.Command
	executors  map[string]func(redis.Command) *protocol.Reply
	aofHandler *aof.Handler
}

func NewTempDB(dbSize int) *MultiDB {
	db := &MultiDB{
		dbSet:     make([]database.DB, dbSize),
		cmdChan:   make(chan redis.Command, 0),
		executors: make(map[string]func(redis.Command) *protocol.Reply),
	}
	db.initCommandExecutors()
	// initialize single databases in db set
	for i := 0; i < dbSize; i++ {
		db.dbSet[i] = NewSingleDB(i)
	}
	return db
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
	// initialize AOF
	if config.Properties.AppendOnly {
		aofHandler, err := aof.NewAofHandler(db, func() database.DB {
			return NewTempDB(config.Properties.Databases)
		})
		if err != nil {
			panic(err)
		}
		for _, sdb := range db.dbSet {
			singleDB := sdb.(*SingleDB)
			// make singleDB's addAof call aofHandler's AddAOF
			singleDB.addAof = func(command [][]byte) {
				aofHandler.AddAof(command, singleDB.idx)
			}
		}
		db.aofHandler = aofHandler
	}
	return db
}

// Register MultiDB commands here
func (m *MultiDB) initCommandExecutors() {
	m.executors["command"] = func(command redis.Command) *protocol.Reply {
		return protocol.OKReply
	}
	m.executors["select"] = m.execSelectDB
	m.executors["ping"] = m.execPing
	m.executors["bgrewriteaof"] = m.execBGRewriteAOF
	m.executors["dbsize"] = m.execDBSize
	m.executors["flushdb"] = m.execFlushDB
	m.executors["multi"] = m.execMulti
	m.executors["exec"] = m.execMultiExec
	m.executors["watch"] = m.execWatch
	m.executors["unwatch"] = m.execUnWatch
	m.executors["discard"] = m.execMultiDiscard
}

func (m *MultiDB) SubmitCommand(command redis.Command) {
	m.cmdChan <- command
}

func (m *MultiDB) Close() {
	if m.aofHandler != nil {
		// close aof handler
		m.aofHandler.Close()
	}
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

func (m *MultiDB) Len(dbIdx int) int {
	if dbIdx < len(m.dbSet) {
		return m.dbSet[dbIdx].Len(dbIdx)
	}
	return 0
}

func (m *MultiDB) Execute(command redis.Command) *protocol.Reply {
	conn := command.Connection()
	name := command.Name()
	if name == "multi" {
		return m.execMulti(command)
	} else if name == "exec" {
		return m.execMultiExec(command)
	} else if conn.IsMulti() {
		return EnqueueCommand(conn, command)
	} else {
		return m.executeCommand(command)
	}
}

func (m *MultiDB) executeCommand(command redis.Command) *protocol.Reply {
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

func (m *MultiDB) ForEach(dbIdx int, fun func(key string, entry *database.Entry, expire *time.Time) bool) {
	if dbIdx < len(m.dbSet) {
		m.dbSet[dbIdx].ForEach(dbIdx, fun)
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

func (m *MultiDB) execBGRewriteAOF(command redis.Command) *protocol.Reply {
	err := m.aofHandler.StartRewrite()
	if err != nil {
		return protocol.NewErrorReply(err)
	}
	return protocol.NewSingleStringReply("Background append only file rewriting started")
}

func (m *MultiDB) execDBSize(command redis.Command) *protocol.Reply {
	conn := command.Connection()
	index := conn.DBIndex()
	return protocol.NewNumberReply(m.Len(index))
}

func (m *MultiDB) execFlushDB(command redis.Command) *protocol.Reply {
	conn := command.Connection()
	index := conn.DBIndex()
	args := command.Args()
	if len(args) > 1 {
		return protocol.NewErrorReply(protocol.CreateWrongArgumentNumberError("FLUSHDB"))
	}
	// check if flush in async mode
	async := len(args) == 1 && string(args[0]) == "ASYNC"
	m.dbSet[index].(*SingleDB).flushDB(async)
	// use single database to write AOF
	m.dbSet[index].(*SingleDB).addAof([][]byte{[]byte("FLUSHDB")})
	return protocol.OKReply
}

func (m *MultiDB) execMulti(command redis.Command) *protocol.Reply {
	conn := command.Connection()
	return StartMulti(conn)
}

func (m *MultiDB) execMultiExec(command redis.Command) *protocol.Reply {
	conn := command.Connection()
	if !conn.IsMulti() {
		return protocol.NewErrorReply(protocol.ExecWithoutMultiError)
	}
	return Exec(m, conn)
}

func (m *MultiDB) execMultiDiscard(command redis.Command) *protocol.Reply {
	conn := command.Connection()
	return Discard(conn)
}

func (m *MultiDB) execUnWatch(command redis.Command) *protocol.Reply {
	return UnWatch(command.Connection())
}

func (m *MultiDB) execWatch(command redis.Command) *protocol.Reply {
	if len(command.Args()) == 0 {
		return protocol.NewErrorReply(protocol.CreateWrongArgumentNumberError("watch"))
	}
	conn := command.Connection()
	keys := make([]string, len(command.Args()))
	for i, arg := range command.Args() {
		keys[i] = string(arg)
	}
	return Watch(m.dbSet[conn.DBIndex()].(*SingleDB), conn, keys)
}

func (m *MultiDB) getVersion(dbIndex int, key string) int64 {
	if dbIndex >= len(m.dbSet) {
		return -2
	}
	db := m.dbSet[dbIndex]
	return db.(*SingleDB).getVersion(key)
}
