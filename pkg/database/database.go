package database

import (
	"redigo/pkg/aof"
	"redigo/pkg/config"
	"redigo/pkg/interface/database"
	"redigo/pkg/pubsub"
	"redigo/pkg/rdb"
	"redigo/pkg/redis"
	"redigo/pkg/util/log"
	"strconv"
	"time"
)

/*
MultiDB multiDB 是DB接口的多数据库实现。
提供了Redis中的切换数据库、移动key、持久化等跨数据库命令。
*/
type MultiDB struct {
	dbSet       []database.DB                                     // dbSet 数据库集合，默认是16个单独的数据库
	cmdChan     chan redis.Command                                // cmdChan 并发模式下的命令缓冲channel
	executors   map[string]func(redis.Command) *redis.RespCommand // executors 命令执行器map，记录命令与executor的映射
	aofHandler  *aof.Handler                                      // aofHandler AOF持久化功能组件
	hub         *pubsub.Hub                                       // hub 发布订阅功能组件
	modifyCount int
}

// NewTempDB 创建临时数据库，临时数据库只用在AOF重写上
func NewTempDB(dbSize int) *MultiDB {
	db := &MultiDB{
		dbSet:     make([]database.DB, dbSize),
		cmdChan:   make(chan redis.Command, 0),
		executors: make(map[string]func(redis.Command) *redis.RespCommand),
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
		executors: make(map[string]func(redis.Command) *redis.RespCommand),
		hub:       pubsub.MakeHub(),
	}
	db.initCommandExecutors()
	for i := 0; i < dbSize; i++ {
		db.dbSet[i] = NewSingleDB(i)
	}
	// 初始化aof
	if config.Properties.AppendOnly {
		aofHandler, err := aof.NewAofHandler(db, func() database.DB {
			return NewTempDB(config.Properties.Databases)
		})
		if err != nil {
			panic(err)
		}
		// 设置每个数据库的aofHandler
		for _, sdb := range db.dbSet {
			singleDB := sdb.(*SingleDB)
			singleDB.addAof = func(command [][]byte) {
				aofHandler.AddAof(command, singleDB.idx)
			}
		}
		db.aofHandler = aofHandler
	} else {
		// dummyHandler 是没有开启aof时的空handler
		db.aofHandler = aof.NewDummyAofHandler()
	}
	rdbStart := time.Now()
	err := loadRDB(db)
	if err != nil {
		log.Errorf("load rdb error: %v", err)
	} else {
		log.Info("RDB Loaded time used: %d ms", time.Now().Sub(rdbStart).Milliseconds())
	}
	scheduleSaving(db)
	return db
}

// 注册服务器级别的命令
func (m *MultiDB) initCommandExecutors() {
	// command命令直接返回OK
	m.executors["command"] = func(command redis.Command) *redis.RespCommand {
		return redis.OKCommand
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
	m.executors["subscribe"] = m.execSubscribe
	m.executors["publish"] = m.execPublish
	m.executors["psubscribe"] = m.execPSubscribe
	m.executors["move"] = m.execMove
	m.executors["save"] = m.execSave
	m.executors["bgsave"] = m.execBGSave
	m.executors["timed-bgsave"] = m.execTimedBGSave
}

func (m *MultiDB) SubmitCommand(command redis.Command) {
	m.cmdChan <- command
}

func (m *MultiDB) Close() {
	if m.aofHandler != nil {
		// close aof handler
		m.aofHandler.Close()
	}
	close(m.cmdChan)
}

// ExecuteLoop of Multi Database
func (m *MultiDB) ExecuteLoop() error {
	for {
		cmd, ok := <-m.cmdChan
		if !ok {
			return nil
		}
		// 服务器级别的命令会返回reply，数据库命令会由数据库处理器执行
		reply := m.Execute(cmd)
		if reply != nil {
			cmd.Connection().SendCommand(reply)
		}
	}
}

func (m *MultiDB) Len(dbIdx int) int {
	if dbIdx < len(m.dbSet) {
		return m.dbSet[dbIdx].Len(dbIdx)
	}
	return 0
}

func (m *MultiDB) Execute(command redis.Command) *redis.RespCommand {
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

func (m *MultiDB) OnConnectionClosed(conn redis.Connection) {
	m.hub.UnSubscribeAll(conn)
}

func (m *MultiDB) executeCommand(command redis.Command) *redis.RespCommand {
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

func (m *MultiDB) execSelectDB(command redis.Command) *redis.RespCommand {
	args := command.Args()
	if len(args) != 1 {
		return redis.NewErrorCommand(redis.CreateWrongArgumentNumberError("select"))
	}
	index, err := strconv.Atoi(string(args[0]))
	// check database index
	if err != nil {
		return redis.NewErrorCommand(redis.InvalidDBIndexError)
	} else if index >= len(m.dbSet) {
		return redis.NewErrorCommand(redis.DBIndexOutOfRangeError)
	} else {
		connection := command.Connection()
		connection.SelectDB(index)
		return redis.OKCommand
	}
}

func (m *MultiDB) execPing(command redis.Command) *redis.RespCommand {
	args := command.Args()
	var message string
	if len(args) < 1 {
		message = "PONG"
	} else {
		message = string(args[0])
	}
	return redis.NewSingleLineCommand([]byte(message))
}

func (m *MultiDB) execBGRewriteAOF(command redis.Command) *redis.RespCommand {
	err := m.aofHandler.StartRewrite()
	if err != nil {
		return redis.NewErrorCommand(err)
	}
	return redis.NewSingleLineCommand([]byte("Background append only file rewriting started"))
}

func (m *MultiDB) execDBSize(command redis.Command) *redis.RespCommand {
	conn := command.Connection()
	index := conn.DBIndex()
	return redis.NewNumberCommand(m.Len(index))
}

func (m *MultiDB) execFlushDB(command redis.Command) *redis.RespCommand {
	conn := command.Connection()
	index := conn.DBIndex()
	args := command.Args()
	if len(args) > 1 {
		return redis.NewErrorCommand(redis.CreateWrongArgumentNumberError("FLUSHDB"))
	}
	// check if flush in async mode
	async := len(args) == 1 && string(args[0]) == "ASYNC"
	m.dbSet[index].(*SingleDB).flushDB(async)
	// use single database to write AOF
	m.dbSet[index].(*SingleDB).addAof([][]byte{[]byte("FLUSHDB")})
	return redis.OKCommand
}

func (m *MultiDB) execMulti(command redis.Command) *redis.RespCommand {
	conn := command.Connection()
	return StartMulti(conn)
}

func (m *MultiDB) execMultiExec(command redis.Command) *redis.RespCommand {
	conn := command.Connection()
	if !conn.IsMulti() {
		return redis.NewErrorCommand(redis.ExecWithoutMultiError)
	}
	return Exec(m, conn)
}

func (m *MultiDB) execMultiDiscard(command redis.Command) *redis.RespCommand {
	conn := command.Connection()
	return Discard(conn)
}

func (m *MultiDB) execUnWatch(command redis.Command) *redis.RespCommand {
	return UnWatch(command.Connection())
}

func (m *MultiDB) execWatch(command redis.Command) *redis.RespCommand {
	if len(command.Args()) == 0 {
		return redis.NewErrorCommand(redis.CreateWrongArgumentNumberError("watch"))
	}
	conn := command.Connection()
	// multi内部不能watch
	if conn.IsMulti() {
		return redis.NewErrorCommand(redis.WatchInsideMultiError)
	}
	keys := make([]string, len(command.Args()))
	for i, arg := range command.Args() {
		keys[i] = string(arg)
	}
	return Watch(m.dbSet[conn.DBIndex()].(*SingleDB), conn, keys)
}

func (m *MultiDB) execSubscribe(command redis.Command) *redis.RespCommand {
	if len(command.Args()) == 0 {
		return redis.NewErrorCommand(redis.CreateWrongArgumentNumberError("subscribe"))
	}
	m.hub.Subscribe(command.Connection(), command.Args())
	return nil
}

func (m *MultiDB) execPublish(command redis.Command) *redis.RespCommand {
	if len(command.Args()) != 2 {
		return redis.NewErrorCommand(redis.CreateWrongArgumentNumberError("publish"))
	}
	args := command.Args()
	return redis.NewNumberCommand(m.hub.Publish(string(args[0]), args[1]))
}

func (m *MultiDB) execPSubscribe(command redis.Command) *redis.RespCommand {
	if len(command.Args()) == 0 {
		return redis.NewErrorCommand(redis.CreateWrongArgumentNumberError("psubscribe"))
	}
	patterns := make([]string, len(command.Args()))
	for i, arg := range command.Args() {
		patterns[i] = string(arg)
	}
	m.hub.PSubscribe(command.Connection(), patterns)
	return nil
}

func (m *MultiDB) execMove(command redis.Command) *redis.RespCommand {
	args := command.Args()
	if len(args) != 2 {
		return redis.NewErrorCommand(redis.CreateWrongArgumentNumberError("move"))
	}
	key := string(args[0])
	// parse database index, check if index in range
	dbIndex, err := strconv.Atoi(string(args[1]))
	if err != nil || dbIndex < 0 || dbIndex >= config.Properties.Databases {
		return redis.NewErrorCommand(redis.ValueNotIntegerOrOutOfRangeError)
	}
	currentIndex := command.Connection().DBIndex()
	// target database is current database
	if dbIndex == currentIndex {
		return redis.OKCommand
	}
	currentDB := m.dbSet[currentIndex].(*SingleDB)
	targetDB := m.dbSet[dbIndex].(*SingleDB)

	// get key's value and target db's value
	entry, exists := currentDB.GetEntry(key)
	_, duplicate := targetDB.GetEntry(key)
	// if key doesn't exist or target database already has key
	if !exists || duplicate {
		return redis.NewNumberCommand(0)
	}
	// remove key in current db, put key into target db
	_ = currentDB.data.Remove(key)
	_ = targetDB.data.Put(key, entry)
	return redis.NewNumberCommand(1)
}

func (m *MultiDB) getVersion(dbIndex int, key string) int64 {
	if dbIndex >= len(m.dbSet) {
		return -2
	}
	db := m.dbSet[dbIndex]
	return db.(*SingleDB).getVersion(key)
}

func (m *MultiDB) execSave(command redis.Command) *redis.RespCommand {
	// prevent running BGSave and Save at the same time
	if !m.aofHandler.RewriteStarted.CompareAndSwap(false, true) {
		return redis.NewErrorCommand(redis.BackgroundSaveInProgressError)
	}
	defer m.aofHandler.RewriteStarted.Store(false)
	startTime := time.Now()
	err := rdb.Save(m)
	if err != nil {
		return redis.NilCommand
	}
	log.Info("RDB saved, time used: %d ms", time.Now().Sub(startTime).Milliseconds())
	return redis.OKCommand
}

func (m *MultiDB) execBGSave(command redis.Command) *redis.RespCommand {
	return BGSave(m, command)
}

func (m *MultiDB) execTimedBGSave(command redis.Command) *redis.RespCommand {
	if m.modifyCount >= config.Properties.RdbThreshold {
		// do rdb saving
		BGSave(m, command)
	}
	m.modifyCount = 0
	// reschedule rdb saving
	scheduleSaving(m)
	return nil
}

func (m *MultiDB) increaseModifyCount() {
	m.modifyCount++
}
