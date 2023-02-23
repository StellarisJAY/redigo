package aof

import (
	"bufio"
	"io"
	"os"
	"redigo/config"
	"redigo/interface/database"
	"redigo/redis"
	"redigo/tcp"
	"redigo/util/log"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

type Payload struct {
	command [][]byte
	idx     int
}

type Handler struct {
	db             database.DB
	aofChan        chan Payload // aofChan AOF持久化缓冲，AOF异步写入磁盘
	aofFileName    string
	aofFile        *os.File
	currentDB      int          // currentDB aof持久化过程中需要记录当前数据库，在切换时aof要插入select命令
	ticker         *time.Ticker // ticker everysec 策略的计时器
	closeChan      chan struct{}
	aofLock        sync.Mutex
	dbMaker        func() database.DB // dbMaker 在aof重写时用来创建临时的内存数据库
	RewriteStarted atomic.Value
}

func NewDummyAofHandler() *Handler {
	handler := &Handler{RewriteStarted: atomic.Value{}}
	handler.RewriteStarted.Store(false)
	return handler
}

func NewAofHandler(db database.DB, dbMaker func() database.DB) (*Handler, error) {
	handler := &Handler{db: db}
	handler.aofChan = make(chan Payload, 1<<16)
	handler.closeChan = make(chan struct{})
	handler.aofLock = sync.Mutex{}
	handler.RewriteStarted = atomic.Value{}
	handler.RewriteStarted.Store(false)
	handler.dbMaker = dbMaker
	// create a ticker for EverySec AOF
	if config.Properties.AppendFsync == config.FsyncEverySec {
		handler.ticker = time.NewTicker(1 * time.Second)
	}
	handler.aofFileName = config.Properties.AofFileName
	// open append file
	file, err := os.OpenFile(handler.aofFileName, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}
	handler.aofFile = file
	start := time.Now()
	err = handler.loadAof(-1)
	if err != nil {
		panic(err)
	}
	log.Info("AOF loaded, time used: %d ms", time.Now().Sub(start).Milliseconds())
	go func() {
		// fsync policy every sec
		if config.Properties.AppendFsync == config.FsyncEverySec {
			handler.handleEverySec()
		} else {
			handler.handle()
		}
	}()
	return handler, nil
}

func (h *Handler) AddAof(command [][]byte, index int) {
	payload := Payload{
		command: command,
		idx:     index,
	}
	h.aofChan <- payload
}

// handle commands every second
func (h *Handler) handleEverySec() {
	for {
		select {
		case <-h.closeChan:
			h.aofLock.Lock()
			// receive close signal, break handle loop
			h.handleRemaining(len(h.aofChan))
			h.aofLock.Unlock()
			break
		case <-h.ticker.C:
			h.aofLock.Lock()
			// handle remaining commands in aof queue every sec
			remaining := len(h.aofChan)
			if remaining > 0 {
				h.handleRemaining(len(h.aofChan))
			}
			h.aofLock.Unlock()
			continue
		}
	}
}

// handle func of AOF
func (h *Handler) handle() {
	for {
		select {
		case <-h.closeChan:
			h.aofLock.Lock()
			// receive close signal, break handle loop
			h.handleRemaining(len(h.aofChan))
			h.aofLock.Unlock()
			break
		case payload := <-h.aofChan:
			h.aofLock.Lock()
			h.handlePayload(payload)
			h.aofLock.Unlock()
		}
	}
}

// handle the remaining un-written commands before closing
func (h *Handler) handleRemaining(remaining int) {
	for i := 0; i < remaining; i++ {
		payload := <-h.aofChan
		h.handlePayload(payload)
	}
}

func (h *Handler) handlePayload(p Payload) {
	// Add select DB command if payload's database is not aof handler's current db
	if p.idx != h.currentDB {
		cmd := []string{"SELECT", strconv.Itoa(p.idx)}
		raw := redis.NewStringArrayCommand(cmd)
		_, err := h.aofFile.Write(raw.ToBytes())
		if err != nil {
			log.Errorf("aof write select db error: %v", err)
			return
		}
		h.currentDB = p.idx
	}
	// Get RESP from command line
	raw := redis.NewArrayCommand(p.command)
	_, err := h.aofFile.Write(raw.ToBytes())
	if err != nil {
		log.Errorf("aof write command error: %v", err)
	}
}

func (h *Handler) loadAof(maxBytes int64) error {
	file, err := os.Open(h.aofFileName)
	if err != nil {
		log.Errorf("open aof file error: %v", err)
		return err
	}
	defer func(file *os.File) {
		_ = file.Close()
	}(file)
	var r io.Reader
	if maxBytes > 0 {
		r = io.LimitReader(file, maxBytes)
	} else {
		r = file
	}
	reader := bufio.NewReader(r)
	// a fake connection, to hold the database index
	fakeConn := tcp.Connection{}
	for {
		cmd, err := redis.Decode(reader)
		if err != nil {
			if err != io.EOF {
				return err
			}
			break
		}
		// change database index
		if cmd.Name() == "select" {
			idx, err := strconv.Atoi(string(cmd.Args()[0]))
			if err != nil {
				continue
			}
			fakeConn.SelectDB(idx)
		}

		// bind the fake connection with command and execute command
		cmd.BindConnection(&fakeConn)
		h.db.Execute(cmd)
	}
	return nil
}

func (h *Handler) Close() {
	if h.closeChan != nil {
		h.closeChan <- struct{}{}
	}
}
