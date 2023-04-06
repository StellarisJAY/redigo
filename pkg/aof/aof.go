package aof

import (
	"bufio"
	"io"
	"os"
	"redigo/pkg/config"
	"redigo/pkg/interface/database"
	"redigo/pkg/redis"
	"redigo/pkg/tcp"
	"redigo/pkg/util/log"
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
	handler.aofChan = make(chan Payload, 1<<20)
	handler.closeChan = make(chan struct{})
	handler.aofLock = sync.Mutex{}
	handler.RewriteStarted = atomic.Value{}
	handler.RewriteStarted.Store(false)
	handler.dbMaker = dbMaker
	// 每秒aof的ticker
	if config.Properties.AppendFsync == config.FsyncEverySec {
		handler.ticker = time.NewTicker(1 * time.Second)
	}
	handler.aofFileName = config.Properties.AofFileName
	if file, err := os.OpenFile(handler.aofFileName, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0666); err != nil {
		return nil, err
	}else {
		handler.aofFile = file
	}
	start := time.Now()
	if err := handler.loadAof(-1); err != nil {
		panic(err)
	}
	log.Info("AOF loaded, time used: %d ms", time.Now().Sub(start).Milliseconds())
	// 处理fsync
	go func() {
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
LOOP:
	for {
		select {
		case <-h.closeChan:
			h.aofLock.Lock()
			// fsync剩余的aof任务
			h.handleRemaining(len(h.aofChan))
			h.aofLock.Unlock()
			break LOOP
		case <-h.ticker.C:
			h.aofLock.Lock()
			// 每秒将chan中所有的aof都sync
			remaining := len(h.aofChan)
			if remaining > 0 {
				h.handleRemaining(len(h.aofChan))
			}
			h.aofLock.Unlock()
		}
	}
}

func (h *Handler) handle() {
LOOP:
	for {
		select {
		case <-h.closeChan:
			h.aofLock.Lock()
			h.handleRemaining(len(h.aofChan))
			h.aofLock.Unlock()
			break LOOP
		case payload := <-h.aofChan:
			h.aofLock.Lock()
			h.handlePayload(payload)
			h.aofLock.Unlock()
		}
	}
}

func (h *Handler) handleRemaining(remaining int) {
	for i := 0; i < remaining; i++ {
		payload := <-h.aofChan
		h.handlePayload(payload)
	}
}

func (h *Handler) handlePayload(p Payload) {
	//当前数据库idx和payload不同，需要追加select命令
	if p.idx != h.currentDB {
		cmd := []string{"SELECT", strconv.Itoa(p.idx)}
		raw := redis.NewStringArrayCommand(cmd)
		if _, err := h.aofFile.Write(raw.ToBytes()); err != nil {
			log.Errorf("aof write select db error: %v", err)
			return
		}
		h.currentDB = p.idx
	}

	raw := redis.NewArrayCommand(p.command)
	if _, err := h.aofFile.Write(raw.ToBytes()); err != nil {
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
	//fake conn 用来记录当前的db index
	fakeConn := tcp.Connection{}
	for {
		cmd, err := redis.Decode(reader)
		if err != nil {
			if err != io.EOF {
				return err
			}
			break
		}
		if cmd.Name() == "select" {
			idx, err := strconv.Atoi(string(cmd.Args()[0]))
			if err != nil {
				continue
			}
			fakeConn.SelectDB(idx)
		}
		
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
