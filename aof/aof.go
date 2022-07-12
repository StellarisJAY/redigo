package aof

import (
	"bufio"
	"io"
	"log"
	"os"
	"redigo/config"
	"redigo/interface/database"
	"redigo/redis/parser"
	"redigo/redis/protocol"
	"redigo/tcp"
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
	aofChan        chan Payload // aof command buffer, commands stored in here before writes to file
	aofFileName    string
	aofFile        *os.File
	currentDB      int          // current database index, if index changed, must append a SELECT command
	ticker         *time.Ticker // ticker for EverySec policy
	closeChan      chan struct{}
	aofLock        sync.Mutex
	dbMaker        func() database.DB
	RewriteStarted atomic.Value
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
	log.Println("AOF enabled, aof file: ", config.Properties.AofFileName)
	start := time.Now()
	err = handler.loadAof(-1)
	if err != nil {
		panic(err)
	}
	log.Println("AOF loaded, time used: ", time.Now().Sub(start).Milliseconds(), "ms")
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
		raw := protocol.NewStringArrayReply(cmd)
		_, err := h.aofFile.Write(raw.ToBytes())
		if err != nil {
			log.Println(err)
			return
		}
		h.currentDB = p.idx
	}
	// Get RESP from command line
	raw := protocol.NewArrayReply(p.command)
	_, err := h.aofFile.Write(raw.ToBytes())
	if err != nil {
		log.Println(err)
	}
}

func (h *Handler) loadAof(maxBytes int64) error {
	file, err := os.Open(h.aofFileName)
	if err != nil {
		log.Println(err)
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
		cmd, err := parser.Parse(reader)
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
	h.closeChan <- struct{}{}
}
