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
	"time"
)

type Payload struct {
	command [][]byte
	idx     int
}

type Handler struct {
	db        database.DB
	aofChan   chan Payload // aof command buffer, commands stored in here before writes to file
	aofFile   *os.File
	currentDB int          // current database index, if index changed, must append a SELECT command
	ticker    *time.Ticker // ticker for EverySec policy
	closeChan chan struct{}
}

func NewAofHandler(db database.DB) (*Handler, error) {
	handler := &Handler{db: db}
	handler.aofChan = make(chan Payload, 1024)
	handler.closeChan = make(chan struct{})
	// create a ticker for EverySec AOF
	if config.Properties.AppendFsync == config.AofEverySec {
		handler.ticker = time.NewTicker(1 * time.Second)
	}
	// open append file
	file, err := os.OpenFile(config.Properties.AofFileName, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	handler.aofFile = file
	log.Println("AOF enabled, aof file: ", config.Properties.AofFileName)
	start := time.Now()
	err = handler.loadAof()
	if err != nil {
		panic(err)
	}
	log.Println("AOF loaded, time used: ", time.Now().Sub(start).Milliseconds(), "ms")
	go func() {
		handler.handle()
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

// handle func of AOF
func (h *Handler) handle() {
	for {
		select {
		case <-h.closeChan:
			// receive close signal, break handle loop
			h.handleClose(len(h.aofChan))
			break
		case payload := <-h.aofChan:
			h.handlePayload(payload)
		}
	}
}

// handle the remaining un-written commands before closing
func (h *Handler) handleClose(remaining int) {
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

func (h *Handler) loadAof() error {
	reader := bufio.NewReader(h.aofFile)
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
		h.db.Execute(*cmd)
	}
	return nil
}

func (h *Handler) Close() {
	h.closeChan <- struct{}{}
}
