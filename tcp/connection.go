package tcp

import (
	"bufio"
	"context"
	"io"
	"net"
	"redigo/interface/database"
	"redigo/redis"
	"sync/atomic"
)

type Connection struct {
	Conn       net.Conn
	ReplyChan  chan *redis.RespCommand
	db         database.DB
	ctx        context.Context
	cancel     context.CancelFunc
	selectedDB int
	multi      bool
	watching   map[string]int64
	cmdQueue   []*redis.RespCommand
	active     atomic.Value
}

func NewConnection(conn net.Conn, db database.DB) *Connection {
	ctx, cancel := context.WithCancel(context.Background())
	connect := &Connection{
		Conn:       conn,
		ReplyChan:  make(chan *redis.RespCommand, 1024),
		db:         db,
		cancel:     cancel,
		ctx:        ctx,
		selectedDB: 0,
		multi:      false,
		active:     atomic.Value{},
	}
	connect.active.Store(true)
	return connect
}

/*
	readLoop for a connection
	Continuously read data from connection and dispatch command to handler
*/
func (c *Connection) ReadLoop() error {
	reader := bufio.NewReader(c.Conn)
	for {
		//parse RESP
		cmd, err := redis.Decode(reader)
		// push result to connection's write chan
		if err != nil {
			// Read failed, connection closed
			if err == io.EOF {
				return err
			}
		} else {
			cmd.BindConnection(c)
			c.db.SubmitCommand(cmd)
		}
	}
}

/*
	writeLoop for a connection
	Poll bytes from write channel and write to remote client
*/
func (c *Connection) WriteLoop() error {
	for {
		select {
		case reply := <-c.ReplyChan:
			_, err := c.Conn.Write(redis.Encode(reply))
			if err != nil {
				return err
			}
		case <-c.ctx.Done():
			return nil
		}
	}
}

/*
	close a connection
*/
func (c *Connection) Close() {
	c.active.Store(false)
	_ = c.Conn.Close()
	c.ReplyChan = nil
	c.cancel()
}

func (c *Connection) SendCommand(command *redis.RespCommand) {
	c.ReplyChan <- command
}

func (c *Connection) SelectDB(index int) {
	c.selectedDB = index
}

func (c *Connection) DBIndex() int {
	return c.selectedDB
}

func (c *Connection) SetMulti(multi bool) {
	if !multi {
		c.watching = nil
		c.cmdQueue = nil
	}
	c.multi = multi
}

func (c *Connection) IsMulti() bool {
	return c.multi
}

func (c *Connection) EnqueueCommand(command *redis.RespCommand) {
	if c.cmdQueue == nil {
		c.cmdQueue = make([]*redis.RespCommand, 0)
	}
	c.cmdQueue = append(c.cmdQueue, command)
}

func (c *Connection) GetQueuedCommands() []*redis.RespCommand {
	return c.cmdQueue
}

func (c *Connection) AddWatching(key string, version int64) {
	if c.watching == nil {
		c.watching = make(map[string]int64)
	}
	c.watching[key] = version
}

func (c *Connection) UnWatch() {
	if c.watching != nil {
		c.watching = nil
	}
}

func (c *Connection) GetWatching() map[string]int64 {
	return c.watching
}

func (c *Connection) Active() bool {
	return c.active.Load().(bool)
}

func (c *Connection) RemoteAddr() string {
	return c.Conn.RemoteAddr().String()
}
