package tcp

import (
	"bufio"
	"net"
	"redigo/interface/database"
	"redigo/redis"
	"sync/atomic"
)

type Connection struct {
	conn       net.Conn
	db         database.DB
	selectedDB int
	multi      bool
	watching   map[string]int64
	cmdQueue   []*redis.RespCommand
	active     int32
}

func NewConnection(conn net.Conn, db database.DB) *Connection {
	connect := &Connection{
		conn:       conn,
		db:         db,
		selectedDB: 0,
		multi:      false,
		active:     1,
	}
	return connect
}

/*
ReadLoop
read from a connection
Continuously read data from connection and dispatch command to handler
*/
func (c *Connection) ReadLoop() error {
	reader := bufio.NewReader(c.conn)
	for {
		//parse RESP
		cmd, err := redis.Decode(reader)
		if err != nil {
			return err
		} else {
			cmd.BindConnection(c)
			c.db.SubmitCommand(cmd)
		}
	}
}

/*
Close connection
*/
func (c *Connection) Close() {
	atomic.StoreInt32(&c.active, 0)
	_ = c.conn.Close()
}

func (c *Connection) SendCommand(command *redis.RespCommand) {
	encode := redis.Encode(command)
	_, _ = c.conn.Write(encode)
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
	return atomic.LoadInt32(&c.active) == 1
}

func (c *Connection) RemoteAddr() string {
	return c.conn.RemoteAddr().String()
}
