package tcp

import (
	"bufio"
	"context"
	"io"
	"net"
	"redigo/interface/database"
	"redigo/interface/redis"
	"redigo/redis/parser"
	"redigo/redis/protocol"
)

type Connection struct {
	Conn       net.Conn
	ReplyChan  chan *protocol.Reply
	db         database.DB
	ctx        context.Context
	cancel     context.CancelFunc
	selectedDB int
	multi      bool
	watching   map[string]int64
	cmdQueue   []redis.Command
}

func NewConnection(conn net.Conn, db database.DB) *Connection {
	ctx, cancel := context.WithCancel(context.Background())
	return &Connection{
		Conn:       conn,
		ReplyChan:  make(chan *protocol.Reply, 1024),
		db:         db,
		cancel:     cancel,
		ctx:        ctx,
		selectedDB: 0,
		multi:      false,
	}
}

/*
	readLoop for a connection
	Continuously read data from connection and dispatch command to handler
*/
func (c *Connection) ReadLoop() error {
	reader := bufio.NewReader(c.Conn)
	for {
		//parse RESP
		cmd, err := parser.Parse(reader)
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
			_, err := c.Conn.Write(reply.ToBytes())
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
	_ = c.Conn.Close()
	c.ReplyChan = nil
	c.cancel()
}

func (c *Connection) SendReply(reply *protocol.Reply) {
	c.ReplyChan <- reply
}

func (c *Connection) SelectDB(index int) {
	c.selectedDB = index
}

func (c *Connection) DBIndex() int {
	return c.selectedDB
}

func (c *Connection) SetMulti(multi bool) {
	c.multi = multi
}

func (c *Connection) IsMulti() bool {
	return c.multi
}

func (c *Connection) EnqueueCommand(command redis.Command) {
	if c.cmdQueue == nil {
		c.cmdQueue = make([]redis.Command, 0)
	}
	c.cmdQueue = append(c.cmdQueue, command)
}

func (c *Connection) GetQueuedCommands() []redis.Command {
	return c.cmdQueue
}

func (c *Connection) AddWatching(key string, version int64) {
	if c.watching == nil {
		c.watching = make(map[string]int64)
	}
	c.watching[key] = version
}

func (c *Connection) GetWatching() map[string]int64 {
	return c.watching
}
