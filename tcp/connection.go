package tcp

import (
	"bufio"
	"io"
	"log"
	"net"
	"redigo/interface/database"
	"redigo/redis/parser"
	"redigo/redis/protocol"
)

type Connection struct {
	Conn      net.Conn
	Id        string
	WriteChan chan []byte
	db        database.DB
}

func NewConnection(conn net.Conn, id string, db database.DB) *Connection {
	return &Connection{
		Conn:      conn,
		Id:        id,
		WriteChan: make(chan []byte, 102400),
		db:        db,
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
				log.Println("Connection closed by remote client: ", c.Conn.RemoteAddr().String())
				return err
			}
			c.WriteChan <- protocol.ProtocolError
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
		case payload := <-c.WriteChan:
			_, err := c.Conn.Write(payload)
			if err != nil {
				return err
			}
		}
	}
}

/*
	close a connection
*/
func (c *Connection) Close() {
	_ = c.Conn.Close()
}

func (c *Connection) Write(data []byte) {
	c.WriteChan <- data
}
