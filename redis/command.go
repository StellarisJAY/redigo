package redis

import "redigo/interface/tcp"

type Command struct {
	Parts [][]byte
	conn  tcp.Connection
}

func NewCommand() *Command {
	return &Command{Parts: make([][]byte, 0)}
}

func (c *Command) Append(part []byte) {
	c.Parts = append(c.Parts, part)
}

func (c *Command) Get(idx int) string {
	return string(c.Parts[idx])
}

func (c *Command) Len() int {
	return len(c.Parts)
}

func (c *Command) Args() [][]byte {
	return c.Parts[1:]
}

func (c *Command) BindConnection(conn tcp.Connection) {
	c.conn = conn
}

func (c *Command) Connection() tcp.Connection {
	return c.conn
}
