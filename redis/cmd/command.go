package cmd

import (
	"redigo/interface/redis"
	"strings"
)

type Command struct {
	parts [][]byte
	conn  redis.Connection
}

func NewEmptyCommand() *Command {
	return &Command{}
}

func NewCommand(parts [][]byte) *Command {
	return &Command{parts: parts}
}

func NewBulkStringCommand(bulk []byte) *Command {
	return &Command{parts: [][]byte{bulk}}
}

func (c *Command) Append(part []byte) {
	c.parts = append(c.parts, part)
}

func (c Command) Get(idx int) string {
	return string(c.parts[idx])
}

func (c *Command) Len() int {
	return len(c.parts)
}

func (c *Command) Args() [][]byte {
	return c.parts[1:]
}

func (c *Command) BindConnection(conn redis.Connection) {
	c.conn = conn
}

func (c *Command) Connection() redis.Connection {
	return c.conn
}

func (c *Command) Name() string {
	return strings.ToLower(string(c.parts[0]))
}

func (c *Command) Parts() [][]byte {
	return c.parts
}

func (c *Command) SetParts(parts [][]byte) {
	c.parts = parts
}
