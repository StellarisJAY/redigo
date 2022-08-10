package cmd

import (
	"redigo/interface/redis"
	"strconv"
	"strings"
)

const CRLF = "\r\n"

type Command struct {
	parts       [][]byte
	conn        redis.Connection
	commandType byte
}

func NewEmptyCommand() *Command {
	return &Command{}
}

func NewCommand(parts [][]byte) *Command {
	return &Command{parts: parts, commandType: redis.CommandTypeArray}
}

func NewBulkStringCommand(bulk []byte) *Command {
	return &Command{parts: [][]byte{bulk}, commandType: redis.CommandTypeBulk}
}

func NewErrorCommand(message []byte) *Command {
	return &Command{
		parts:       [][]byte{message},
		commandType: redis.CommandTypeError,
	}
}

func NewNumberCommand(number []byte) *Command {
	return &Command{
		parts:       [][]byte{number},
		commandType: redis.CommandTypeNumber,
	}
}

func NewSingleLineCommand(message []byte) *Command {
	return &Command{
		parts:       [][]byte{message},
		commandType: redis.CommandTypeSingleLine,
	}
}

func NewNilCommand() *Command {
	return &Command{
		commandType: redis.ReplyTypeNil,
	}
}

func NewEmptyListCommand() *Command {
	return &Command{
		commandType: redis.ReplyEmptyList,
	}
}

func (c *Command) ToBytes() []byte {
	parts := c.Parts()
	if len(parts) == 1 {
		return createSingleStringReply(string(parts[0]))
	} else {
		return createBulkStringArrayReply(parts)
	}
}

func (c *Command) Append(part []byte) {
	c.parts = append(c.parts, part)
}

func (c *Command) Get(idx int) string {
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

func (c *Command) Type() byte {
	return c.commandType
}

func createSingleStringReply(value string) []byte {
	return []byte("+" + value + CRLF)
}

func createBulkStringArrayReply(array [][]byte) []byte {
	builder := strings.Builder{}
	builder.WriteString("*" + strconv.Itoa(len(array)) + CRLF)
	for _, bulk := range array {
		builder.WriteString("$" + strconv.Itoa(len(bulk)) + CRLF + string(bulk) + CRLF)
	}
	return []byte(builder.String())
}
