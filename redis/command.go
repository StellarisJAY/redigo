package redis

import (
	"strconv"
	"strings"
)

// Command 类型常量
const (
	CommandTypeSingleLine byte = iota // 单行字符串
	CommandTypeBulk                   // 多行字符串
	CommandTypeArray                  // 字符串数组
	CommandTypeNumber                 // 整数
	CommandTypeError                  // 错误
	ReplyTypeNil                      // 空返回，Nil
	ReplyEmptyList                    // 空列表返回
)

const (
	SingleLinePrefix = '+'
	BulkPrefix       = '$'
	ArrayPrefix      = '*'
	ErrorPrefix      = '-'
	NumberPrefix     = ':'
	ClusterPrefix    = '!'
)

// Command Redis命令接口
type Command interface {
	Append(part []byte)
	Len() int
	//Args 命令的参数，第一个参数为 key
	Args() [][]byte
	BindConnection(conn Connection)
	// Connection 获取发送命令的客户端连接
	Connection() Connection
	// Name 命令名称
	Name() string
	Parts() [][]byte

	ToBytes() []byte
	Type() byte
	// IsFromCluster 命令是否来自集群节点
	IsFromCluster() bool
	SetFromCluster(bool)
}

type RespCommand struct {
	parts       [][]byte
	conn        Connection
	commandType byte
	fromCluster bool
	err         error
	number      int
	nested      bool
}

var (
	OKCommand        = NewSingleLineCommand([]byte("OK"))
	NilCommand       = &RespCommand{commandType: ReplyTypeNil}
	EmptyListCommand = &RespCommand{commandType: ReplyEmptyList}
	QueuedCommand    = NewSingleLineCommand([]byte("QUEUED"))
)

func NewEmptyCommand() *RespCommand {
	return &RespCommand{}
}

func NewCommand(parts [][]byte) *RespCommand {
	return &RespCommand{parts: parts, commandType: CommandTypeArray}
}

func NewBulkStringCommand(bulk []byte) *RespCommand {
	return &RespCommand{parts: [][]byte{bulk}, commandType: CommandTypeBulk}
}

func NewNumberCommand(number int) *RespCommand {
	return &RespCommand{
		number:      number,
		commandType: CommandTypeNumber,
	}
}

func NewSingleLineCommand(message []byte) *RespCommand {
	return &RespCommand{
		parts:       [][]byte{message},
		commandType: CommandTypeSingleLine,
	}
}

func NewNilCommand() *RespCommand {
	return &RespCommand{
		commandType: ReplyTypeNil,
	}
}

func NewEmptyListCommand() *RespCommand {
	return &RespCommand{
		commandType: ReplyEmptyList,
	}
}

func NewErrorCommand(err error) *RespCommand {
	return &RespCommand{
		err:         err,
		commandType: CommandTypeError,
	}
}

func NewStringArrayCommand(array []string) *RespCommand {
	res := make([][]byte, len(array))
	for i, a := range array {
		res[i] = []byte(a)
	}
	return &RespCommand{commandType: CommandTypeArray, parts: res}
}

func NewArrayCommand(array [][]byte) *RespCommand {
	return &RespCommand{commandType: CommandTypeArray, nested: false, parts: array}
}

func NewNestedArrayCommand(array [][]byte) *RespCommand {
	return &RespCommand{commandType: CommandTypeArray, nested: true, parts: array}
}

func (c *RespCommand) ToBytes() []byte {
	parts := c.Parts()
	if len(parts) == 1 {
		return createSingleStringReply(string(parts[0]), c.fromCluster)
	} else {
		return createBulkStringArrayReply(parts, c.fromCluster)
	}
}

func (c *RespCommand) Append(part []byte) {
	c.parts = append(c.parts, part)
}

func (c *RespCommand) Get(idx int) string {
	return string(c.parts[idx])
}

func (c *RespCommand) Len() int {
	return len(c.parts)
}

func (c *RespCommand) Args() [][]byte {
	return c.parts[1:]
}

func (c *RespCommand) BindConnection(conn Connection) {
	c.conn = conn
}

func (c *RespCommand) Connection() Connection {
	return c.conn
}

func (c *RespCommand) Name() string {
	return strings.ToLower(string(c.parts[0]))
}

func (c *RespCommand) Parts() [][]byte {
	return c.parts
}

func (c *RespCommand) SetParts(parts [][]byte) {
	c.parts = parts
}

func (c *RespCommand) Type() byte {
	return c.commandType
}

func (c *RespCommand) IsFromCluster() bool {
	return c.fromCluster
}

func (c *RespCommand) SetFromCluster(b bool) {
	c.fromCluster = b
}

func createSingleStringReply(value string, fromCluster bool) []byte {
	if fromCluster {
		return []byte("!+" + value + CRLF)
	}
	return []byte("+" + value + CRLF)
}

func createBulkStringArrayReply(array [][]byte, fromCluster bool) []byte {
	builder := strings.Builder{}
	if fromCluster {
		builder.WriteString("!")
	}
	builder.WriteString("*" + strconv.Itoa(len(array)) + CRLF)
	for _, bulk := range array {
		builder.WriteString("$" + strconv.Itoa(len(bulk)) + CRLF + string(bulk) + CRLF)
	}
	return []byte(builder.String())
}
