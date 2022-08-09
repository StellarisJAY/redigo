package redis

const (
	CommandTypeSingleLine byte = iota
	CommandTypeBulk
	CommandTypeArray
	CommandTypeNumber
	CommandTypeError
)

const (
	SingleLinePrefix = '+'
	BulkPrefix       = '$'
	ArrayPrefix      = '*'
	ErrorPrefix      = '-'
	NumberPrefix     = ':'
)

type Command interface {
	Append(part []byte)
	Len() int
	Args() [][]byte
	BindConnection(conn Connection)
	Connection() Connection
	Name() string
	Parts() [][]byte

	ToBytes() []byte
	Type() byte
}
