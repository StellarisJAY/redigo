package redis

const (
	CommandTypeSingleLine byte = iota
	CommandTypeBulk
	CommandTypeArray
	CommandTypeNumber
	CommandTypeError
	ReplyTypeNil
	ReplyEmptyList
)

const (
	SingleLinePrefix = '+'
	BulkPrefix       = '$'
	ArrayPrefix      = '*'
	ErrorPrefix      = '-'
	NumberPrefix     = ':'
	ClusterPrefix    = '!'
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
	IsFromCluster() bool
	SetFromCluster(bool)
}
