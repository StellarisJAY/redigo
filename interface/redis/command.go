package redis

type Command interface {
	Append(part []byte)
	Len() int
	Args() [][]byte
	BindConnection(conn Connection)
	Connection() Connection
	Name() string
	Parts() [][]byte
}
