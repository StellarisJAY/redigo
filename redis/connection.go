package redis

type Connection interface {
	ReadLoop() error
	WriteLoop() error
	Close()
	SendCommand(command *RespCommand)

	SelectDB(index int)
	DBIndex() int

	SetMulti(bool)
	IsMulti() bool
	EnqueueCommand(command *RespCommand)
	GetQueuedCommands() []*RespCommand
	AddWatching(key string, version int64)
	GetWatching() map[string]int64
	UnWatch()

	Active() bool
	RemoteAddr() string
}
