package redis

// Connection Redis 客户端连接接口
type Connection interface {
	ReadLoop() error
	Close()
	SendCommand(command *RespCommand)

	// SelectDB 切换当前数据库
	SelectDB(index int)
	DBIndex() int

	// SetMulti 开启事务
	SetMulti(bool)
	IsMulti() bool
	// EnqueueCommand 添加事务命令，事务是基于单个连接实现的，事务的命令队列保存在连接中
	EnqueueCommand(command *RespCommand)
	GetQueuedCommands() []*RespCommand
	// AddWatching 添加事务要监听的 key
	AddWatching(key string, version int64)
	GetWatching() map[string]int64
	UnWatch()

	Active() bool
	RemoteAddr() string
}
