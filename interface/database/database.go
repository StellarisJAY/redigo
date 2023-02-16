package database

import (
	"redigo/redis"
	"time"
)

type DB interface {
	// SubmitCommand 提交命令到channel
	SubmitCommand(command redis.Command)
	// Close DB
	Close()
	// ExecuteLoop 命令执行循环，不断从channel获取新命令并执行
	ExecuteLoop() error
	// Execute 执行一个命令，并返回结果
	Execute(command redis.Command) *redis.RespCommand
	// ForEach 遍历 dbIdx 数据库中的所有key，该方法没有线程安全处理
	ForEach(dbIdx int, fun func(key string, entry *Entry, expire *time.Time) bool)
	Len(dbIdx int) int
	// OnConnectionClosed 连接中断callback，主要用在pub/sub
	OnConnectionClosed(conn redis.Connection)
}

/*
Entry key-value数据库entry，包括了key、value属性
*/
type Entry struct {
	Key      string
	Data     interface{}
	DataSize int64 // DataSize 数据大小
	Accessed bool  // 最近是否被访问过
}
