package conn

import (
	"redigo/interface/redis"
	"redigo/redis/protocol"
)

// FakeConnection 虚假连接，用于接收本地的异步返回结果
type FakeConnection struct {
	Replies  chan *protocol.Reply
	RealConn redis.Connection
}

func NewFakeConnection(conn redis.Connection) *FakeConnection {
	return &FakeConnection{Replies: make(chan *protocol.Reply), RealConn: conn}
}

func (f *FakeConnection) ReadLoop() error {
	panic("method not allowed")
}

func (f *FakeConnection) WriteLoop() error {
	panic("method not allowed")
}

func (f *FakeConnection) Close() {
	panic("method not allowed")
}

func (f *FakeConnection) SendReply(reply *protocol.Reply) {
	f.Replies <- reply
}

func (f *FakeConnection) SelectDB(index int) {
	panic("method not allowed")
}

func (f *FakeConnection) DBIndex() int {
	return f.RealConn.DBIndex()
}

func (f *FakeConnection) SetMulti(b bool) {
	panic("method not allowed")
}

func (f *FakeConnection) IsMulti() bool {
	return false
}

func (f *FakeConnection) EnqueueCommand(command redis.Command) {
	panic("method not allowed")
}

func (f *FakeConnection) GetQueuedCommands() []redis.Command {
	panic("method not allowed")
}

func (f *FakeConnection) AddWatching(key string, version int64) {

	panic("method not allowed")
}

func (f *FakeConnection) GetWatching() map[string]int64 {

	panic("method not allowed")
}

func (f *FakeConnection) UnWatch() {
	panic("method not allowed")
}

func (f *FakeConnection) Active() bool {
	return true
}

func (f *FakeConnection) RemoteAddr() string {
	//TODO implement me
	panic("implement me")
}
