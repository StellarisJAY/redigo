package tcp

import (
	"bytes"
	"redigo/pkg/redis"
	"redigo/pkg/util/buffer"
	"sync"
	"sync/atomic"
	"syscall"
)

type EpollConnection struct {
	fd           int
	selectedDB   int
	multi        bool
	watching     map[string]int64
	cmdQueue     []*redis.RespCommand
	epollManager *EpollEventLoop

	wMutex      *sync.Mutex
	readBuffer  buffer.Buffer
	writeBuffer *bytes.Buffer
	active      uint32
}

func NewEpollConnection(fd int, epollManager *EpollEventLoop) *EpollConnection {
	return &EpollConnection{
		fd:           fd,
		selectedDB:   0,
		multi:        false,
		watching:     make(map[string]int64),
		cmdQueue:     make([]*redis.RespCommand, 0),
		epollManager: epollManager,
		writeBuffer:  &bytes.Buffer{},
		readBuffer:   buffer.NewRingBuffer(1024),
		active:       1,
		wMutex:       &sync.Mutex{},
	}
}

func (c *EpollConnection) Read(payload []byte) (int, error) {
	return syscall.Read(c.fd, payload)
}

func (c *EpollConnection) Write(payload []byte) (int, error) {
	return syscall.Write(c.fd, payload)
}

func (c *EpollConnection) ReadBuffered() (int, error) {
	buf := bytesPool.Get().([]byte)
	defer bytesPool.Put(buf)
	n, err := syscall.Read(c.fd, buf)
	if err != nil {
		return 0, err
	}
	return c.readBuffer.Write(buf[:n])
}

func (c *EpollConnection) ReadLoop() error {
	panic("read loop not available in epoll")
}

func (c *EpollConnection) WriteLoop() error {
	panic("write loop not available in epoll")
}

func (c *EpollConnection) Close() {
	_ = c.epollManager.CloseConn(c)
}

func (c *EpollConnection) SendCommand(command *redis.RespCommand) {
	payload := redis.Encode(command)
	c.wMutex.Lock()
	defer c.wMutex.Unlock()
	if c.writeBuffer.Len() > 0 {
		c.writeBuffer.Write(payload)
		return
	}
	n, err := c.Write(payload)
	// 如果write缓冲区满会返回EAGAIN，此时需要等待EPOLLOUT
	if err != nil && err == syscall.EAGAIN {
		// 把没有写完的部分写入缓冲区
		c.writeBuffer.Write(payload[n:])
	}
}

func (c *EpollConnection) SelectDB(index int) {
	c.selectedDB = index
}

func (c *EpollConnection) DBIndex() int {
	return c.selectedDB
}

func (c *EpollConnection) SetMulti(b bool) {
	c.multi = b
}

func (c *EpollConnection) IsMulti() bool {
	return c.multi
}

func (c *EpollConnection) EnqueueCommand(command *redis.RespCommand) {
	c.cmdQueue = append(c.cmdQueue, command)
}

func (c *EpollConnection) GetQueuedCommands() []*redis.RespCommand {
	return c.cmdQueue
}

func (c *EpollConnection) AddWatching(key string, version int64) {
	c.watching[key] = version
}

func (c *EpollConnection) GetWatching() map[string]int64 {
	return c.watching
}

func (c *EpollConnection) UnWatch() {
	panic("operation not available")
}

func (c *EpollConnection) Active() bool {
	return atomic.LoadUint32(&c.active) == 1
}

func (c *EpollConnection) RemoteAddr() string {
	panic("operation not available")
}
