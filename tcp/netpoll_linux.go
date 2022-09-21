//go:build linux
// +build linux

package tcp

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"golang.org/x/sys/unix"
	"io"
	"net"
	"redigo/redis"
	"redigo/util/log"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"unsafe"
)

const (
	EpollRead     = syscall.EPOLLIN | syscall.EPOLLPRI | syscall.EPOLLERR | syscall.EPOLLHUP | syscall.EPOLLRDHUP
	EpollClose    = syscall.EPOLLIN | syscall.EPOLLHUP
	EpollWritable = syscall.EPOLLOUT
)

type EpollManager struct {
	conns       *sync.Map
	sockFd      int
	epollFd     int
	onReadEvent func(conn *EpollConnection) error
	waitMsec    int
}

type EpollConnection struct {
	fd           int
	selectedDB   int
	multi        bool
	watching     map[string]int64
	cmdQueue     []*redis.RespCommand
	epollManager *EpollManager

	readBuffer  *bytes.Buffer
	writeBuffer *bytes.Buffer
	active      uint32
}

func NewEpoll() *EpollManager {
	return &EpollManager{conns: &sync.Map{}}
}

func NewEpollConnection(fd int, epollManager *EpollManager) *EpollConnection {
	return &EpollConnection{
		fd:           fd,
		selectedDB:   0,
		multi:        false,
		watching:     make(map[string]int64),
		cmdQueue:     make([]*redis.RespCommand, 0),
		epollManager: epollManager,
		writeBuffer:  &bytes.Buffer{},
		readBuffer:   &bytes.Buffer{},
		active:       1,
	}
}

func (e *EpollManager) Listen(address string) error {
	parts := strings.Split(address, ":")
	var sockPort int
	if len(parts) != 2 {
		return errors.New("invalid address")
	}
	if port, err := strconv.Atoi(parts[1]); err != nil {
		return errors.New("invalid address")
	} else {
		sockPort = port
	}
	var ipAddr [4]byte
	copy(ipAddr[:], net.ParseIP(parts[0]).To4())
	// 创建 TCP Socket
	fd, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_STREAM, syscall.IPPROTO_TCP)
	if err != nil {
		return err
	}
	// Socket Bind 地址
	err = syscall.Bind(fd, &syscall.SockaddrInet4{Addr: ipAddr, Port: sockPort})
	if err != nil {
		return err
	}

	err = syscall.Listen(fd, 10)
	if err != nil {
		return err
	}

	epollFd, err := syscall.EpollCreate1(0)
	if err != nil {
		return err
	}
	e.sockFd = fd
	e.epollFd = epollFd
	return nil
}

func (e *EpollManager) Accept() error {
	// Accept连接，获得连接的fd，暂时忽略远程地址
	nfd, _, err := syscall.Accept(e.sockFd)
	if err != nil {
		return err
	}
	// 将连接设置为非阻塞模式
	if err = syscall.SetNonblock(nfd, true); err != nil {
		return err
	}
	e.conns.Store(nfd, NewEpollConnection(nfd, e))
	// 使用EpollCtl控制连接FD，Epoll订阅Read和Write事件
	err = syscall.EpollCtl(e.epollFd, syscall.EPOLL_CTL_ADD, nfd, &syscall.EpollEvent{
		Events: EpollRead | EpollWritable,
		Fd:     int32(nfd),
	})
	if err != nil {
		e.conns.Delete(nfd)
		return err
	}
	return nil
}

// CloseConn 连接关闭事件处理
func (e *EpollManager) CloseConn(conn *EpollConnection) error {
	// set conn inactive
	atomic.StoreUint32(&conn.active, 0)
	// epoll ctrl del
	err := syscall.EpollCtl(e.epollFd, syscall.EPOLL_CTL_DEL, conn.fd, nil)
	if err != nil {
		return err
	}
	e.conns.Delete(conn.fd)
	// close connection
	return syscall.Close(conn.fd)
}

// Handle Epoll 事件循环
func (e *EpollManager) Handle(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}
		events := make([]syscall.EpollEvent, 1024)
		n, err := EpollWait(e.epollFd, events, e.waitMsec)
		if err != nil {
			if err.Error() == "interrupted system call" {
				continue
			}
			return fmt.Errorf("epoll wait error: %v", err)
		}
		// 没有事件，进入阻塞模式
		if n <= 0 {
			e.waitMsec = -1
			runtime.Gosched()
			continue
		}
		// 有事件，继续无阻塞循环
		e.waitMsec = 0
		for i := 0; i < n; i++ {
			// 通过fd查询到一个连接对象
			v, ok := e.conns.Load(int(events[i].Fd))
			if !ok {
				log.Errorf("unknown connection fd: %d", events[i].Fd)
				continue
			}
			conn := v.(*EpollConnection)
			// epoll关闭事件
			if events[i].Events == uint32(EpollClose) {
				if err := e.CloseConn(conn); err != nil {
					return fmt.Errorf("close conn error: %v", err)
				}
				continue
			}
			if events[i].Events&syscall.EPOLLIN == syscall.EPOLLIN {
				err := e.onReadEvent(conn)
				if err != nil {
					if !errors.Is(err, io.EOF) {
						log.Errorf("read error: %v", err)
						_ = e.CloseConn(conn)
					}
				}
			}
			if events[i].Events&EpollWritable == EpollWritable {
				// 批量写入数据，减少系统调用次数
				if n := conn.writeBuffer.Len(); n > 0 {
					payload := conn.writeBuffer.Next(n)
					_, err := conn.Write(payload)
					if err != nil {
						log.Errorf("write error: %v", err)
						_ = e.CloseConn(conn)
						break
					}
				}
			}
		}
	}
}

// EpollWait 封装EpollWait系统调用，使用RawSyscall来避免runtime.
func EpollWait(epfd int, events []syscall.EpollEvent, msec int) (n int, err error) {
	var r0 uintptr
	var _p0 = unsafe.Pointer(&events[0])
	if msec == 0 {
		r0, _, err = syscall.RawSyscall6(syscall.SYS_EPOLL_WAIT, uintptr(epfd), uintptr(_p0), uintptr(len(events)), 0, 0, 0)
	} else {
		r0, _, err = syscall.Syscall6(syscall.SYS_EPOLL_WAIT, uintptr(epfd), uintptr(_p0), uintptr(len(events)), uintptr(msec), 0, 0)
	}
	if err == syscall.Errno(0) {
		err = nil
	}
	return int(r0), err
}

func (e *EpollManager) Close() {

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
	// 写入writeBuffer，等待批量写入 连接
	c.writeBuffer.Write(payload)
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
