//go:build linux
// +build linux

package tcp

import (
	"errors"
	"fmt"
	"golang.org/x/sys/unix"
	"io"
	"log"
	"net"
	"redigo/datastruct/list"
	"redigo/redis"
	"strconv"
	"strings"
	"sync"
	"syscall"
)

const (
	EpollRead     = syscall.EPOLLIN | syscall.EPOLLPRI | syscall.EPOLLERR | syscall.EPOLLHUP | unix.EPOLLET | syscall.EPOLLRDHUP
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
	replyQueue   *list.LinkedList
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
		replyQueue:   list.NewLinkedList(),
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
	fd, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_STREAM, syscall.IPPROTO_TCP)
	if err != nil {
		return err
	}
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

func (e *EpollManager) accept() error {
	nfd, _, err := syscall.Accept(e.sockFd)
	if err != nil {
		return err
	}
	if err = syscall.SetNonblock(nfd, true); err != nil {
		return err
	}
	e.conns.Store(nfd, NewEpollConnection(nfd, e))
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

func (e *EpollManager) CloseConn(conn *EpollConnection) error {
	err := syscall.EpollCtl(e.epollFd, syscall.EPOLL_CTL_DEL, conn.fd, nil)
	if err != nil {
		return err
	}
	e.conns.Delete(conn.fd)
	return syscall.Close(conn.fd)
}

func (e *EpollManager) Handle() error {
	for {
		events := make([]syscall.EpollEvent, 1024)
		n, err := syscall.EpollWait(e.epollFd, events, e.waitMsec)
		if err != nil {
			if err.Error() == "interrupted system call" {
				log.Println("interrupted system call")
				continue
			}
			return fmt.Errorf("epoll wait error: %v", err)
		}
		if n <= 0 {
			e.waitMsec = -1
			continue
		}
		e.waitMsec = 0
		for i := 0; i < n; i++ {
			// 通过fd查询到一个连接对象
			v, ok := e.conns.Load(int(events[i].Fd))
			if !ok {
				log.Println("unknown connection fd: ", events[i].Fd)
				continue
			}
			conn := v.(*EpollConnection)
			// epoll关闭事件
			if events[i].Events == uint32(EpollClose) {
				if err := e.CloseConn(conn); err != nil {
					return fmt.Errorf("close conn error: %v", err)
				}
			} else if events[i].Events&syscall.EPOLLIN == syscall.EPOLLIN {
				err := e.onReadEvent(conn)
				if err != nil {
					if !errors.Is(err, io.EOF) {
						log.Println("read error: ", err)
					}
					_ = e.CloseConn(conn)
				}
			}
			if events[i].Events&EpollWritable == EpollWritable {
				for conn.replyQueue.Size() > 0 {
					payload := conn.replyQueue.RemoveLeft()
					_, err := conn.Write(payload)
					if err != nil {
						log.Println("write error: ", err)
						_ = e.CloseConn(conn)
						break
					}
				}
			}
		}
	}
}

func (e *EpollManager) Close() {

}

func (c *EpollConnection) Read(payload []byte) (int, error) {
	return syscall.Read(c.fd, payload)
}

func (c *EpollConnection) Write(payload []byte) (int, error) {
	return syscall.Write(c.fd, payload)
}

func (c *EpollConnection) ReadLoop() error {
	panic("implement me")
}

func (c *EpollConnection) WriteLoop() error {
	panic("implement me")
}

func (c *EpollConnection) Close() {
	_ = c.epollManager.CloseConn(c)
}

func (c *EpollConnection) SendCommand(command *redis.RespCommand) {
	payload := redis.Encode(command)
	c.replyQueue.AddRight(payload)
	//_, _ = c.Write(payload)
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
	panic("operation not available")
}

func (c *EpollConnection) RemoteAddr() string {
	panic("operation not available")
}
