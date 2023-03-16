//go:build linux

package tcp

import (
	"context"
	"errors"
	"fmt"
	"golang.org/x/sys/unix"
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
	EpollRead     = syscall.EPOLLIN | unix.EPOLLET
	EpollClose    = syscall.EPOLLRDHUP
	EpollWritable = syscall.EPOLLOUT
)

type EpollEventLoop struct {
	conns       *sync.Map
	sockFd      int
	epollFd     int
	onReadEvent func(conn *EpollConnection) error
	waitMsec    int
	ioHandlers  []*EpollIOHandler
	nextHandler int
	closeChan   chan struct{}
}

type EpollIOHandler struct {
	tasks   chan IOTask
	manager *EpollEventLoop
}

func NewEpoll() *EpollEventLoop {
	e := &EpollEventLoop{conns: &sync.Map{}}
	e.ioHandlers = make([]*EpollIOHandler, 10)
	e.closeChan = make(chan struct{})
	for i := 0; i < len(e.ioHandlers); i++ {
		e.ioHandlers[i] = &EpollIOHandler{tasks: make(chan IOTask, 1024), manager: e}
		go e.ioHandlers[i].Handle()
	}
	return e
}

func (e *EpollEventLoop) Listen(address string) error {
	ipAddr, sockPort, err := parseIPAddr(address)
	if err != nil {
		e.closeChan <- struct{}{}
		return fmt.Errorf("invalid address format, parse IP Error: %w", err)
	}
	// 创建 TCP Socket
	fd, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_STREAM, syscall.IPPROTO_TCP)
	if err != nil {
		e.closeChan <- struct{}{}
		return err
	}
	// Socket Bind 地址
	if err := syscall.Bind(fd, &syscall.SockaddrInet4{Addr: ipAddr, Port: sockPort}); err != nil {
		e.closeChan <- struct{}{}
		return fmt.Errorf("bind socket error: %w", err)
	}
	// listen
	if err := syscall.Listen(fd, 128); err != nil {
		e.closeChan <- struct{}{}
		return fmt.Errorf("listen fd error: %w", err)
	}

	// epoll create
	if epfd, err := syscall.EpollCreate1(0); err != nil {
		e.closeChan <- struct{}{}
		return fmt.Errorf("epoll create error: %w", err)
	} else {
		e.sockFd = fd
		e.epollFd = epfd
	}
	return nil
}

func (e *EpollEventLoop) Accept() error {
	// Accept连接，获得连接的fd，暂时忽略远程地址
	nfd, _, err := syscall.Accept(e.sockFd)
	if err != nil {
		return fmt.Errorf("accept conn error %w", err)
	}
	// 将连接设置为非阻塞模式
	if err := syscall.SetNonblock(nfd, true); err != nil {
		return fmt.Errorf("set socket non-block error %w", err)
	}
	e.conns.Store(nfd, NewEpollConnection(nfd, e))
	// epoll ctrl，Read、Write、对端Close事件
	if err := epollCtl(e.epollFd, int32(nfd), syscall.EPOLL_CTL_ADD, EpollRead|EpollWritable|EpollClose); err != nil {
		e.conns.Delete(nfd)
		return err
	}
	return nil
}

// CloseConn 连接关闭事件处理
func (e *EpollEventLoop) CloseConn(conn *EpollConnection) error {
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
func (e *EpollEventLoop) Handle(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}
		events := make([]syscall.EpollEvent, 1024)
		n, err := epollWait(e.epollFd, events, e.waitMsec)
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
		for _, event := range events {
			// 通过fd查询到一个连接对象
			v, ok := e.conns.Load(int(event.Fd))
			if !ok {
				log.Errorf("unknown connection fd: %d", event.Fd)
				continue
			}
			conn := v.(*EpollConnection)
			// epoll关闭事件
			if event.Events&EpollClose == uint32(EpollClose) {
				if err := e.CloseConn(conn); err != nil {
					return fmt.Errorf("close conn error: %v", err)
				}
				continue
			}
			// epoll in
			if event.Events&syscall.EPOLLIN == syscall.EPOLLIN {
				e.DispatchIO(conn, Read)
			}
			// epoll out
			if event.Events&EpollWritable == EpollWritable {
				e.DispatchIO(conn, Write)
			}
		}
	}
}

// DispatchIO 主循环将io事件交给某个handler处理，handler通过round-robin策略选择
func (e *EpollEventLoop) DispatchIO(conn redis.Connection, flag byte) {
	if e.nextHandler == len(e.ioHandlers) {
		e.nextHandler = 0
	}
	e.ioHandlers[e.nextHandler].tasks <- IOTask{conn: conn, flag: flag}
	e.nextHandler++
}

func (e *EpollIOHandler) Handle() {
	for {
		select {
		case <-e.manager.closeChan:
			break
		case task := <-e.tasks:
			e.handleConn(task.conn.(*EpollConnection), task.flag)
		}
	}
}

// handleConn 处理一个连接的io事件
func (e *EpollIOHandler) handleConn(conn *EpollConnection, flag byte) {
	switch flag {
	case Read:
		if err := e.manager.onReadEvent(conn); err != nil {
			log.Errorf("read event error: %v", err)
		}
	case Write:
		if n := conn.writeBuffer.Len(); n > 0 {
			if _, err := conn.writeBuffer.WriteTo(conn); err != nil {
				log.Errorf("write error: %v", err)
				_ = e.manager.CloseConn(conn)
			}
		}
	}
}

// epollWait 封装EpollWait系统调用，使用RawSyscall来避免runtime.
func epollWait(epfd int, events []syscall.EpollEvent, msec int) (n int, err error) {
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

func epollCtl(epfd int, fd int32, op int, events uint32) error {
	return syscall.EpollCtl(epfd, op, int(fd), &syscall.EpollEvent{
		Events: events,
		Fd:     fd,
	})
}

func (e *EpollEventLoop) Close() {

}

func parseIPAddr(address string) (ipAddr [4]byte, sockPort int, parseErr error) {
	parts := strings.Split(address, ":")
	if len(parts) != 2 {
		parseErr = errors.New("invalid address")
		return
	}
	if port, err := strconv.Atoi(parts[1]); err != nil {
		parseErr = errors.New("invalid address")
		return
	} else {
		sockPort = port
	}
	copy(ipAddr[:], net.ParseIP(parts[0]).To4())
	return
}
