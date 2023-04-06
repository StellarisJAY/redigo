//go:build !linux

package tcp

import (
	"context"
	"fmt"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"redigo/interface/database"
	"sync"
	"syscall"
)

// GoNetServer 非Linux系统下使用go原生net实现的TCP服务器
type GoNetServer struct {
	address     string
	activeConns sync.Map
	listener    net.Listener
	db          database.DB
	cancel context.CancelFunc
}

func NewServer(address string, db database.DB) *GoNetServer {
	return &GoNetServer{
		address:     address,
		activeConns: sync.Map{},
		db:          db,
	}
}

func (s *GoNetServer) Start() error {
	listener, err := net.Listen("tcp", s.address)
	if err != nil {
		return fmt.Errorf("create listener error: %w", err)
	}
	s.listener = listener
	ctx, cancel := context.WithCancel(context.Background())

	// 启动database和server
	go s.commandExecutor()
	go s.acceptor(ctx)
	go s.gracefullyShutdown(ctx)
	// pprof
	go http.ListenAndServe(":8899", nil)

	// listen close signal
	sigCh := make(chan os.Signal)
	signal.Notify(sigCh, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		sig := <-sigCh
		switch sig {
		case syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT:
			cancel()
		}
	}()
	<-ctx.Done()
	return nil
}

/*
TCP GoNetServer acceptor
*/
func (s *GoNetServer) acceptLoop(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			_ = s.listener.Close()
			return nil
		default:
		}
		conn, err := s.listener.Accept()
		if err != nil {
			return nil
		}
		// 创建链接并绑定数据库
		connection := NewConnection(conn, s.db)
		s.activeConns.Store(connection, 1)
		// 开启 ReadLoop
		go func(connect *Connection) {
			rErr := connect.ReadLoop()
			if rErr != nil {
				connect.Close()
				// 连接关闭回调，数据库在连接关闭时的特殊处理，比如删除连接的订阅
				s.db.OnConnectionClosed(connect)
			}
			s.activeConns.Delete(connect)
		}(connection)

		// start write loop
		go func(connect *Connection) {
			wErr := connect.WriteLoop()
			if wErr != nil {
				connect.Close()
				s.db.OnConnectionClosed(connect)
			}
			s.activeConns.Delete(connect)
		}(connection)
	}
}

func (s *GoNetServer) commandExecutor(ctx context.Context) {
	execErr := s.db.ExecuteLoop()
	if execErr != nil {
		// database 正常情况下不会返回错误
		panic(err)
	}
}

func (s *GoNetServer) gracefullyShutdown(ctx context.Context) {
	<-ctx.Done()
	s.shutdown()
}

func (s *GoNetServer) acceptor(ctx context.Context) {
	err = s.acceptLoop(ctx)
	if err != nil {
		cancel()
	}
}

func (s *GoNetServer) Close() {
	log.Info("Shutting down RediGO server...")
	_ = s.listener.Close()
	// close database
	s.db.Close()
}
