//go:build !linux

package tcp

import (
	"context"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"redigo/interface/database"
	"redigo/util/log"
	"sync"
	"syscall"
)

type GoNetServer struct {
	address     string
	activeConns sync.Map
	listener    net.Listener
	closeChan   chan struct{}
	db          database.DB
}

func NewServer(address string, db database.DB) *GoNetServer {
	return &GoNetServer{
		address:     address,
		activeConns: sync.Map{},
		listener:    nil,
		db:          db,
	}
}

func (s *GoNetServer) Start() error {
	listener, err := net.Listen("tcp", s.address)
	if err != nil {
		return err
	}
	s.listener = listener
	ctx, cancel := context.WithCancel(context.Background())
	// start database execution loop
	go func() {
		execErr := s.db.ExecuteLoop()
		if execErr != nil {
			panic(err)
		}
	}()

	go func() {
		// wait for close signal
		<-ctx.Done()
		log.Info("Shutting down RediGO server...")
		_ = s.listener.Close()
		// close database
		s.db.Close()
	}()
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

	// pprof here
	go func() {
		_ = http.ListenAndServe(":8899", nil)
	}()

	// run acceptor
	err = s.acceptLoop(ctx)
	if err != nil {
		cancel()
	}
	return nil
}

/*
TCP GoNetServer acceptor
Accept conns in a loop, make connections and create read/write loop for each connection
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
		// create connection struct
		connection := NewConnection(conn, s.db)
		// Store active conn
		s.activeConns.Store(connection, 1)
		// start read loop
		go func(connect *Connection) {
			rErr := connect.ReadLoop()
			if rErr != nil {
				connect.Close()
				s.db.OnConnectionClosed(connect)
			}
			s.activeConns.Delete(connect)
		}(connection)
	}
}
