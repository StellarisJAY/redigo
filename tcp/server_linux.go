//go:build linux
// +build linux

package tcp

import (
	"fmt"
	"io"
	"os"
	"os/signal"
	"redigo/interface/database"
	"redigo/redis"
	"redigo/util/log"
	"syscall"
)

type EpollServer struct {
	em        *EpollManager
	address   string
	closeChan chan struct{}
	db        database.DB
}

func NewServer(address string, db database.DB) *EpollServer {
	s := &EpollServer{
		address:   address,
		closeChan: make(chan struct{}),
		db:        db,
	}
	return s
}

func (es *EpollServer) Start() error {
	es.em = NewEpoll()
	es.em.onReadEvent = es.onReadEvent
	err := es.em.Listen(es.address)
	if err != nil {
		return err
	}

	go func() {
		err := es.db.ExecuteLoop()
		if err != nil {
			es.closeChan <- struct{}{}
		}
	}()

	go func() {
		// wait for close signal
		<-es.closeChan
		log.Info("Shutting down RediGO server...")
		// close database
		es.db.Close()
	}()

	go func() {
		for {
			err := es.em.Accept()
			if err != nil {
				log.Errorf("accept error: %v", err)
				close(es.closeChan)
			}
		}
	}()

	go func() {
		err := es.em.Handle()
		if err != nil {
			log.Errorf("epoll handler error: %v", err)
		}
	}()

	// Read sys calls
	sigCh := make(chan os.Signal)
	signal.Notify(sigCh, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		sig := <-sigCh
		switch sig {
		case syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT:
			// send close signal
			close(es.closeChan)
		}
	}()
	select {
	case <-es.closeChan:

	}
	return nil
}

func (es *EpollServer) onReadEvent(conn *EpollConnection) error {
	// 尽可能一次读取所有可读数据，减少Read系统调用
	_, _ = conn.ReadBuffered()
	command, err := redis.Decode(conn.readBuffer)
	if err != nil {
		if err != io.EOF {
			return fmt.Errorf("decode error: %w", err)
		}
	}
	if command == nil {
		return nil
	}
	command.BindConnection(conn)
	reply := es.db.Execute(command)
	if reply != nil {
		conn.SendCommand(reply)
	}
	return nil
}
