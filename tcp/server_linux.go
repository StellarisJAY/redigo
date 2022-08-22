//go:build linux
// +build linux

package tcp

import (
	"bufio"
	"log"
	"os"
	"os/signal"
	"redigo/interface/database"
	"redigo/redis"
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
		log.Println("Shutting down RediGO server...")
		// close database
		es.db.Close()
	}()

	go func() {
		for {
			err := es.em.accept()
			if err != nil {
				log.Println("accept error: ", err)
				close(es.closeChan)
			}
		}
	}()

	go func() {
		err := es.em.Handle()
		if err != nil {
			log.Println("epoll handler error: ", err)
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
	reader := bufio.NewReader(conn)
	command, err := redis.Decode(reader)
	if err != nil {
		return err
	}
	command.BindConnection(conn)
	reply := es.db.Execute(command)
	if reply != nil {
		conn.SendCommand(reply)
	}
	return nil
}
