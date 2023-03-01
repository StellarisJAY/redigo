//go:build linux

package tcp

import (
	"context"
	"fmt"
	"io"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"redigo/interface/database"
	"redigo/redis"
	"redigo/util/log"
	"syscall"
)

type EpollServer struct {
	em      *EpollManager
	address string
	db      database.DB
}

func NewServer(address string, db database.DB) *EpollServer {
	s := &EpollServer{
		address: address,
		db:      db,
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
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		err := es.db.ExecuteLoop()
		if err != nil {
			// database正常情况不会返回error
			panic(err)
		}
	}()

	go func() {
		// wait for close signal
		<-ctx.Done()
		log.Info("Shutting down RediGO server...")
		// close database
		es.db.Close()
	}()

	// pprof
	go func() {
		_ = http.ListenAndServe(":8899", nil)
	}()

	go func() {
		log.Info("server started, Ready to accept connections...")
		for {
			select {
			case <-ctx.Done():
				break
			default:
			}
			err := es.em.Accept()
			if err != nil {
				log.Errorf("accept error: %v", err)
				cancel()
			}
		}
	}()

	go func() {
		err := es.em.Handle(ctx)
		if err != nil {
			log.Errorf("epoll handler error: %v", err)
		}
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
	<-ctx.Done()
	return nil
}

func (es *EpollServer) onReadEvent(conn *EpollConnection) error {
	// 尽可能一次读取所有可读数据，减少Read系统调用
	for {
		// socket无数据可读
		if _, err := conn.ReadBuffered(); err == syscall.EAGAIN {
			break
		}
		// 将buffer中的数据全部decode，并提交到DB处理
		for {
			conn.readBuffer.MarkReadIndex()
			command, err := redis.Decode(conn.readBuffer)
			if err != nil && err != io.EOF {
				return fmt.Errorf("decode error: %w", err)
			}
			// buffer中数据不完整
			if err == io.EOF || command == nil {
				conn.readBuffer.ResetReadIndex()
				return nil
			}
			command.BindConnection(conn)
			es.db.SubmitCommand(command)
		}
	}
	return nil
}
