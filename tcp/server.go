package tcp

import (
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"redigo/interface/database"
	"sync"
	"syscall"
)

type Handler interface {
	Handle(string) error
}

type Server struct {
	address        string
	activeConns    sync.Map
	commandHandler *Handler
	listener       net.Listener
	closeChan      chan struct{}
	db             database.DB
}

func NewServer(address string, db database.DB) *Server {
	return &Server{
		address:        address,
		activeConns:    sync.Map{},
		commandHandler: nil,
		listener:       nil,
		closeChan:      make(chan struct{}),
		db:             db,
	}
}

func (s *Server) Start() error {
	listener, err := net.Listen("tcp", s.address)
	if err != nil {
		return err
	}
	s.listener = listener

	// start database execution loop
	go func() {
		execErr := s.db.ExecuteLoop()
		if execErr != nil {
			panic(err)
		}
	}()

	go func() {
		// wait for close signal
		<-s.closeChan
		log.Println("Shutting down RediGO server...")
		// close database
		s.db.Close()
		_ = s.listener.Close()
	}()
	// Read sys calls
	sigCh := make(chan os.Signal)
	signal.Notify(sigCh, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		sig := <-sigCh
		switch sig {
		case syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT:
			// send close signal
			s.closeChan <- struct{}{}
		}
	}()

	// pprof here
	go func() {
		_ = http.ListenAndServe(":8899", nil)
	}()

	log.Println("Redigo Server Started, listen:", listener.Addr())
	// run acceptor
	err = s.acceptLoop()
	if err != nil {
		// signal close server
		s.closeChan <- struct{}{}
	}
	return nil
}

/*
	TCP Server acceptor
	Accept conns in a loop, make connections and create read/write loop for each connection
*/
func (s *Server) acceptLoop() error {
	for {
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
				//log.Println("Connection closed by remote client: ", connect.Conn.RemoteAddr().String())
			}
			s.activeConns.Delete(connect)
		}(connection)

		// start write loop
		go func(connect *Connection) {
			wErr := connect.WriteLoop()
			if wErr != nil {
				connect.Close()
				s.db.OnConnectionClosed(connect)
				//log.Println("Connection closed by remote client: ", connect.Conn.RemoteAddr().String())
			}
			s.activeConns.Delete(connect)
		}(connection)
	}
}
