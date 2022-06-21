package tcp

import (
	"log"
	"net"
	"redigo/interface/database"
	"redigo/interface/tcp"
	"sync"
)

type Handler interface {
	Handle(string) error
}

type Server struct {
	address        string
	activeConns    map[string]tcp.Connection
	commandHandler *Handler
	listener       net.Listener
	closeChan      chan struct{}
	db             database.DB
	mutex          sync.Mutex
}

func NewServer(address string, db database.DB) *Server {
	return &Server{
		address:        address,
		activeConns:    make(map[string]tcp.Connection),
		commandHandler: nil,
		listener:       nil,
		closeChan:      make(chan struct{}),
		mutex:          sync.Mutex{},
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
		// close all connections
		s.mutex.Lock()
		defer s.mutex.Unlock()
		for _, v := range s.activeConns {
			v.Close()
		}
	}()

	// run acceptor
	err = s.acceptLoop()
	if err != nil {
		log.Println("Accept loop error: ", err)
	}
	// signal close server
	s.closeChan <- struct{}{}
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
			log.Println("Accept connection error: ", err)
			continue
		}
		// create connection struct
		connection := NewConnection(conn, "01", s.db)
		// Store active conn
		s.activeConns[connection.Id] = connection
		// start read loop
		go func() {
			rErr := connection.ReadLoop()
			if rErr != nil {
				connection.Close()
				s.mutex.Lock()
				defer s.mutex.Unlock()
				delete(s.activeConns, connection.Id)
			}
		}()

		// start write loop
		go func() {
			wErr := connection.WriteLoop()
			if wErr != nil {
				connection.Close()
				s.mutex.Lock()
				defer s.mutex.Unlock()
				delete(s.activeConns, connection.Id)
			}
		}()
	}
}
