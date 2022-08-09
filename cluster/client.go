package cluster

import (
	"bufio"
	"context"
	"errors"
	"log"
	"net"
	"redigo/interface/redis"
	"redigo/redis/parser"
	"redigo/redis/protocol"
	"redigo/util/pool"
	"strconv"
	"sync"
	"time"
)

type PeerClient struct {
	connPool       *pool.Pool
	maxConnections int
	peerAddr       string
}

type PeerConn struct {
	Conn    net.Conn
	pending chan *protocol.Reply
	sync.Mutex
}

func NewPeerClient(peerAddr string, maxConns int) *PeerClient {
	pc := &PeerClient{
		peerAddr:       peerAddr,
		maxConnections: maxConns,
	}
	pc.connPool = pool.Empty(maxConns, func() interface{} {
		conn := connect(peerAddr)
		return conn
	})
	return pc
}

// connect 与peer建立新的连接
func connect(addr string) *PeerConn {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		log.Println("connect to peer server failed: ", err)
		return nil
	}
	return &PeerConn{Conn: conn}
}

func (c *PeerConn) sendCommand(ctx context.Context, command redis.Command) *protocol.Reply {
	// 将command转换为RESP字节流
	payload := command.ToBytes()
	// 设置网络发送和接收的deadline
	if deadline, ok := ctx.Deadline(); ok {
		_ = c.Conn.SetWriteDeadline(deadline)
		_ = c.Conn.SetReadDeadline(deadline)
	}
	// 发送给peer
	_, err := c.Conn.Write(payload)
	if err != nil {
		return protocol.NewErrorReply(protocol.ClusterPeerUnreachableError)
	}
	// 等待、读取、解析回复
	reader := bufio.NewReader(c.Conn)
	parsed, err := parser.Parse(reader)
	// 网络接收超时或解析发生错误
	if err != nil {
		log.Println("parse peer reply error: ", err)
		return protocol.NewErrorReply(protocol.ClusterPeerUnreachableError)
	}
	return commandToReply(parsed)
}

// RelayCommand 转发消息到目标peer，并等待结果
func (pc *PeerClient) RelayCommand(command redis.Command) *protocol.Reply {
	ctx, cancelFunc := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelFunc()
	// 获取一个连接，等待超时或连接失败都会返回nil
	c := pc.connPool.Load(ctx)
	conn := c.(*PeerConn)
	if conn == nil {
		return protocol.NewErrorReply(protocol.ClusterPeerUnreachableError)
	}
	defer pc.connPool.Put(c)
	return conn.sendCommand(ctx, command)
}

func commandToReply(command redis.Command) *protocol.Reply {
	switch command.Type() {
	case redis.CommandTypeSingleLine:
		return protocol.NewSingleStringReply(string(command.Parts()[0]))
	case redis.CommandTypeBulk:
		return protocol.NewBulkValueReply(command.Parts()[0])
	case redis.CommandTypeArray:
		return protocol.NewArrayReply(command.Parts())
	case redis.CommandTypeNumber:
		number, _ := strconv.Atoi(string(command.Parts()[0]))
		return protocol.NewNumberReply(number)
	case redis.CommandTypeError:
		return protocol.NewErrorReply(errors.New(string(command.Parts()[0])))
	}
	return protocol.NilReply
}
