package cluster

import (
	"redigo/pkg/cluster/peer"
	"redigo/pkg/interface/database"
	"redigo/pkg/redis"
	"redigo/pkg/tcp"
	"redigo/pkg/util/log"
	"time"
)

type Cluster struct {
	multiDB  database.DB
	peers    map[string]*PeerClient
	selector peer.Selector
	address  string
	server   tcp.Server // 集群模式的节点server，不对客户端开放，只在集群内部使用
}

func NewCluster(db database.DB, address string, peers []string) *Cluster {
	c := &Cluster{
		multiDB:  db,
		peers:    make(map[string]*PeerClient),
		selector: peer.NewConsistentHashSelector(),
		address:  address,
		server:   tcp.NewServer(address, db),
	}
	for _, peer := range peers {
		c.selector.AddPeer(peer)
		// TODO 设置最大连接数量
		c.peers[peer] = NewPeerClient(peer, 10)
	}
	c.selector.AddPeer(address)
	return c
}

func (c *Cluster) SubmitCommand(command redis.Command) {
	// execute command
	reply := c.Execute(command)
	if reply != nil {
		command.Connection().SendCommand(reply)
	}
}

func (c *Cluster) Close() {
	// TODO Close Embed Database and Peer Clients
}

func (c *Cluster) ExecuteLoop() error {
	log.Info("redigo cluster server started, listening: %s", c.address)
	// 集群内部服务器启动，同时触发multiDB的启动
	return c.server.Start()
}

func (c *Cluster) Execute(command redis.Command) *redis.RespCommand {
	// 命令来自集群节点，调用本地数据库执行
	if command.IsFromCluster() {
		c.multiDB.SubmitCommand(command)
		return nil
	}
	if handler, ok := router[command.Name()]; ok {
		return handler(c, command)
	} else {
		return redis.NewErrorCommand(redis.CreateUnknownCommandError(command.Name()))
	}
}

func (c *Cluster) ForEach(dbIdx int, fun func(key string, entry *database.Entry, expire *time.Time) bool) {
	panic("foreach is not available in cluster handler")
}

func (c *Cluster) Len(dbIdx int) int {
	panic("len is not available in cluster handler")
}

func (c *Cluster) OnConnectionClosed(conn redis.Connection) {
	c.multiDB.OnConnectionClosed(conn)
}

func (c *Cluster) Peers() []*PeerClient {
	clients := make([]*PeerClient, 0, len(c.peers))
	for _, client := range c.peers {
		clients = append(clients, client)
	}
	return clients
}

func (c *Cluster) LookForKey(key string) *PeerClient {
	addr := c.selector.SelectPeer(key)
	return c.peers[addr]
}

func (c *Cluster) GetEntry(key string, dbIndex ...int) (*database.Entry, bool) {
	//TODO implement me
	panic("implement me")
}

func (c *Cluster) DeleteEntry(key string, dbIndex ...int) (*database.Entry, bool) {
	//TODO implement me
	panic("implement me")
}
