package cluster

import (
	"redigo/cluster/peer"
	"redigo/interface/database"
	"redigo/interface/redis"
	"redigo/redis/protocol"
	"time"
)

type Cluster struct {
	multiDB  database.DB
	peers    map[string]*PeerClient
	selector peer.Selector
	address  string
}

func NewCluster(db database.DB, address string, peers []string) *Cluster {
	c := &Cluster{
		multiDB:  db,
		peers:    make(map[string]*PeerClient),
		selector: peer.NewConsistentHashSelector(),
		address:  address,
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
		command.Connection().SendReply(reply)
	}
}

func (c *Cluster) Close() {
	// TODO Close Embed Database and Peer Clients
}

func (c *Cluster) ExecuteLoop() error {
	return c.multiDB.ExecuteLoop()
}

func (c *Cluster) Execute(command redis.Command) *protocol.Reply {
	if handler, ok := router[command.Name()]; ok {
		return handler(c, command)
	} else {
		return protocol.NewErrorReply(protocol.CreateUnknownCommandError(command.Name()))
	}
}

func (c *Cluster) ForEach(dbIdx int, fun func(key string, entry *database.Entry, expire *time.Time) bool) {
	panic("foreach is not available in cluster handler")
}

func (c *Cluster) Len(dbIdx int) int {
	panic("len is not available in cluster handler")
}

func (c *Cluster) OnConnectionClosed(conn redis.Connection) {
	//TODO implement me
	panic("implement me")
}
