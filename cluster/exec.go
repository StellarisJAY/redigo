package cluster

import (
	"log"
	"redigo/redis"
	"redigo/util/conn"
)

// execKeys 集群模式下执行keys命令
// 1. 开启多个goroutine从集群节点获取keys结果
// 2. 开启一个goroutine获取本地结果
func execKeys(cluster *Cluster, command redis.Command) *redis.RespCommand {
	realConn := command.Connection()
	// fakeConn 用于接收本地数据库的结果
	fakeConn := conn.NewFakeConnection(realConn)
	command.BindConnection(fakeConn)
	peers := cluster.Peers()
	replies := make(chan []byte, len(peers)+1)
	go func() {
		cluster.multiDB.SubmitCommand(command)
		replies <- (<-command.Connection().(*conn.FakeConnection).Replies).ToBytes()
	}()
	for _, peer := range peers {
		go func(peer *PeerClient) {
			reply := peer.RelayCommand(command)
			replies <- reply.ToBytes()
			log.Println("received keys reply from peer: ", peer.peerAddr)
		}(peer)
	}
	// 等待每个节点的结果
	result := make([][]byte, len(peers)+1)
	for i := 0; i < len(peers)+1; i++ {
		result[i] = <-replies
	}
	command.BindConnection(realConn)
	return redis.NewNestedArrayCommand(result)
}
