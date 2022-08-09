package cluster

import (
	"redigo/interface/redis"
	"redigo/redis/protocol"
)

type CommandHandler func(cluster *Cluster, command redis.Command) *protocol.Reply
type CommandRouter map[string]CommandHandler

var router CommandRouter = make(map[string]CommandHandler)

func init() {
	router["set"] = normalCommandHandler
	router["get"] = normalCommandHandler
}

// normalCommandHandler 普通命令处理器
func normalCommandHandler(cluster *Cluster, command redis.Command) *protocol.Reply {
	key := string(command.Args()[0])
	// 通过选择器找到key所在的节点
	peer := cluster.selector.SelectPeer(key)
	if peer == cluster.address {
		// 目标节点就是当前服务器，提交命令到当前节点的multiDB
		cluster.multiDB.SubmitCommand(command)
		return nil
	}
	if client, ok := cluster.peers[peer]; ok {
		// 转发命令并等待回复
		return client.RelayCommand(command)
	}
	return protocol.NewErrorReply(protocol.ClusterPeerNotFoundError)
}
