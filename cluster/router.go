package cluster

import (
	"redigo/redis"
	"redigo/util/log"
)

type CommandHandler func(cluster *Cluster, command redis.Command) *redis.RespCommand
type CommandRouter map[string]CommandHandler

var router CommandRouter = make(map[string]CommandHandler)

func init() {

	router["keys"] = execKeys

	router["del"] = normalCommandHandler
	router["ttl"] = normalCommandHandler
	router["pttl"] = normalCommandHandler
	router["expire"] = normalCommandHandler
	router["persist"] = normalCommandHandler
	router["pexpireat"] = normalCommandHandler
	router["type"] = normalCommandHandler

	router["set"] = normalCommandHandler
	router["get"] = normalCommandHandler
	router["setnx"] = normalCommandHandler
	router["incr"] = normalCommandHandler
	router["decr"] = normalCommandHandler
	router["incrby"] = normalCommandHandler
	router["decrby"] = normalCommandHandler
	router["strlen"] = normalCommandHandler
	router["setbit"] = normalCommandHandler
	router["getbit"] = normalCommandHandler
	router["bitcount"] = normalCommandHandler

	router["lpush"] = normalCommandHandler
	router["lpop"] = normalCommandHandler
	router["rpush"] = normalCommandHandler
	router["rpop"] = normalCommandHandler
	router["lrange"] = normalCommandHandler
	router["lindex"] = normalCommandHandler
	router["llen"] = normalCommandHandler

	router["hset"] = normalCommandHandler
	router["hget"] = normalCommandHandler
	router["hdel"] = normalCommandHandler
	router["hexists"] = normalCommandHandler
	router["hgetall"] = normalCommandHandler
	router["hkeys"] = normalCommandHandler
	router["hlen"] = normalCommandHandler
	router["hmget"] = normalCommandHandler
	router["hsetnx"] = normalCommandHandler
	router["hincrby"] = normalCommandHandler
	router["hstrlen"] = normalCommandHandler
	router["hvals"] = normalCommandHandler

	router["sadd"] = normalCommandHandler
	router["sismember"] = normalCommandHandler
	router["smembers"] = normalCommandHandler
	router["srandmember"] = normalCommandHandler
	router["srem"] = normalCommandHandler
	router["spop"] = normalCommandHandler
	router["scard"] = normalCommandHandler

	router["zadd"] = normalCommandHandler
	router["zscore"] = normalCommandHandler
	router["zrem"] = normalCommandHandler
	router["zrank"] = normalCommandHandler
	router["zpopmin"] = normalCommandHandler
	router["zpopmax"] = normalCommandHandler
	router["zcard"] = normalCommandHandler
	router["zrange"] = normalCommandHandler
	router["zrangebyscore"] = normalCommandHandler
	router["scard"] = normalCommandHandler
}

// normalCommandHandler 普通命令处理器
func normalCommandHandler(cluster *Cluster, command redis.Command) *redis.RespCommand {
	if len(command.Args()) < 1 {
		return redis.NewErrorCommand(redis.CreateWrongArgumentNumberError(command.Name()))
	}
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
		response := client.RelayCommand(command)
		log.Debug("received command result from peer: %s", peer)
		return response
	}
	return redis.NewErrorCommand(redis.ClusterPeerNotFoundError)
}
