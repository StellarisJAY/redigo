package cluster

import (
	"redigo/redis"
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
	// 目前DBSize 只获取当前集群节点的key-value数量
	router["dbsize"] = executeLocal

	router["multi"] = multiHandler
	router["exec"] = multiHandler
	router["discard"] = executeLocal
	router["watch"] = handleWatchOrUnwatch
	router["unwatch"] = handleWatchOrUnwatch
}

func multiHandler(cluster *Cluster, command redis.Command) *redis.RespCommand {
	conn := command.Connection()
	if cmd := command.Name(); cmd == "multi" {
		// 不允许出现嵌套的multi命令
		if conn.IsMulti() {
			return redis.NewErrorCommand(redis.NestedMultiCallError)
		}
		conn.SetMulti(true)
		return redis.OKCommand
	} else if cmd == "exec" {
		if !conn.IsMulti() {
			return redis.NewErrorCommand(redis.ExecWithoutMultiError)
		}
		commands := conn.GetQueuedCommands()
		conn.SetMulti(false)
		reply := handleQueuedCommands(cluster, commands)
		return reply
	}
	return redis.NewErrorCommand(redis.CreateUnknownCommandError(command.Name()))
}

// normalCommandHandler 普通命令处理器
func normalCommandHandler(cluster *Cluster, command redis.Command) *redis.RespCommand {
	if len(command.Args()) < 1 {
		return redis.NewErrorCommand(redis.CreateWrongArgumentNumberError(command.Name()))
	}
	conn := command.Connection()
	// 当前已经处于multi状态，新的命令全部加入队列
	if conn.IsMulti() {
		conn.EnqueueCommand(command.(*redis.RespCommand))
		return redis.NewSingleLineCommand([]byte("QUEUED"))
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
		return response
	}
	return redis.NewErrorCommand(redis.ClusterPeerNotFoundError)
}

// execLocal 本地执行命令
func executeLocal(cluster *Cluster, command redis.Command) *redis.RespCommand {
	reply := cluster.multiDB.Execute(command)
	return reply
}

// handleQueuedCommands 集群模式下处理队列中的命令
func handleQueuedCommands(cluster *Cluster, commands []*redis.RespCommand) *redis.RespCommand {
	replies := make([][]byte, len(commands))
	for i, command := range commands {
		key := string(command.Args()[0])
		// 只执行集群模式允许且key在本地的命令
		if peer := cluster.selector.SelectPeer(key); peer == cluster.address && router[command.Name()] != nil {
			reply := cluster.multiDB.Execute(command)
			replies[i] = redis.Encode(reply)
		} else {
			replies[i] = redis.Encode(redis.NewErrorCommand(redis.CreateMovedError(peer)))
		}
	}
	return redis.NewNestedArrayCommand(replies)
}

func handleWatchOrUnwatch(cluster *Cluster, command redis.Command) *redis.RespCommand {
	if len(command.Args()) != 1 {
		return redis.NewErrorCommand(redis.CreateWrongArgumentNumberError("watch"))
	}
	if addr := cluster.selector.SelectPeer(string(command.Args()[0])); addr != cluster.address {
		return redis.NewErrorCommand(redis.CreateMovedError(addr))
	}
	return executeLocal(cluster, command)
}
