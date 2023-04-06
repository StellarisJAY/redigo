package aof

import (
	"redigo/datastruct/bitmap"
	"redigo/datastruct/dict"
	"redigo/datastruct/list"
	"redigo/datastruct/set"
	"redigo/datastruct/zset"
	"redigo/interface/database"
	"redigo/redis"
	"strconv"
	"time"
)

var (
	setCmd       = []byte("SET")
	rPushCmd     = []byte("RPUSH")
	hsetCmd      = []byte("HSET")
	sAddCmd      = []byte("SADD")
	zAddCmd      = []byte("ZADD")
	pExpireAtCmd = []byte("PEXPIREAT")
)

// EntryToCommand 将key-value数据转换成redis命令
func EntryToCommand(key string, entry *database.Entry) *redis.RespCommand {
	if entry == nil {
		return nil
	}
	var command *redis.RespCommand
	switch entry.Data.(type) {
	case []byte:
		command = stringToCommand(key, entry.Data.([]byte))
	case *list.LinkedList:
		command = listToCommand(key, entry.Data.(*list.LinkedList))
	case dict.Dict:
		command = hashToCommand(key, entry.Data.(dict.Dict))
	case *set.Set:
		command = setToCommand(key, entry.Data.(*set.Set))
	case *zset.SortedSet:
		command = zsetToCommand(key, entry.Data.(*zset.SortedSet))
	case *bitmap.BitMap:
		// 将bitmap转换成[]byte保存
		bm := entry.Data.(*bitmap.BitMap)
		command = stringToCommand(key, *bm)
	}
	return command
}

func stringToCommand(key string, value []byte) *redis.RespCommand {
	command := make([][]byte, 3)
	command[0] = setCmd
	command[1] = []byte(key)
	command[2] = value
	return redis.NewArrayCommand(command)
}

func listToCommand(key string, list *list.LinkedList) *redis.RespCommand {
	command := make([][]byte, 2+list.Size())
	command[0] = rPushCmd
	command[1] = []byte(key)
	list.ForEach(func(idx int, value []byte) bool {
		command[2+idx] = value
		return true
	})
	return redis.NewArrayCommand(command)
}

func hashToCommand(key string, hash dict.Dict) *redis.RespCommand {
	command := make([][]byte, 2+2*hash.Len())
	command[0] = hsetCmd
	command[1] = []byte(key)
	i := 0
	hash.ForEach(func(key string, value interface{}) bool {
		v := value.([]byte)
		command[2+i] = []byte(key)
		command[2+i+1] = v
		i += 2
		return true
	})
	return redis.NewArrayCommand(command)
}

func setToCommand(key string, set *set.Set) *redis.RespCommand {
	command := make([][]byte, 2+set.Len())
	command[0] = sAddCmd
	command[1] = []byte(key)
	i := 0
	set.ForEach(func(s string) bool {
		command[2+i] = []byte(s)
		i++
		return true
	})
	return redis.NewArrayCommand(command)
}

func zsetToCommand(key string, zs *zset.SortedSet) *redis.RespCommand {
	command := make([][]byte, 2+zs.Size()*2)
	command[0] = zAddCmd
	command[1] = []byte(key)
	i := 0
	zs.ForEach(func(score float64, value string) bool {
		command[2+i] = []byte(value)
		command[2+i+1] = []byte(strconv.FormatFloat(score, 'f', -1, 64))
		i += 2
		return true
	})
	return redis.NewArrayCommand(command)
}

func makeExpireCommand(key string, expire *time.Time) *redis.RespCommand {
	command := make([][]byte, 3)
	command[0] = pExpireAtCmd
	command[1] = []byte(key)
	command[2] = []byte(strconv.FormatInt(expire.UnixMilli(), 10))
	return redis.NewArrayCommand(command)
}
