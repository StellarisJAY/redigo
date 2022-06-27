package database

import (
	"redigo/datastruct/zset"
	"redigo/redis"
	"redigo/redis/protocol"
	"strconv"
)

func init() {
	RegisterCommandExecutor("zadd", execZAdd)
	RegisterCommandExecutor("zscore", execZScore)
	RegisterCommandExecutor("zrem", execZRem)
	RegisterCommandExecutor("zrank", execZRank)
}

func execZAdd(db *SingleDB, command redis.Command) *protocol.Reply {
	args := command.Args()
	if len(args) < 3 || (len(args)-1)%2 != 0 {
		return protocol.NewErrorReply(protocol.CreateWrongArgumentNumberError("ZADD"))
	}
	count := len(args) - 1
	key := string(args[0])
	elements := make([]zset.Element, count/2)
	eleArgs := args[1:]
	for i := 0; i < count; i += 2 {
		score, err := strconv.ParseFloat(string(eleArgs[i]), 64)
		if err != nil {
			return protocol.NewErrorReply(protocol.ValueNotFloatError)
		}
		elements[i/2] = zset.Element{
			Member: string(eleArgs[i+1]),
			Score:  score,
		}
	}

	zs, err := getOrInitSortedSet(db, key)
	if err != nil {
		return protocol.NewErrorReply(err)
	}
	result := 0
	for _, ele := range elements {
		result += zs.Add(ele.Member, ele.Score)
	}
	return protocol.NewNumberReply(result)
}

func execZScore(db *SingleDB, command redis.Command) *protocol.Reply {
	args := command.Args()
	if len(args) != 2 {
		return protocol.NewErrorReply(protocol.CreateWrongArgumentNumberError("ZSCORE"))
	}
	zs, err := getSortedSet(db, string(args[0]))
	if err != nil {
		return protocol.NewErrorReply(err)
	}
	if zs != nil {
		element, exists := zs.GetScore(string(args[1]))
		if exists {
			return protocol.NewBulkStringReply(strconv.FormatFloat(element.Score, 'f', -1, 64))
		}
	}
	return protocol.NilReply
}

func execZRem(db *SingleDB, command redis.Command) *protocol.Reply {
	args := command.Args()
	if len(args) < 2 {
		return protocol.NewErrorReply(protocol.CreateWrongArgumentNumberError("ZREM"))
	}
	zs, err := getSortedSet(db, string(args[0]))
	if err != nil {
		return protocol.NewErrorReply(err)
	}
	if zs == nil {
		return protocol.NilReply
	}
	result := 0
	for _, member := range args[1:] {
		result += zs.Remove(string(member))
	}
	return protocol.NewNumberReply(result)
}

func execZRank(db *SingleDB, command redis.Command) *protocol.Reply {
	args := command.Args()
	if len(args) < 2 {
		return protocol.NewErrorReply(protocol.CreateWrongArgumentNumberError("ZRANK"))
	}
	zs, err := getSortedSet(db, string(args[0]))
	if err != nil {
		return protocol.NewErrorReply(err)
	}
	if zs == nil {
		return protocol.NilReply
	}
	if rank := zs.Rank(string(args[1])); rank != -1 {
		return protocol.NewNumberReply(int(rank))
	}
	return protocol.NilReply
}

func isSortedSet(entry Entry) bool {
	switch entry.Data.(type) {
	case *zset.SortedSet:
		return true
	}
	return false
}

func getSortedSet(db *SingleDB, key string) (*zset.SortedSet, error) {
	v, exists := db.data.Get(key)
	if !exists {
		return nil, nil
	} else {
		entry := v.(*Entry)
		if isSortedSet(*entry) {
			return entry.Data.(*zset.SortedSet), nil
		}
		return nil, protocol.WrongTypeOperationError
	}
}

func getOrInitSortedSet(db *SingleDB, key string) (*zset.SortedSet, error) {
	v, exists := db.data.Get(key)
	if !exists {
		zs := zset.NewSortedSet()
		db.data.Put(key, &Entry{Data: zs})
		return zs, nil
	} else {
		entry := v.(*Entry)
		if isSortedSet(*entry) {
			return entry.Data.(*zset.SortedSet), nil
		}
		return nil, protocol.WrongTypeOperationError
	}
}
