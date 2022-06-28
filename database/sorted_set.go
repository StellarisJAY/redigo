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
	RegisterCommandExecutor("zpopmin", execPopMin)
	RegisterCommandExecutor("zpopmax", execPopMax)
	RegisterCommandExecutor("zcard", execZCard)
	RegisterCommandExecutor("zrange", execZRange)
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
	if zs != nil {
		if rank := zs.Rank(string(args[1])); rank != -1 {
			return protocol.NewNumberReply(int(rank))
		}
	}
	return protocol.NilReply
}

func execPopMax(db *SingleDB, command redis.Command) *protocol.Reply {
	args := command.Args()
	if len(args) < 1 {
		return protocol.NewErrorReply(protocol.CreateWrongArgumentNumberError("ZPOPMAX"))
	}
	count := 1
	if len(args) > 1 {
		n, err := strconv.Atoi(string(args[1]))
		if err != nil {
			return protocol.NewErrorReply(err)
		}
		count = n
	}
	zs, err := getSortedSet(db, string(args[0]))
	if err != nil {
		return protocol.NewErrorReply(err)
	}
	if zs != nil && zs.Size() != 0 {
		if zs.Size() < count {
			count = zs.Size()
		}
		result := make([]string, 2*count)
		j := 0
		for i := 0; i < count; i++ {
			if max := zs.PopMax(); max != nil {
				result[j] = max.Member
				result[j+1] = strconv.FormatFloat(max.Score, 'f', -1, 64)
				j += 2
			}
		}
		return protocol.NewStringArrayReply(result)
	}
	return protocol.EmptyListReply
}

func execPopMin(db *SingleDB, command redis.Command) *protocol.Reply {
	args := command.Args()
	if len(args) < 1 {
		return protocol.NewErrorReply(protocol.CreateWrongArgumentNumberError("ZPOPMIN"))
	}
	count := 1
	if len(args) > 1 {
		n, err := strconv.Atoi(string(args[1]))
		if err != nil {
			return protocol.NewErrorReply(err)
		}
		count = n
	}
	zs, err := getSortedSet(db, string(args[0]))
	if err != nil {
		return protocol.NewErrorReply(err)
	}
	if zs != nil && zs.Size() != 0 {
		if zs.Size() < count {
			count = zs.Size()
		}
		result := make([]string, 2*count)
		j := 0
		for i := 0; i < count; i++ {
			if min := zs.PopMin(); min != nil {
				result[j] = min.Member
				result[j+1] = strconv.FormatFloat(min.Score, 'f', -1, 64)
				j += 2
			}
		}
		return protocol.NewStringArrayReply(result)
	}
	return protocol.EmptyListReply
}

func execZCard(db *SingleDB, command redis.Command) *protocol.Reply {
	args := command.Args()
	if len(args) != 1 {
		return protocol.NewErrorReply(protocol.CreateWrongArgumentNumberError("ZCARD"))
	}
	sortedSet, err := getSortedSet(db, string(args[0]))
	if err != nil {
		return protocol.NewErrorReply(err)
	}
	if sortedSet != nil {
		return protocol.NewNumberReply(sortedSet.Size())
	}
	return protocol.NewNumberReply(0)
}

func execZRange(db *SingleDB, command redis.Command) *protocol.Reply {
	args := command.Args()
	if len(args) < 3 {
		return protocol.NewErrorReply(protocol.CreateWrongArgumentNumberError("ZRANGE"))
	}
	// parse start and end values
	start, err1 := strconv.Atoi(string(args[1]))
	end, err2 := strconv.Atoi(string(args[2]))
	if err1 != nil || err2 != nil {
		return protocol.NewErrorReply(protocol.HashValueNotIntegerError)
	}
	withScores := false
	if len(args) == 4 && string(args[3]) == "WITHSCORES" {
		withScores = true
	}
	// get sorted set structure
	sortedSet, err := getSortedSet(db, string(args[0]))
	if err != nil {
		return protocol.NewErrorReply(err)
	}
	if sortedSet != nil {
		elements := sortedSet.Range(start, end)
		if elements == nil {
			return protocol.EmptyListReply
		}
		var result []string
		if withScores {
			result = make([]string, len(elements)*2)
			i := 0
			for _, e := range elements {
				result[i] = e.Member
				result[i+1] = strconv.FormatFloat(e.Score, 'f', -1, 64)
				i += 2
			}
		} else {
			result = make([]string, len(elements))
			for i, e := range elements {
				result[i] = e.Member
			}
		}
		return protocol.NewStringArrayReply(result)
	}
	return protocol.EmptyListReply
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
