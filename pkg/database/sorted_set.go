package database

import (
	"math"
	"redigo/pkg/datastruct/zset"
	"redigo/pkg/interface/database"
	"redigo/pkg/redis"
	"strconv"
	"strings"
)

var (
	negativeInfinity float64 = math.MinInt64
	positiveInfinity         = math.MaxFloat64
)

func init() {
	RegisterCommandExecutor("zadd", execZAdd, -3)
	RegisterCommandExecutor("zscore", execZScore, 2)
	RegisterCommandExecutor("zrem", execZRem, -2)
	RegisterCommandExecutor("zrank", execZRank, 2)
	RegisterCommandExecutor("zpopmin", execPopMin, 2)
	RegisterCommandExecutor("zpopmax", execPopMax, 2)
	RegisterCommandExecutor("zcard", execZCard, 1)
	RegisterCommandExecutor("zrange", execZRange, -3)
	RegisterCommandExecutor("zrangebyscore", execZRangeByScore, -3)
}

func execZAdd(db *SingleDB, command redis.Command) *redis.RespCommand {
	args := command.Args()
	if !ValidateArgCount(command.Name(), len(args)) || (len(args)-1)%2 != 0 {
		return redis.NewErrorCommand(redis.CreateWrongArgumentNumberError("ZADD"))
	}
	count := len(args) - 1
	key := string(args[0])
	elements := make([]zset.Element, count/2)
	eleArgs := args[1:]
	for i := 0; i < count; i += 2 {
		score, err := strconv.ParseFloat(string(eleArgs[i]), 64)
		if err != nil {
			return redis.NewErrorCommand(redis.ValueNotFloatError)
		}
		elements[i/2] = zset.Element{
			Member: string(eleArgs[i+1]),
			Score:  score,
		}
	}

	zs, err := getOrInitSortedSet(db, key)
	if err != nil {
		return redis.NewErrorCommand(err)
	}
	result := 0
	for _, ele := range elements {
		result += zs.Add(ele.Member, ele.Score)
	}
	db.addVersion(key)
	db.addAof(command.Parts())
	return redis.NewNumberCommand(result)
}

func execZScore(db *SingleDB, command redis.Command) *redis.RespCommand {
	args := command.Args()
	if !ValidateArgCount(command.Name(), len(args)) {
		return redis.NewErrorCommand(redis.CreateWrongArgumentNumberError("ZSCORE"))
	}
	zs, err := getSortedSet(db, string(args[0]))
	if err != nil {
		return redis.NewErrorCommand(err)
	}
	if zs != nil {
		element, exists := zs.GetScore(string(args[1]))
		if exists {
			return redis.NewBulkStringCommand([]byte(strconv.FormatFloat(element.Score, 'f', -1, 64)))
		}
	}
	return redis.NilCommand
}

func execZRem(db *SingleDB, command redis.Command) *redis.RespCommand {
	args := command.Args()
	if !ValidateArgCount(command.Name(), len(args)) {
		return redis.NewErrorCommand(redis.CreateWrongArgumentNumberError("ZREM"))
	}
	key := string(args[0])
	zs, err := getSortedSet(db, key)
	if err != nil {
		return redis.NewErrorCommand(err)
	}
	if zs == nil {
		return redis.NilCommand
	}
	result := 0
	for _, member := range args[1:] {
		result += zs.Remove(string(member))
	}
	db.addVersion(key)
	db.addAof(command.Parts())
	return redis.NewNumberCommand(result)
}

func execZRank(db *SingleDB, command redis.Command) *redis.RespCommand {
	args := command.Args()
	if !ValidateArgCount(command.Name(), len(args)) {
		return redis.NewErrorCommand(redis.CreateWrongArgumentNumberError("ZRANK"))
	}
	zs, err := getSortedSet(db, string(args[0]))
	if err != nil {
		return redis.NewErrorCommand(err)
	}
	if zs != nil {
		if rank := zs.Rank(string(args[1])); rank != -1 {
			return redis.NewNumberCommand(int(rank))
		}
	}
	return redis.NilCommand
}

func execPopMax(db *SingleDB, command redis.Command) *redis.RespCommand {
	args := command.Args()
	if !ValidateArgCount(command.Name(), len(args)) {
		return redis.NewErrorCommand(redis.CreateWrongArgumentNumberError("ZPOPMAX"))
	}
	key := string(args[0])
	count := 1
	if len(args) > 1 {
		n, err := strconv.Atoi(string(args[1]))
		if err != nil {
			return redis.NewErrorCommand(err)
		}
		count = n
	}
	zs, err := getSortedSet(db, key)
	if err != nil {
		return redis.NewErrorCommand(err)
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
		db.addVersion(key)
		db.addAof(command.Parts())
		return redis.NewStringArrayCommand(result)
	}
	return redis.EmptyListCommand
}

func execPopMin(db *SingleDB, command redis.Command) *redis.RespCommand {
	args := command.Args()
	if !ValidateArgCount(command.Name(), len(args)) {
		return redis.NewErrorCommand(redis.CreateWrongArgumentNumberError("ZPOPMIN"))
	}
	key := string(args[0])
	count := 1
	if len(args) > 1 {
		n, err := strconv.Atoi(string(args[1]))
		if err != nil {
			return redis.NewErrorCommand(err)
		}
		count = n
	}
	zs, err := getSortedSet(db, key)
	if err != nil {
		return redis.NewErrorCommand(err)
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
		db.addVersion(key)
		db.addAof(command.Parts())
		return redis.NewStringArrayCommand(result)
	}
	return redis.EmptyListCommand
}

func execZCard(db *SingleDB, command redis.Command) *redis.RespCommand {
	args := command.Args()
	if !ValidateArgCount(command.Name(), len(args)) {
		return redis.NewErrorCommand(redis.CreateWrongArgumentNumberError("ZCARD"))
	}
	sortedSet, err := getSortedSet(db, string(args[0]))
	if err != nil {
		return redis.NewErrorCommand(err)
	}
	if sortedSet != nil {
		return redis.NewNumberCommand(sortedSet.Size())
	}
	return redis.NewNumberCommand(0)
}

func execZRange(db *SingleDB, command redis.Command) *redis.RespCommand {
	args := command.Args()
	if !ValidateArgCount(command.Name(), len(args)) {
		return redis.NewErrorCommand(redis.CreateWrongArgumentNumberError("ZRANGE"))
	}
	// parse start and end values
	start, err1 := strconv.Atoi(string(args[1]))
	end, err2 := strconv.Atoi(string(args[2]))
	if err1 != nil || err2 != nil {
		return redis.NewErrorCommand(redis.HashValueNotIntegerError)
	}
	withScores := false
	if len(args) == 4 && string(args[3]) == "WITHSCORES" {
		withScores = true
	}
	// get sorted set structure
	sortedSet, err := getSortedSet(db, string(args[0]))
	if err != nil {
		return redis.NewErrorCommand(err)
	}
	if sortedSet != nil {
		elements := sortedSet.Range(start, end)
		if elements == nil {
			return redis.EmptyListCommand
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
		return redis.NewStringArrayCommand(result)
	}
	return redis.EmptyListCommand
}

func execZRangeByScore(db *SingleDB, command redis.Command) *redis.RespCommand {
	args := command.Args()
	if !ValidateArgCount(command.Name(), len(args)) {
		return redis.NewErrorCommand(redis.CreateWrongArgumentNumberError("ZRANGEBYSCORE"))
	}
	// parse interval, get min,max value and open options
	min, max, lOpen, rOpen, err := parseInterval(string(args[1]), string(args[2]))
	if err != nil {
		return redis.NewErrorCommand(redis.ValueNotFloatError)
	}
	withScores := false
	offset := 0
	count := -1
	if len(args) > 3 {
		additions := args[3:]
		for i := 0; i < len(additions); {
			arg := string(additions[i])
			if arg == "WITHSCORES" {
				withScores = true
				i++
			} else if arg == "LIMIT" {
				if i >= len(additions)-2 {
					return redis.NewErrorCommand(redis.SyntaxError)
				}
				offset, err = strconv.Atoi(string(additions[i+1]))
				count, err = strconv.Atoi(string(additions[i+2]))
				if err != nil {
					return redis.NewErrorCommand(redis.ValueNotIntegerOrOutOfRangeError)
				}
				i += 3
			} else {
				return redis.NewErrorCommand(redis.SyntaxError)
			}
		}
	}
	set, err := getSortedSet(db, string(args[0]))
	if err != nil {
		return redis.NewErrorCommand(redis.WrongTypeOperationError)
	}
	if count == -1 {
		count = set.Size()
	}
	elements := set.RangeByScore(min, max, offset, count, lOpen, rOpen)
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
	return redis.NewStringArrayCommand(result)
}

func parseInterval(arg1, arg2 string) (min, max float64, lOpen, rOpen bool, err error) {
	if arg1 == "-inf" && arg2 == "+inf" {
		min = negativeInfinity
		max = positiveInfinity
		return
	} else if arg1 == "-inf" {
		min = negativeInfinity
	} else if arg2 == "+inf" {
		max = positiveInfinity
	}

	if strings.HasPrefix(arg1, "(") {
		lOpen = true
		arg1 = strings.TrimPrefix(arg1, "(")
	}
	if strings.HasPrefix(arg2, "(") {
		rOpen = true
		arg2 = strings.TrimPrefix(arg2, "(")
	}
	min, err = strconv.ParseFloat(arg1, 64)
	max, err = strconv.ParseFloat(arg2, 64)
	return
}

func isSortedSet(entry database.Entry) bool {
	switch entry.Data.(type) {
	case *zset.SortedSet:
		return true
	}
	return false
}

func getSortedSet(db *SingleDB, key string) (*zset.SortedSet, error) {
	entry, exists := db.GetEntry(key)
	if !exists {
		return nil, nil
	} else {
		if isSortedSet(*entry) {
			return entry.Data.(*zset.SortedSet), nil
		}
		return nil, redis.WrongTypeOperationError
	}
}

func getOrInitSortedSet(db *SingleDB, key string) (*zset.SortedSet, error) {
	entry, exists := db.GetEntry(key)
	if !exists {
		zs := zset.NewSortedSet()
		db.data.Put(key, &database.Entry{Data: zs})
		return zs, nil
	} else {
		if isSortedSet(*entry) {
			return entry.Data.(*zset.SortedSet), nil
		}
		return nil, redis.WrongTypeOperationError
	}
}
