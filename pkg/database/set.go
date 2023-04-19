package database

import (
	"redigo/pkg/config"
	"redigo/pkg/datastruct/set"
	"redigo/pkg/interface/database"
	"redigo/pkg/redis"
	"reflect"
	"strconv"
)

func init() {
	RegisterCommandExecutor("sadd", execSAdd, -2)
	RegisterCommandExecutor("sismember", execSIsMember, 2)
	RegisterCommandExecutor("smembers", execSMembers, 1)
	RegisterCommandExecutor("srandmember", execSRandomMember, 2)
	RegisterCommandExecutor("srem", execSRem, -2)
	RegisterCommandExecutor("spop", execSPop, 2)
	RegisterCommandExecutor("sdiff", execSDiff, 2)
	RegisterCommandExecutor("sinter", execSInter, 2)
	RegisterCommandExecutor("scard", execSCard, 1)
	RegisterCommandExecutor("sdiffstore", execSDiffStore, 3)
	RegisterCommandExecutor("sinterstore", execSInterStore, 3)
	RegisterCommandExecutor("sunion", execSUnion, 2)
}

func execSAdd(db *SingleDB, command redis.Command) *redis.RespCommand {
	args := command.Args()
	if !ValidateArgCount(command.Name(), len(args)) {
		return redis.NewErrorCommand(redis.CreateWrongArgumentNumberError("SADD"))
	}
	key := string(args[0])
	s, err := getOrCreateSet(db, key)
	if err != nil {
		return redis.NewErrorCommand(err)
	}
	vals := args[1:]
	count := 0
	for _, val := range vals {
		count += s.Add(string(val))
	}
	db.addVersion(key)
	db.addAof(command.Parts())
	return redis.NewNumberCommand(count)
}

func execSIsMember(db *SingleDB, command redis.Command) *redis.RespCommand {
	args := command.Args()
	if !ValidateArgCount(command.Name(), len(args)) {
		return redis.NewErrorCommand(redis.CreateWrongArgumentNumberError("SISMEMBER"))
	}
	s, err := getSet(db, string(args[0]))
	if err != nil {
		return redis.NewErrorCommand(err)
	}
	if s != nil {
		return redis.NewNumberCommand(s.Has(string(args[1])))
	}
	return redis.NewNumberCommand(0)
}

func execSMembers(db *SingleDB, command redis.Command) *redis.RespCommand {
	args := command.Args()
	if !ValidateArgCount(command.Name(), len(args)) {
		return redis.NewErrorCommand(redis.CreateWrongArgumentNumberError("SMEMBERS"))
	}
	s, err := getSet(db, string(args[0]))
	if err != nil {
		return redis.NewErrorCommand(err)
	}
	if s != nil {
		return redis.NewStringArrayCommand(s.Members())
	}
	return redis.EmptyListCommand
}

func execSRandomMember(db *SingleDB, command redis.Command) *redis.RespCommand {
	args := command.Args()
	if !ValidateArgCount(command.Name(), len(args)) {
		return redis.NewErrorCommand(redis.CreateWrongArgumentNumberError("SRANDMEMBER"))
	}
	// parse random member count
	count, err := strconv.Atoi(string(args[1]))
	if err != nil {
		return redis.NewErrorCommand(redis.HashValueNotIntegerError)
	}
	s, err := getSet(db, string(args[0]))
	if err != nil {
		return redis.NewErrorCommand(err)
	}
	if s != nil {
		return redis.NewStringArrayCommand(s.RandomMembers(count))
	}
	return redis.EmptyListCommand
}

func execSRem(db *SingleDB, command redis.Command) *redis.RespCommand {
	args := command.Args()
	if !ValidateArgCount(command.Name(), len(args)) {
		return redis.NewErrorCommand(redis.CreateWrongArgumentNumberError("SREM"))
	}
	key := string(args[0])
	s, err := getSet(db, key)
	if err != nil {
		return redis.NewErrorCommand(err)
	}
	if s != nil {
		values := args[1:]
		count := 0
		for _, value := range values {
			count += s.Remove(string(value))
		}
		db.addAof(command.Parts())
		db.addVersion(key)
		return redis.NewNumberCommand(count)
	}
	return redis.NewNumberCommand(0)
}

func execSPop(db *SingleDB, command redis.Command) *redis.RespCommand {
	args := command.Args()
	if !ValidateArgCount(command.Name(), len(args)) {
		return redis.NewErrorCommand(redis.CreateWrongArgumentNumberError("SPOP"))
	}
	key := string(args[0])
	// parse pop count, check if is integer
	count, err := strconv.Atoi(string(args[1]))
	if err != nil {
		return redis.NewErrorCommand(redis.HashValueNotIntegerError)
	}
	s, err := getSet(db, key)
	if err != nil {
		return redis.NewErrorCommand(err)
	}
	if s != nil {
		members := s.RandomMembersDistinct(count)
		var aofCmdLine [][]byte
		if config.Properties.AppendOnly {
			aofCmdLine = make([][]byte, len(members)+2)
			aofCmdLine[0] = []byte("SREM")
			aofCmdLine[1] = args[0]
		}
		for i, member := range members {
			s.Remove(member)
			if aofCmdLine != nil {
				aofCmdLine[i+2] = []byte(member)
			}
		}
		db.addVersion(key)
		db.addAof(aofCmdLine)
		return redis.NewStringArrayCommand(members)
	}
	return redis.EmptyListCommand
}

func execSDiff(db *SingleDB, command redis.Command) *redis.RespCommand {
	args := command.Args()
	if !ValidateArgCount(command.Name(), len(args)) {
		return redis.NewErrorCommand(redis.CreateWrongArgumentNumberError("SDIFF"))
	}
	s1, err := getSet(db, string(args[0]))
	if err != nil {
		return redis.NewErrorCommand(err)
	}
	s2, err := getSet(db, string(args[1]))
	if err != nil {
		return redis.NewErrorCommand(err)
	}

	if s1 != nil && s2 != nil {
		diff := s1.Diff(s2)
		return redis.NewStringArrayCommand(diff)
	} else if s1 != nil {
		return redis.NewStringArrayCommand(s1.Members())
	} else {
		return redis.EmptyListCommand
	}
}

func execSDiffStore(db *SingleDB, command redis.Command) *redis.RespCommand {
	args := command.Args()
	if !ValidateArgCount(command.Name(), len(args)) {
		return redis.NewErrorCommand(redis.CreateWrongArgumentNumberError("SDIFFSTORE"))
	}
	s1, err := getSet(db, string(args[0]))
	if err != nil {
		return redis.NewErrorCommand(err)
	}
	s2, err := getSet(db, string(args[1]))
	if err != nil {
		return redis.NewErrorCommand(err)
	}
	// check set1 and set2 existence and data type
	var diff []string
	if s1 != nil && s2 != nil {
		// get diff from s1 and s2
		diff = s1.Diff(s2)
	} else if s1 != nil {
		diff = s1.Members()
	} else {
		diff = []string{}
	}
	dest := set.NewSet()
	// Add diff values into destination
	for _, value := range diff {
		dest.Add(value)
	}
	db.data.Put(string(args[2]), &database.Entry{Data: dest})
	db.addAof(command.Parts())
	return redis.NewStringArrayCommand(diff)
}

func execSInter(db *SingleDB, command redis.Command) *redis.RespCommand {
	args := command.Args()
	if !ValidateArgCount(command.Name(), len(args)) {
		return redis.NewErrorCommand(redis.CreateWrongArgumentNumberError("SINTER"))
	}
	s1, err := getSet(db, string(args[0]))
	if err != nil {
		return redis.NewErrorCommand(err)
	}
	s2, err := getSet(db, string(args[1]))
	if err != nil {
		return redis.NewErrorCommand(err)
	}
	if s1 != nil && s2 != nil {
		inter := s1.Inter(s2)
		return redis.NewStringArrayCommand(inter)
	} else {
		return redis.EmptyListCommand
	}
}

func execSInterStore(db *SingleDB, command redis.Command) *redis.RespCommand {
	args := command.Args()
	if !ValidateArgCount(command.Name(), len(args)) {
		return redis.NewErrorCommand(redis.CreateWrongArgumentNumberError("SINTERSTORE"))
	}
	s1, err := getSet(db, string(args[0]))
	if err != nil {
		return redis.NewErrorCommand(err)
	}
	s2, err := getSet(db, string(args[1]))
	if err != nil {
		return redis.NewErrorCommand(err)
	}
	var inter []string
	// check set1 and set2 existence and data type
	if s1 != nil && s2 != nil {
		inter = s1.Inter(s2)
	} else {
		inter = []string{}
	}
	dest := set.NewSet()
	for _, value := range inter {
		dest.Add(value)
	}
	db.data.Put(string(args[0]), &database.Entry{Data: dest})
	db.addAof(command.Parts())
	return redis.NewStringArrayCommand(inter)
}

func execSCard(db *SingleDB, command redis.Command) *redis.RespCommand {
	args := command.Args()
	if !ValidateArgCount(command.Name(), len(args)) {
		return redis.NewErrorCommand(redis.CreateWrongArgumentNumberError("SCARD"))
	}
	entry, exists := db.GetEntry(string(args[0]))
	if exists {
		s := entry.Data.(*set.Set)
		return redis.NewNumberCommand(s.Len())
	}
	return redis.NewNumberCommand(0)
}

func execSUnion(db *SingleDB, command redis.Command) *redis.RespCommand {
	args := command.Args()
	if !ValidateArgCount(command.Name(), len(args)) {
		return redis.NewErrorCommand(redis.CreateWrongArgumentNumberError("SUNION"))
	}
	s1, err := getSet(db, string(args[0]))
	if err != nil {
		return redis.NewErrorCommand(err)
	}
	s2, err := getSet(db, string(args[1]))
	if err != nil {
		return redis.NewErrorCommand(err)
	}
	if s1 != nil && s2 != nil {
		union := s1.Union(s2)
		return redis.NewStringArrayCommand(union)
	} else if s1 != nil {
		return redis.NewStringArrayCommand(s1.Members())
	} else if s2 != nil {
		return redis.NewStringArrayCommand(s2.Members())
	} else {
		return redis.EmptyListCommand
	}
}

func getOrCreateSet(db *SingleDB, key string) (*set.Set, error) {
	entry, exists := db.GetEntry(key)
	if !exists {
		s := set.NewSet()
		entry = &database.Entry{Data: s}
		db.data.Put(key, entry)
		return s, nil
	} else {
		if isSet(*entry) {
			return entry.Data.(*set.Set), nil
		}
		return nil, redis.WrongTypeOperationError
	}
}

func getSet(db *SingleDB, key string) (*set.Set, error) {
	entry, exists := db.GetEntry(key)
	if !exists {
		return nil, nil
	} else {
		if isSet(*entry) {
			return entry.Data.(*set.Set), nil
		} else {
			return nil, redis.WrongTypeOperationError
		}
	}
}

func isSet(entry database.Entry) bool {
	return reflect.TypeOf(entry.Data).String() == "*set.Set"
}
