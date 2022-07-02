package database

import (
	"redigo/datastruct/set"
	"redigo/redis"
	"redigo/redis/protocol"
	"reflect"
	"strconv"
)

func init() {
	RegisterCommandExecutor("sadd", execSAdd)
	RegisterCommandExecutor("sismember", execSIsMember)
	RegisterCommandExecutor("smembers", execSMembers)
	RegisterCommandExecutor("srandmember", execSRandomMember)
	RegisterCommandExecutor("srem", execSRem)
	RegisterCommandExecutor("spop", execSPop)
	RegisterCommandExecutor("sdiff", execSDiff)
	RegisterCommandExecutor("sinter", execSInter)
	RegisterCommandExecutor("scard", execSCard)
	RegisterCommandExecutor("sdiffstore", execSDiffStore)
	RegisterCommandExecutor("sinterstore", execSInterStore)
	RegisterCommandExecutor("sunion", execSUnion)
}

func execSAdd(db *SingleDB, command redis.Command) *protocol.Reply {
	args := command.Args()
	if len(args) < 2 {
		return protocol.NewErrorReply(protocol.CreateWrongArgumentNumberError("SADD"))
	}
	s, err := getOrCreateSet(db, string(args[0]))
	if err != nil {
		return protocol.NewErrorReply(err)
	}
	vals := args[1:]
	count := 0
	for _, val := range vals {
		count += s.Add(string(val))
	}
	return protocol.NewNumberReply(count)
}

func execSIsMember(db *SingleDB, command redis.Command) *protocol.Reply {
	args := command.Args()
	if len(args) < 2 {
		return protocol.NewErrorReply(protocol.CreateWrongArgumentNumberError("SISMEMBER"))
	}
	s, err := getSet(db, string(args[0]))
	if err != nil {
		return protocol.NewErrorReply(err)
	}
	if s != nil {
		return protocol.NewNumberReply(s.Has(string(args[1])))
	}
	return protocol.NewNumberReply(0)
}

func execSMembers(db *SingleDB, command redis.Command) *protocol.Reply {
	args := command.Args()
	if len(args) != 1 {
		return protocol.NewErrorReply(protocol.CreateWrongArgumentNumberError("SMEMBERS"))
	}
	s, err := getSet(db, string(args[0]))
	if err != nil {
		return protocol.NewErrorReply(err)
	}
	if s != nil {
		return protocol.NewStringArrayReply(s.Members())
	}
	return protocol.EmptyListReply
}

func execSRandomMember(db *SingleDB, command redis.Command) *protocol.Reply {
	args := command.Args()
	if len(args) != 2 {
		return protocol.NewErrorReply(protocol.CreateWrongArgumentNumberError("SRANDMEMBER"))
	}
	// parse random member count
	count, err := strconv.Atoi(string(args[1]))
	if err != nil {
		return protocol.NewErrorReply(protocol.HashValueNotIntegerError)
	}
	s, err := getSet(db, string(args[0]))
	if err != nil {
		return protocol.NewErrorReply(err)
	}
	if s != nil {
		return protocol.NewStringArrayReply(s.RandomMembers(count))
	}
	return protocol.EmptyListReply
}

func execSRem(db *SingleDB, command redis.Command) *protocol.Reply {
	args := command.Args()
	if len(args) < 2 {
		return protocol.NewErrorReply(protocol.CreateWrongArgumentNumberError("LREM"))
	}
	s, err := getSet(db, string(args[0]))
	if err != nil {
		return protocol.NewErrorReply(err)
	}
	if s != nil {
		values := args[1:]
		count := 0
		for _, value := range values {
			count += s.Remove(string(value))
		}
		return protocol.NewNumberReply(count)
	}
	return protocol.NewNumberReply(0)
}

func execSPop(db *SingleDB, command redis.Command) *protocol.Reply {
	args := command.Args()
	if len(args) != 2 {
		return protocol.NewErrorReply(protocol.CreateWrongArgumentNumberError("SPOP"))
	}
	// parse pop count, check if is integer
	count, err := strconv.Atoi(string(args[1]))
	if err != nil {
		return protocol.NewErrorReply(protocol.HashValueNotIntegerError)
	}
	s, err := getSet(db, string(args[0]))
	if err != nil {
		return protocol.NewErrorReply(err)
	}
	if s != nil {
		members := s.RandomMembersDistinct(count)
		for _, member := range members {
			s.Remove(member)
		}
		return protocol.NewStringArrayReply(members)
	}
	return protocol.EmptyListReply
}

func execSDiff(db *SingleDB, command redis.Command) *protocol.Reply {
	args := command.Args()
	if len(args) != 2 {
		return protocol.NewErrorReply(protocol.CreateWrongArgumentNumberError("SDIFF"))
	}
	s1, err := getSet(db, string(args[0]))
	if err != nil {
		return protocol.NewErrorReply(err)
	}
	s2, err := getSet(db, string(args[1]))
	if err != nil {
		return protocol.NewErrorReply(err)
	}

	if s1 != nil && s2 != nil {
		diff := s1.Diff(s2)
		return protocol.NewStringArrayReply(diff)
	} else if s1 != nil {
		return protocol.NewStringArrayReply(s1.Members())
	} else {
		return protocol.EmptyListReply
	}
}

func execSDiffStore(db *SingleDB, command redis.Command) *protocol.Reply {
	args := command.Args()
	if len(args) != 3 {
		return protocol.NewErrorReply(protocol.CreateWrongArgumentNumberError("SDIFF"))
	}
	s1, err := getSet(db, string(args[0]))
	if err != nil {
		return protocol.NewErrorReply(err)
	}
	s2, err := getSet(db, string(args[1]))
	if err != nil {
		return protocol.NewErrorReply(err)
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
	db.data.Put(string(args[0]), &Entry{Data: dest})
	return protocol.NewStringArrayReply(diff)
}

func execSInter(db *SingleDB, command redis.Command) *protocol.Reply {
	args := command.Args()
	if len(args) != 2 {
		return protocol.NewErrorReply(protocol.CreateWrongArgumentNumberError("SINTER"))
	}
	s1, err := getSet(db, string(args[0]))
	if err != nil {
		return protocol.NewErrorReply(err)
	}
	s2, err := getSet(db, string(args[1]))
	if err != nil {
		return protocol.NewErrorReply(err)
	}
	if s1 != nil && s2 != nil {
		inter := s1.Inter(s2)
		return protocol.NewStringArrayReply(inter)
	} else {
		return protocol.EmptyListReply
	}
}

func execSInterStore(db *SingleDB, command redis.Command) *protocol.Reply {
	args := command.Args()
	if len(args) != 3 {
		return protocol.NewErrorReply(protocol.CreateWrongArgumentNumberError("SINTERSTORE"))
	}
	s1, err := getSet(db, string(args[0]))
	if err != nil {
		return protocol.NewErrorReply(err)
	}
	s2, err := getSet(db, string(args[1]))
	if err != nil {
		return protocol.NewErrorReply(err)
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
	db.data.Put(string(args[0]), &Entry{Data: dest})
	return protocol.NewStringArrayReply(inter)
}

func execSCard(db *SingleDB, command redis.Command) *protocol.Reply {
	args := command.Args()
	if len(args) != 1 {
		return protocol.NewErrorReply(protocol.CreateWrongArgumentNumberError("SCARD"))
	}
	entry, exists := db.getEntry(string(args[0]))
	if exists {
		s := entry.Data.(*set.Set)
		return protocol.NewNumberReply(s.Len())
	}
	return protocol.NewNumberReply(0)
}

func execSUnion(db *SingleDB, command redis.Command) *protocol.Reply {
	args := command.Args()
	if len(args) != 2 {
		return protocol.NewErrorReply(protocol.CreateWrongArgumentNumberError("SINTER"))
	}
	s1, err := getSet(db, string(args[0]))
	if err != nil {
		return protocol.NewErrorReply(err)
	}
	s2, err := getSet(db, string(args[1]))
	if err != nil {
		return protocol.NewErrorReply(err)
	}
	if s1 != nil && s2 != nil {
		union := s1.Union(s2)
		return protocol.NewStringArrayReply(union)
	} else if s1 != nil {
		return protocol.NewStringArrayReply(s1.Members())
	} else if s2 != nil {
		return protocol.NewStringArrayReply(s2.Members())
	} else {
		return protocol.EmptyListReply
	}
}

func getOrCreateSet(db *SingleDB, key string) (*set.Set, error) {
	entry, exists := db.getEntry(key)
	if !exists {
		s := set.NewSet()
		entry = &Entry{Data: s}
		db.data.Put(key, entry)
		return s, nil
	} else {
		if isSet(*entry) {
			return entry.Data.(*set.Set), nil
		}
		return nil, protocol.WrongTypeOperationError
	}
}

func getSet(db *SingleDB, key string) (*set.Set, error) {
	entry, exists := db.getEntry(key)
	if !exists {
		return nil, nil
	} else {
		if isSet(*entry) {
			return entry.Data.(*set.Set), nil
		} else {
			return nil, protocol.WrongTypeOperationError
		}
	}
}

func isSet(entry Entry) bool {
	return reflect.TypeOf(entry.Data).String() == "*set.Set"
}
