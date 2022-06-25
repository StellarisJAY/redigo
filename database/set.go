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
	v, exists := db.data.Get(string(args[0]))
	var s *set.Set
	if exists {
		entry := v.(*Entry)
		if !isSet(*entry) {
			return protocol.NewErrorReply(protocol.WrongTypeOperationError)
		}
		s = entry.Data.(*set.Set)
	} else {
		s = set.NewSet()
		db.data.Put(string(args[0]), &Entry{Data: s})
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
	v, exists := db.data.Get(string(args[0]))
	if exists {
		entry := v.(*Entry)
		if !isSet(*entry) {
			return protocol.NewErrorReply(protocol.WrongTypeOperationError)
		}
		s := entry.Data.(*set.Set)
		return protocol.NewNumberReply(s.Has(string(args[1])))
	}
	return protocol.NewNumberReply(0)
}

func execSMembers(db *SingleDB, command redis.Command) *protocol.Reply {
	args := command.Args()
	if len(args) != 1 {
		return protocol.NewErrorReply(protocol.CreateWrongArgumentNumberError("SMEMBERS"))
	}
	v, exists := db.data.Get(string(args[0]))
	if exists {
		entry := v.(*Entry)
		if !isSet(*entry) {
			return protocol.NewErrorReply(protocol.WrongTypeOperationError)
		}
		s := entry.Data.(*set.Set)
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
	v, exists := db.data.Get(string(args[0]))
	if exists {
		entry := v.(*Entry)
		if !isSet(*entry) {
			return protocol.NewErrorReply(protocol.WrongTypeOperationError)
		}
		s := entry.Data.(*set.Set)
		return protocol.NewStringArrayReply(s.RandomMembers(count))
	}
	return protocol.EmptyListReply
}

func execSRem(db *SingleDB, command redis.Command) *protocol.Reply {
	args := command.Args()
	if len(args) < 2 {
		return protocol.NewErrorReply(protocol.CreateWrongArgumentNumberError("LREM"))
	}
	v, exists := db.data.Get(string(args[0]))
	if exists {
		entry := v.(*Entry)
		if !isSet(*entry) {
			return protocol.NewErrorReply(protocol.WrongTypeOperationError)
		}
		s := entry.Data.(*set.Set)
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
	v, exists := db.data.Get(string(args[0]))
	if exists {
		// check if entry is Set data structure
		entry := v.(*Entry)
		if !isSet(*entry) {
			return protocol.NewErrorReply(protocol.WrongTypeOperationError)
		}
		s := entry.Data.(*set.Set)
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
	v1, exists1 := db.data.Get(string(args[0]))
	v2, exists2 := db.data.Get(string(args[1]))
	if exists1 && exists2 {
		entry1 := v1.(*Entry)
		entry2 := v2.(*Entry)
		if !isSet(*entry1) || !isSet(*entry2) {
			return protocol.NewErrorReply(protocol.WrongTypeOperationError)
		}
		s1 := entry1.Data.(*set.Set)
		diff := s1.Diff(entry2.Data.(*set.Set))
		return protocol.NewStringArrayReply(diff)
	} else if exists1 {
		entry1 := v1.(*Entry)
		if !isSet(*entry1) {
			return protocol.NewErrorReply(protocol.WrongTypeOperationError)
		}
		return protocol.NewStringArrayReply(entry1.Data.(*set.Set).Members())
	} else {
		return protocol.EmptyListReply
	}
}

func execSDiffStore(db *SingleDB, command redis.Command) *protocol.Reply {
	args := command.Args()
	if len(args) != 3 {
		return protocol.NewErrorReply(protocol.CreateWrongArgumentNumberError("SDIFF"))
	}
	v1, exists1 := db.data.Get(string(args[1]))
	v2, exists2 := db.data.Get(string(args[2]))
	// check set1 and set2 existence and data type
	var diff []string
	if exists1 && exists2 {
		entry1 := v1.(*Entry)
		entry2 := v2.(*Entry)
		if !isSet(*entry1) || !isSet(*entry2) {
			return protocol.NewErrorReply(protocol.WrongTypeOperationError)
		}
		s1 := entry1.Data.(*set.Set)
		// get diff from s1 and s2
		diff = s1.Diff(entry2.Data.(*set.Set))
	} else if exists1 {
		entry1 := v1.(*Entry)
		if !isSet(*entry1) {
			return protocol.NewErrorReply(protocol.WrongTypeOperationError)
		}
		diff = entry1.Data.(*set.Set).Members()
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
	v1, exists1 := db.data.Get(string(args[0]))
	v2, exists2 := db.data.Get(string(args[1]))
	if exists1 && exists2 {
		entry1 := v1.(*Entry)
		entry2 := v2.(*Entry)
		if !isSet(*entry1) || !isSet(*entry2) {
			return protocol.NewErrorReply(protocol.WrongTypeOperationError)
		}
		s1 := entry1.Data.(*set.Set)
		diff := s1.Inter(entry2.Data.(*set.Set))
		return protocol.NewStringArrayReply(diff)
	} else {
		return protocol.EmptyListReply
	}
}

func execSInterStore(db *SingleDB, command redis.Command) *protocol.Reply {
	args := command.Args()
	if len(args) != 3 {
		return protocol.NewErrorReply(protocol.CreateWrongArgumentNumberError("SINTERSTORE"))
	}
	v1, exists1 := db.data.Get(string(args[1]))
	v2, exists2 := db.data.Get(string(args[2]))

	var inter []string
	// check set1 and set2 existence and data type
	if exists1 && exists2 {
		entry1 := v1.(*Entry)
		entry2 := v2.(*Entry)
		if !isSet(*entry1) || !isSet(*entry2) {
			return protocol.NewErrorReply(protocol.WrongTypeOperationError)
		}
		s1 := entry1.Data.(*set.Set)
		inter = s1.Inter(entry2.Data.(*set.Set))
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
	v, exists := db.data.Get(string(args[0]))
	if exists {
		// check if entry is Set data structure
		entry := v.(*Entry)
		if !isSet(*entry) {
			return protocol.NewErrorReply(protocol.WrongTypeOperationError)
		}
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
	v1, exists1 := db.data.Get(string(args[0]))
	v2, exists2 := db.data.Get(string(args[1]))
	if exists1 && exists2 {
		entry1 := v1.(*Entry)
		entry2 := v2.(*Entry)
		if !isSet(*entry1) || !isSet(*entry2) {
			return protocol.NewErrorReply(protocol.WrongTypeOperationError)
		}
		s1 := entry1.Data.(*set.Set)
		union := s1.Union(entry2.Data.(*set.Set))
		return protocol.NewStringArrayReply(union)
	} else if exists1 {
		entry1 := v1.(*Entry)
		if !isSet(*entry1) {
			return protocol.NewErrorReply(protocol.WrongTypeOperationError)
		}
		return protocol.NewStringArrayReply(entry1.Data.(*set.Set).Members())
	} else if exists2 {
		entry2 := v2.(*Entry)
		if !isSet(*entry2) {
			return protocol.NewErrorReply(protocol.WrongTypeOperationError)
		}
		return protocol.NewStringArrayReply(entry2.Data.(*set.Set).Members())
	} else {
		return protocol.EmptyListReply
	}
}

func isSet(entry Entry) bool {
	return reflect.TypeOf(entry.Data).String() == "*set.Set"
}
