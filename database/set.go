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

func isSet(entry Entry) bool {
	return reflect.TypeOf(entry.Data).String() == "*set.Set"
}
