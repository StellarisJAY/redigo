package database

import (
	"redigo/datastruct/list"
	"redigo/redis"
	"redigo/redis/protocol"
	"reflect"
	"strconv"
)

func init() {
	RegisterCommandExecutor("lpush", execLPush)
	RegisterCommandExecutor("lpop", execLPop)
	RegisterCommandExecutor("rpush", execRPush)
	RegisterCommandExecutor("rpop", execRPop)
	RegisterCommandExecutor("lrange", execLRange)
	RegisterCommandExecutor("lindex", execLIndex)
	RegisterCommandExecutor("llen", execLLen)
	RegisterCommandExecutor("rpoplpush", execRPopLPush)
}

func execLPush(db *SingleDB, command redis.Command) *protocol.Reply {
	args := command.Args()
	if len(args) < 2 {
		return protocol.NewErrorReply(protocol.CreateWrongArgumentNumberError("lpush"))
	}
	linkedList, err := getOrInitLinkedList(db, string(args[0]))
	if err != nil {
		return protocol.NewErrorReply(err)
	}
	for _, arg := range args[1:] {
		linkedList.AddLeft(arg)
	}
	return protocol.NewNumberReply(linkedList.Size())
}

func execLPop(db *SingleDB, command redis.Command) *protocol.Reply {
	args := command.Args()
	if len(args) != 1 {
		return protocol.NewErrorReply(protocol.CreateWrongArgumentNumberError("lpush"))
	}
	linkedList, err := getLinkedList(db, string(args[0]))
	if err != nil {
		return protocol.NewErrorReply(err)
	}
	if linkedList != nil {
		left := linkedList.RemoveLeft()
		if left != nil {
			return protocol.NewBulkValueReply(left)
		} else {
			return protocol.NilReply
		}
	}
	return protocol.NilReply
}

func execRPush(db *SingleDB, command redis.Command) *protocol.Reply {
	args := command.Args()
	if len(args) < 2 {
		return protocol.NewErrorReply(protocol.CreateWrongArgumentNumberError("rpush"))
	}
	linkedList, err := getOrInitLinkedList(db, string(args[0]))
	if err != nil {
		return protocol.NewErrorReply(err)
	}
	for _, arg := range args[1:] {
		linkedList.AddRight(arg)
	}
	return protocol.NewNumberReply(linkedList.Size())
}

func execRPop(db *SingleDB, command redis.Command) *protocol.Reply {
	args := command.Args()
	if len(args) != 1 {
		return protocol.NewErrorReply(protocol.CreateWrongArgumentNumberError("rpop"))
	}
	linkedList, err := getLinkedList(db, string(args[0]))
	if err != nil {
		return protocol.NewErrorReply(err)
	}
	if linkedList != nil {
		// pop right element
		right := linkedList.RemoveRight()
		if right != nil {
			return protocol.NewBulkValueReply(right)
		}
	}
	return protocol.NilReply
}

func execLRange(db *SingleDB, command redis.Command) *protocol.Reply {
	args := command.Args()
	if len(args) != 3 {
		return protocol.NewErrorReply(protocol.CreateWrongArgumentNumberError("lrange"))
	}
	// parse start index and end index
	start, err1 := strconv.Atoi(string(args[1]))
	end, err2 := strconv.Atoi(string(args[2]))
	if err1 != nil || err2 != nil {
		return protocol.NewErrorReply(protocol.ValueNotIntegerOrOutOfRangeError)
	}

	linkedList, err := getLinkedList(db, string(args[0]))
	if err != nil {
		return protocol.NewErrorReply(err)
	}
	if linkedList != nil {
		if start < 0 {
			start = linkedList.Size() + start
		}
		if end < 0 {
			end = linkedList.Size() + end
		}
		if result := linkedList.LeftRange(start, end); result != nil {
			return protocol.NewArrayReply(result)
		}
	}
	return protocol.EmptyListReply
}

func execLIndex(db *SingleDB, command redis.Command) *protocol.Reply {
	args := command.Args()
	if len(args) != 2 {
		return protocol.NewErrorReply(protocol.CreateWrongArgumentNumberError("LINDEX"))
	}
	// check index arg
	index, err := strconv.Atoi(string(args[1]))
	if err != nil {
		return protocol.NewErrorReply(protocol.ValueNotIntegerOrOutOfRangeError)
	}
	// get linked list data structure
	linkedList, err := getLinkedList(db, string(args[0]))
	if err != nil {
		return protocol.NewErrorReply(err)
	}
	if linkedList != nil {
		// set index to positive value
		if index < 0 {
			index = linkedList.Size() + index
		}
		// out of range
		if index >= linkedList.Size() {
			return protocol.NewErrorReply(protocol.ValueNotIntegerOrOutOfRangeError)
		}
		return protocol.NewBulkValueReply(linkedList.Get(index))
	}
	return protocol.NilReply
}

func execLLen(db *SingleDB, command redis.Command) *protocol.Reply {
	args := command.Args()
	if len(args) != 1 {
		return protocol.NewErrorReply(protocol.CreateWrongArgumentNumberError("LLEN"))
	}
	// get linked list data structure
	linkedList, err := getLinkedList(db, string(args[0]))
	if err != nil {
		return protocol.NewErrorReply(err)
	}
	if linkedList != nil {
		return protocol.NewNumberReply(linkedList.Size())
	}
	return protocol.NewNumberReply(0)
}

func execRPopLPush(db *SingleDB, command redis.Command) *protocol.Reply {
	args := command.Args()
	if len(args) != 2 {
		return protocol.NewErrorReply(protocol.CreateWrongArgumentNumberError("RPopLPush"))
	}
	srcList, err1 := getLinkedList(db, string(args[0]))
	if err1 != nil {
		return protocol.NewErrorReply(err1)
	}
	if srcList == nil || srcList.Size() == 0 {
		return protocol.NilReply
	}
	destList, err2 := getOrInitLinkedList(db, string(args[1]))
	if err2 != nil {
		return protocol.NewErrorReply(err2)
	}
	// pop src right element, put into dest left
	element := srcList.RemoveRight()
	destList.AddLeft(element)
	return protocol.NewBulkValueReply(element)
}

func isLinkedList(entry *Entry) bool {
	return reflect.TypeOf(entry.Data).String() == "*list.LinkedList"
}

func getOrInitLinkedList(db *SingleDB, key string) (*list.LinkedList, error) {
	entry, exists := db.getEntry(key)
	var linkedList *list.LinkedList
	if !exists {
		linkedList = list.NewLinkedList()
		entry = &Entry{Data: linkedList}
		db.data.Put(key, entry)
		return linkedList, nil
	} else {
		if isLinkedList(entry) {
			linkedList = entry.Data.(*list.LinkedList)
			return linkedList, nil
		} else {
			return nil, protocol.WrongTypeOperationError
		}
	}
}

func getLinkedList(db *SingleDB, key string) (*list.LinkedList, error) {
	entry, exists := db.getEntry(key)
	var linkedList *list.LinkedList
	if !exists {
		return nil, nil
	} else {
		if isLinkedList(entry) {
			linkedList = entry.Data.(*list.LinkedList)
			return linkedList, nil
		} else {
			return nil, protocol.WrongTypeOperationError
		}
	}
}
