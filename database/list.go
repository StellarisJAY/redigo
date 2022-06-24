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
}

func execLPush(db *SingleDB, command redis.Command) *protocol.Reply {
	args := command.Args()
	if len(args) < 2 {
		return protocol.NewErrorReply(protocol.CreateWrongArgumentNumberError("lpush"))
	}
	key := string(args[0])
	v, exist := db.data.Get(key)
	var linkedList *list.LinkedList
	if !exist {
		linkedList = list.NewLinkedList()
		entry := &Entry{Data: linkedList}
		db.data.Put(key, entry)
	} else {
		entry := v.(*Entry)
		if isLinkedList(entry) {
			linkedList = entry.Data.(*list.LinkedList)
		} else {
			return protocol.NewErrorReply(protocol.WrongTypeOperationError)
		}
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
	key := string(args[0])
	v, exist := db.data.Get(key)
	if exist {
		entry := v.(*Entry)
		if isLinkedList(entry) {
			linkedList := entry.Data.(*list.LinkedList)
			left := linkedList.RemoveLeft()
			if left != nil {
				return protocol.NewBulkValueReply(left)
			} else {
				return protocol.NilReply
			}
		} else {
			return protocol.NewErrorReply(protocol.WrongTypeOperationError)
		}
	}
	return protocol.NilReply
}

func execRPush(db *SingleDB, command redis.Command) *protocol.Reply {
	args := command.Args()
	if len(args) < 2 {
		return protocol.NewErrorReply(protocol.CreateWrongArgumentNumberError("rpush"))
	}
	key := string(args[0])
	v, exists := db.data.Get(key)
	var linkedList *list.LinkedList
	if exists {
		entry := v.(*Entry)
		if !isLinkedList(entry) {
			return protocol.NewErrorReply(protocol.WrongTypeOperationError)
		}
		linkedList = entry.Data.(*list.LinkedList)
	} else {
		linkedList = list.NewLinkedList()
		entry := &Entry{Data: linkedList}
		db.data.Put(key, entry)
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
	key := string(args[0])
	v, exists := db.data.Get(key)
	if exists {
		entry := v.(*Entry)
		// check if entry is linked list
		if !isLinkedList(entry) {
			return protocol.NewErrorReply(protocol.WrongTypeOperationError)
		}
		linkedList := entry.Data.(*list.LinkedList)
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
	key := string(args[0])
	// parse start index and end index
	start, err1 := strconv.Atoi(string(args[1]))
	end, err2 := strconv.Atoi(string(args[2]))
	if err1 != nil || err2 != nil {
		return protocol.NewErrorReply(protocol.ValueNotIntegerOrOutOfRangeError)
	}

	v, exists := db.data.Get(key)
	if !exists {
		return protocol.EmptyListReply
	}
	entry := v.(*Entry)
	// check key dataStructure
	if !isLinkedList(entry) {
		return protocol.NewErrorReply(protocol.WrongTypeOperationError)
	}
	linkedList := entry.Data.(*list.LinkedList)
	if start < 0 {
		start = linkedList.Size() + start
	}
	if end < 0 {
		end = linkedList.Size() + end
	}
	if result := linkedList.LeftRange(start, end); result != nil {
		return protocol.NewArrayReply(result)
	}
	return protocol.EmptyListReply
}

func execLIndex(db *SingleDB, command redis.Command) *protocol.Reply {
	args := command.Args()
	if len(args) != 2 {
		return protocol.NewErrorReply(protocol.CreateWrongArgumentNumberError("LINDEX"))
	}
	key := string(args[0])
	// check index arg
	index, err := strconv.Atoi(string(args[1]))
	if err != nil {
		return protocol.NewErrorReply(protocol.ValueNotIntegerOrOutOfRangeError)
	}
	// get linked list data structure
	v, exists := db.data.Get(key)
	if !exists {
		return protocol.NilReply
	}
	entry := v.(*Entry)
	// check if entry is linked list
	if !isLinkedList(entry) {
		return protocol.NewErrorReply(protocol.WrongTypeOperationError)
	}
	linkedList := entry.Data.(*list.LinkedList)
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

func isLinkedList(entry *Entry) bool {
	return reflect.TypeOf(entry.Data).String() == "*list.LinkedList"
}
