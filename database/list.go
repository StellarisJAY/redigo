package database

import (
	"log"
	"redigo/datastruct/list"
	"redigo/redis"
	"redigo/redis/protocol"
	"reflect"
)

func init() {
	RegisterCommandExecutor("lpush", execLPush)
	RegisterCommandExecutor("lpop", execLPop)
	RegisterCommandExecutor("rpush", execRPush)
	RegisterCommandExecutor("rpop", execRPop)
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
		if reflect.TypeOf(entry.Data).String() == "*list.LinkedList" {
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
		if reflect.TypeOf(entry.Data).String() == "*list.LinkedList" {
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
		if reflect.TypeOf(entry.Data).String() != "*list.LinkedList" {
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
		if reflect.TypeOf(entry.Data).String() != "*list.LinkedList" {
			log.Println("Type of entry: ", reflect.TypeOf(entry.Data).String())
			return protocol.NewErrorReply(protocol.WrongTypeOperationError)
		}
		linkedList := entry.Data.(*list.LinkedList)
		right := linkedList.RemoveRight()
		if right != nil {
			return protocol.NewBulkValueReply(right)
		}
	}
	return protocol.NilReply
}
