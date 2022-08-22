package database

import (
	"redigo/datastruct/list"
	"redigo/interface/database"
	"redigo/redis"
	"reflect"
	"strconv"
)

func init() {
	RegisterCommandExecutor("lpush", execLPush, -2)
	RegisterCommandExecutor("lpop", execLPop, 1)
	RegisterCommandExecutor("rpush", execRPush, -2)
	RegisterCommandExecutor("rpop", execRPop, 1)
	RegisterCommandExecutor("lrange", execLRange, 3)
	RegisterCommandExecutor("lindex", execLIndex, 2)
	RegisterCommandExecutor("llen", execLLen, 1)
	RegisterCommandExecutor("rpoplpush", execRPopLPush, 2)
}

func execLPush(db *SingleDB, command redis.Command) *redis.RespCommand {
	args := command.Args()
	if !ValidateArgCount(command.Name(), len(args)) {
		return redis.NewErrorCommand(redis.CreateWrongArgumentNumberError("lpush"))
	}
	linkedList, err := getOrInitLinkedList(db, string(args[0]))
	if err != nil {
		return redis.NewErrorCommand(err)
	}
	for _, arg := range args[1:] {
		linkedList.AddLeft(arg)
	}
	db.addAof(command.Parts())
	return redis.NewNumberCommand(linkedList.Size())
}

func execLPop(db *SingleDB, command redis.Command) *redis.RespCommand {
	args := command.Args()
	if !ValidateArgCount(command.Name(), len(args)) {
		return redis.NewErrorCommand(redis.CreateWrongArgumentNumberError("lpush"))
	}
	linkedList, err := getLinkedList(db, string(args[0]))
	if err != nil {
		return redis.NewErrorCommand(err)
	}
	if linkedList != nil {
		left := linkedList.RemoveLeft()
		if left != nil {
			db.addAof(command.Parts())
			return redis.NewBulkStringCommand(left)
		} else {
			return redis.NilCommand
		}
	}
	return redis.NilCommand
}

func execRPush(db *SingleDB, command redis.Command) *redis.RespCommand {
	args := command.Args()
	if !ValidateArgCount(command.Name(), len(args)) {
		return redis.NewErrorCommand(redis.CreateWrongArgumentNumberError("rpush"))
	}
	linkedList, err := getOrInitLinkedList(db, string(args[0]))
	if err != nil {
		return redis.NewErrorCommand(err)
	}
	for _, arg := range args[1:] {
		linkedList.AddRight(arg)
	}
	db.addAof(command.Parts())
	return redis.NewNumberCommand(linkedList.Size())
}

func execRPop(db *SingleDB, command redis.Command) *redis.RespCommand {
	args := command.Args()
	if !ValidateArgCount(command.Name(), len(args)) {
		return redis.NewErrorCommand(redis.CreateWrongArgumentNumberError("rpop"))
	}
	linkedList, err := getLinkedList(db, string(args[0]))
	if err != nil {
		return redis.NewErrorCommand(err)
	}
	if linkedList != nil {
		// pop right element
		right := linkedList.RemoveRight()
		if right != nil {
			db.addAof(command.Parts())
			return redis.NewBulkStringCommand(right)
		}
	}
	return redis.NilCommand
}

func execLRange(db *SingleDB, command redis.Command) *redis.RespCommand {
	args := command.Args()
	if !ValidateArgCount(command.Name(), len(args)) {
		return redis.NewErrorCommand(redis.CreateWrongArgumentNumberError("lrange"))
	}
	// parse start index and end index
	start, err1 := strconv.Atoi(string(args[1]))
	end, err2 := strconv.Atoi(string(args[2]))
	if err1 != nil || err2 != nil {
		return redis.NewErrorCommand(redis.ValueNotIntegerOrOutOfRangeError)
	}

	linkedList, err := getLinkedList(db, string(args[0]))
	if err != nil {
		return redis.NewErrorCommand(err)
	}
	if linkedList != nil {
		if start < 0 {
			start = linkedList.Size() + start
		}
		if end < 0 {
			end = linkedList.Size() + end
		}
		if result := linkedList.LeftRange(start, end); result != nil {
			return redis.NewArrayCommand(result)
		}
	}
	return redis.EmptyListCommand
}

func execLIndex(db *SingleDB, command redis.Command) *redis.RespCommand {
	args := command.Args()
	if !ValidateArgCount(command.Name(), len(args)) {
		return redis.NewErrorCommand(redis.CreateWrongArgumentNumberError("LINDEX"))
	}
	// check index arg
	index, err := strconv.Atoi(string(args[1]))
	if err != nil {
		return redis.NewErrorCommand(redis.ValueNotIntegerOrOutOfRangeError)
	}
	// get linked list data structure
	linkedList, err := getLinkedList(db, string(args[0]))
	if err != nil {
		return redis.NewErrorCommand(err)
	}
	if linkedList != nil {
		// set index to positive value
		if index < 0 {
			index = linkedList.Size() + index
		}
		// out of range
		if index >= linkedList.Size() {
			return redis.NewErrorCommand(redis.ValueNotIntegerOrOutOfRangeError)
		}
		return redis.NewBulkStringCommand(linkedList.Get(index))
	}
	return redis.NilCommand
}

func execLLen(db *SingleDB, command redis.Command) *redis.RespCommand {
	args := command.Args()
	if !ValidateArgCount(command.Name(), len(args)) {
		return redis.NewErrorCommand(redis.CreateWrongArgumentNumberError("LLEN"))
	}
	// get linked list data structure
	linkedList, err := getLinkedList(db, string(args[0]))
	if err != nil {
		return redis.NewErrorCommand(err)
	}
	if linkedList != nil {
		return redis.NewNumberCommand(linkedList.Size())
	}
	return redis.NewNumberCommand(0)
}

func execRPopLPush(db *SingleDB, command redis.Command) *redis.RespCommand {
	args := command.Args()
	if !ValidateArgCount(command.Name(), len(args)) {
		return redis.NewErrorCommand(redis.CreateWrongArgumentNumberError("RPopLPush"))
	}
	srcList, err1 := getLinkedList(db, string(args[0]))
	if err1 != nil {
		return redis.NewErrorCommand(err1)
	}
	if srcList == nil || srcList.Size() == 0 {
		return redis.NilCommand
	}
	destList, err2 := getOrInitLinkedList(db, string(args[1]))
	if err2 != nil {
		return redis.NewErrorCommand(err2)
	}
	// pop src right element, put into dest left
	element := srcList.RemoveRight()
	if element != nil {
		db.addAof(command.Parts())
		destList.AddLeft(element)
	}
	return redis.NewBulkStringCommand(element)
}

func isLinkedList(entry *database.Entry) bool {
	return reflect.TypeOf(entry.Data).String() == "*list.LinkedList"
}

func getOrInitLinkedList(db *SingleDB, key string) (*list.LinkedList, error) {
	entry, exists := db.GetEntry(key)
	var linkedList *list.LinkedList
	if !exists {
		linkedList = list.NewLinkedList()
		entry = &database.Entry{Data: linkedList}
		db.data.Put(key, entry)
		return linkedList, nil
	} else {
		if isLinkedList(entry) {
			linkedList = entry.Data.(*list.LinkedList)
			return linkedList, nil
		} else {
			return nil, redis.WrongTypeOperationError
		}
	}
}

func getLinkedList(db *SingleDB, key string) (*list.LinkedList, error) {
	entry, exists := db.GetEntry(key)
	var linkedList *list.LinkedList
	if !exists {
		return nil, nil
	} else {
		if isLinkedList(entry) {
			linkedList = entry.Data.(*list.LinkedList)
			return linkedList, nil
		} else {
			return nil, redis.WrongTypeOperationError
		}
	}
}
