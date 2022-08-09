package database

import (
	"errors"
	"log"
	"redigo/config"
	"redigo/datastruct/dict"
	"redigo/datastruct/lock"
	"redigo/interface/database"
	"redigo/interface/redis"
	"redigo/rdb"
	"redigo/redis/protocol"
	"redigo/util/timewheel"
	"time"
)

type SingleDB struct {
	data       dict.Dict
	ttlMap     dict.Dict
	lock       *lock.Locker
	idx        int
	addAof     func([][]byte)
	versionMap dict.Dict

	lruHead *database.Entry
	lruTail *database.Entry

	maxMemory  int64
	usedMemory int64
}

func NewSingleDB(idx int) *SingleDB {
	db := &SingleDB{
		data:       dict.NewSimpleDict(),
		ttlMap:     dict.NewSimpleDict(),
		lock:       lock.NewLock(1024),
		idx:        idx,
		versionMap: dict.NewSimpleDict(),
		addAof:     func(i [][]byte) {},
		lruHead:    &database.Entry{},
		lruTail:    &database.Entry{},
	}
	db.lruHead.NextLRUEntry = db.lruTail
	db.lruTail.PrevLRUEntry = db.lruHead
	db.maxMemory = config.Properties.MaxMemory
	return db
}

func (db *SingleDB) ExecuteLoop() error {
	panic(errors.New("unsupported operation"))
}

func (db *SingleDB) SubmitCommand(command redis.Command) {
	panic(errors.New("unsupported operation"))
}

/*
	Execute a command
	Finds the executor in executor map, then call execFunc to handle it
*/
func (db *SingleDB) Execute(command redis.Command) *protocol.Reply {
	cmd := command.Name()
	if cmd == "keys" {
		// get all keys from db, but don't match pattern now
		keys := db.data.Keys()
		// start a new goroutine to do pattern matching
		go func(command redis.Command, keys []string) {
			reply := execKeys(db, command, keys)
			command.Connection().SendReply(reply)
		}(command, keys)
		return nil
	} else {
		exec, exists := executors[cmd]
		if exists {
			reply := exec.execFunc(db, command)
			return reply
		} else {
			// command executor doesn't exist, send unknown command to client
			return protocol.NewErrorReply(protocol.CreateUnknownCommandError(cmd))
		}
	}
}

func (db *SingleDB) ForEach(dbIdx int, fun func(key string, entry *database.Entry, expire *time.Time) bool) {
	db.data.ForEach(func(key string, value interface{}) bool {
		entry := value.(*database.Entry)
		ttl, ok := db.ttlMap.Get(key)
		if ok {
			return fun(key, entry, ttl.(*time.Time))
		} else {
			return fun(key, entry, nil)
		}
	})
}

func (db *SingleDB) Len(dbIdx int) int {
	return db.data.Len()
}

func (db *SingleDB) OnConnectionClosed(conn redis.Connection) {

}

// Expire set a key's expire time
func (db *SingleDB) Expire(key string, ttl time.Duration) {
	expireTime := time.Now().Add(ttl)
	db.ttlMap.Put(key, &expireTime)
	// if server enabled scheduler for expiring
	if config.Properties.UseScheduleExpire {
		// schedule auto remove in time wheel
		timewheel.ScheduleDelayed(ttl, "expire_"+key, func() {
			_, exists := db.ttlMap.Get(key)
			if !exists {
				return
			}
			db.ttlMap.Remove(key)
			db.data.Remove(key)
			// add delete key aof
			db.addAof([][]byte{[]byte("del"), []byte(key)})
			log.Println("Expired Key removed: ", key)
		})
	}
}

func (db *SingleDB) ExpireAt(key string, expire *time.Time) {
	db.ttlMap.Put(key, expire)
	// if server enabled scheduler for expiring
	if config.Properties.UseScheduleExpire {
		ttl := expire.Sub(time.Now())
		// schedule auto remove in time wheel
		timewheel.ScheduleDelayed(ttl, "expire_"+key, func() {
			_, exists := db.ttlMap.Get(key)
			if !exists {
				return
			}
			db.ttlMap.Remove(key)
			db.data.Remove(key)
			log.Println("Expired Key removed: ", key)
		})
	}
}

func (db *SingleDB) TTL(key string) time.Duration {
	v, exists := db.ttlMap.Get(key)
	if exists {
		expireTime := v.(*time.Time)
		ttl := expireTime.Sub(time.Now())
		if ttl < 0 {
			return 0
		}
		return ttl
	}
	return -1
}

func (db *SingleDB) CancelTTL(key string) int {
	_, exists := db.ttlMap.Get(key)
	if exists {
		db.ttlMap.Remove(key)
		if config.Properties.UseScheduleExpire {
			timewheel.Cancel("expire_" + key)
		}
		return 1
	}
	return 0
}

// Check if key expired, remove key if necessary
func (db *SingleDB) expireIfNeeded(key string) bool {
	v, ok := db.ttlMap.Get(key)
	if !ok {
		return false
	}
	expireAt := v.(*time.Time)
	if expireAt.Before(time.Now()) {
		// remove key
		db.data.Remove(key)
		// remove the scheduler task for key's ttl
		if config.Properties.UseScheduleExpire {
			db.CancelTTL(key)
		}
		// add delete key to aof
		db.addAof([][]byte{[]byte("del"), []byte(key)})
		log.Println("Lazy expire key: ", key)
		return true
	}
	return false
}

// get the data entry holding the key's value. Checking key's existence and expire time
func (db *SingleDB) getEntry(key string) (*database.Entry, bool) {
	v, ok := db.data.Get(key)
	if !ok || db.expireIfNeeded(key) {
		return nil, false
	}
	entry := v.(*database.Entry)
	// get触发将数据移动到LRU队列尾部
	db.lruMoveEntryToTail(entry)
	return entry, true
}

func (db *SingleDB) addVersion(key string) {
	v, ok := db.versionMap.Get(key)
	if ok {
		db.versionMap.Put(key, v.(int64)+1)
	} else {
		db.versionMap.Put(key, int64(1))
	}
}

func (db *SingleDB) getVersion(key string) int64 {
	v, ok := db.versionMap.Get(key)
	if !ok {
		return -1
	}
	return v.(int64)
}

func (db *SingleDB) flushDB(async bool) {
	if !async {
		db.data.Clear()
	} else {
		keys := db.data.Keys()
		go func(keys []string) {
			for _, key := range keys {
				db.data.Remove(key)
			}
		}(keys)
	}
}

// Rename key, returns error if key doesn't exist
func (db *SingleDB) Rename(old, key string) error {
	entry, exists := db.getEntry(old)
	if !exists {
		return protocol.NoSuchKeyError
	}
	// remove old key, put new key
	db.data.Remove(old)
	db.data.Put(key, entry)
	return nil
}

func (db *SingleDB) RenameNX(oldKey, newKey string) (int, error) {
	entry, exists := db.getEntry(oldKey)
	if !exists {
		return 0, protocol.NoSuchKeyError
	}
	_, exists = db.getEntry(newKey)
	if exists {
		return 0, nil
	}
	// remove old key, put new key
	db.data.Remove(oldKey)
	db.data.Put(newKey, entry)
	return 1, nil
}

func (db *SingleDB) Close() {
	//TODO implement me
	panic("implement me")
}

func (db *SingleDB) randomKeys(samples int) []string {
	keys := db.data.RandomKeysDistinct(samples)
	return keys
}

func (db *SingleDB) Dump(key string) ([]byte, error) {
	entry, exists := db.getEntry(key)
	if !exists {
		return nil, nil
	}
	return rdb.SerializeEntry(key, entry)
}

// lruMoveEntryToTail 将entry移动到LRU队列尾部
func (db *SingleDB) lruMoveEntryToTail(entry *database.Entry) {
	db.lruRemoveEntry(entry)
	db.lruAddEntry(entry)
}

// lruRemoveEntry 从LRU队列删除某个entry
func (db *SingleDB) lruRemoveEntry(entry *database.Entry) {
	entry.NextLRUEntry.PrevLRUEntry = entry.PrevLRUEntry
	entry.PrevLRUEntry.NextLRUEntry = entry.NextLRUEntry
	entry.NextLRUEntry = nil
	entry.PrevLRUEntry = nil
}

// lruAddEntry 在LRU队列尾部添加entry
func (db *SingleDB) lruAddEntry(entry *database.Entry) {
	entry.PrevLRUEntry = db.lruTail.PrevLRUEntry
	entry.NextLRUEntry = db.lruTail
	db.lruTail.PrevLRUEntry.NextLRUEntry = entry
	db.lruTail.PrevLRUEntry = entry
}

// evict 内存淘汰，直到已占用内存达到小于等于目标值
func (db *SingleDB) evict(targetMemory int64) {
	for targetMemory < db.usedMemory {
		// 如果lru队列已经没有entry了
		if entry := db.lruHead.NextLRUEntry; entry == db.lruTail {
			break
		} else {
			db.lruRemoveEntry(entry)
			db.usedMemory -= entry.DataSize
			db.data.Remove(entry.Key)
			log.Println("evict entry, key: ", entry.Key, ", value size: ", entry.DataSize, "bytes")
		}
	}
}

func (db *SingleDB) putEntry(entry *database.Entry) int {
	// 放入新的数据前，先进行内存淘汰
	db.evict(db.maxMemory - entry.DataSize)
	result := db.data.Put(entry.Key, entry)
	if result != 0 {
		db.lruAddEntry(entry)
		db.usedMemory += entry.DataSize
	}
	return result
}

func (db *SingleDB) putOrUpdateEntry(entry *database.Entry) int {
	if old, ok := db.data.Get(entry.Key); ok {
		oldEntry := old.(*database.Entry)
		db.updateEntry(oldEntry, entry.Data.([]byte))
		return 1
	} else {
		return db.putEntry(entry)
	}
}

func (db *SingleDB) putIfAbsent(entry *database.Entry) int {
	if _, ok := db.data.Get(entry.Key); ok {
		return 0
	} else {
		return db.putEntry(entry)
	}
}

// putIfExists key存在的情况下更新value
func (db *SingleDB) putIfExists(key string, value []byte) int {
	if v, ok := db.data.Get(key); !ok {
		return 0
	} else if entry, ok := v.(*database.Entry); ok {
		entry.Data = value
		oldSize := entry.DataSize
		entry.DataSize = int64(len(value))
		db.lruMoveEntryToTail(entry)
		db.evict(db.maxMemory - entry.DataSize)
		db.usedMemory += entry.DataSize
		db.usedMemory -= oldSize
	}
	return 0
}

// updateEntry 更新entry中的值，该方法只能由于字符串类型的value
func (db *SingleDB) updateEntry(entry *database.Entry, value []byte) {
	entry.Data = value
	oldSize := entry.DataSize
	entry.DataSize = int64(len(value))
	// 先更新数据，然后再淘汰内存
	db.lruMoveEntryToTail(entry)
	db.evict(db.maxMemory - entry.DataSize + oldSize)
	db.usedMemory += entry.DataSize
	db.usedMemory -= oldSize
}
