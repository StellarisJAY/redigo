package database

import (
	"errors"
	"log"
	"redigo/config"
	"redigo/datastruct/dict"
	"redigo/datastruct/lock"
	"redigo/interface/database"
	"redigo/rdb"
	"redigo/redis"
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

	lru LRU
}

func NewSingleDB(idx int) *SingleDB {
	maxMemory := config.Properties.MaxMemory
	db := &SingleDB{
		data:       dict.NewSimpleDict(),
		ttlMap:     dict.NewSimpleDict(),
		lock:       lock.NewLock(1024),
		idx:        idx,
		versionMap: dict.NewSimpleDict(),
		addAof:     func(i [][]byte) {},
	}
	if maxMemory == -1 {
		db.lru = &NoLRU{}
	} else {
		// todo 最佳的lru-k策略？
		db.lru = NewTwoQueueLRU(maxMemory, maxMemory/3, 3, db.onKeyEvict)
	}
	//db.lruHead.NextLRUEntry = db.lruTail
	//db.lruTail.PrevLRUEntry = db.lruHead
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
func (db *SingleDB) Execute(command redis.Command) *redis.RespCommand {
	cmd := command.Name()
	if cmd == "keys" {
		// get all keys from db, but don't match pattern now
		keys := db.data.Keys()
		// start a new goroutine to do pattern matching
		go func(command redis.Command, keys []string) {
			reply := execKeys(db, command, keys)
			command.Connection().SendCommand(reply)
		}(command, keys)
		return nil
	} else {
		exec, exists := executors[cmd]
		if exists {
			reply := exec.execFunc(db, command)
			return reply
		} else {
			// command executor doesn't exist, send unknown command to client
			return redis.NewErrorCommand(redis.CreateUnknownCommandError(cmd))
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

// GetEntry 获取一个Key的Entry，获取的同时检查TTL，并进行LRU
func (db *SingleDB) GetEntry(key string) (*database.Entry, bool) {
	v, ok := db.data.Get(key)
	if !ok || db.expireIfNeeded(key) {
		return nil, false
	}
	entry := v.(*database.Entry)
	// get触发将数据移动到LRU队列尾部
	//db.lruMoveEntryToTail(entry)
	db.lru.addAccessHistory(entry, entry.DataSize)
	return entry, true
}

// DeleteEntry 删除一个key，并将key关联的LRU、TTL删除
func (db *SingleDB) DeleteEntry(key string) (*database.Entry, bool) {
	if entry, ok := db.GetEntry(key); !ok {
		return nil, false
	} else {
		db.data.Remove(key)
		db.ttlMap.Remove(key)
		db.versionMap.Remove(key)
		//db.lruRemoveEntry(entry)
		db.lru.removeEntry(entry)
		return entry, true
	}
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
		// 获取当前存在的所有key
		keys := db.data.Keys()
		// 开启goroutine删除每一个key
		go func(keys []string) {
			for _, key := range keys {
				db.data.Remove(key)
			}
		}(keys)
	}
}

// Rename key, returns error if key doesn't exist
func (db *SingleDB) Rename(old, key string) error {
	entry, exists := db.GetEntry(old)
	if !exists {
		return redis.NoSuchKeyError
	}
	// remove old key, put new key
	db.data.Remove(old)
	db.data.Put(key, entry)
	return nil
}

func (db *SingleDB) RenameNX(oldKey, newKey string) (int, error) {
	entry, exists := db.GetEntry(oldKey)
	if !exists {
		return 0, redis.NoSuchKeyError
	}
	_, exists = db.GetEntry(newKey)
	if exists {
		return 0, nil
	}
	// remove old key, put new key
	db.data.Remove(oldKey)
	db.data.Put(newKey, entry)
	return 1, nil
}

func (db *SingleDB) Close() {

}

// randomKeys 获取 samples 个数的随机keys
func (db *SingleDB) randomKeys(samples int) []string {
	keys := db.data.RandomKeysDistinct(samples)
	return keys
}

func (db *SingleDB) Dump(key string) ([]byte, error) {
	entry, exists := db.GetEntry(key)
	if !exists {
		return nil, nil
	}
	return rdb.SerializeEntry(key, entry)
}

// lruMoveEntryToTail 将entry移动到LRU队列尾部
func (db *SingleDB) lruMoveEntryToTail(entry *database.Entry) {
	//if db.memCounter.maxMemory != -1 {
	//	db.lruRemoveEntry(entry)
	//	db.lruAddEntry(entry)
	//}
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
	//entry.PrevLRUEntry = db.lruTail.PrevLRUEntry
	//entry.NextLRUEntry = db.lruTail
	//db.lruTail.PrevLRUEntry.NextLRUEntry = entry
	//db.lruTail.PrevLRUEntry = entry
}

// evict 内存淘汰，直到已占用内存达到小于等于目标值
func (db *SingleDB) evict(targetMemory int64) {
	//for targetMemory < db.memCounter.usedMemory {
	//	// 如果lru队列已经没有entry了
	//	if entry := db.lruHead.NextLRUEntry; entry == db.lruTail {
	//		break
	//	} else {
	//		// volatile-lru，跳过没有超时时间的key
	//		if config.Properties.EvictPolicy == config.EvictVolatileLRU {
	//			if _, ok := db.ttlMap.Get(entry.Key); ok {
	//				continue
	//			}
	//		}
	//		db.lruRemoveEntry(entry)
	//		//db.memCounter.usedMemory -= entry.DataSize
	//		db.data.Remove(entry.Key)
	//		log.Println("evict entry, key: ", entry.Key, ", value size: ", entry.DataSize, "bytes")
	//	}
	//}
}

// freeMemoryIfNeeded 如果开启了最大内存配置，该方法会进行内存淘汰
func (db *SingleDB) freeMemoryIfNeeded(targetMemory int64) {
	//if db.memCounter.maxMemory == -1 {
	//	return
	//}
	//db.evict(targetMemory)
}

// putEntry 添加新的Entry，添加前进行内存淘汰
func (db *SingleDB) putEntry(entry *database.Entry) int {

	// 放入新的数据前，先进行内存淘汰
	//db.freeMemoryIfNeeded(db.memCounter.maxMemory - entry.DataSize)
	result := db.data.Put(entry.Key, entry)
	if result != 0 {
		db.lru.addEntry(entry)
		//db.lruAddEntry(entry)
		//db.memCounter.usedMemory += entry.DataSize
	}
	return result
}

// putOrUpdateEntry 添加新的Entry或者更新entry的值，该操作会导致LRU
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
		db.lru.addAccessHistory(entry, oldSize)
		//db.lruMoveEntryToTail(entry)
		//db.freeMemoryIfNeeded(db.memCounter.maxMemory - entry.DataSize)
		//db.memCounter.usedMemory += entry.DataSize
		//db.memCounter.usedMemory -= oldSize
	}
	return 0
}

// updateEntry 更新entry中的值，该方法只能由于字符串类型的value
func (db *SingleDB) updateEntry(entry *database.Entry, value []byte) {
	entry.Data = value
	oldSize := entry.DataSize
	entry.DataSize = int64(len(value))
	db.lru.addAccessHistory(entry, oldSize)
	//// 先更新数据，然后再淘汰内存
	//db.lruMoveEntryToTail(entry)
	//db.freeMemoryIfNeeded(db.memCounter.maxMemory - entry.DataSize + oldSize)
	//db.memCounter.usedMemory += entry.DataSize
	//db.memCounter.usedMemory -= oldSize
}

func (db *SingleDB) onKeyEvict(key string, value interface{}) {
	db.data.Remove(key)
	db.ttlMap.Remove(key)
	db.versionMap.Remove(key)
	log.Printf("key: %s evicted", key)
}
