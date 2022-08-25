package database

import (
	"log"
	"redigo/interface/database"
)

// LRU 接口，提供一个LRU算法必备的几个方法
type LRU interface {
	// addAccessHistory 增加访问次数记录
	addAccessHistory(entry *database.Entry, oldSize int64)
	// addEntry 添加新的 entry
	addEntry(entry *database.Entry)
	// removeEntry 删除 entry
	removeEntry(entry *database.Entry)
}

// NoLRU maxMemory没有开启时使用空的LRU方法
type NoLRU struct {
}

// LRUQueue LRU队列
type LRUQueue struct {
	head *database.Entry
	tail *database.Entry
}

// TwoQueueLRU 双队列LRU
type TwoQueueLRU struct {
	capacity    int64
	recentCap   int64
	frequentCap int64

	recentSize   int64
	frequentSize int64

	k int // k LRU-K 算法的k值，当一个key的访问计数达到 k 才能移动到队尾

	recent   *LRUQueue // recent 记录最新加入缓存的数据
	frequent *LRUQueue // frequent 当 recent 中的数据被访问超过 k 次后，会晋升到 frequent 队列中

	onEvict func(key string, value interface{}, dataSize int64)
}

func newLRUQueue() *LRUQueue {
	q := &LRUQueue{
		head: &database.Entry{},
		tail: &database.Entry{},
	}
	q.head.NextLRUEntry = q.tail
	q.tail.PrevLRUEntry = q.head
	return q
}

// NewTwoQueueLRU 创建双队列LRU，参数：内存上限、recent队列上限、晋升队列的访问次数、evict淘汰回调
func NewTwoQueueLRU(capacity int64, recentCap int64, k int, onEvict func(key string, value interface{})) *TwoQueueLRU {
	return &TwoQueueLRU{
		capacity:     capacity,
		recentCap:    recentCap,
		recentSize:   0,
		frequentCap:  capacity - recentCap,
		frequentSize: 0,
		k:            k,
		recent:       newLRUQueue(),
		frequent:     newLRUQueue(),
		onEvict: func(key string, value interface{}, dataSize int64) {
			// 避免回调方法出错
			defer func() {
				if err := recover(); err != nil {
					log.Println("evict function error:", err)
				}
			}()
			onEvict(key, value)
		},
	}
}

// addTail 在LRU队列添加一个新entry
func (f *LRUQueue) addTail(entry *database.Entry) {
	f.tail.PrevLRUEntry.NextLRUEntry = entry
	entry.NextLRUEntry = f.tail
	entry.PrevLRUEntry = f.tail.PrevLRUEntry
	f.tail.PrevLRUEntry = entry
}

// remove 删除LRU队列的任意一个entry
func (f *LRUQueue) remove(entry *database.Entry) {
	if entry.PrevLRUEntry != nil {
		entry.PrevLRUEntry.NextLRUEntry = entry.NextLRUEntry
	}
	if entry.NextLRUEntry != nil {
		entry.NextLRUEntry.PrevLRUEntry = entry.PrevLRUEntry
	}
	entry.NextLRUEntry = nil
	entry.PrevLRUEntry = nil
}

// removeOldest 删除LRU队列的头元素
func (f *LRUQueue) removeOldest() *database.Entry {
	entry := f.head.NextLRUEntry
	f.remove(entry)
	return entry
}

func (f *LRUQueue) moveToTail(entry *database.Entry) {
	f.remove(entry)
	f.addTail(entry)
}

// addAccessHistory 增加访问记录，如果entry当前在recent中且访问次数超过了晋升条件，则转移到frequent队列
func (tq *TwoQueueLRU) addAccessHistory(entry *database.Entry, oldSize int64) {
	if entry.AccessCount < 0 {
		entry.AccessCount -= 1
		// 当frequent队列的计数达到k以后，才能将key移动到队列尾部
		if entry.AccessCount == tq.k {
			tq.frequent.moveToTail(entry)
		}
		if oldSize > entry.DataSize {
			tq.frequentSize -= oldSize - entry.DataSize
		} else if oldSize < entry.DataSize {
			tq.freeFrequentMemory(tq.frequentCap - (entry.DataSize - oldSize))
		}
	} else {
		entry.AccessCount++
		// 如果访问次数达到晋升要求，将entry从recent转移到frequent
		if entry.AccessCount == tq.k {
			tq.recentSize -= oldSize
			tq.freeFrequentMemory(tq.recentCap - entry.DataSize)
			tq.recent.remove(entry)
			tq.frequent.addTail(entry)
			tq.frequentSize += entry.DataSize
			// 将访问次数设置为-1，在frequent中不再计数
			entry.AccessCount = -1
		} else {
			tq.recent.moveToTail(entry)
			if oldSize > entry.DataSize {
				tq.recentSize -= oldSize - entry.DataSize
			} else if oldSize < entry.DataSize {
				tq.freeFrequentMemory(tq.recentCap - (entry.DataSize - oldSize))
			}
		}
	}
}

// freeRecentMemory 释放recent队列的内存
func (tq *TwoQueueLRU) freeRecentMemory(targetSize int64) {
	for tq.recentSize > targetSize {
		entry := tq.recent.removeOldest()
		tq.onEvict(entry.Key, entry.Data, entry.DataSize)
		tq.recentSize -= entry.DataSize
	}
}

// freeFrequentMemory 释放frequent队列内存
func (tq *TwoQueueLRU) freeFrequentMemory(targetSize int64) {
	for tq.frequentSize > targetSize {
		entry := tq.frequent.removeOldest()
		tq.onEvict(entry.Key, entry.Data, entry.DataSize)
		tq.frequentSize -= entry.DataSize
	}
}

func (tq *TwoQueueLRU) addEntry(entry *database.Entry) {
	size := entry.DataSize
	// 新的entry都添加到recent中，在添加前先释放recent内存
	tq.freeRecentMemory(tq.recentCap - size)
	tq.recent.addTail(entry)
	tq.recentSize += entry.DataSize
}

func (tq *TwoQueueLRU) removeEntry(entry *database.Entry) {
	entry.PrevLRUEntry.NextLRUEntry = entry.NextLRUEntry
	entry.NextLRUEntry.PrevLRUEntry = entry.PrevLRUEntry
	entry.NextLRUEntry = nil
	entry.PrevLRUEntry = nil
	if entry.AccessCount >= 0 {
		tq.recentSize -= entry.DataSize
	} else {
		tq.frequentSize -= entry.DataSize
	}
	tq.onEvict(entry.Key, entry.Data, entry.DataSize)
}

// 不开启LRU的空实现
func (n *NoLRU) addAccessHistory(entry *database.Entry, oldSize int64) {
}

func (n *NoLRU) addEntry(entry *database.Entry) {
}

func (n *NoLRU) removeEntry(entry *database.Entry) {
}
