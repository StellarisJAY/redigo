package lock

import (
	"sort"
	"sync"
)

var prime32 uint32 = 123456

type Locker struct {
	slots         []*sync.RWMutex
	intentionWait *sync.WaitGroup
	intentionLock *sync.RWMutex
}

func NewLock(slots int) *Locker {
	lock := &Locker{slots: make([]*sync.RWMutex, slots)}
	for i := 0; i < slots; i++ {
		lock.slots[i] = &sync.RWMutex{}
	}
	lock.intentionLock = &sync.RWMutex{}
	lock.intentionWait = &sync.WaitGroup{}
	return lock
}

func hash(key string) uint32 {
	hash := uint32(2166136261)
	for i := 0; i < len(key); i++ {
		hash *= prime32
		hash ^= uint32(key[i])
	}
	return hash
}

func (l *Locker) indexFor(key string) uint32 {
	if l.slots == nil {
		panic("locker has not initialized")
	}
	length := len(l.slots)
	return uint32(length-1) & hash(key)
}

func (l *Locker) Lock(key string) {
	idx := l.indexFor(key)
	l.slots[idx].Lock()
}

func (l *Locker) Unlock(key string) {
	idx := l.indexFor(key)
	l.slots[idx].Unlock()
}

func (l *Locker) RLock(key string) {
	idx := l.indexFor(key)
	l.slots[idx].RLock()
}

func (l *Locker) RUnlock(key string) {
	idx := l.indexFor(key)
	l.slots[idx].RUnlock()
}

func (l *Locker) getLockSlots(keys ...string) []uint32 {
	slotMap := make(map[uint32]bool)
	for _, key := range keys {
		index := l.indexFor(key)
		slotMap[index] = true
	}
	slots := make([]uint32, 0, len(keys))
	for slot, _ := range slotMap {
		slots = append(slots, slot)
	}

	sort.Slice(slots, func(i, j int) bool {
		return slots[i] > slots[j]
	})
	return slots
}

func (l *Locker) RLockAll(keys ...string) {
	slots := l.getLockSlots(keys...)
	for _, slot := range slots {
		mutex := l.slots[slot]
		mutex.RLock()
	}
}

func (l *Locker) RUnlockAll(keys ...string) {
	slots := l.getLockSlots(keys...)
	for _, slot := range slots {
		mutex := l.slots[slot]
		mutex.RUnlock()
	}
}

func (l *Locker) LockAll(keys ...string) {
	slots := l.getLockSlots(keys...)
	for _, slot := range slots {
		mutex := l.slots[slot]
		mutex.Lock()
	}
}

func (l *Locker) UnlockAll(keys ...string) {
	slots := l.getLockSlots(keys...)
	for _, slot := range slots {
		mutex := l.slots[slot]
		mutex.Unlock()
	}
}
