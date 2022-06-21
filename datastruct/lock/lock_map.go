package lock

import "sync"

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

func (l *Locker) RLockAll() {

}

func (l *Locker) RUnlockAll() {

}

func (l *Locker) LockAll() {

}

func (l *Locker) UnlockAll() {

}
