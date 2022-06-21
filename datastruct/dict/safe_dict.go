package dict

import "redigo/datastruct/lock"

type SafeDict struct {
	store map[string]interface{}
	lock  *lock.Locker
}

func NewSafeDict(slots int) *SafeDict {
	return &SafeDict{
		store: make(map[string]interface{}),
		lock:  lock.NewLock(slots),
	}
}

func (dict *SafeDict) Put(key string, value interface{}) int {
	dict.lock.Lock(key)
	defer dict.lock.Unlock(key)
	_, exists := dict.store[key]
	dict.store[key] = value
	if exists {
		return 0
	}
	return 1
}

func (dict *SafeDict) Get(key string) (interface{}, bool) {
	dict.lock.RLock(key)
	defer dict.lock.RUnlock(key)
	val, ok := dict.store[key]
	return val, ok
}

func (dict *SafeDict) PutIfAbsent(key string, value interface{}) int {
	dict.lock.Lock(key)
	defer dict.lock.Unlock(key)
	if _, exists := dict.store[key]; exists {
		return 0
	}
	dict.store[key] = value
	return 1
}

func (dict *SafeDict) PutIfExists(key string, value interface{}) int {
	dict.lock.Lock(key)
	defer dict.lock.Unlock(key)
	if _, exists := dict.store[key]; exists {
		dict.store[key] = value
		return 1
	}
	return 0
}

func (dict *SafeDict) ForEach(consumer Consumer) {
	//TODO implement me
	panic("implement me")
}

func (dict *SafeDict) Remove(key string) int {
	dict.lock.Lock(key)
	defer dict.lock.Unlock(key)
	return 0
}

func (dict *SafeDict) Keys() []string {
	//TODO implement me
	panic("implement me")
}

func (dict *SafeDict) Clear() {
	//TODO implement me
	panic("implement me")
}

func (dict *SafeDict) Len() int {
	//TODO implement me
	panic("implement me")
}
