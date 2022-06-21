package dict

// Consumer is used in ForEach. If this function returns false, the traverse will end
type Consumer func(key string, value interface{}) bool

type Dict interface {
	Put(key string, value interface{}) int
	Get(key string) (interface{}, bool)
	PutIfAbsent(key string, value interface{}) int
	PutIfExists(key string, value interface{}) int
	ForEach(consumer Consumer)
	Remove(key string) int
	Keys() []string
	Clear()
	Len() int
}
