package dict

//Consumer 用于foreach遍历，返回false表示结束遍历
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
	RandomKeys(int) []string
	RandomKeysDistinct(int) []string
}
