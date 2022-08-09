package peer

import (
	"fmt"
	"hash/crc32"
	"sort"
)

// Selector 集群模式下的节点选择器
type Selector interface {
	// SelectPeer 根据key找到key所在的集群节点地址
	SelectPeer(key string) string
	// AddPeer 添加一个新的节点
	AddPeer(peer string)
}

const consistentHashVirtualNode = 10

// ConsistentHashSelector 一致性hash节点选择器
type ConsistentHashSelector struct {
	ring  []int          // ring 有序的记录每个节点的hash值
	peers map[int]string // peers 从hash值到节点地址的映射
}

func NewConsistentHashSelector() *ConsistentHashSelector {
	return &ConsistentHashSelector{
		ring:  make([]int, 0),
		peers: make(map[int]string),
	}
}

func (c *ConsistentHashSelector) AddPeer(peer string) {
	// 在一致性hash中添加虚节点来提高随机性
	for i := 0; i < consistentHashVirtualNode; i++ {
		virtualKey := fmt.Sprintf("%s-%d", peer, i)
		hash := hashKey(virtualKey)
		c.peers[hash] = peer
		c.ring = append(c.ring, hash)
	}
	sort.Ints(c.ring)
}

func (c *ConsistentHashSelector) SelectPeer(key string) string {
	hash := hashKey(key)
	// 用二分查找，寻找第一个hash值大于等于keyHash的节点
	idx := sort.Search(len(c.ring), func(i int) bool {
		return c.ring[i] >= hash
	})
	// 没有找到目标，返回环形结构的第一个节点
	if idx == len(c.ring) {
		idx = 0
	}
	return c.peers[c.ring[idx]]
}

func hashKey(key string) int {
	return int(crc32.ChecksumIEEE([]byte(key)))
}
