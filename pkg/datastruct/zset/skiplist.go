package zset

import (
	"fmt"
	"math/rand"
)

const maxLevels = 32

type Element struct {
	Member string
	Score  float64
}

type Level struct {
	forward *node // 当前节点在该层的下一个节点
	span    int64 // 该节点到下一个节点的跨度
}

type node struct {
	Element
	backward *node
	level    []*Level // 每个节点可以有多层
}

type skipList struct {
	head, tail *node
	level      int
	size       int64
}

func newSkipList() *skipList {
	skl := &skipList{level: 1, size: 0}
	skl.head = &node{level: make([]*Level, maxLevels)}
	// 头节点拥有最大层数
	for i := 0; i < maxLevels; i++ {
		skl.head.level[i] = &Level{forward: nil, span: 0}
	}
	skl.head.Member = ""
	skl.head.Score = 0
	skl.head.backward = nil
	skl.tail = nil
	return skl
}

func newNode(member string, score float64, levels int) *node {
	n := &node{
		Element: Element{Member: member, Score: score},
		level:   make([]*Level, levels),
	}
	for i := 0; i < levels; i++ {
		n.level[i] = &Level{}
	}
	return n
}

// A copy of redis's random level code
func randomLevel() int {
	level := 1
	for float32(rand.Int31()&0xFFFF) < (0.25 * 0xFFFF) {
		level++
	}
	if level < maxLevels {
		return level
	}
	return maxLevels
}

func (skl *skipList) Insert(member string, score float64) *node {
	// 路过的需要更新的nodes
	prevNodes := make([]*node, maxLevels)
	// 插入节点每一层的排位（距离最小Score有多少个node）
	ranks := make([]int64, maxLevels)

	n := skl.head
	// 从顶层开始遍历跳表，直到最底层的有序链表
	for i := skl.level - 1; i >= 0; i-- {
		// 初始化当前层的rank，如果是顶层则为0，其他层为上一层的rank
		if i == skl.level-1 {
			ranks[i] = 0
		} else {
			ranks[i] = ranks[i+1]
		}
		// 在当前层遍历，如果右边有节点，且右节点的score大于目标score 或者 右节点的score等于score且右节点的值小于目标值
		for n.level[i].forward != nil && ((n.level[i].forward.Score < score) ||
			(n.level[i].forward.Score == score && n.level[i].forward.Member < member)) {
			// 路过该层的该节点，把该节点到下一个节点的距离加到rank中
			ranks[i] += n.level[i].span
			n = n.level[i].forward
		}
		// 保存这一层路过的最后一个节点
		prevNodes[i] = n
	}
	// 随机生成新节点的层数
	level := randomLevel()
	// 如果新节点的层数大于跳表目前的层数，将多余的层数的路过节点设置为head，rank设置为0
	if level > skl.level {
		for l := skl.level; l < level; l++ {
			ranks[l] = 0
			prevNodes[l] = skl.head
			// 更新head在多出层的span为目前跳表的总大小
			prevNodes[l].level[l].span = skl.size
		}
		skl.level = level
	}
	// 创建新节点
	node := newNode(member, score, level)
	// 遍历新节点的每一层，更新每层前驱节点的forward、span
	for i := 0; i < level; i++ {
		node.level[i].forward = prevNodes[i].level[i].forward
		prevNodes[i].level[i].forward = node
		node.level[i].span = prevNodes[i].level[i].span - (ranks[0] - ranks[1])
		prevNodes[i].level[i].span = ranks[0] - ranks[i] + 1
	}
	// 超出当前节点层数的前驱节点，只更新span大小
	for i := level; i < skl.level; i++ {
		prevNodes[i].level[i].span++
	}
	// 如果最底层的前驱节点不是head，设置新节点的前驱节点
	if prevNodes[0] == skl.head {
		node.backward = nil
	} else {
		node.backward = prevNodes[0]
	}
	// 如果新节点在0层的后继节点不为nil，设置后继节点的前驱节点
	if node.level[0].forward != nil {
		node.level[0].forward.backward = node
	} else {
		skl.tail = node
	}
	skl.size++
	return node
}

func (skl *skipList) Remove(member string, score float64) int {
	prevNodes := make([]*node, maxLevels)
	n := skl.head
	// 从最顶层开始遍历
	for i := skl.level - 1; i >= 0; i-- {
		// 如果下一个节点的score小于目标score，或者，下一个节点score与目标相同但是member小于目标member
		for n.level[i].forward != nil && (n.level[i].forward.Score < score ||
			(n.level[i].forward.Score == score && n.level[i].forward.Member < member)) {
			// 在当前level向前遍历
			n = n.level[i].forward
		}
		// 该层最接近目标的最右侧节点
		prevNodes[i] = n
	}
	n = n.level[0].forward
	// 如果第0层的下一个节点的member与目标相同
	if n != nil && n.Member == member {
		// 删除该节点
		skl.removeNode(n, prevNodes)
		return 1
	}
	// member不存在，但是外层的map中却存在
	return 0
}

func (skl *skipList) removeNode(n *node, prevNodes []*node) {
	// 从最顶层开始遍历
	for i := skl.level - 1; i >= 0; i-- {
		// 判断被删除节点在该层是否存在
		if prevNodes[i].level[i].forward == n {
			prevNodes[i].level[i].span += n.level[i].span - 1
			// 路过的前驱节点的forward更新为被删除节点的forward
			prevNodes[i].level[i].forward = n.level[i].forward
		} else {
			// 被删除节点在该层不存在，将span减一
			prevNodes[i].level[i].span -= 1
		}
	}
	// 更新被删除节点的backward
	if n.level[0].forward != nil {
		n.level[0].forward.backward = n.backward
	} else {
		skl.tail = prevNodes[0]
	}
	// 更新跳表的level
	for skl.level > 0 && skl.head.level[skl.level-1].forward == nil {
		skl.level--
	}
	skl.size--
}

func (skl *skipList) Rank(member string, score float64) int64 {
	var rank int64 = 0
	n := skl.head
	for i := skl.level - 1; i >= 0; i-- {
		for n.level[i].forward != nil &&
			(n.level[i].forward.Score < score ||
				(n.level[i].forward.Score == score && n.level[i].forward.Member < member)) {
			rank += n.level[i].span
			n = n.level[i].forward
		}
	}
	n = n.level[0].forward
	if n == nil {
		return -1
	}
	for n.level[0].forward != nil && n.level[0].forward.Score < score && n.level[0].forward.Member < member {
		rank++
		n = n.level[0].forward
	}
	return rank
}

func (skl *skipList) PopMax() *node {
	if skl.tail != nil {
		rem := skl.tail
		skl.Remove(rem.Member, rem.Score)
		return rem
	}
	return nil
}

func (skl *skipList) PopMin() *node {
	if skl.head.level[0].forward != nil {
		rem := skl.head.level[0].forward
		skl.Remove(rem.Member, rem.Score)
		return rem
	}
	return nil
}

func (skl *skipList) Range(start, end int) []Element {
	node := skl.head
	// 起始位置的rank值，加一跳过head
	startRank := start + 1
	count := end - start + 1
	// 从顶层开始遍历
	for i := skl.level - 1; i >= 0; i-- {
		// 每层遍历跨度不超过start位置的节点
		for node.level[i].forward != nil && int(node.level[i].span) <= startRank {
			startRank -= int(node.level[i].span)
			node = node.level[i].forward
		}
	}
	// 在第0层遍历，直到start位置
	for node.level[0].forward != nil && startRank > 0 {
		startRank--
		node = node.level[0].forward
	}
	// 从start位置遍历到end位置
	result := make([]Element, count)
	for i := 0; i < count && node != nil; i++ {
		result[i] = node.Element
		node = node.level[0].forward
	}
	return result
}

func (skl *skipList) CountBetween(min, max float64, lOpen, rOpen bool) int {
	node := skl.head
	// 从顶层开始遍历
	for i := skl.level - 1; i >= 0; i-- {
		// 每层遍历直到下一个节点的score大于min
		for node.level[i].forward != nil && node.level[i].forward.Score < min {
			node = node.level[i].forward
		}
	}
	// 在第0层继续遍历，找到大于等于min的节点
	for node != nil && ((lOpen && node.Score <= min) || (!lOpen && node.Score < min)) {
		node = node.level[0].forward
	}
	// 找到第一个大于等于min的节点后，开始在第0层遍历小于等于max的节点
	count := 0
	for node != nil && ((!rOpen && node.Score <= max) || (rOpen && node.Score < max)) {
		if node != skl.head {
			count++
		}
		node = node.level[0].forward
	}
	return count
}

func (skl *skipList) RangeByScore(min, max float64, offset, count int, lOpen, rOpen bool) []Element {
	node := skl.head
	for i := skl.level - 1; i >= 0; i-- {
		for node.level[i].forward != nil && node.level[i].forward.Score < min {
			node = node.level[i].forward
		}
	}
	for node != nil && ((!lOpen && node.Score < min) || (lOpen && node.Score <= min)) {
		node = node.level[0].forward
	}
	result := make([]Element, 0)
	for i := 0; i < count+offset && node != nil && ((!rOpen && node.Score <= max) || (rOpen && node.Score < max)); {
		if node == skl.head {
			node = node.level[0].forward
			continue
		}
		if i < offset {
			i++
			node = node.level[0].forward
			continue
		}
		result = append(result, node.Element)
		node = node.level[0].forward
		i++
	}
	return result
}

// printLevels of skipList. use in test-cases only
func (skl *skipList) printLevels() {
	for i := skl.level - 1; i >= 0; i-- {
		for n := skl.head; n != nil; n = n.level[i].forward {
			if n != skl.head {
				fmt.Printf("%d:%s ", int(n.Score), n.Member)
			}
			if i != 0 {
				for j := 0; j < int(n.level[i].span)-1; j++ {
					fmt.Printf("      ")
				}
			}
		}
		fmt.Println()
	}
}

// forEach iterator
func (skl *skipList) forEach(fun func(score float64, member string) bool) {
	n := skl.head.level[0].forward
	for n != nil {
		if !fun(n.Score, n.Member) {
			break
		}
		n = n.level[0].forward
	}
}
