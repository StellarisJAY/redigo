package pqueue

type Item struct {
	Value    interface{}
	priority int64
	index    int
	key      string
}

type PriorityQueue []*Item

func NewPriorityQueue() PriorityQueue {
	items := make([]*Item, 0, 8)
	return items
}

func (pq *PriorityQueue) Len() int {
	return len(*pq)
}

func (pq *PriorityQueue) Less(i, j int) bool {
	return (*pq)[i].priority < (*pq)[j].priority
}

func (pq *PriorityQueue) Swap(i, j int) {
	(*pq)[i], (*pq)[j] = (*pq)[j], (*pq)[i]
	(*pq)[j].index = j
	(*pq)[i].index = i
}

func (pq *PriorityQueue) Push(x any) {
	n := (*pq).Len()
	c := cap(*pq)
	if n+1 >= c {
		newPq := make(PriorityQueue, n, c*2)
		copy(newPq, *pq)
		*pq = newPq
	}
	*pq = (*pq)[0 : n+1]
	item := x.(*Item)
	item.index = n
	(*pq)[n] = item
}

func (pq *PriorityQueue) Pop() any {
	n := (*pq).Len()
	item := (*pq)[n-1]
	item.index = -1
	*pq = (*pq)[0 : n-1]
	return item
}

func (pq *PriorityQueue) Peek() any {
	n := (*pq).Len()
	if n == 0 {
		return nil
	}
	return (*pq)[0]
}
