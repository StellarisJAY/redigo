package list

type node struct {
	prev  *node
	next  *node
	value []byte
}

type LinkedList struct {
	left  *node
	right *node
	size  int
}

func (l *LinkedList) AddRight(val []byte) int {
	n := &node{
		prev:  nil,
		next:  nil,
		value: val,
	}
	if l.right == nil {
		l.right = n
		l.left = n
	} else {
		n.prev = l.right
		l.right.next = n
		l.right = n
	}
	l.size++
	return l.size
}

func (l *LinkedList) AddLeft(val []byte) int {
	n := &node{value: val}
	if l.left == nil {
		l.left = n
		l.right = n
	} else {
		l.left.prev = n
		n.next = l.left
		l.left = n
	}
	l.size++
	return l.size
}

func (l *LinkedList) Get(index int) []byte {
	if index < 0 {
		index = l.size + index
	}
	if index >= l.size {
		return nil
	}
	n := l.left
	for i := 0; i < index && n != nil; i++ {
		n = n.next
	}
	return n.value
}

func (l *LinkedList) Left() []byte {
	left := l.left
	if left != nil {
		return left.value
	} else {
		return nil
	}
}

func (l *LinkedList) Right() []byte {
	right := l.right
	if right != nil {
		return right.value
	} else {
		return nil
	}
}

func (l *LinkedList) Size() int {
	return l.size
}

func (l *LinkedList) RemoveLeft() []byte {
	if l.left == nil {
		return nil
	}
	if l.left == l.right {
		val := l.left.value
		l.left = nil
		l.right = nil
		l.size--
		return val
	}
	if l.left != nil {
		next := l.left.next
		left := l.left
		next.prev = nil
		left.next = nil
		l.left = next
		l.size--
		return left.value
	}
	return nil
}

func NewLinkedList(vals ...[]byte) *LinkedList {
	l := &LinkedList{}
	for _, val := range vals {
		l.AddLeft(val)
	}
	return l
}
