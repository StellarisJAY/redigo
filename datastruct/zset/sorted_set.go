package zset

type SortedSet struct {
	dict map[string]*node
	skl  *skipList
}

func NewSortedSet() *SortedSet {
	return &SortedSet{
		dict: make(map[string]*node),
		skl:  NewSkipList(),
	}
}

func (zs *SortedSet) Add(member string, score float64) int {
	n, ok := zs.dict[member]
	if ok && n.Score != score {
		zs.skl.Remove(member, score)
		n = zs.skl.Insert(member, score)
		zs.dict[member] = n
		return 1
	} else if !ok {
		n = zs.skl.Insert(member, score)
		zs.dict[member] = n
		return 1
	}
	return 0
}

func (zs *SortedSet) GetScore(member string) (*Element, bool) {
	n, ok := zs.dict[member]
	if ok {
		return &n.Element, true
	}
	return nil, false
}

func (zs *SortedSet) Remove(member string) int {
	n, ok := zs.dict[member]
	if !ok {
		return 0
	}
	delete(zs.dict, member)
	return zs.skl.Remove(member, n.Score)
}

func (zs *SortedSet) Rank(member string) int64 {
	n, ok := zs.dict[member]
	if !ok {
		return -1
	}
	return zs.skl.Rank(n.Member, n.Score)
}

func (zs *SortedSet) PopMax() *Element {
	if n := zs.skl.PopMax(); n != nil {
		return &n.Element
	}
	return nil
}
func (zs *SortedSet) PopMin() *Element {
	if n := zs.skl.PopMin(); n != nil {
		return &n.Element
	}
	return nil
}

func (zs *SortedSet) Size() int {
	return len(zs.dict)
}
