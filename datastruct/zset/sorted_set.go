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

func (zs *SortedSet) Range(start, end int) []Element {
	if start < 0 {
		start = zs.Size() + start
	}
	if end < 0 {
		end = zs.Size() + end
	}
	// make sure start index not negative
	if start < 0 {
		start = 0
	}
	if start > end {
		return nil
	}
	return zs.skl.Range(start, end)
}

func (zs *SortedSet) CountBetween(min, max float64, lOpen, rOpen bool) int {
	if min > max {
		return 0
	}
	return zs.skl.CountBetween(min, max, lOpen, rOpen)
}

func (zs *SortedSet) RangeByScore(min, max float64, offset, count int, lOpen, rOpen bool) []Element {
	if min > max || count <= 0 {
		return nil
	}
	return zs.skl.RangeByScore(min, max, offset, count, lOpen, rOpen)
}

func (zs *SortedSet) ForEach(fun func(score float64, value string) bool) {
	zs.skl.forEach(fun)
}
