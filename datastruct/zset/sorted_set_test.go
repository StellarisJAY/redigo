package zset

import (
	"strconv"
	"testing"
)

func initTest(count int) *SortedSet {
	set := NewSortedSet()
	for i := 1; i <= count; i++ {
		set.Add(strconv.Itoa(i), float64(i))
	}
	return set
}

func TestSortedSet_GetScore(t *testing.T) {
	set := initTest(100)
	for i := 1; i <= 100; i++ {
		if e, ok := set.GetScore(strconv.Itoa(i)); !ok || int(e.Score) != i {
			t.Fail()
		}
	}
}

func TestSortedSet_Rank(t *testing.T) {
	set := initTest(100)
	for i := 1; i <= 100; i++ {
		if int(set.Rank(strconv.Itoa(i))) != i-1 {
			t.Fail()
		}
	}
}