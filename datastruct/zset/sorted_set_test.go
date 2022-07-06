package zset

import (
	"fmt"
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

func TestSortedSet_PopMax(t *testing.T) {
	set := initTest(100)
	for i := 100; i >= 1; i-- {
		if e := set.PopMax(); e == nil || e.Member != strconv.Itoa(i) || int(e.Score) != i {
			t.Fail()
		}
	}
}

func TestSortedSet_PopMin(t *testing.T) {
	set := initTest(100)
	for i := 1; i <= 100; i++ {
		if e := set.PopMin(); e == nil || e.Member != strconv.Itoa(i) || int(e.Score) != i {
			t.Fail()
		}
	}
}

func TestSortedSet_Range(t *testing.T) {
	set := initTest(100)
	elements := set.Range(0, -1)
	if len(elements) != 100 || elements[0].Member != "1" || elements[99].Member != "100" {
		t.Fail()
	} else {
		t.Log("Range 0 ~ -1 PASSED")
	}
	elements = set.Range(-10, -1)
	if len(elements) != 10 || elements[0].Member != "91" || elements[9].Member != "100" {
		t.Fail()
	} else {
		t.Log("Range -10 ~ -1 PASSED")
	}
	elements = set.Range(10, 20)
	if len(elements) != 11 || elements[0].Member != "11" || elements[9].Member != "20" {
		t.Fail()
	} else {
		t.Log("Range 10 ~ 20 PASSED")
	}
	elements = set.Range(5, 3)
	if elements != nil {
		t.Fail()
	} else {
		t.Log("Range start > end PASSED")
	}
	fmt.Println()
}

func TestSortedSet_CountBetween(t *testing.T) {
	set := initTest(100)
	count := set.CountBetween(0, 100, true, true)
	if count != 99 {
		t.Log("Count between 0 and 100 failed, count: ", count)
		t.Fail()
	}
	count = set.CountBetween(10.5, 20, false, false)
	if count != 10 {
		t.Log("Count between 10.5 and 20 failed, count: ", count)
		t.Fail()
	}
	count = set.CountBetween(90.5, 105, true, false)
	if count != 10 {
		t.Log("Count between 90.5 and 105 failed, count: ", count)
		t.Fail()
	}
}

// Benchmark ZADD
func BenchmarkSortedSet_Add(b *testing.B) {
	set := NewSortedSet()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		set.Add(strconv.Itoa(i), float64(i))
	}
}

// Benchmark ZRANK
func BenchmarkSortedSet_Rank(b *testing.B) {
	set := NewSortedSet()
	members := make([]string, b.N)
	for i := 0; i < b.N; i++ {
		members[i] = strconv.Itoa(i)
		set.Add(members[i], float64(i))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = set.Rank(members[i])
	}
}

func BenchmarkSortedSet_GetScore(b *testing.B) {
	set := NewSortedSet()
	members := make([]string, b.N)
	for i := 0; i < b.N; i++ {
		members[i] = strconv.Itoa(i)
		set.Add(members[i], float64(i))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		set.GetScore(members[i])
	}
}
