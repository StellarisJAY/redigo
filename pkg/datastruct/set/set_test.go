package set

import (
	"strconv"
	"testing"
)

func TestSet_Add(t *testing.T) {
	s := NewSet()
	s.Add("1")
	s.Add("2")
	s.Add("3")

	if s.Has("1") == 0 || s.Has("2") == 0 || s.Has("3") == 0 {
		t.Fail()
	}
}

func TestSet_Members(t *testing.T) {
	s := NewSet()
	s.Add("1")
	s.Add("2")
	s.Add("3")

	exists := make(map[string]int)
	members := s.Members()
	for _, mem := range members {
		if mem != "1" && mem != "2" && mem != "3" {
			t.Fail()
		}
		exists[mem] = 1
	}
	if _, ok := exists["1"]; !ok {
		t.Fail()
	}
	if _, ok := exists["2"]; !ok {
		t.Fail()
	}
	if _, ok := exists["3"]; !ok {
		t.Fail()
	}
}

func TestSet_Remove(t *testing.T) {
	s := NewSet()
	s.Add("1")
	s.Add("2")
	s.Add("3")
	if s.Has("1") == 0 || s.Has("2") == 0 || s.Has("3") == 0 {
		t.Fail()
	}
	s.Remove("2")
	if s.Has("2") == 1 {
		t.Fail()
	}

	s.Remove("1")
	if s.Has("1") == 1 {
		t.Fail()
	}
}

func TestSet_Len(t *testing.T) {
	s := NewSet()
	s.Add("1")
	s.Add("2")
	s.Add("3")
	if s.Len() != 3 {
		t.Fail()
	}
	s.Remove("2")
	if s.Len() != 2 {
		t.Fail()
	}
}

func TestSet_Diff(t *testing.T) {
	s1 := NewSet()
	s1.Add("1")
	s1.Add("2")
	s1.Add("3")
	s1.Add("4")

	s2 := NewSet()
	s2.Add("1")
	s2.Add("3")
	s2.Add("5")

	diff12 := s1.Diff(s2)
	diff21 := s2.Diff(s1)

	for _, d := range diff12 {
		if d != "2" && d != "4" {
			t.Fail()
		}
	}

	for _, d := range diff21 {
		if d != "1" && d != "3" && d != "5" {
			t.Fail()
		}
	}
}

func TestSet_Inter(t *testing.T) {
	s1 := NewSet()
	s1.Add("1")
	s1.Add("2")
	s1.Add("3")
	s1.Add("4")

	s2 := NewSet()
	s2.Add("1")
	s2.Add("3")
	s2.Add("5")

	inter := s1.Inter(s2)
	for _, i := range inter {
		if i != "1" && i != "3" {
			t.Fail()
		}
	}
}

func TestSet_Inter2(t *testing.T) {
	s1 := NewSet()
	s1.Add("2")
	s1.Add("4")
	s1.Add("6")

	s2 := NewSet()
	s2.Add("1")
	s2.Add("3")
	s2.Add("5")

	inter := s1.Inter(s2)
	if len(inter) != 0 {
		t.Fail()
	}

	inter2 := s2.Inter(s1)
	if len(inter2) != 0 {
		t.Fail()
	}
}

func BenchmarkSet_Add(b *testing.B) {
	s := NewSet()
	members := make([]string, b.N)
	for i := 0; i < b.N; i++ {
		members[i] = strconv.Itoa(i)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Add(members[i])
	}
}

func BenchmarkSet_Has(b *testing.B) {
	s := NewSet()
	members := make([]string, b.N)
	for i := 0; i < b.N; i++ {
		members[i] = strconv.Itoa(i)
	}
	for i := 0; i < b.N; i++ {
		s.Add(members[i])
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Has(members[i])
	}
}

func BenchmarkSet_Remove(b *testing.B) {
	s := NewSet()
	members := make([]string, b.N)
	for i := 0; i < b.N; i++ {
		members[i] = strconv.Itoa(i)
	}
	for i := 0; i < b.N; i++ {
		s.Add(members[i])
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Remove(members[i])
	}
}

func BenchmarkSet_Diff(b *testing.B) {
	s1 := NewSet()
	for i := 1; i <= 1000; i += 2 {
		s1.Add(strconv.Itoa(i))
	}
	s2 := NewSet()
	for i := 1; i <= 1000; i++ {
		s2.Add(strconv.Itoa(i))
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		s2.Diff(s1)
	}
}

func BenchmarkSet_Inter(b *testing.B) {
	s1 := NewSet()
	for i := 1; i <= 1000; i += 2 {
		s1.Add(strconv.Itoa(i))
	}
	s2 := NewSet()
	for i := 1; i <= 1000; i++ {
		s2.Add(strconv.Itoa(i))
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		s1.Inter(s2)
	}
}

func BenchmarkSet_Union(b *testing.B) {
	s1 := NewSet()
	for i := 1; i <= 1000; i += 2 {
		s1.Add(strconv.Itoa(i))
	}
	s2 := NewSet()
	for i := 2; i <= 1000; i += 2 {
		s2.Add(strconv.Itoa(i))
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		s1.Union(s2)
	}
}
