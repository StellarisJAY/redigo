package set

import "redigo/datastruct/dict"

type Set struct {
	data dict.Dict
}

func NewSet() *Set {
	return &Set{data: dict.NewSimpleDict()}
}

func (s *Set) Add(value string) int {
	return s.data.PutIfAbsent(value, nil)
}

func (s *Set) Remove(value string) int {
	return s.data.Remove(value)
}

func (s *Set) Has(value string) int {
	_, exists := s.data.Get(value)
	if exists {
		return 1
	} else {
		return 0
	}
}

func (s *Set) Len() int {
	return s.data.Len()
}

func (s *Set) Members() []string {
	return s.data.Keys()
}

func (s *Set) ForEach(consumer func(string) bool) {
	s.data.ForEach(func(value string, _ interface{}) bool {
		return consumer(value)
	})
}

func (s *Set) RandomMembers(count int) []string {
	return s.data.RandomKeys(count)
}

func (s *Set) RandomMembersDistinct(count int) []string {
	return s.data.RandomKeysDistinct(count)
}

func (s *Set) Diff(other *Set) []string {
	result := make([]string, 0)
	s.ForEach(func(val string) bool {
		if other.Has(val) == 0 {
			result = append(result, val)
		}
		return true
	})
	return result
}
func (s *Set) Inter(other *Set) []string {
	result := make([]string, 0)
	s.ForEach(func(val string) bool {
		if other.Has(val) == 1 {
			result = append(result, val)
		}
		return true
	})
	return result
}

func (s *Set) Union(other *Set) []string {
	diff := s.Diff(other)
	result := make([]string, other.Len()+len(diff))
	i := 0
	other.ForEach(func(val string) bool {
		result[i] = val
		i++
		return true
	})
	for _, val := range diff {
		result[i] = val
		i++
	}
	return result
}
