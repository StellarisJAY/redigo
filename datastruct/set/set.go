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
