package dict

type SimpleDict struct {
	store map[string]interface{}
}

func NewSimpleDict() *SimpleDict {
	return &SimpleDict{store: make(map[string]interface{})}
}

func (s *SimpleDict) Put(key string, value interface{}) int {
	_, _ = s.store[key]
	s.store[key] = value
	return 1
}

func (s *SimpleDict) Get(key string) (interface{}, bool) {
	val, ok := s.store[key]
	return val, ok
}

func (s *SimpleDict) PutIfAbsent(key string, value interface{}) int {
	if _, exists := s.store[key]; exists {
		return 0
	}
	s.store[key] = value
	return 1
}

func (s *SimpleDict) PutIfExists(key string, value interface{}) int {
	if _, exists := s.store[key]; exists {
		s.store[key] = value
		return 1
	}
	return 0
}

func (s *SimpleDict) ForEach(consumer Consumer) {
	for k, v := range s.store {
		ok := consumer(k, v)
		if !ok {
			break
		}
	}
}

func (s *SimpleDict) Remove(key string) int {
	_, exists := s.store[key]
	delete(s.store, key)
	if exists {
		return 1
	}
	return 0
}

func (s *SimpleDict) Keys() []string {
	count := len(s.store)
	result := make([]string, count)
	i := 0
	for k, _ := range s.store {
		result[i] = k
		i++
	}
	return result
}

func (s *SimpleDict) Clear() {
	*s = *NewSimpleDict()
}

func (s *SimpleDict) Len() int {
	return len(s.store)
}
