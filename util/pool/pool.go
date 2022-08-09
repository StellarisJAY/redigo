package pool

import (
	"fmt"
	"sync"
)

// Pool is similar to sync.Pool, except that Pool has limited capacity
type Pool struct {
	size     int
	capacity int
	cache    chan interface{}
	lock     sync.Mutex
	newFunc  func() interface{}
}

func Empty(capacity int, newFunc func() interface{}) *Pool {
	if capacity <= 0 {
		panic(fmt.Errorf("invalid argument for New Pool"))
	}
	p := new(Pool)
	p.capacity = capacity
	p.cache = make(chan interface{}, capacity)
	p.newFunc = newFunc
	return p
}

func New(capacity int, initSize int, newFunc func() interface{}) *Pool {
	if capacity <= 0 || initSize < 0 {
		panic(fmt.Errorf("invalid argument for New Pool"))
	}
	if initSize > capacity {
		initSize = capacity
	}
	p := new(Pool)
	p.capacity = capacity
	p.size = initSize
	p.newFunc = newFunc
	p.cache = make(chan interface{}, capacity)
	for i := 0; i < initSize; i++ {
		p.cache <- newFunc()
	}
	return p
}

// Get a resource from Pool. Create a new resource or wait for a resource if Pool has no resource left
func (p *Pool) Get() interface{} {
	if e := p.TryGet(); e != nil {
		return e
	}
	if e := p.createNew(); e != nil {
		return e
	}
	return <-p.cache
}

// TryGet try pop one resource from pool channel. If channel is empty, function returns nil immediately
func (p *Pool) TryGet() interface{} {
	select {
	case e := <-p.cache:
		return e
	default:
		return nil
	}
}

func (p *Pool) Put(element interface{}) {
	p.cache <- element
}

func (p *Pool) Cap() int {
	return p.capacity
}

func (p *Pool) Size() int {
	return p.size
}

func (p *Pool) createNew() interface{} {
	p.lock.Lock()
	defer p.lock.Unlock()
	if p.size < p.capacity {
		p.size++
		return p.newFunc()
	} else {
		return nil
	}
}
