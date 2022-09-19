package pqueue

import (
	"container/heap"
	"redigo/util/log"
	"sync"
	"time"
)

type Job func()

type Scheduler struct {
	sync.RWMutex
	pq         *PriorityQueue   // pq 优先级队列，用最小堆表示定时任务，堆顶即为时间最接近的任务
	schedules  map[string]*Item // schedules 任务集合，用于取消任务时找到任务的Item对象
	jobChan    chan *Item       // jobChan 提交任务channel
	removeChan chan *Item       // removeChan 取消任务channel
	closeChan  chan struct{}    // closeChan 关闭Scheduler
	closed     bool
}

func NewScheduler() *Scheduler {
	s := &Scheduler{}
	pq := NewPriorityQueue()
	s.pq = &pq
	s.jobChan = make(chan *Item, 1024)
	s.schedules = make(map[string]*Item)
	s.closeChan = make(chan struct{}, 1)
	s.closed = false
	return s
}

func (s *Scheduler) schedulerLoop() {
	// 每秒执行一次任务，实现秒级别的key过期
	ticker := time.NewTicker(time.Second)
	for {
		select {
		case <-s.closeChan:
			// 加锁，避免向关闭的channel发送
			s.Lock()
			close(s.removeChan)
			close(s.jobChan)
			s.closed = true
			s.Unlock()
			break
		case item := <-s.removeChan:
			delete(s.schedules, item.key)
			heap.Remove(s.pq, item.index)
		case item := <-s.jobChan:
			if old, ok := s.schedules[item.key]; ok {
				heap.Remove(s.pq, old.index)
			}
			s.schedules[item.key] = item
			heap.Push(s.pq, item)
		case <-ticker.C:
			s.handle()
		}
	}
}

func (s *Scheduler) handle() {
	var top = s.pq.Peek()
	// 取堆顶任务执行，堆顶的定时最近的任务
	for top != nil && top.(*Item).priority <= time.Now().UnixMilli() {
		item := heap.Pop(s.pq).(*Item)
		delete(s.schedules, item.key)
		go func() {
			defer func() {
				if err := recover(); err != nil {
					log.Warn("scheduler job error: %v", err)
				}
			}()
			item.Value.(func())()
		}()
		top = s.pq.Peek()
	}
}

// ScheduleDelayed 提交延迟执行任务
func (s *Scheduler) ScheduleDelayed(delay time.Duration, key string, job func()) {
	s.RLock()
	defer s.RUnlock()
	if s.closed {
		return
	}
	expireAt := time.Now().Add(delay).UnixMilli()
	item := &Item{Value: job, priority: expireAt, key: key}
	s.jobChan <- item
}

// ScheduleAt 提交指定时间执行的定时任务
func (s *Scheduler) ScheduleAt(at time.Time, key string, job func()) {
	s.RLock()
	defer s.RUnlock()
	if s.closed {
		return
	}
	expireAt := at.UnixMilli()
	item := &Item{Value: job, priority: expireAt, key: key}
	s.jobChan <- item
}

// Cancel 取消任务
func (s *Scheduler) Cancel(key string) {
	if item, ok := s.schedules[key]; ok {
		s.removeChan <- item
	}
}

func (s *Scheduler) Close() {
	s.closeChan <- struct{}{}
}
