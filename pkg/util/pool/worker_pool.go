package pool

import (
	"context"
	"math/rand"
	"redigo/pkg/util/log"
	"time"
)

type worker struct {
	taskQueue chan func()
	id        int
}

type WorkerPool struct {
	n       int
	workers []*worker
}

func NewWorkerPool(n int) *WorkerPool {
	p := &WorkerPool{n: n}
	p.workers = make([]*worker, n)
	for i := 0; i < n; i++ {
		p.workers[i] = &worker{taskQueue: make(chan func(), 1024), id: i}
	}
	return p
}

func (p *WorkerPool) Start(ctx context.Context) {
	for i := 0; i < p.n; i++ {
		go p.workers[i].work(ctx)
	}
	log.Info("worker pool started, total %d goroutines", p.n)
}

func (p *WorkerPool) Submit(task func()) {
	rand.Seed(time.Now().UnixMilli())
	i := rand.Intn(p.n)
	p.workers[i].taskQueue <- task
}

func (p *WorkerPool) SubmitHashBalance(task func(), hash int) {
	i := hash % p.n
	p.workers[i].taskQueue <- task
}

func (w *worker) work(ctx context.Context) {
	defer func() {
		if err := recover(); err != nil {
			log.Warn("worker-%d error: %v", w.id, err)
		}
	}()
	for {
		select {
		case task := <-w.taskQueue:
			task()
		case <-ctx.Done():
			return
		}
	}
}
