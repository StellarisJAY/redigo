package pool

import (
	"context"
	"redigo/util/log"
	"runtime"
	"sync"
	"testing"
)

func TestWorkerPool(t *testing.T) {
	pool := NewWorkerPool(runtime.NumCPU())
	pool.Start(context.Background())
	wg := sync.WaitGroup{}
	wg.Add(1)
	pool.Submit(func() {
		for i := 0; i < 1000; i++ {

		}
		log.Info("task 1 done")
		wg.Done()
	})
	wg.Add(1)
	pool.Submit(func() {
		for i := 0; i < 1000; i++ {

		}
		log.Info("task 2 done")
		wg.Done()
	})
	wg.Add(1)
	pool.Submit(func() {
		for i := 0; i < 1000; i++ {

		}
		log.Info("task 3 done")
		wg.Done()
	})
	wg.Wait()
}
