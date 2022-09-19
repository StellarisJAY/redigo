package pqueue

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestScheduler_ScheduleAt(t *testing.T) {
	scheduler := NewScheduler()
	go scheduler.schedulerLoop()
	wg := sync.WaitGroup{}
	start := time.Now()
	for i := 0; i < 100; i++ {
		index := i
		wg.Add(1)
		scheduler.ScheduleAt(time.Now().Add(5*time.Second), fmt.Sprintf("key%d", index), func() {
			wg.Done()
		})
	}

	wg.Wait()
	if d := time.Now().Sub(start).Seconds(); int(d) != 5 {
		t.FailNow()
	}
}
