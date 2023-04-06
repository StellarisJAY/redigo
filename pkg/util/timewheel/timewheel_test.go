package timewheel

import (
	"log"
	"sync"
	"testing"
	"time"
)

func TestTimeWheel_AddTask(t *testing.T) {
	tw := NewTimeWheel(1*time.Second, 60)
	go tw.Start()

	wg := sync.WaitGroup{}
	wg.Add(1)
	ScheduleDelayed(1*time.Second, "expire_k1", func() {
		log.Println("k1 expired")
		wg.Done()
	})
	wg.Add(1)
	ScheduleAt(time.Now().Add(10*time.Second), "expire_k2", func() {
		log.Println("k2 expired")
		wg.Done()
	})
	wg.Add(1)
	ScheduleDelayed(10*time.Second, "expire_k3", func() {
		log.Println("k3 expired")
		wg.Done()
	})
	wg.Wait()
}
