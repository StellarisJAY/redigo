package timewheel

import (
	"time"
)

var tw = NewTimeWheel(1*time.Second, 60)

func init() {
	tw.Start()
}

func ScheduleDelayed(delay time.Duration, key string, job func()) {
	tw.schedule(delay, key, job)
}

func ScheduleAt(at time.Time, key string, job func()) {
	delay := at.Sub(time.Now())
	tw.schedule(delay, key, job)
}

func Cancel(key string) {
	tw.removeTaskChan <- key
}
