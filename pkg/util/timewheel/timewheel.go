package timewheel

import (
	"container/list"
	"log"
	"time"
)

type Task struct {
	delay time.Duration
	round int
	key   string
	job   func()
}

type TimeWheel struct {
	// interval between 2 ticks
	interval time.Duration
	ticker   *time.Ticker

	// slots on the wheel, each slot has a list of tasks
	slots []*list.List
	// current pos of slots
	currentPos int
	slotNum    int

	addTaskChan    chan Task
	removeTaskChan chan string

	timer map[string]int
}

func NewTimeWheel(interval time.Duration, slotNum int) *TimeWheel {
	timeWheel := &TimeWheel{
		interval:       interval,
		ticker:         time.NewTicker(interval),
		slots:          make([]*list.List, slotNum),
		currentPos:     0,
		slotNum:        slotNum,
		addTaskChan:    make(chan Task),
		removeTaskChan: make(chan string),
		timer:          make(map[string]int),
	}
	for i := 0; i < slotNum; i++ {
		timeWheel.slots[i] = list.New()
	}
	return timeWheel
}

func (tw *TimeWheel) schedule(delay time.Duration, key string, job func()) {
	task := newTask(delay, key, job)
	tw.AddTask(task)
}

func newTask(delay time.Duration, key string, job func()) *Task {
	return &Task{
		delay: delay,
		key:   key,
		job:   job,
	}
}

func (tw *TimeWheel) Start() {
	go tw.loop()
}

func (tw *TimeWheel) loop() {
	for {
		select {
		case <-tw.ticker.C:
			tw.handle()
		case task := <-tw.addTaskChan:
			tw.addTask(&task)
		case key := <-tw.removeTaskChan:
			tw.removeTask(key)
		}
	}
}

func (tw *TimeWheel) handle() {
	l := tw.slots[tw.currentPos]
	for element := l.Front(); element != nil; {
		task := element.Value.(Task)
		next := element.Next()
		// Task not for this round
		if task.round > 0 {
			task.round--
			element = next
			continue
		}
		// do job
		go func() {
			defer func() {
				err := recover()
				if err != nil {
					log.Println("TimeWheel task error ", err)
				}
			}()
			job := task.job
			job()
		}()
		l.Remove(element)
		if task.key != "" {
			tw.removeTask(task.key)
		}
		element = next
	}
	// go to next slot
	if tw.currentPos == tw.slotNum-1 {
		tw.currentPos = 0
	} else {
		tw.currentPos++
	}
}

func (tw *TimeWheel) AddTask(task *Task) {
	tw.addTaskChan <- *task
}

func (tw *TimeWheel) addTask(task *Task) {
	// get how many rounds before task and the slot for task
	round, slot := tw.getRoundAndSlot(*task)
	task.round = round
	tw.slots[slot].PushBack(*task)
	if task.key != "" {
		tw.timer[task.key] = slot
	}
}

func (tw *TimeWheel) getRoundAndSlot(task Task) (int, int) {
	delaySeconds := int(task.delay.Seconds())
	intervalSeconds := int(tw.interval.Seconds())
	rounds := delaySeconds / intervalSeconds / tw.slotNum
	slot := (tw.currentPos + delaySeconds/intervalSeconds) % tw.slotNum
	return rounds, slot
}

func (tw *TimeWheel) removeTask(key string) {
	slot, exists := tw.timer[key]
	if !exists {
		return
	}
	l := tw.slots[slot]
	for ele := l.Front(); ele != nil; {
		next := ele.Next()
		task := ele.Value.(Task)
		if task.key == key {
			l.Remove(ele)
			delete(tw.timer, key)
			break
		}
		ele = next
	}
}
