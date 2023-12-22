package timewheel

import (
	"container/list"
	"time"

	"github.com/hdt3213/godis/lib/logger"
)

/*
如果我有一个任务设定过期时间为 0*time.Second,该任务会放到 currentPos的链表中

该
*/
type location struct {
	slot  int
	etask *list.Element
}

// TimeWheel can execute job after waiting given duration
type TimeWheel struct {
	interval time.Duration
	ticker   *time.Ticker
	slots    []*list.List

	// 不然的话，我要找key需要查询list，这个效率就是O(n)的复杂度了
	timer             map[string]*location // timer的作用，就是用来对key做搜索，是否重复（去重的作用），删除历史的任务，新增新的任务
	currentPos        int
	slotNum           int
	addTaskChannel    chan task
	removeTaskChannel chan string
	stopChannel       chan bool

	//	scanningIndex []int // 正在扫描到( 1 表示正在扫 0表示扫结束/未进行扫)

}

type task struct {
	delay  time.Duration
	circle int
	key    string
	job    func()
}

// New creates a new time wheel
func New(interval time.Duration, slotNum int) *TimeWheel {

	if interval <= 0 || slotNum <= 0 {
		return nil
	}

	// interval 时间刻度 1s  slotNum 表示一圈有多少个刻度 3600s = 60min = 1hour
	tw := &TimeWheel{
		interval:          interval,
		slots:             make([]*list.List, slotNum), // 每个槽，有一个任务链表
		timer:             make(map[string]*location),
		currentPos:        0, // 当前秒针指向哪个位置
		slotNum:           slotNum,
		addTaskChannel:    make(chan task),
		removeTaskChannel: make(chan string),
		stopChannel:       make(chan bool),
		//scanningIndex:     make([]int, slotNum),
	}
	tw.initSlots()

	return tw
}

func (tw *TimeWheel) initSlots() {
	for i := 0; i < tw.slotNum; i++ {
		tw.slots[i] = list.New()
	}
}

// Start starts ticker for time wheel
func (tw *TimeWheel) Start() {
	tw.ticker = time.NewTicker(tw.interval)
	go tw.start() // 启动死循环，监听任务到期
}

// Stop stops the time wheel
func (tw *TimeWheel) Stop() {
	tw.stopChannel <- true
}

// AddJob add new job into pending queue
func (tw *TimeWheel) AddJob(delay time.Duration, key string, job func()) {
	if delay < 0 {
		return
	}
	tw.addTaskChannel <- task{delay: delay, key: key, job: job}
}

// RemoveJob add remove job from pending queue
// if job is done or not found, then nothing happened
func (tw *TimeWheel) RemoveJob(key string) {
	if key == "" {
		return
	}
	tw.removeTaskChannel <- key
}

func (tw *TimeWheel) start() {
	for {
		select {
		case <-tw.ticker.C: // 到了1s
			tw.tickHandler()
		case task := <-tw.addTaskChannel:
			tw.addTask(&task)
		case key := <-tw.removeTaskChannel:
			tw.removeTask(key)
		case <-tw.stopChannel:
			tw.ticker.Stop()
			return
		}
	}
}

func (tw *TimeWheel) tickHandler() {
	// 对当前currentPos 指向的链表，进行全部扫描
	l := tw.slots[tw.currentPos]
	idx := tw.currentPos

	// 表示一圈完成，继续下一圈
	if tw.currentPos == tw.slotNum-1 {
		tw.currentPos = 0
	} else {
		tw.currentPos++
	}

	// 对当前currentPos 指向的链表，进行全部扫描
	go tw.scanAndRunTask(l, idx)
}

func (tw *TimeWheel) scanAndRunTask(l *list.List, idx int) {

	// tw.scanningIndex[idx] = 1
	// defer func() {
	// 	tw.scanningIndex[idx] = 0
	// }()

	for e := l.Front(); e != nil; {
		task := e.Value.(*task)
		// 如果circle > 0 说明该任务不是本圈要执行的
		if task.circle > 0 {
			task.circle--
			e = e.Next()
			continue
		}

		// 说明该任务是本圈要执行的任务，开启goroutine执行回调函数
		go func() {
			defer func() {
				if err := recover(); err != nil {
					logger.Error(err)
				}
			}()
			job := task.job
			job()
		}()
		next := e.Next()
		// 从链表删除
		l.Remove(e)
		// 从timer 删除
		if task.key != "" {
			delete(tw.timer, task.key)
		}
		e = next
	}

}

// func (tw *TimeWheel) addScanningTask(task *task, pos int) {

// 	for {
// 		if tw.scanningIndex[pos] == 0 {

// 			// 保存到链表中
// 			e := tw.slots[pos].PushBack(task)

//				loc := &location{
//					slot:  pos,
//					etask: e,
//				}
//				// 相当于重复对同一个key设置延迟过期任务
//				if task.key != "" {
//					_, ok := tw.timer[task.key]
//					if ok {
//						tw.removeTask(task.key)
//					}
//				}
//				// 保存新的任务
//				tw.timer[task.key] = loc
//				break
//			}
//		}
//	}
func (tw *TimeWheel) addTask(task *task) {
	pos, circle := tw.getPositionAndCircle(task.delay)
	task.circle = circle

	// if tw.scanningIndex[pos] == 1 {
	// 	logger.Infof("delay add task index:%d\n", pos)
	// 	go tw.addScanningTask(task, pos)
	// 	return
	// }

	logger.Infof("rightnow add task index:%d\n", pos)
	// 保存到链表中
	e := tw.slots[pos].PushBack(task)

	loc := &location{
		slot:  pos,
		etask: e,
	}
	// 相当于重复对同一个key设置延迟过期任务
	if task.key != "" {
		_, ok := tw.timer[task.key]
		if ok {
			tw.removeTask(task.key)
		}
	}
	// 保存新的任务
	tw.timer[task.key] = loc
}

func (tw *TimeWheel) getPositionAndCircle(d time.Duration) (pos int, circle int) {

	// 通过延迟任务的时间间隔
	delaySeconds := int(d.Seconds())
	intervalSeconds := int(tw.interval.Seconds())
	// 计算，需要几圈 delaySeconds / intervalSeconds 相当于有多少个
	circle = int(delaySeconds / intervalSeconds / tw.slotNum)
	// 计算，在哪个pos位置
	pos = int(tw.currentPos+delaySeconds/intervalSeconds) % tw.slotNum

	return
}

func (tw *TimeWheel) removeTask(key string) {
	pos, ok := tw.timer[key]
	if !ok {
		return
	}

	// 删除历史的过期任务
	l := tw.slots[pos.slot]
	l.Remove(pos.etask)
	delete(tw.timer, key)
}
