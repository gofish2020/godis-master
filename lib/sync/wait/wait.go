package wait

import (
	"sync"
	"time"
)

// Wait is similar with sync.WaitGroup which can wait with timeout
type Wait struct {
	wg sync.WaitGroup
}

// Add adds delta, which may be negative, to the WaitGroup counter.
func (w *Wait) Add(delta int) {
	w.wg.Add(delta)
}

// Done decrements the WaitGroup counter by one
func (w *Wait) Done() {
	w.wg.Done()
}

// Wait blocks until the WaitGroup counter is zero.
func (w *Wait) Wait() {
	w.wg.Wait()
}

// WaitWithTimeout blocks until the WaitGroup counter is zero or timeout
// returns true if timeout
func (w *Wait) WaitWithTimeout(timeout time.Duration) bool {

	// 这里c是有缓冲的，避免goroutine泄漏
	c := make(chan struct{}, 1)
	go func() {
		defer close(c)
		w.Wait()
		c <- struct{}{} // 因为如果 虾米啊的select先执行完成，就没有 <-c 从c中读取数据，这个时候c又是没有缓冲的，那么这里一定一直阻塞，goroutine就永远无法停止
	}()

	// 要么c 成功 or 超时退出
	select {
	case <-c:
		return false // completed normally
	case <-time.After(timeout):
		return true // timed out
	}
}
