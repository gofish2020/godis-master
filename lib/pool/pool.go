package pool

import (
	"errors"
	"sync"
)

var (
	ErrClosed = errors.New("pool closed")
	ErrMax    = errors.New("reach max connection limit")
)

type request chan interface{}

type Config struct {
	MaxIdle   uint
	MaxActive uint
}

// Pool stores object for reusing, such as redis connection
type Pool struct {
	Config
	factory     func() (interface{}, error) // 对象生成器
	finalizer   func(x interface{})         // 对象释放器
	idles       chan interface{}
	waitingReqs []request
	activeCount uint // increases during creating connection, decrease during destroying connection
	mu          sync.Mutex
	closed      bool
}

func New(factory func() (interface{}, error), finalizer func(x interface{}), cfg Config) *Pool {
	return &Pool{
		factory:     factory,
		finalizer:   finalizer,
		idles:       make(chan interface{}, cfg.MaxIdle),
		waitingReqs: make([]request, 0),
		Config:      cfg,
	}
}

// getOnNoIdle try to create a new connection or waiting for connection being returned
// invoker should have pool.mu
func (pool *Pool) getOnNoIdle() (interface{}, error) {

	// 如果生成的太多了，就不能生成新的，就要一直阻塞等待
	if pool.activeCount >= pool.MaxActive {
		// waiting for connection being returned
		req := make(chan interface{}, 1)
		pool.waitingReqs = append(pool.waitingReqs, req)
		pool.mu.Unlock()
		x, ok := <-req
		if !ok {
			return nil, ErrMax
		}
		return x, nil
	}

	// 否则就可以直接生成一个新对象
	// create a new connection
	pool.activeCount++ // hold a place for new connection
	pool.mu.Unlock()
	x, err := pool.factory()
	if err != nil {
		// create failed return token
		pool.mu.Lock()
		pool.activeCount-- // release the holding place
		pool.mu.Unlock()
		return nil, err
	}
	return x, nil
}

func (pool *Pool) Get() (interface{}, error) {
	pool.mu.Lock()
	if pool.closed {
		pool.mu.Unlock()
		return nil, ErrClosed
	}

	select {

	// 从空闲获取一个对象
	case item := <-pool.idles:
		pool.mu.Unlock()
		return item, nil
	default:
		// no pooled item, create one
		return pool.getOnNoIdle() // 如果没有空闲，主动生成一个新的
	}
}

func (pool *Pool) Put(x interface{}) {
	pool.mu.Lock()

	if pool.closed {
		pool.mu.Unlock()
		pool.finalizer(x)
		return
	}

	// 有等待的协程
	if len(pool.waitingReqs) > 0 {
		req := pool.waitingReqs[0]
		copy(pool.waitingReqs, pool.waitingReqs[1:])
		pool.waitingReqs = pool.waitingReqs[:len(pool.waitingReqs)-1]
		req <- x
		pool.mu.Unlock()
		return
	}

	select {
	case pool.idles <- x:
		pool.mu.Unlock()
		return
	default:
		// reach max idle, destroy redundant item
		pool.mu.Unlock()
		pool.activeCount--
		pool.finalizer(x)
	}
}

func (pool *Pool) Close() {
	pool.mu.Lock()
	if pool.closed {
		pool.mu.Unlock()
		return
	}
	pool.closed = true
	close(pool.idles)

	for _, req := range pool.waitingReqs {
		close(req)
	}
	pool.mu.Unlock()

	for x := range pool.idles {
		pool.finalizer(x)
	}
}
