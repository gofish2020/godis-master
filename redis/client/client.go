package client

import (
	"errors"
	"net"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/lib/logger"
	"github.com/hdt3213/godis/lib/sync/wait"
	"github.com/hdt3213/godis/redis/parser"
	"github.com/hdt3213/godis/redis/protocol"
)

const (
	created = iota
	running
	closed
)

// Client is a pipeline mode redis client
type Client struct {
	conn        net.Conn
	pendingReqs chan *request // wait to send
	waitingReqs chan *request // waiting response
	ticker      *time.Ticker
	addr        string

	status  int32
	working *sync.WaitGroup // its counter presents unfinished requests(pending and waiting)
}

// request is a message sends to redis server
type request struct {
	id        uint64
	args      [][]byte
	reply     redis.Reply
	heartbeat bool
	waiting   *wait.Wait
	err       error
}

const (
	chanSize = 256
	maxWait  = 3 * time.Second
)

// MakeClient creates a new client
func MakeClient(addr string) (*Client, error) {

	// 连接服务器
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	return &Client{
		addr:        addr,                          // 服务器地址
		conn:        conn,                          // 连接句柄
		pendingReqs: make(chan *request, chanSize), // 保存 用户的发送的请求（同一个请求，会先保存到pendingReqs中，又会保存在 waitingReqs中）
		waitingReqs: make(chan *request, chanSize), // 请求发送以后成功后，保存等待回复的请求
		working:     &sync.WaitGroup{},             // 目的在于等待所有的网络请求数据包，处理完成（包括超时也算处理完成）
	}, nil
}

// Start starts asynchronous goroutines
func (client *Client) Start() {
	client.ticker = time.NewTicker(10 * time.Second)
	go client.handleWrite() //  启动协程，负责将 pendingReqs 中的请求，通过conn发送给服务端
	go client.handleRead()  // 负责通过conn读取服务端返回的请求，然后 对waitingReqs中对待回复请求处理
	go client.heartbeat()
	atomic.StoreInt32(&client.status, running)
}

// Close stops asynchronous goroutines and close connection
func (client *Client) Close() {

	// 目的：都是为了阻止继续往pendingReqs中继续写入数据（但是：我如果写入的数据很快，但是发送的很慢，很多都阻塞在pendingReqs的写入），
	// 虽然可以阻止新的，但是老的在阻塞中
	atomic.StoreInt32(&client.status, closed)
	client.ticker.Stop()

	// wait stop process
	client.working.Wait() // 等待所有的request全部都完成

	// stop new request
	close(client.pendingReqs) //放到这里合适，等待所有的req请求全部都处理完毕，在关闭（避免因为提前关闭，可能有部分的请求还正在 <-req放入pendingReqs 中，导致painc

	// clean
	_ = client.conn.Close()
	close(client.waitingReqs) // waitingReqs 的数据来源于 pendingReqs（如果pendingReqs 没有数据了，waitingReqs也就不会继续写入）
}

func (client *Client) reconnect() {
	logger.Info("reconnect with: " + client.addr)
	_ = client.conn.Close() // ignore possible errors from repeated closes

	var conn net.Conn
	for i := 0; i < 3; i++ {
		var err error
		conn, err = net.Dial("tcp", client.addr)
		if err != nil {
			logger.Error("reconnect error: " + err.Error())
			time.Sleep(time.Second)
			continue
		} else {
			break
		}
	}
	if conn == nil { // reach max retry, abort
		client.Close()
		return
	}

	// 因为连接重置，所以新conn里面，是读取不到用之前的conn发送的request数据包，响应数据的
	close(client.waitingReqs)
	for req := range client.waitingReqs {
		req.err = errors.New("connection closed")
		req.waiting.Done()
	}

	client.waitingReqs = make(chan *request, chanSize)

	// 需要放到这里赋值
	client.conn = conn
	// restart handle read
	go client.handleRead()
}

func (client *Client) heartbeat() {
	for range client.ticker.C {
		client.doHeartbeat()
	}
}

func (client *Client) handleWrite() {

	// 读取pendingReqs 中的数据
	for req := range client.pendingReqs {
		client.doRequest(req)
	}
}

// Send sends a request to redis server
func (client *Client) Send(args [][]byte) redis.Reply {
	// 说明客户端已经关闭
	if atomic.LoadInt32(&client.status) != running {
		return protocol.MakeErrReply("client closed")
	}

	req := &request{
		args:      args,
		heartbeat: false,
		waiting:   &wait.Wait{},
	}
	req.waiting.Add(1)

	client.working.Add(1)
	defer client.working.Done()

	// 发送到chan中，等待执行
	client.pendingReqs <- req

	// 等待执行完成
	timeout := req.waiting.WaitWithTimeout(maxWait)
	if timeout {
		return protocol.MakeErrReply("server time out")
	}
	if req.err != nil {
		return protocol.MakeErrReply("request failed " + req.err.Error())
	}
	return req.reply
}

func (client *Client) doHeartbeat() {
	request := &request{
		args:      [][]byte{[]byte("PING")},
		heartbeat: true,
		waiting:   &wait.Wait{},
	}
	// 设定 request 在处理中
	request.waiting.Add(1)

	client.working.Add(1)
	defer client.working.Done()

	// 保存到pendingReqs 中
	client.pendingReqs <- request

	// 阻塞等待（服务端的pong响应）
	request.waiting.WaitWithTimeout(maxWait)
}

func (client *Client) doRequest(req *request) {
	if req == nil || len(req.args) == 0 {
		return
	}

	// 构造请求（包）
	re := protocol.MakeMultiBulkReply(req.args)
	bytes := re.ToBytes()

	// 发送，最多重试3词
	var err error
	for i := 0; i < 3; i++ { // only retry, waiting for handleRead
		_, err = client.conn.Write(bytes)
		if err == nil ||
			(!strings.Contains(err.Error(), "timeout") && // only retry timeout
				!strings.Contains(err.Error(), "deadline exceeded")) {
			break
		}
	}
	// 通过网络发送成功（但是并不代表服务端执行成功），所以放入 waitingReqs中，表示等待服务端处理完成后的响应
	if err == nil {
		client.waitingReqs <- req
	} else {
		// 发送失败，直接设定该请求包执行失败
		req.err = err
		req.waiting.Done()
	}
}

func (client *Client) finishRequest(reply redis.Reply) {
	defer func() {
		if err := recover(); err != nil {
			debug.PrintStack()
			logger.Error(err)
		}
	}()
	// 将服务端端结果，保存到waitingReqs中的request中
	request := <-client.waitingReqs
	if request == nil {
		return
	}

	request.reply = reply
	if request.waiting != nil {
		request.waiting.Done()
	}
}

func (client *Client) handleRead() {

	// 读取网络请求
	ch := parser.ParseStream(client.conn)
	//
	for payload := range ch {
		if payload.Err != nil { // 如果出错，看下是否是客户端关闭了
			status := atomic.LoadInt32(&client.status)
			if status == closed { // 如果关闭了，就跳出死循环
				return
			}
			// 否则，需要重连，服务器
			client.reconnect()
			return
		}
		// 如果没有出错
		client.finishRequest(payload.Data)
	}
}
