package tcp

/**
 * A tcp server
 */

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/hdt3213/godis/interface/tcp"
	"github.com/hdt3213/godis/lib/logger"
)

// Config stores tcp server properties
type Config struct {
	Address    string        `yaml:"address"`
	MaxConnect uint32        `yaml:"max-connect"`
	Timeout    time.Duration `yaml:"timeout"`
}

// ClientCounter Record the number of clients in the current Godis server
var ClientCounter int

// ListenAndServeWithSignal binds port and handle requests, blocking until receive stop signal
func ListenAndServeWithSignal(cfg *Config, handler tcp.Handler) error {

	closeChan := make(chan struct{})

	// *接收用户信号
	sigCh := make(chan os.Signal)
	signal.Notify(sigCh, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		sig := <-sigCh
		switch sig {
		case syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT:
			closeChan <- struct{}{} // 如果收到上面的几种信号，激活 closeChan
		}
	}()

	// *初始化监听socket
	listener, err := net.Listen("tcp", cfg.Address) //  cfg.Address = ip:port
	if err != nil {
		return err
	}
	//cfg.Address = listener.Addr().String()
	logger.Info(fmt.Sprintf("bind: %s, start listening...", cfg.Address))

	// *开始正式监听和接收客户端连接
	ListenAndServe(listener, handler, closeChan)
	return nil
}

// ListenAndServe binds port and handle requests, blocking until close
func ListenAndServe(listener net.Listener, handler tcp.Handler, closeChan <-chan struct{}) {
	// listen signal
	errCh := make(chan error, 1)

	defer close(errCh)

	// ***这里负责做所有的监听（也就是对所有的异常进行收口）**
	go func() {
		select {
		case <-closeChan:
			logger.Info("get exit signal")
		case er := <-errCh:
			logger.Info(fmt.Sprintf("accept error: %s", er.Error()))
		}
		logger.Info("shutting down...")
		_ = listener.Close() // listener.Accept() will return err immediately
		_ = handler.Close()  // close connections
	}()

	ctx := context.Background()
	var waitDone sync.WaitGroup

	for {

		// 死循环
		conn, err := listener.Accept()
		if err != nil {
			// learn from net/http/serve.go#Serve()
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				logger.Infof("accept occurs temporary error: %v, retry in 5ms", err)
				time.Sleep(5 * time.Millisecond)
				continue
			}
			// 如果出错，整个死循环会结束
			errCh <- err
			break
		}

		// handle
		logger.Info("accept link")
		ClientCounter++
		waitDone.Add(1)
		go func() {
			defer func() {
				waitDone.Done()
				ClientCounter--
			}()
			// 每个客户端启动一个goroutine处理
			handler.Handle(ctx, conn)
		}()
	}
	// 这里相当于优雅结束（也就是要等 每个处理客户单的goroutine停止，这里才会退出）
	waitDone.Wait()
}
