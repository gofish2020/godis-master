package aof

import (
	"context"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	rdb "github.com/hdt3213/rdb/core"

	"github.com/hdt3213/godis/config"

	"github.com/hdt3213/godis/interface/database"
	"github.com/hdt3213/godis/lib/logger"
	"github.com/hdt3213/godis/lib/utils"
	"github.com/hdt3213/godis/redis/connection"
	"github.com/hdt3213/godis/redis/parser"
	"github.com/hdt3213/godis/redis/protocol"
)

// CmdLine is alias for [][]byte, represents a command line
type CmdLine = [][]byte

const (
	aofQueueSize = 1 << 20 // 1048576
)

const (
	// FsyncAlways do fsync for every command
	FsyncAlways = "always"
	// FsyncEverySec do fsync every second
	FsyncEverySec = "everysec"
	// FsyncNo lets operating system decides when to do fsync
	FsyncNo = "no"
)

type payload struct {
	cmdLine CmdLine
	dbIndex int
	wg      *sync.WaitGroup
}

/*

AOF 这个的代码逻辑：

1.打开 aof文件
2.写入数据：分为直接写入文件&刷盘 or 直接写入到chan缓冲中（然后每隔1s中，刷盘）
3.写入到文件中的数据格式：按照 redis aof格式写入，如下：

*3  // 表示有几个部分（3个）
$3	// 表示set字符长度
set
$3	// 表示 key字符长度
key
$4	// 表示value字符长度
value

*/

// Listener will be called-back after receiving a aof payload
// with a listener we can forward the updates to slave nodes etc.
type Listener interface {
	// Callback will be called-back after receiving a aof payload
	Callback([]CmdLine)
}

// Persister receive msgs from channel and write to AOF file
type Persister struct {
	ctx        context.Context
	cancel     context.CancelFunc
	db         database.DBEngine
	tmpDBMaker func() database.DBEngine
	// aofChan is the channel to receive aof payload(listenCmd will send payload to this channel)
	aofChan chan *payload
	// aofFile is the file handler of aof file
	aofFile *os.File
	// aofFilename is the path of aof file
	aofFilename string
	// aofFsync is the strategy of fsync
	aofFsync string
	// aof goroutine will send msg to main goroutine through this channel when aof tasks finished and ready to shut down
	aofFinished chan struct{}
	// pause aof for start/finish aof rewrite progress
	pausingAof sync.Mutex
	currentDB  int // aof文件中，【当前】最后选中的db索引,加载完aof后，这个会改变
	listeners  map[Listener]struct{}
	// reuse cmdLine buffer
	buffer []CmdLine
}

// NewPersister creates a new aof.Persister
func NewPersister(db database.DBEngine, filename string, load bool, fsync string, tmpDBMaker func() database.DBEngine) (*Persister, error) {
	persister := &Persister{}
	persister.aofFilename = filename
	persister.aofFsync = strings.ToLower(fsync)
	persister.db = db // 是*server对象
	persister.tmpDBMaker = tmpDBMaker
	persister.currentDB = 0
	// load aof file if needed
	if load {
		persister.LoadAof(0)
	}
	aofFile, err := os.OpenFile(persister.aofFilename, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	persister.aofFile = aofFile
	persister.aofChan = make(chan *payload, aofQueueSize)
	persister.aofFinished = make(chan struct{})
	persister.listeners = make(map[Listener]struct{})
	// start aof goroutine to write aof file in background and fsync periodically if needed (see fsyncEverySecond)
	go func() {
		persister.listenCmd()
	}()
	ctx, cancel := context.WithCancel(context.Background())
	persister.ctx = ctx
	persister.cancel = cancel
	// fsync every second if needed
	if persister.aofFsync == FsyncEverySec {
		persister.fsyncEverySecond()
	}
	return persister, nil
}

// RemoveListener removes a listener from aof handler, so we can close the listener
func (persister *Persister) RemoveListener(listener Listener) {
	persister.pausingAof.Lock()
	defer persister.pausingAof.Unlock()
	delete(persister.listeners, listener)
}

// SaveCmdLine send command to aof goroutine through channel
func (persister *Persister) SaveCmdLine(dbIndex int, cmdLine CmdLine) {
	// aofChan will be set as nil temporarily during load aof see Persister.LoadAof
	if persister.aofChan == nil {
		return
	}

	// 如果是每次都写入& 刷盘

	if persister.aofFsync == FsyncAlways {
		p := &payload{
			cmdLine: cmdLine,
			dbIndex: dbIndex,
		}
		persister.writeAof(p)
		return
	}

	// 写入到chan 缓冲中
	persister.aofChan <- &payload{
		cmdLine: cmdLine,
		dbIndex: dbIndex,
	}

}

// listenCmd listen aof channel and write into file
func (persister *Persister) listenCmd() {

	// 从chan缓冲中读取数据
	for p := range persister.aofChan {
		persister.writeAof(p)
	}
	persister.aofFinished <- struct{}{}
}

func (persister *Persister) writeAof(p *payload) {

	// 清空 buffer
	persister.buffer = persister.buffer[:0] // reuse underlying array
	persister.pausingAof.Lock()             // prevent other goroutines from pausing aof
	// 加锁 （写锁）
	defer persister.pausingAof.Unlock()
	// ensure aof is in the right database

	// payload的db和 currentDB 不同
	if p.dbIndex != persister.currentDB {
		// select db
		// 这里其实就是拼接出 " SELECT 0",字符串
		selectCmd := utils.ToCmdLine("SELECT", strconv.Itoa(p.dbIndex))
		// 在buffer中缓存一份
		persister.buffer = append(persister.buffer, selectCmd)

		// 将 命令进行 格式转化
		data := protocol.MakeMultiBulkReply(selectCmd).ToBytes()
		// 保存到 文件中
		_, err := persister.aofFile.Write(data)
		if err != nil {
			logger.Warn(err)
			return // skip this command
		}
		persister.currentDB = p.dbIndex
	}
	// save command
	data := protocol.MakeMultiBulkReply(p.cmdLine).ToBytes()
	persister.buffer = append(persister.buffer, p.cmdLine)
	_, err := persister.aofFile.Write(data)
	if err != nil {
		logger.Warn(err)
	}

	// buffer中保存到是命令字符串
	for listener := range persister.listeners {
		listener.Callback(persister.buffer)
	}

	// 主动 刷盘一次
	if persister.aofFsync == FsyncAlways {
		_ = persister.aofFile.Sync()
	}
}

// LoadAof read aof file, can only be used before Persister.listenCmd started
func (persister *Persister) LoadAof(maxBytes int) {
	// persister.db.Exec may call persister.AddAof
	// delete aofChan to prevent loaded commands back into aofChan
	aofChan := persister.aofChan
	persister.aofChan = nil
	defer func(aofChan chan *payload) {
		persister.aofChan = aofChan
	}(aofChan)

	// 只读的方式，读取 aof 内的数据
	file, err := os.Open(persister.aofFilename)
	if err != nil {
		if _, ok := err.(*os.PathError); ok {
			return
		}
		logger.Warn(err)
		return
	}
	defer file.Close()

	// load rdb preamble if needed
	decoder := rdb.NewDecoder(file)
	err = persister.db.LoadRDB(decoder)
	if err != nil {
		// no rdb preamble
		file.Seek(0, io.SeekStart)
	} else {
		// has rdb preamble
		_, _ = file.Seek(int64(decoder.GetReadCount())+1, io.SeekStart)
		maxBytes = maxBytes - decoder.GetReadCount()
	}

	// 从文件中读取 maxBytes大小的数据
	var reader io.Reader
	if maxBytes > 0 {
		reader = io.LimitReader(file, int64(maxBytes))
	} else {
		reader = file
	}

	// aof文件中保存的命令格式，和肉眼看到的格式是不同的；
	// 从aof文件中，接解析出命令：例如 set key value
	ch := parser.ParseStream(reader)
	fakeConn := connection.NewFakeConn() // only used for save dbIndex
	for p := range ch {
		if p.Err != nil {
			if p.Err == io.EOF {
				break
			}
			logger.Error("parse error: " + p.Err.Error())
			continue
		}
		if p.Data == nil {
			logger.Error("empty payload")
			continue
		}
		r, ok := p.Data.(*protocol.MultiBulkReply)
		if !ok {
			logger.Error("require multi bulk protocol")
			continue
		}

		// 这里应该是【重放命令】 -- 其实就是重新执行命令，保存到 db 内存中
		ret := persister.db.Exec(fakeConn, r.Args)

		if protocol.IsErrorReply(ret) {
			logger.Error("exec err", string(ret.ToBytes()))
		}
		if strings.ToLower(string(r.Args[0])) == "select" {
			// execSelect success, here must be no error
			dbIndex, err := strconv.Atoi(string(r.Args[1]))
			if err == nil {
				persister.currentDB = dbIndex
			}
		}
	}
}

// Fsync flushes aof file to disk
func (persister *Persister) Fsync() {
	persister.pausingAof.Lock()

	// 就是调用系统api，将 系统缓冲区中的数据，刷入到磁盘中
	if err := persister.aofFile.Sync(); err != nil {
		logger.Errorf("fsync failed: %v", err)
	}

	persister.pausingAof.Unlock()
}

// Close gracefully stops aof persistence procedure
func (persister *Persister) Close() {
	if persister.aofFile != nil {
		close(persister.aofChan)
		<-persister.aofFinished          // 说明 aofChan中的所有，有效数据全部读取完毕了
		err := persister.aofFile.Close() // 可以关闭文件句柄
		if err != nil {
			logger.Warn(err)
		}
	}
	persister.cancel()
}

// fsyncEverySecond fsync aof file every second
func (persister *Persister) fsyncEverySecond() {
	ticker := time.NewTicker(time.Second)
	go func() {
		for {
			select {
			case <-ticker.C: // 每隔 1s钟，执行一下Sync()
				persister.Fsync()
			case <-persister.ctx.Done():
				return
			}
		}
	}()
}

/*


也就是全部都创建新的对象（目的在于区别于之前的对象，避免影响）
然后重新加载aof文件到内存中，然后再将内存中的数据保存会aof文件


数据的三种呈现：一种是aof文件 -> 基本的命令行 -> 内存中的数据

*Persister ，*server 都是新的对象

*/

func (persister *Persister) generateAof(ctx *RewriteCtx) error {
	// rewrite aof tmpFile
	tmpFile := ctx.tmpFile
	// load aof tmpFile

	// 读取当前的aof文件中的数据，并且保存到新内存对象中【 这里相当于新创建了一个*Persister 对象；】
	tmpAof := persister.newRewriteHandler()

	// 将aof文件内容，加载到内存中
	tmpAof.LoadAof(int(ctx.fileSize))

	// 遍历每一个数据库，然后读取数据
	for i := 0; i < config.Properties.Databases; i++ {
		// select db
		data := protocol.MakeMultiBulkReply(utils.ToCmdLine("SELECT", strconv.Itoa(i))).ToBytes()
		_, err := tmpFile.Write(data)
		if err != nil {
			return err
		}
		// dump db

		// 遍历 某个数据库 内存中的map数据，保存到 tmpFile文件中
		tmpAof.db.ForEach(i, func(key string, entity *database.DataEntity, expiration *time.Time) bool {
			// 转成 set key value 这种命令
			cmd := EntityToCmd(key, entity)
			if cmd != nil {
				// 将命令转成aof数据格式，保存到文件中
				_, _ = tmpFile.Write(cmd.ToBytes())
			}

			// 如果有过期时间，再写入过期时间aof数据格式
			if expiration != nil {
				cmd := MakeExpireCmd(key, *expiration)
				if cmd != nil {
					_, _ = tmpFile.Write(cmd.ToBytes())
				}
			}
			return true
		})
	}
	return nil
}
