package database

import (
	"fmt"
	"os"
	"runtime/debug"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/hdt3213/godis/aof"
	"github.com/hdt3213/godis/config"
	"github.com/hdt3213/godis/interface/database"
	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/lib/logger"
	"github.com/hdt3213/godis/lib/utils"
	"github.com/hdt3213/godis/pubsub"
	"github.com/hdt3213/godis/redis/protocol"
)

var godisVersion = "1.2.8" // do not modify

// Server is a redis-server with full capabilities including multiple database, rdb loader, replication
type Server struct {
	dbSet []*atomic.Value // *DB

	// handle publish/subscribe
	hub *pubsub.Hub
	// handle aof persistence
	persister *aof.Persister

	// for replication
	role         int32
	slaveStatus  *slaveStatus
	masterStatus *masterStatus

	// hooks
	insertCallback database.KeyEventCallback
	deleteCallback database.KeyEventCallback
}

func fileExists(filename string) bool {
	info, err := os.Stat(filename)
	return err == nil && !info.IsDir()
}

// NewStandaloneServer creates a standalone redis server, with multi database and all other funtions
func NewStandaloneServer() *Server {
	server := &Server{}

	// 默认有16个数据库
	if config.Properties.Databases == 0 {
		config.Properties.Databases = 16
	}
	// create tmp dir
	err := os.MkdirAll(config.GetTmpDir(), os.ModePerm)
	if err != nil {
		panic(fmt.Errorf("create tmp dir failed: %v", err))
	}
	// make db set

	// 有config.Properties.Databases个数据库，每个数据库有个编号index,
	// 每个数据库的底层其实是一个多个分片的map
	server.dbSet = make([]*atomic.Value, config.Properties.Databases)
	for i := range server.dbSet {

		// 创建数据库对象
		singleDB := makeDB()
		singleDB.index = i
		holder := &atomic.Value{}
		holder.Store(singleDB)
		server.dbSet[i] = holder
	}
	server.hub = pubsub.MakeHub()
	// record aof
	validAof := false
	if config.Properties.AppendOnly {
		validAof = fileExists(config.Properties.AppendFilename)
		aofHandler, err := NewPersister(server,
			config.Properties.AppendFilename, true, config.Properties.AppendFsync)
		if err != nil {
			panic(err)
		}

		//  aof文件句柄，保存在Server中，同时 16个数据库，都使用同一个aof文件
		server.bindPersister(aofHandler)
	}

	// 如果 aof没有加载，就加载rdb（这两个应该是互斥的），只会加载一个恢复内存
	if config.Properties.RDBFilename != "" && !validAof {
		// load rdb
		err := server.loadRdbFile()
		if err != nil {
			logger.Error(err)
		}
	}
	server.slaveStatus = initReplSlaveStatus()
	server.initMaster()
	server.startReplCron()
	server.role = masterRole // The initialization process does not require atomicity
	return server
}

// Exec executes command
// parameter `cmdLine` contains command and its arguments, for example: "set key value"
func (server *Server) Exec(c redis.Connection, cmdLine [][]byte) (result redis.Reply) {
	defer func() {
		if err := recover(); err != nil {
			logger.Warn(fmt.Sprintf("error occurs: %v\n%s", err, string(debug.Stack())))
			result = &protocol.UnknownErrReply{}
		}
	}()

	// 提取命令：名称
	cmdName := strings.ToLower(string(cmdLine[0]))
	// ping
	if cmdName == "ping" {
		return Ping(c, cmdLine[1:])
	}
	// authenticate
	if cmdName == "auth" {
		return Auth(c, cmdLine[1:])
	}
	if !isAuthenticated(c) {
		return protocol.MakeErrReply("NOAUTH Authentication required")
	}
	// info
	if cmdName == "info" {
		return Info(server, cmdLine[1:])
	}
	if cmdName == "slaveof" {
		if c != nil && c.InMultiState() {
			return protocol.MakeErrReply("cannot use slave of database within multi")
		}
		if len(cmdLine) != 3 {
			return protocol.MakeArgNumErrReply("SLAVEOF")
		}
		return server.execSlaveOf(c, cmdLine[1:])
	} else if cmdName == "command" {
		return execCommand(cmdLine[1:])
	}

	// read only slave
	role := atomic.LoadInt32(&server.role)
	if role == slaveRole && !c.IsMaster() {
		// only allow read only command, forbid all special commands except `auth` and `slaveof`
		if !isReadOnlyCommand(cmdName) {
			return protocol.MakeErrReply("READONLY You can't write against a read only slave.")
		}
	}

	// special commands which cannot execute within transaction
	if cmdName == "subscribe" {
		if len(cmdLine) < 2 {
			return protocol.MakeArgNumErrReply("subscribe")
		}
		return pubsub.Subscribe(server.hub, c, cmdLine[1:])
	} else if cmdName == "publish" {
		return pubsub.Publish(server.hub, cmdLine[1:])
	} else if cmdName == "unsubscribe" {
		return pubsub.UnSubscribe(server.hub, c, cmdLine[1:])
	} else if cmdName == "bgrewriteaof" { // 先定一个新的内存对象，然后加载aof文件，到内存中，最后将内存的数据，保存会tmpAof中，同时将aof中增量的命令也保存到tmpAof中，此时的tmpAof就是全量的数据
		if !config.Properties.AppendOnly {
			return protocol.MakeErrReply("AppendOnly is false, you can't rewrite aof file")
		}

		// aof.go imports router.go, router.go cannot import BGRewriteAOF from aof.go
		return BGRewriteAOF(server, cmdLine[1:])
	} else if cmdName == "rewriteaof" {
		if !config.Properties.AppendOnly {
			return protocol.MakeErrReply("AppendOnly is false, you can't rewrite aof file")
		}
		return RewriteAOF(server, cmdLine[1:])
	} else if cmdName == "flushall" {
		return server.flushAll()
	} else if cmdName == "flushdb" {
		if !validateArity(1, cmdLine) {
			return protocol.MakeArgNumErrReply(cmdName)
		}
		if c.InMultiState() {
			return protocol.MakeErrReply("ERR command 'FlushDB' cannot be used in MULTI")
		}
		return server.execFlushDB(c.GetDBIndex())
	} else if cmdName == "save" {
		return SaveRDB(server, cmdLine[1:])
	} else if cmdName == "bgsave" {
		return BGSaveRDB(server, cmdLine[1:])
	} else if cmdName == "select" { // 表示当前的 client连接，在哪个db下执行后序的操作命令（这个类似于服务端记录session信息的行为）
		if c != nil && c.InMultiState() {
			return protocol.MakeErrReply("cannot select database within multi")
		}
		if len(cmdLine) != 2 {
			return protocol.MakeArgNumErrReply("select")
		}
		return execSelect(c, server, cmdLine[1:])
	} else if cmdName == "copy" {
		if len(cmdLine) < 3 {
			return protocol.MakeArgNumErrReply("copy")
		}
		return execCopy(server, c, cmdLine[1:])
	} else if cmdName == "replconf" {
		return server.execReplConf(c, cmdLine[1:])
	} else if cmdName == "psync" {
		return server.execPSync(c, cmdLine[1:])
	}
	// todo: support multi database transaction

	// normal commands

	// 当前连接要选中哪个数据库
	dbIndex := c.GetDBIndex()
	// 这里相当于返回 该数据库对象
	selectedDB, errReply := server.selectDB(dbIndex)
	if errReply != nil {
		return errReply
	}

	// 【1个server -> 多个 db】 -> 【每个db -> 多个shard 】 -> 每个shard 就是底层的存储结构（锁 + map 构成）
	// 在选中的数据库中，执行命令
	return selectedDB.Exec(c, cmdLine)
}

// AfterClientClose does some clean after client close connection
func (server *Server) AfterClientClose(c redis.Connection) {
	pubsub.UnsubscribeAll(server.hub, c)
}

// Close graceful shutdown database
func (server *Server) Close() {
	// stop slaveStatus first
	server.slaveStatus.close()
	if server.persister != nil {
		server.persister.Close()
	}
	server.stopMaster()
}

func execSelect(c redis.Connection, mdb *Server, args [][]byte) redis.Reply {
	dbIndex, err := strconv.Atoi(string(args[0]))
	if err != nil {
		return protocol.MakeErrReply("ERR invalid DB index")
	}
	if dbIndex >= len(mdb.dbSet) || dbIndex < 0 {
		return protocol.MakeErrReply("ERR DB index is out of range")
	}
	c.SelectDB(dbIndex)
	return protocol.MakeOkReply()
}

func (server *Server) execFlushDB(dbIndex int) redis.Reply {
	if server.persister != nil {
		server.persister.SaveCmdLine(dbIndex, utils.ToCmdLine("FlushDB"))
	}
	return server.flushDB(dbIndex)
}

// flushDB flushes the selected database
func (server *Server) flushDB(dbIndex int) redis.Reply {
	if dbIndex >= len(server.dbSet) || dbIndex < 0 {
		return protocol.MakeErrReply("ERR DB index is out of range")
	}
	newDB := makeDB()
	server.loadDB(dbIndex, newDB)
	return &protocol.OkReply{}
}

func (server *Server) loadDB(dbIndex int, newDB *DB) redis.Reply {
	if dbIndex >= len(server.dbSet) || dbIndex < 0 {
		return protocol.MakeErrReply("ERR DB index is out of range")
	}
	oldDB := server.mustSelectDB(dbIndex)
	newDB.index = dbIndex
	newDB.addAof = oldDB.addAof // inherit oldDB
	server.dbSet[dbIndex].Store(newDB)
	return &protocol.OkReply{}
}

// flushAll flushes all databases.
func (server *Server) flushAll() redis.Reply {
	for i := range server.dbSet {
		server.flushDB(i)
	}
	if server.persister != nil {
		server.persister.SaveCmdLine(0, utils.ToCmdLine("FlushAll"))
	}
	return &protocol.OkReply{}
}

// selectDB returns the database with the given index, or an error if the index is out of range.
func (server *Server) selectDB(dbIndex int) (*DB, *protocol.StandardErrReply) {
	if dbIndex >= len(server.dbSet) || dbIndex < 0 {
		return nil, protocol.MakeErrReply("ERR DB index is out of range")
	}
	return server.dbSet[dbIndex].Load().(*DB), nil
}

// mustSelectDB is like selectDB, but panics if an error occurs.
func (server *Server) mustSelectDB(dbIndex int) *DB {
	selectedDB, err := server.selectDB(dbIndex)
	if err != nil {
		panic(err)
	}
	return selectedDB
}

// ForEach traverses all the keys in the given database
func (server *Server) ForEach(dbIndex int, cb func(key string, data *database.DataEntity, expiration *time.Time) bool) {
	// 选中某个db，遍历其中的shard
	server.mustSelectDB(dbIndex).ForEach(cb)
}

// GetEntity returns the data entity to the given key
func (server *Server) GetEntity(dbIndex int, key string) (*database.DataEntity, bool) {
	return server.mustSelectDB(dbIndex).GetEntity(key)
}

func (server *Server) GetExpiration(dbIndex int, key string) *time.Time {
	raw, ok := server.mustSelectDB(dbIndex).ttlMap.Get(key)
	if !ok {
		return nil
	}
	expireTime, _ := raw.(time.Time)
	return &expireTime
}

// ExecMulti executes multi commands transaction Atomically and Isolated
func (server *Server) ExecMulti(conn redis.Connection, watching map[string]uint32, cmdLines []CmdLine) redis.Reply {
	selectedDB, errReply := server.selectDB(conn.GetDBIndex())
	if errReply != nil {
		return errReply
	}
	return selectedDB.ExecMulti(conn, watching, cmdLines)
}

// RWLocks lock keys for writing and reading
func (server *Server) RWLocks(dbIndex int, writeKeys []string, readKeys []string) {
	server.mustSelectDB(dbIndex).RWLocks(writeKeys, readKeys)
}

// RWUnLocks unlock keys for writing and reading
func (server *Server) RWUnLocks(dbIndex int, writeKeys []string, readKeys []string) {
	server.mustSelectDB(dbIndex).RWUnLocks(writeKeys, readKeys)
}

// GetUndoLogs return rollback commands
func (server *Server) GetUndoLogs(dbIndex int, cmdLine [][]byte) []CmdLine {
	return server.mustSelectDB(dbIndex).GetUndoLogs(cmdLine)
}

// ExecWithLock executes normal commands, invoker should provide locks
func (server *Server) ExecWithLock(conn redis.Connection, cmdLine [][]byte) redis.Reply {
	db, errReply := server.selectDB(conn.GetDBIndex())
	if errReply != nil {
		return errReply
	}
	return db.execWithLock(cmdLine)
}

// BGRewriteAOF asynchronously rewrites Append-Only-File
func BGRewriteAOF(db *Server, args [][]byte) redis.Reply {
	go db.persister.Rewrite()
	return protocol.MakeStatusReply("Background append only file rewriting started")
}

// RewriteAOF start Append-Only-File rewriting and blocked until it finished
func RewriteAOF(db *Server, args [][]byte) redis.Reply {
	err := db.persister.Rewrite()
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}
	return protocol.MakeOkReply()
}

// SaveRDB start RDB writing and blocked until it finished
func SaveRDB(db *Server, args [][]byte) redis.Reply {
	if db.persister == nil {
		return protocol.MakeErrReply("please enable aof before using save")
	}
	rdbFilename := config.Properties.RDBFilename
	if rdbFilename == "" {
		rdbFilename = "dump.rdb"
	}
	err := db.persister.GenerateRDB(rdbFilename)
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}
	return protocol.MakeOkReply()
}

// BGSaveRDB asynchronously save RDB
func BGSaveRDB(db *Server, args [][]byte) redis.Reply {
	if db.persister == nil {
		return protocol.MakeErrReply("please enable aof before using save")
	}
	go func() {
		defer func() {
			if err := recover(); err != nil {
				logger.Error(err)
			}
		}()
		rdbFilename := config.Properties.RDBFilename
		if rdbFilename == "" {
			rdbFilename = "dump.rdb"
		}
		err := db.persister.GenerateRDB(rdbFilename)
		if err != nil {
			logger.Error(err)
		}
	}()
	return protocol.MakeStatusReply("Background saving started")
}

// GetDBSize returns keys count and ttl key count
func (server *Server) GetDBSize(dbIndex int) (int, int) {
	db := server.mustSelectDB(dbIndex)
	return db.data.Len(), db.ttlMap.Len()
}

func (server *Server) startReplCron() {
	go func(mdb *Server) {
		ticker := time.Tick(time.Second * 10)
		for range ticker {
			mdb.slaveCron()
			mdb.masterCron()
		}
	}(server)
}

// GetAvgTTL Calculate the average expiration time of keys
func (server *Server) GetAvgTTL(dbIndex, randomKeyCount int) int64 {
	var ttlCount int64
	db := server.mustSelectDB(dbIndex)
	keys := db.data.RandomKeys(randomKeyCount)
	for _, k := range keys {
		t := time.Now()
		rawExpireTime, ok := db.ttlMap.Get(k)
		if !ok {
			continue
		}
		expireTime, _ := rawExpireTime.(time.Time)
		// if the key has already reached its expiration time during calculation, ignore it
		if expireTime.Sub(t).Microseconds() > 0 {
			ttlCount += expireTime.Sub(t).Microseconds()
		}
	}
	return ttlCount / int64(len(keys))
}

func (server *Server) SetKeyInsertedCallback(cb database.KeyEventCallback) {
	server.insertCallback = cb
	for i := range server.dbSet {
		db := server.mustSelectDB(i)
		db.insertCallback = cb
	}

}

func (server *Server) SetKeyDeletedCallback(cb database.KeyEventCallback) {
	server.deleteCallback = cb
	for i := range server.dbSet {
		db := server.mustSelectDB(i)
		db.deleteCallback = cb
	}
}
