// Package database is a memory database with redis compatible interface
package database

import (
	"strings"
	"time"

	"github.com/hdt3213/godis/datastruct/dict"
	"github.com/hdt3213/godis/interface/database"
	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/lib/logger"
	"github.com/hdt3213/godis/lib/timewheel"
	"github.com/hdt3213/godis/redis/protocol"
)

const (
	dataDictSize = 1 << 16
	ttlDictSize  = 1 << 10
)

// DB stores data and execute user's commands
type DB struct {

	// 表示数据库编号
	index int

	// key -> DataEntity
	data *dict.ConcurrentDict
	// key -> expireTime (time.Time)
	ttlMap *dict.ConcurrentDict
	// key -> version(uint32)
	versionMap *dict.ConcurrentDict

	// addaof is used to add command to aof
	addAof func(CmdLine)

	// callbacks
	insertCallback database.KeyEventCallback
	deleteCallback database.KeyEventCallback
}

// ExecFunc is interface for command executor
// args don't include cmd line
type ExecFunc func(db *DB, args [][]byte) redis.Reply

// PreFunc analyses command line when queued command to `multi`
// returns related write keys and read keys
type PreFunc func(args [][]byte) ([]string, []string)

// CmdLine is alias for [][]byte, represents a command line
type CmdLine = [][]byte

// UndoFunc returns undo logs for the given command line
// execute from head to tail when undo
type UndoFunc func(db *DB, args [][]byte) []CmdLine

// makeDB create DB instance
func makeDB() *DB {
	db := &DB{
		data:       dict.MakeConcurrent(dataDictSize),
		ttlMap:     dict.MakeConcurrent(ttlDictSize),
		versionMap: dict.MakeConcurrent(dataDictSize),
		addAof:     func(line CmdLine) {},
	}
	return db
}

// makeBasicDB create DB instance only with basic abilities.
func makeBasicDB() *DB {
	db := &DB{
		data:       dict.MakeConcurrent(dataDictSize),
		ttlMap:     dict.MakeConcurrent(ttlDictSize),
		versionMap: dict.MakeConcurrent(dataDictSize),
		addAof:     func(line CmdLine) {},
	}
	return db
}

// Exec executes command within one database

/*
c 客户端网络连接
cmdLine 客户端发过来的（待执行）命令
*/
func (db *DB) Exec(c redis.Connection, cmdLine [][]byte) redis.Reply {
	// transaction control commands and other commands which cannot execute within transaction
	cmdName := strings.ToLower(string(cmdLine[0]))
	if cmdName == "multi" {
		if len(cmdLine) != 1 {
			return protocol.MakeArgNumErrReply(cmdName)
		}
		return StartMulti(c)
	} else if cmdName == "discard" {
		if len(cmdLine) != 1 {
			return protocol.MakeArgNumErrReply(cmdName)
		}
		return DiscardMulti(c)
	} else if cmdName == "exec" {
		if len(cmdLine) != 1 {
			return protocol.MakeArgNumErrReply(cmdName)
		}
		return execMulti(db, c)
	} else if cmdName == "watch" {
		if !validateArity(-2, cmdLine) {
			return protocol.MakeArgNumErrReply(cmdName)
		}
		return Watch(db, c, cmdLine[1:])
	}
	if c != nil && c.InMultiState() {
		return EnqueueCmd(c, cmdLine)
	}

	// 如果是普通的 command ，在这里执行
	return db.execNormalCommand(cmdLine)
}

// 如果是普通的 command ，在这里执行
func (db *DB) execNormalCommand(cmdLine [][]byte) redis.Reply {
	cmdName := strings.ToLower(string(cmdLine[0]))

	// 从命令注册中心，获取 命令对象 cmd
	cmd, ok := cmdTable[cmdName]
	if !ok {
		return protocol.MakeErrReply("ERR unknown command '" + cmdName + "'")
	}

	// 对命令的参数个数进行 校验
	if !validateArity(cmd.arity, cmdLine) {
		return protocol.MakeArgNumErrReply(cmdName)
	}

	prepare := cmd.prepare
	write, read := prepare(cmdLine[1:])
	db.addVersion(write...)

	// 一堆的keys，肯定是要【读/写】到不同的shard中的，对不同的shard上的锁肯定也是不同的
	// 这里相当于对shard上锁
	db.RWLocks(write, read)
	// 这里对shard解锁
	defer db.RWUnLocks(write, read)
	fun := cmd.executor
	return fun(db, cmdLine[1:])
}

// execWithLock executes normal commands, invoker should provide locks
func (db *DB) execWithLock(cmdLine [][]byte) redis.Reply {
	cmdName := strings.ToLower(string(cmdLine[0]))
	cmd, ok := cmdTable[cmdName]
	if !ok {
		return protocol.MakeErrReply("ERR unknown command '" + cmdName + "'")
	}
	if !validateArity(cmd.arity, cmdLine) {
		return protocol.MakeArgNumErrReply(cmdName)
	}
	fun := cmd.executor
	return fun(db, cmdLine[1:])
}

func validateArity(arity int, cmdArgs [][]byte) bool {
	argNum := len(cmdArgs)
	if arity >= 0 {
		return argNum == arity
	}
	return argNum >= -arity
}

/* ---- Data Access ----- */

// GetEntity returns DataEntity bind to given key
func (db *DB) GetEntity(key string) (*database.DataEntity, bool) {
	raw, ok := db.data.GetWithLock(key)
	if !ok {
		return nil, false
	}
	if db.IsExpired(key) {
		return nil, false
	}
	entity, _ := raw.(*database.DataEntity)
	return entity, true
}

// PutEntity a DataEntity into DB
func (db *DB) PutEntity(key string, entity *database.DataEntity) int {
	ret := db.data.PutWithLock(key, entity)
	// db.insertCallback may be set as nil, during `if` and actually callback
	// so introduce a local variable `cb`
	if cb := db.insertCallback; ret > 0 && cb != nil {
		cb(db.index, key, entity)
	}
	return ret
}

// PutIfExists edit an existing DataEntity
func (db *DB) PutIfExists(key string, entity *database.DataEntity) int {
	return db.data.PutIfExistsWithLock(key, entity)
}

// PutIfAbsent insert an DataEntity only if the key not exists
func (db *DB) PutIfAbsent(key string, entity *database.DataEntity) int {
	ret := db.data.PutIfAbsentWithLock(key, entity)
	// db.insertCallback may be set as nil, during `if` and actually callback
	// so introduce a local variable `cb`
	if cb := db.insertCallback; ret > 0 && cb != nil {
		cb(db.index, key, entity)
	}
	return ret
}

// Remove the given key from db
func (db *DB) Remove(key string) {

	// 从 data中删除
	raw, deleted := db.data.RemoveWithLock(key)
	// 从 ttlMap中删除
	db.ttlMap.Remove(key)
	// 从时间轮中删除任务
	taskKey := genExpireTask(key)
	timewheel.Cancel(taskKey)

	if cb := db.deleteCallback; cb != nil {
		var entity *database.DataEntity
		if deleted > 0 {
			entity = raw.(*database.DataEntity)
		}
		cb(db.index, key, entity)
	}
}

// Removes the given keys from db
func (db *DB) Removes(keys ...string) (deleted int) {
	deleted = 0
	for _, key := range keys {
		_, exists := db.data.GetWithLock(key)
		if exists {
			db.Remove(key)
			deleted++
		}
	}
	return deleted
}

// Flush clean database
// deprecated
// for test only
func (db *DB) Flush() {
	db.data.Clear()
	db.ttlMap.Clear()
}

/* ---- Lock Function ----- */

// RWLocks lock keys for writing and reading
func (db *DB) RWLocks(writeKeys []string, readKeys []string) {
	db.data.RWLocks(writeKeys, readKeys)
}

// RWUnLocks unlock keys for writing and reading
func (db *DB) RWUnLocks(writeKeys []string, readKeys []string) {
	db.data.RWUnLocks(writeKeys, readKeys)
}

/* ---- TTL Functions ---- */

func genExpireTask(key string) string {
	return "expire:" + key
}

// Expire sets ttlCmd of key
func (db *DB) Expire(key string, expireTime time.Time) {

	// 在ttlMap中设置key的过期时间
	db.ttlMap.Put(key, expireTime)

	// 获取过期key 字符串
	taskKey := genExpireTask(key)

	// 设置延迟任务，在expireTime的时候，执行回调函数，
	timewheel.At(expireTime, taskKey, func() {
		keys := []string{key}

		// 锁定shard
		db.RWLocks(keys, nil)
		defer db.RWUnLocks(keys, nil)
		// check-lock-check, ttl may be updated during waiting lock
		logger.Info("expire " + key)
		rawExpireTime, ok := db.ttlMap.Get(key) // 所以这里额外读取了一次当前保存的真实的过期时间
		if !ok {
			return
		}
		// 比如时间轮为1s时间轮到期执行，但是用户可能又修改了到期时间为3s后
		expireTime, _ := rawExpireTime.(time.Time)
		expired := time.Now().After(expireTime) // 所以虽然回调任务要执行，但是是不能删除的
		if expired {

			// 确实过期了
			db.Remove(key)
		}
	})
}

// Persist cancel ttlCmd of key
func (db *DB) Persist(key string) {
	db.ttlMap.Remove(key)
	taskKey := genExpireTask(key)
	timewheel.Cancel(taskKey)
}

// IsExpired check whether a key is expired
func (db *DB) IsExpired(key string) bool {
	rawExpireTime, ok := db.ttlMap.Get(key)
	if !ok {
		return false
	}
	expireTime, _ := rawExpireTime.(time.Time)
	expired := time.Now().After(expireTime)
	if expired {
		db.Remove(key)
	}
	return expired
}

/* --- add version --- */

func (db *DB) addVersion(keys ...string) {
	for _, key := range keys {
		versionCode := db.GetVersion(key)
		db.versionMap.Put(key, versionCode+1)
	}
}

// GetVersion returns version code for given key
func (db *DB) GetVersion(key string) uint32 {
	entity, ok := db.versionMap.Get(key)
	if !ok {
		return 0
	}
	return entity.(uint32)
}

// ForEach traverses all the keys in the database
func (db *DB) ForEach(cb func(key string, data *database.DataEntity, expiration *time.Time) bool) {
	db.data.ForEach(func(key string, raw interface{}) bool {
		entity, _ := raw.(*database.DataEntity)
		var expiration *time.Time
		rawExpireTime, ok := db.ttlMap.Get(key)
		if ok {
			expireTime, _ := rawExpireTime.(time.Time)
			expiration = &expireTime
		}

		return cb(key, entity, expiration)
	})
}
