package cluster

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/hdt3213/godis/database"
	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/lib/logger"
	"github.com/hdt3213/godis/lib/timewheel"
	"github.com/hdt3213/godis/redis/protocol"
)

// prepareFunc executed after related key locked, and use additional logic to determine whether the transaction can be committed
// For example, prepareMSetNX  will return error to prevent MSetNx transaction from committing if any related key already exists
var prepareFuncMap = make(map[string]CmdFunc)

func registerPrepareFunc(cmdName string, fn CmdFunc) {
	prepareFuncMap[strings.ToLower(cmdName)] = fn
}

// Transaction stores state and data for a try-commit-catch distributed transaction
type Transaction struct {
	id      string           // transaction id
	cmdLine [][]byte         // redis命令
	cluster *Cluster         // 集群对象
	conn    redis.Connection // socket连接
	dbIndex int              // 数据库索引

	writeKeys  []string  // 写key
	readKeys   []string  // 读key
	keysLocked bool      // 是否对写key/读key已经上锁
	undoLog    []CmdLine // 回滚日志

	status int8        // 事务状态
	mu     *sync.Mutex // 事务锁（操作事务对象的时候上锁）
}

const (
	maxLockTime       = 3 * time.Second // 3s
	waitBeforeCleanTx = 2 * maxLockTime // 6s

	createdStatus    = 0
	preparedStatus   = 1
	committedStatus  = 2
	rolledBackStatus = 3
)

func genTaskKey(txID string) string {
	return "tx:" + txID
}

// NewTransaction creates a try-commit-catch distributed transaction
func NewTransaction(cluster *Cluster, c redis.Connection, id string, cmdLine [][]byte) *Transaction {
	return &Transaction{
		id:      id,              // 事务id
		cmdLine: cmdLine,         // 命令
		cluster: cluster,         // 集群对象
		conn:    c,               // socket连接
		dbIndex: c.GetDBIndex(),  // 数据库
		status:  createdStatus,   // 事务状态
		mu:      new(sync.Mutex), // 锁
	}
}

// Reentrant
// invoker should hold tx.mu
func (tx *Transaction) lockKeys() {
	if !tx.keysLocked {
		tx.cluster.db.RWLocks(tx.dbIndex, tx.writeKeys, tx.readKeys)
		tx.keysLocked = true
	}
}

func (tx *Transaction) unLockKeys() {
	if tx.keysLocked {
		tx.cluster.db.RWUnLocks(tx.dbIndex, tx.writeKeys, tx.readKeys)
		tx.keysLocked = false
	}
}

// t should contain Keys and ID field
func (tx *Transaction) prepare() error { // 上锁

	// 加锁
	tx.mu.Lock()
	defer tx.mu.Unlock()

	// 获取命令中的key:比如 mset key value [key1 value1],得到的就是key/key1...
	tx.writeKeys, tx.readKeys = database.GetRelatedKeys(tx.cmdLine)
	// lock [writeKeys readKeys]
	tx.lockKeys() // 对key上锁

	for _, key := range tx.writeKeys {
		err := tx.cluster.ensureKey(key)
		if err != nil {
			return err
		}
	}
	for _, key := range tx.readKeys {
		err := tx.cluster.ensureKey(key)
		if err != nil {
			return err
		}
	}
	// build undoLog
	tx.undoLog = tx.cluster.db.GetUndoLogs(tx.dbIndex, tx.cmdLine) // 生成回滚日志
	tx.status = preparedStatus

	// 延迟任务
	// 超时没有提交，自动回滚
	taskKey := genTaskKey(tx.id)
	timewheel.Delay(maxLockTime, taskKey, func() { // 避免锁长时间的锁定
		if tx.status == preparedStatus { // rollback transaction uncommitted until expire
			logger.Info("abort transaction: " + tx.id)
			tx.mu.Lock()
			defer tx.mu.Unlock()
			_ = tx.rollbackWithLock()
		}
	})
	return nil
}

func (tx *Transaction) rollbackWithLock() error {
	curStatus := tx.status

	if tx.status != curStatus { // ensure status not changed by other goroutine
		return fmt.Errorf("tx %s status changed", tx.id)
	}
	if tx.status == rolledBackStatus { // no need to rollback a rolled-back transaction
		return nil
	}
	tx.lockKeys()
	for _, cmdLine := range tx.undoLog {
		tx.cluster.db.ExecWithLock(tx.conn, cmdLine)
	}
	tx.unLockKeys()
	tx.status = rolledBackStatus
	return nil
}

// 例如：Prepare trxid mset key value [key value ...]
func execPrepare(cluster *Cluster, c redis.Connection, cmdLine CmdLine) redis.Reply {
	if len(cmdLine) < 3 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'prepare' command")
	}
	// prepare trxid command key value [key value]
	txID := string(cmdLine[1])
	cmdName := strings.ToLower(string(cmdLine[2]))

	// 创建事务
	tx := NewTransaction(cluster, c, txID, cmdLine[2:])
	// 存储事务对象
	cluster.transactionMu.Lock()
	cluster.transactions.Put(txID, tx)
	cluster.transactionMu.Unlock()

	err := tx.prepare() // 本质就是上锁,生成回滚日志
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}
	prepareFunc, ok := prepareFuncMap[cmdName]
	if ok {
		return prepareFunc(cluster, c, cmdLine[2:])
	}
	return &protocol.OkReply{}
}

// execRollback rollbacks local transaction
func execRollback(cluster *Cluster, c redis.Connection, cmdLine CmdLine) redis.Reply {
	if len(cmdLine) != 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'rollback' command")
	}
	txID := string(cmdLine[1])

	// 通过事务id，得到事务对象
	cluster.transactionMu.RLock()
	raw, ok := cluster.transactions.Get(txID)
	cluster.transactionMu.RUnlock()
	if !ok {
		return protocol.MakeIntReply(0)
	}
	tx, _ := raw.(*Transaction)

	// 对事务对象上锁
	tx.mu.Lock()
	defer tx.mu.Unlock()
	// 执行回滚
	err := tx.rollbackWithLock()
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}
	// clean transaction（删除掉事务对象）
	timewheel.Delay(waitBeforeCleanTx, "", func() {
		cluster.transactionMu.Lock()
		cluster.transactions.Remove(tx.id)
		cluster.transactionMu.Unlock()
	})
	return protocol.MakeIntReply(1)
}

// execCommit commits local transaction as a worker when receive execCommit command from coordinator
func execCommit(cluster *Cluster, c redis.Connection, cmdLine CmdLine) redis.Reply {
	if len(cmdLine) != 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'commit' command")
	}

	// 获取事务id   commit trxid
	txID := string(cmdLine[1])
	cluster.transactionMu.RLock()
	raw, ok := cluster.transactions.Get(txID)
	cluster.transactionMu.RUnlock()
	if !ok {
		return protocol.MakeIntReply(0)
	}
	tx, _ := raw.(*Transaction)

	// 对事务对象上锁
	tx.mu.Lock()
	defer tx.mu.Unlock()

	//tx.lockKeys()
	// 执行命令
	result := cluster.db.ExecWithLock(c, tx.cmdLine)

	if protocol.IsErrorReply(result) { // 执行出错，自动回滚
		// failed
		err2 := tx.rollbackWithLock()
		return protocol.MakeErrReply(fmt.Sprintf("err occurs when rollback: %v, origin err: %s", err2, result))
	}
	// after committed
	tx.unLockKeys()
	tx.status = committedStatus
	// clean finished transaction
	// do not clean immediately, in case rollback
	timewheel.Delay(waitBeforeCleanTx, "", func() { // 提交成功以后，可能还会回滚？？？
		cluster.transactionMu.Lock()
		cluster.transactions.Remove(tx.id)
		cluster.transactionMu.Unlock()
	})
	return result
}

// requestCommit commands all node to commit transaction as coordinator
func requestCommit(cluster *Cluster, c redis.Connection, txID int64, groupMap map[string][]string) ([]redis.Reply, protocol.ErrorReply) {
	var errReply protocol.ErrorReply
	txIDStr := strconv.FormatInt(txID, 10)

	// 提交结果
	respList := make([]redis.Reply, 0, len(groupMap))

	for node := range groupMap {
		// ******* commit trxid ********
		resp := cluster.relay(node, c, makeArgs("commit", txIDStr))
		if protocol.IsErrorReply(resp) { // 说明某个提交出现问题了
			errReply = resp.(protocol.ErrorReply)
			break
		}
		respList = append(respList, resp)
	}

	// 如果提交失败了，还是要执行回滚
	if errReply != nil {
		// 执行回滚
		requestRollback(cluster, c, txID, groupMap)
		return nil, errReply
	}
	// 说明整个提交没出现问题
	return respList, nil
}

// requestRollback requests all node rollback transaction as coordinator
// groupMap: node -> keys
func requestRollback(cluster *Cluster, c redis.Connection, txID int64, groupMap map[string][]string) {
	txIDStr := strconv.FormatInt(txID, 10)

	// 对节点发送(对所有的节点）回滚命令
	for node := range groupMap {
		// rollback trxid
		cluster.relay(node, c, makeArgs("rollback", txIDStr))
	}
}
