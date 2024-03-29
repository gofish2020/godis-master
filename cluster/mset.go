package cluster

import (
	"fmt"
	"strconv"

	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/lib/utils"
	"github.com/hdt3213/godis/redis/protocol"
)

const keyExistsErr = "key exists"

// MGet atomically get multi key-value from cluster, writeKeys can be distributed on any node
func MGet(cluster *Cluster, c redis.Connection, cmdLine CmdLine) redis.Reply {
	if len(cmdLine) < 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'mget' command")
	}
	keys := make([]string, len(cmdLine)-1)
	for i := 1; i < len(cmdLine); i++ {
		keys[i-1] = string(cmdLine[i])
	}

	resultMap := make(map[string][]byte)
	groupMap := cluster.groupBy(keys)
	for peer, groupKeys := range groupMap {
		resp := cluster.relay(peer, c, makeArgs("MGet_", groupKeys...))
		if protocol.IsErrorReply(resp) {
			errReply := resp.(protocol.ErrorReply)
			return protocol.MakeErrReply(fmt.Sprintf("ERR during get %s occurs: %v", groupKeys[0], errReply.Error()))
		}
		arrReply, _ := resp.(*protocol.MultiBulkReply)
		for i, v := range arrReply.Args {
			key := groupKeys[i]
			resultMap[key] = v
		}
	}
	result := make([][]byte, len(keys))
	for i, k := range keys {
		result[i] = resultMap[k]
	}
	return protocol.MakeMultiBulkReply(result)
}

// MSET key value [key value ...]
// MSet atomically sets multi key-value in cluster, writeKeys can be distributed on any node
func MSet(cluster *Cluster, c redis.Connection, cmdLine CmdLine) redis.Reply {
	argCount := len(cmdLine) - 1
	if argCount%2 != 0 || argCount < 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'mset' command")
	}

	size := argCount / 2
	keys := make([]string, size)
	valueMap := make(map[string]string)

	// 提取出 key/value 对
	for i := 0; i < size; i++ {
		keys[i] = string(cmdLine[2*i+1])
		valueMap[keys[i]] = string(cmdLine[2*i+2])
	}

	// groupMap -> 计算key分布在哪些节点上（ip -> keys)
	groupMap := cluster.groupBy(keys)
	if len(groupMap) == 1 && allowFastTransaction { // 说明key都分布在同一个节点上

		// 表示只有一个节点，（所有的keys都属于同一个节点）
		for peer := range groupMap {

			return cluster.relay(peer, c, modifyCmd(cmdLine, "MSet_")) // 将 redis命令格式修改为 MSet_ key value [key value...]
		}
	}

	//prepare
	var errReply redis.Reply
	// 生成 事务唯一id
	txID := cluster.idGenerator.NextID()
	txIDStr := strconv.FormatInt(txID, 10)

	rollback := false
	for peer, group := range groupMap { // groupMap ip地址和key的分布

		peerArgs := []string{txIDStr, "MSET"}

		// group 表示多个key
		// 组成 trxid mset key value [key value...]
		for _, k := range group {

			peerArgs = append(peerArgs, k, valueMap[k])
		}

		//peer 表示网络地址
		// 构造出 prepare trxid mset key value[key value...]
		resp := cluster.relay(peer, c, makeArgs("Prepare", peerArgs...))
		if protocol.IsErrorReply(resp) {
			errReply = resp
			rollback = true
			break
		}
	}
	if rollback {
		// rollback
		requestRollback(cluster, c, txID, groupMap)
	} else {

		// commit
		_, errReply = requestCommit(cluster, c, txID, groupMap)
		rollback = errReply != nil
	}

	// 说明整个执行成功
	if !rollback {
		return &protocol.OkReply{}
	}
	// 说明整个执行失败
	return errReply
}

// MSetNX sets multi key-value in database, only if none of the given writeKeys exist and all given writeKeys are on the same node
func MSetNX(cluster *Cluster, c redis.Connection, cmdLine CmdLine) redis.Reply {
	argCount := len(cmdLine) - 1
	if argCount%2 != 0 || argCount < 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'msetnx' command")
	}

	size := argCount / 2
	keys := make([]string, size)
	valueMap := make(map[string]string)
	for i := 0; i < size; i++ {
		keys[i] = string(cmdLine[2*i+1])
		valueMap[keys[i]] = string(cmdLine[2*i+2])
	}

	groupMap := cluster.groupBy(keys)
	if len(groupMap) == 1 && allowFastTransaction { // do fast
		for peer := range groupMap {
			return cluster.relay(peer, c, modifyCmd(cmdLine, "MSetNX_"))
		}
	}

	// prepare procedure:
	// 1. Normal tcc preparation (undo log and lock related keys)
	// 2. Peer checks whether any key already exists, If so it will return keyExistsErr. Then coordinator will request rollback over all participated nodes
	var errReply redis.Reply
	txID := cluster.idGenerator.NextID()
	txIDStr := strconv.FormatInt(txID, 10)
	rollback := false
	for node, group := range groupMap {
		nodeArgs := []string{txIDStr, "MSETNX"}
		for _, k := range group {
			nodeArgs = append(nodeArgs, k, valueMap[k])
		}
		resp := cluster.relay(node, c, makeArgs("Prepare", nodeArgs...))
		if protocol.IsErrorReply(resp) {
			re := resp.(protocol.ErrorReply)
			if re.Error() == keyExistsErr {
				errReply = protocol.MakeIntReply(0)
			} else {
				errReply = resp
			}
			rollback = true
			break
		}
	}
	if rollback {
		// rollback
		requestRollback(cluster, c, txID, groupMap)
		return errReply
	}
	_, errReply = requestCommit(cluster, c, txID, groupMap)
	rollback = errReply != nil
	if !rollback {
		return protocol.MakeIntReply(1)
	}
	return errReply
}

func prepareMSetNx(cluster *Cluster, conn redis.Connection, cmdLine CmdLine) redis.Reply {
	args := cmdLine[1:]
	if len(args)%2 != 0 {
		return protocol.MakeSyntaxErrReply()
	}
	size := len(args) / 2
	values := make([][]byte, size)
	keys := make([]string, size)
	for i := 0; i < size; i++ {
		keys[i] = string(args[2*i])
		values[i] = args[2*i+1]
	}
	re := cluster.db.ExecWithLock(conn, utils.ToCmdLine2("ExistIn", keys...))
	if protocol.IsErrorReply(re) {
		return re
	}
	_, ok := re.(*protocol.EmptyMultiBulkReply)
	if !ok {
		return protocol.MakeErrReply(keyExistsErr)
	}
	return protocol.MakeOkReply()
}

func init() {
	registerPrepareFunc("MSetNx", prepareMSetNx)
}
