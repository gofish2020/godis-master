package pubsub

import (
	"strconv"

	"github.com/hdt3213/godis/datastruct/list"
	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/lib/utils"
	"github.com/hdt3213/godis/redis/protocol"
)

var (
	_subscribe         = "subscribe"
	_unsubscribe       = "unsubscribe"
	messageBytes       = []byte("message")
	unSubscribeNothing = []byte("*3\r\n$11\r\nunsubscribe\r\n$-1\n:0\r\n")
)

func makeMsg(t string, channel string, code int64) []byte {
	return []byte("*3\r\n$" + strconv.FormatInt(int64(len(t)), 10) + protocol.CRLF + t + protocol.CRLF +
		"$" + strconv.FormatInt(int64(len(channel)), 10) + protocol.CRLF + channel + protocol.CRLF +
		":" + strconv.FormatInt(code, 10) + protocol.CRLF)
}

/*
 * invoker should lock channel
 * return: is new subscribed
 */
func subscribe0(hub *Hub, channel string, client redis.Connection) bool {

	// 同时，该channel也会保存到 client的结构体中
	client.Subscribe(channel)

	// add into hub.subs

	// 通过key查询map
	raw, ok := hub.subs.Get(channel)
	// 得到双向链表
	var subscribers *list.LinkedList
	if ok {
		subscribers, _ = raw.(*list.LinkedList)
	} else {
		subscribers = list.Make()
		hub.subs.Put(channel, subscribers) // map[key] = double list
	}

	// 查询链表
	if subscribers.Contains(func(a interface{}) bool {
		return a == client
	}) {
		return false
	}

	// 将 socket 保存到链表中
	subscribers.Add(client)
	return true
}

/*
 * invoker should lock channel
 * return: is actually un-subscribe
 */
func unsubscribe0(hub *Hub, channel string, client redis.Connection) bool {

	// client也取消 掉对channel的记录
	client.UnSubscribe(channel)

	// remove from hub.subs

	raw, ok := hub.subs.Get(channel)
	if ok {
		// 找到channel的链表
		subscribers, _ := raw.(*list.LinkedList)

		// 删除该链表中的所有的client
		subscribers.RemoveAllByVal(func(a interface{}) bool {
			return utils.Equals(a, client)
		})

		// 如果链表为看那个
		if subscribers.Len() == 0 {
			// clean
			hub.subs.Remove(channel) // map中也没有必要记录这个key了
		}
		return true
	}
	return false
}

// Subscribe puts the given connection into the given channel
func Subscribe(hub *Hub, c redis.Connection, args [][]byte) redis.Reply {
	channels := make([]string, len(args))
	for i, b := range args {
		channels[i] = string(b)
	}

	hub.subsLocker.Locks(channels...)
	defer hub.subsLocker.UnLocks(channels...)

	for _, channel := range channels {
		
		if subscribe0(hub, channel, c) {
			_, _ = c.Write(makeMsg(_subscribe, channel, int64(c.SubsCount())))
		}
	}
	return &protocol.NoReply{}
}

// UnsubscribeAll removes the given connection from all subscribing channel
func UnsubscribeAll(hub *Hub, c redis.Connection) {
	channels := c.GetChannels()

	hub.subsLocker.Locks(channels...)
	defer hub.subsLocker.UnLocks(channels...)

	for _, channel := range channels {
		unsubscribe0(hub, channel, c)
	}

}

// UnSubscribe removes the given connection from the given channel
func UnSubscribe(db *Hub, c redis.Connection, args [][]byte) redis.Reply {
	var channels []string
	if len(args) > 0 {
		channels = make([]string, len(args))
		for i, b := range args {
			channels[i] = string(b)
		}
	} else {
		channels = c.GetChannels()
	}

	db.subsLocker.Locks(channels...)
	defer db.subsLocker.UnLocks(channels...)

	if len(channels) == 0 {
		_, _ = c.Write(unSubscribeNothing)
		return &protocol.NoReply{}
	}

	for _, channel := range channels {
		if unsubscribe0(db, channel, c) {
			_, _ = c.Write(makeMsg(_unsubscribe, channel, int64(c.SubsCount())))
		}
	}
	return &protocol.NoReply{}
}

// Publish send msg to all subscribing client

// example : PUBLISH channel message
func Publish(hub *Hub, args [][]byte) redis.Reply {
	if len(args) != 2 {
		return &protocol.ArgNumErrReply{Cmd: "publish"}
	}

	channel := string(args[0])
	message := args[1]

	hub.subsLocker.Lock(channel)
	defer hub.subsLocker.UnLock(channel)

	// 获取 channel对象的双向链表
	raw, ok := hub.subs.Get(channel)
	if !ok {
		return protocol.MakeIntReply(0)
	}

	// 遍历双向链表，
	subscribers, _ := raw.(*list.LinkedList)
	subscribers.ForEach(func(i int, c interface{}) bool {

		// 取出其中保存的 客户端连接
		client, _ := c.(redis.Connection)
		replyArgs := make([][]byte, 3)
		replyArgs[0] = messageBytes
		replyArgs[1] = []byte(channel)
		replyArgs[2] = message

		// 然后给客户端发送消息
		_, _ = client.Write(protocol.MakeMultiBulkReply(replyArgs).ToBytes())
		return true
	})
	return protocol.MakeIntReply(int64(subscribers.Len()))
}
