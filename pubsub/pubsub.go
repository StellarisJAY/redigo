package pubsub

import (
	"container/list"
	"redigo/datastruct/dict"
	"redigo/datastruct/lock"
	"redigo/interface/redis"
	"redigo/redis/protocol"
	"redigo/util/pattern"
	"strconv"
)

type Hub struct {
	channels dict.Dict
	locks    *lock.Locker
}

func MakeHub() *Hub {
	return &Hub{
		channels: dict.NewSimpleDict(),
		locks:    lock.NewLock(1024),
	}
}

func makePublishMessage(channel string, message []byte) *protocol.Reply {
	return protocol.NewNestedArrayReply([][]byte{
		[]byte("$7\r\nmessage\r\n"),
		[]byte("$" + strconv.Itoa(len(channel)) + protocol.CRLF + channel + protocol.CRLF),
		[]byte("$" + strconv.Itoa(len(message)) + protocol.CRLF + string(message) + protocol.CRLF),
	})
}

func makeSubscribeReply(channel string, seq int) *protocol.Reply {
	return protocol.NewNestedArrayReply([][]byte{
		[]byte("$9\r\nsubscribe\r\n"),
		[]byte("$" + strconv.Itoa(len(channel)) + protocol.CRLF + channel + protocol.CRLF),
		[]byte(":" + strconv.Itoa(seq) + protocol.CRLF),
	})
}

func (h *Hub) Subscribe(conn redis.Connection, args [][]byte) {
	for i, arg := range args {
		sub := string(arg)
		// lock subscriber list of this channel, prevents other goroutine changes list
		h.locks.Lock(sub)
		var subscribers *list.List
		if v, ok := h.channels.Get(sub); ok {
			subscribers = v.(*list.List)
		} else {
			subscribers = list.New()
			h.channels.Put(sub, subscribers)
		}
		subscribers.PushFront(conn)
		h.locks.Unlock(sub)
		conn.SendReply(makeSubscribeReply(sub, i+1))
	}
}

func (h *Hub) PSubscribe(conn redis.Connection, patterns []string) {
	parsedPatterns := make([]*pattern.Pattern, len(patterns))
	for i, p := range patterns {
		parsedPatterns[i] = pattern.ParsePattern(p)
	}
	count := 0
	h.channels.ForEach(func(key string, value interface{}) bool {
		h.locks.Lock(key)
		defer h.locks.Unlock(key)
		for _, p := range parsedPatterns {
			if p.Matches(key) {
				subscribers := value.(*list.List)
				subscribers.PushFront(conn)
				conn.SendReply(makeSubscribeReply(key, count))
				count++
				break
			}
		}
		return true
	})
}

// Publish a message to the channel, returns 1 if success, 0 if fails
func (h *Hub) Publish(pubChannel string, message []byte) int {
	// lock publish channel, prevents other goroutine changes subscriber list
	h.locks.Lock(pubChannel)
	defer h.locks.Unlock(pubChannel)

	if v, ok := h.channels.Get(pubChannel); !ok {
		return 0
	} else {
		sent := 0
		subscribers := v.(*list.List)
		msg := makePublishMessage(pubChannel, message)
		length := subscribers.Len()
		for i := 0; i < length; i++ {
			back := subscribers.Back()
			conn := back.Value.(redis.Connection)
			// send message if connection is still active
			if conn.Active() {
				conn.SendReply(msg)
				subscribers.MoveToFront(back)
				sent++
			} else {
				// remove inactive subscribers
				subscribers.Remove(back)
			}
		}
		return sent
	}
}

// UnSubscribeAll channels that this connection subscribed
func (h *Hub) UnSubscribeAll(conn redis.Connection) {
	// for each channel
	h.channels.ForEach(func(key string, value interface{}) bool {
		// lock subscribe list, prevent other goroutine changes list
		h.locks.Lock(key)
		defer h.locks.Unlock(key)
		subscribers := value.(*list.List)
		length := subscribers.Len()
		for i := 0; i < length; i++ {
			subscriber := subscribers.Back()
			// remove connection from subscriber list
			if subscriber.Value.(redis.Connection) == conn {
				subscribers.Remove(subscriber)
			}
		}
		return true
	})
}
