package bus

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
)

// 默认消息队列大小
const DefaultQueueSize = 1000


type MessageBus interface {
	// 发布
	Publish(topic string, msg interface{})

	Call(topic string, msg ...interface{}) (interface{}, error)

	CallWithContext(ctx context.Context, topic string, msg ...interface{}) (interface{}, error)
	// 订阅, 返回订阅号
	Subscribe(topic string, threadCount int, handler Handler) (subscribeId uint32)

	SubscribeWithReply(topic string, threadCount int, handler HReply) (subscribeId uint32)

	// 全局订阅, 会收到所有消息
	SubscribeGlobal(threadCount int, handler Handler) (subscribeId uint32)
	// 取消订阅
	Unsubscribe(topic string, subscribeId uint32)
	// 取消全局订阅
	UnsubscribeGlobal(subscribeId uint32)
	// 关闭主题, 同时关闭所有订阅该主题的订阅者
	CloseTopic(topic string)
	// 关闭
	Close()
}

type result struct {
	data interface{}
	err  error
}

// 消息总线
type msgBus struct {
	global       *msgTopic // 用于接收全局消息
	queueSize    int
	topics       msgTopics
	mx           sync.RWMutex // 用于锁 topics
	globalReply  map[string]HReply
	rl           sync.RWMutex
	globalResult map[string]chan result
}

func (m *msgBus) Publish(topic string, msg interface{}) {
	m.global.Publish(topic, msg) // 发送消息到全局

	m.mx.RLock()
	t, ok := m.topics[topic]
	m.mx.RUnlock()

	if ok {
		t.Publish(topic, msg)
	}
}

func (m *msgBus) CallWithContext(ctx context.Context, topic string, msg ...interface{}) (interface{}, error) {

	id := fmt.Sprintf("%v", nextSubscriberId())

	m.globalResult[id] = make(chan result, 1)

	m.global.Publish(topic, msg) // 发送消息到全局

	m.mx.RLock()
	t, ok := m.topics[topic]
	m.mx.RUnlock()

	if ok {
		t.PublishWithRely(ctx, topic, id, msg)
	}
	select {
	case obj := <-m.globalResult[id]:
		delete(m.globalResult, id)
		return obj.data, obj.err
	}

}

func (m *msgBus) Call(topic string, msg ...interface{}) (interface{}, error) {

	id := fmt.Sprintf("%v", nextSubscriberId())
	m.rl.Lock()
	m.globalResult[id] = make(chan result, 1)
	m.rl.Unlock()
	m.global.Publish(topic, msg) // 发送消息到全局

	m.mx.RLock()
	t, ok := m.topics[topic]
	m.mx.RUnlock()

	if ok {
		t.PublishWithRely(context.Background(), topic, id, msg)
	}
	m.rl.RLock()
	obj2 :=m.globalResult[id]
	m.rl.RUnlock()

	select {
	case obj := <-obj2:
		//m.rl.RUnlock()
		m.rl.Lock()
		delete(m.globalResult, id)
		m.rl.Unlock()
		return obj.data, obj.err

	}

}

func (m *msgBus) SubscribeWithReply(topic string, threadCount int, handler HReply) (subscribeId uint32) {

	m.mx.RLock()
	t, ok := m.topics[topic]
	m.mx.RUnlock()

	if !ok {
		m.mx.Lock()
		t, ok = m.topics[topic]
		if !ok {
			t = newMsgTopic(m)
			m.topics[topic] = t
		}
		m.mx.Unlock()
	}
	return t.SubscribeWithReply(m, m.queueSize, threadCount, handler)
}

func (m *msgBus) Subscribe(topic string, threadCount int, handler Handler) (subscribeId uint32) {
	m.mx.RLock()
	t, ok := m.topics[topic]
	m.mx.RUnlock()

	if !ok {
		m.mx.Lock()
		t, ok = m.topics[topic]
		if !ok {
			t = newMsgTopic(m)
			m.topics[topic] = t
		}
		m.mx.Unlock()
	}
	return t.Subscribe(m.queueSize, threadCount, handler)
}
func (m *msgBus) SubscribeGlobal(threadCount int, handler Handler) (subscribeId uint32) {
	return m.global.Subscribe(m.queueSize, threadCount, handler)
}

func (m *msgBus) Unsubscribe(topic string, subscribeId uint32) {
	m.mx.RLock()
	t, ok := m.topics[topic]
	m.mx.RUnlock()

	if ok {
		t.Unsubscribe(subscribeId)
	}
}
func (m *msgBus) UnsubscribeGlobal(subscribeId uint32) {
	m.global.Unsubscribe(subscribeId)
}

func (m *msgBus) CloseTopic(topic string) {
	m.mx.Lock()
	t, ok := m.topics[topic]
	if ok {
		delete(m.topics, topic)
	}
	m.mx.Unlock()

	if ok {
		t.Close()
	}
}

func (m *msgBus) Close() {
	m.mx.Lock()
	for _, t := range m.topics {
		t.Close()
	}
	m.global.Close()
	m.global = newMsgTopic(m)
	m.topics = make(msgTopics)
	m.mx.Unlock()
}

// 创建一个消息总线
func NewMsgBus() MessageBus {
	return NewMsgBusWithQueueSize(DefaultQueueSize)
}

// 创建一个消息总线并设置队列大小
func NewMsgBusWithQueueSize(queueSize int) MessageBus {
	if queueSize < 1 {
		queueSize = DefaultQueueSize
	}

	m := &msgBus{
		mx: sync.RWMutex{},
		rl:           sync.RWMutex{},
		queueSize:    queueSize,
		topics:       make(msgTopics),
		globalReply:  make(map[string]HReply),
		globalResult: make(map[string]chan result),
	}
	m.global = newMsgTopic(m)
	return m
}

// 全局自增订阅者id
var autoIncrSubscriberId uint32

// 生成下一个订阅者id
func nextSubscriberId() uint32 {
	return atomic.AddUint32(&autoIncrSubscriberId, 1)
}

// 通道消息
type channelMsg struct {
	id    string
	topic string
	msg   interface{}
	ctx   context.Context
}

// 处理函数
type Handler func(topic string, arg interface{})

type HReply func(ctx context.Context, args ...interface{}) (interface{}, error)

// 订阅者
type subscriber struct {
	handler Handler
	queue   chan *channelMsg
}

// 订阅者
type subscriberWithReply struct {
	handler HReply
	queue   chan *channelMsg
}

// 主题们
type msgTopics map[string]*msgTopic

// 主题
type msgTopic struct {
	bus           *msgBus
	subs          map[uint32]*subscriber
	subsWithReply map[uint32]*subscriberWithReply
	mx            sync.RWMutex // 用于锁 subs
}

func newMsgTopic(b *msgBus) *msgTopic {
	return &msgTopic{
		subs:          make(map[uint32]*subscriber),
		subsWithReply: make(map[uint32]*subscriberWithReply),
		bus:           b,
	}
}

func (m *msgTopic) Publish(topic string, msg interface{}) {
	m.mx.RLock()
	for _, sub := range m.subs {
		sub.queue <- &channelMsg{
			topic: topic,
			msg:   msg,
		}
	}
	m.mx.RUnlock()
}

func (m *msgTopic) PublishWithRely(ctx context.Context, topic string, id string, msg interface{}) {
	m.mx.RLock()
	for _, sub := range m.subsWithReply {
		sub.queue <- &channelMsg{
			topic: topic,
			msg:   msg,
			id:    id,
			ctx:   ctx,
		}
	}
	m.mx.RUnlock()
}

func (m *msgTopic) Subscribe(queueSize int, threadCount int, handler Handler) (subscribeId uint32) {
	sub := &subscriber{
		handler: handler,
		queue:   make(chan *channelMsg, queueSize),
	}

	if threadCount < 1 {
		threadCount = runtime.NumCPU() >> 1
		if threadCount < 1 {
			threadCount = 1
		}
	}
	for i := 0; i < threadCount; i++ {
		go m.start(sub)
	}

	subId := nextSubscriberId()

	m.mx.Lock()
	m.subs[subId] = sub
	m.mx.Unlock()
	return subId
}

func (m *msgTopic) SubscribeWithReply(b *msgBus, queueSize int, threadCount int, handler HReply) (subscribeId uint32) {
	sub := &subscriberWithReply{
		handler: handler,
		queue:   make(chan *channelMsg, queueSize),
	}

	if threadCount < 1 {
		threadCount = runtime.NumCPU() >> 1
		if threadCount < 1 {
			threadCount = 1
		}
	}
	for i := 0; i < threadCount; i++ {
		go m.startWithRely(sub)
	}

	subId := nextSubscriberId()

	m.mx.Lock()
	m.subsWithReply[subId] = sub
	m.mx.Unlock()
	return subId
}

func (m *msgTopic) start(sub *subscriber) {
	for msg := range sub.queue {
		sub.handler(msg.topic, msg.msg)
	}
}

func (m *msgTopic) startWithRely(sub *subscriberWithReply) {
	for msg := range sub.queue {
		data, err := sub.handler(msg.ctx, msg.msg.([]interface{})...)
		m.bus.rl.Lock()
		m.bus.globalResult[msg.id] <- result{
			data: data,
			err:  err,
		}
		m.bus.rl.Unlock()
	}
}

func (m *msgTopic) Unsubscribe(subscribeId uint32) {
	m.mx.Lock()
	sub, ok := m.subs[subscribeId]
	if ok {
		close(sub.queue)
		delete(m.subs, subscribeId)
	}
	m.mx.Unlock()
}

func (m *msgTopic) Close() {
	m.mx.Lock()
	for _, sub := range m.subs {
		close(sub.queue)
	}

	// 如果不清除, 在调用 Publish 会导致panic
	m.subs = make(map[uint32]*subscriber)
	m.subsWithReply = make(map[uint32]*subscriberWithReply)

	m.mx.Unlock()
}
var defaultMsgBus = NewMsgBus()

// 发布
func Publish(topic string, msg interface{}) {
	defaultMsgBus.Publish(topic, msg)
}

// 订阅, 返回订阅号
func Subscribe(topic string, threadCount int, handler Handler) (subscribeId uint32) {
	return defaultMsgBus.Subscribe(topic, threadCount, handler)
}

// 全局订阅, 会收到所有消息, 返回订阅号
func SubscribeGlobal(threadCount int, handler Handler) (subscribeId uint32) {
	return defaultMsgBus.SubscribeGlobal(threadCount, handler)
}

// 取消订阅
func Unsubscribe(topic string, subscribeId uint32) {
	defaultMsgBus.Unsubscribe(topic, subscribeId)
}

// 取消全局订阅
func UnsubscribeGlobal(subscribeId uint32) {
	defaultMsgBus.UnsubscribeGlobal(subscribeId)
}

// 关闭主题, 同时关闭所有订阅该主题的订阅者
func CloseTopic(topic string) {
	defaultMsgBus.CloseTopic(topic)
}

// 关闭
func Close() {
	defaultMsgBus.Close()
}

func SubscribeWithReply(topic string, threadCount int, handler HReply) (subscribeId uint32) {
	return defaultMsgBus.SubscribeWithReply(topic,threadCount,handler)
}

func Call(topic string, args ...interface{}) (interface{}, error) {
	return defaultMsgBus.Call(topic,args)
}

func CallWithContext(ctx context.Context, topic string, args ...interface{}) (interface{}, error) {
	return defaultMsgBus.CallWithContext(ctx,topic,args)
}