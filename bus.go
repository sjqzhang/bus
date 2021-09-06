package bus

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// 默认消息队列大小
const DefaultQueueSize = 1000
const MESSAGE_KYE = "__CONTEXT_MESSAGE_KYE__"

type MessageBus interface {
	// 发布
	Publish(topic string, msg interface{})

	PublishWithContext(ctx context.Context, topic string, msg interface{})

	Call(topic string, args ...interface{}) (interface{}, error)

	CallWithContext(ctx context.Context, topic string, args ...interface{}) (interface{}, error)

	CallWithContextDirect(ctx context.Context, topic string, args ...interface{}) (interface{}, error)

	// 订阅, 返回订阅号
	Subscribe(topic string, threadCount int, handler Handler) (subscribeId uint32)

	SubscribeWithReply(topic string, threadCount int, handler HReply) (subscribeId uint32)

	RegistFunc(funcName string, threadCount int, handler HReply) (uint32, error)

	// 全局订阅, 会收到所有消息
	SubscribeGlobal(threadCount int, handler Handler) (subscribeId uint32)
	// 取消订阅
	Unsubscribe(topic string, subscribeId uint32)

	UnRegistFunc(funcIdentification string, subscribeId uint32)
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

func (m *msgBus) PublishWithContext(ctx context.Context, topic string, msg interface{}) {
	m.global.Publish(ctx, topic, msg) // 发送消息到全局
	m.mx.RLock()
	t, ok := m.topics[topic]
	m.mx.RUnlock()
	if ok {
		t.Publish(ctx, topic, msg)
	}
}

func (m *msgBus) Publish(topic string, msg interface{}) {
	m.PublishWithContext(context.Background(), topic, msg)
}

func (m *msgBus) CallWithContextDirect(ctx context.Context, funcName string, args ...interface{}) (interface{}, error) {
	m.mx.RLock()
	handler, ok := m.globalReply[funcName]
	m.mx.RUnlock()
	if !ok {
		return nil, errors.New("handler not found")
	}
	return handler(ctx, args...)
}

func (m *msgBus) CallWithContext(ctx context.Context, funcName string, args ...interface{}) (interface{}, error) {
	id := fmt.Sprintf("%s_%v", funcName, nextReqId())
	m.rl.Lock()
	m.globalResult[id] = make(chan result, 1)
	m.rl.Unlock()
	//m.global.Publish(funcName, args) // 发送消息到全局
	m.mx.RLock()
	t, ok := m.topics[funcName]
	m.mx.RUnlock()
	m.rl.RLock()
	obj2 := m.globalResult[id]
	m.rl.RUnlock()
	if ok {
		t.PublishWithRely(ctx, funcName, id, args)
	} else {
		obj2 <- result{
			data: nil,
			err:  errors.New(fmt.Sprintf("(ERR)funcName %v not registered.", funcName)),
		}

	}

	select {
	case obj := <-obj2:
		m.rl.Lock()
		delete(m.globalResult, id)
		m.rl.Unlock()
		return obj.data, obj.err

	}

}

func (m *msgBus) Call(funcName string, args ...interface{}) (interface{}, error) {

	return m.CallWithContext(context.Background(), funcName, args...)
}

func (m *msgBus) RegistFunc(funcName string, threadCount int, handler HReply) (uint32, error) {
	m.mx.RLock()
	_, ok := m.globalReply[funcName]
	m.mx.RUnlock()
	if ok {
		return 0, errors.New(fmt.Sprintf("(ERR)funcName %v has registered ", funcName))
	}
	return m.SubscribeWithReply(funcName, threadCount, handler), nil
}

func (m *msgBus) SubscribeWithReply(topic string, threadCount int, handler HReply) (subscribeId uint32) {
	m.mx.RLock()
	t, ok := m.topics[topic]
	m.globalReply[topic] = handler // add handler
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

func (m *msgBus) UnRegistFunc(topic string, subscribeId uint32) {
	m.UnRegistFunc(topic, subscribeId)
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
		mx:           sync.RWMutex{},
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
var reqId uint64

// 生成下一个订阅者id
func nextSubscriberId() uint32 {
	return atomic.AddUint32(&autoIncrSubscriberId, 1)
}

func nextReqId() uint64 {
	if reqId > (1 << 63) {
		reqId = 0
	}
	return atomic.AddUint64(&reqId, 1)
}

func getGoroutineID() uint64 {
	b := make([]byte, 64)
	runtime.Stack(b, false)
	b = bytes.TrimPrefix(b, []byte("goroutine "))
	b = b[:bytes.IndexByte(b, ' ')]
	n, _ := strconv.ParseUint(string(b), 10, 64)
	return n
}

func md5sum(str string) string {
	md := md5.New()
	md.Write([]byte(str))
	return fmt.Sprintf("%x", md.Sum(nil))
}

func getUUID() string {
	b := make([]byte, 48)
	if _, err := io.ReadFull(rand.Reader, b); err != nil {
		return ""
	}
	id := md5sum(base64.URLEncoding.EncodeToString(b))
	return fmt.Sprintf("%s-%s-%s-%s-%s", id[0:8], id[8:12], id[12:16], id[16:20], id[20:])
}

// 通道消息
type Message struct {
	Id        string
	Topic     string
	Msg       interface{}
	Ctx       context.Context
	TimeStamp int64
}

// 处理函数
type Handler func(ctx context.Context, topic string, msg interface{})

type HReply func(ctx context.Context, args ...interface{}) (interface{}, error)

// 订阅者
type subscriber struct {
	handler Handler
	queue   chan *Message
}

// 订阅者
type subscriberWithReply struct {
	handler HReply
	queue   chan *Message
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

func (m *msgTopic) Publish(ctx context.Context, topic string, msg interface{}) {
	m.mx.RLock()
	reqId := fmt.Sprintf("%s_%v", topic, nextReqId())
	for _, sub := range m.subs {
		message := Message{
			Topic:     topic,
			Msg:       msg,
			Id:        reqId,
			TimeStamp: time.Now().UnixNano() / 1e6,
		}
		ctx = context.WithValue(ctx, MESSAGE_KYE, &message)
		message.Ctx = ctx
		sub.queue <- &message
	}
	m.mx.RUnlock()
}

func (m *msgTopic) PublishWithRely(ctx context.Context, topic string, id string, msg interface{}) {
	m.mx.RLock()
	for _, sub := range m.subsWithReply {
		msg := &Message{
			Topic:     topic,
			Msg:       msg,
			Id:        id,
			Ctx:       ctx,
			TimeStamp: time.Now().UnixNano() / 1e6,
		}
		ctx = context.WithValue(ctx, MESSAGE_KYE, msg)
		msg.Ctx = ctx
		sub.queue <- msg
	}

	m.mx.RUnlock()
}

func (m *msgTopic) Subscribe(queueSize int, threadCount int, handler Handler) (subscribeId uint32) {
	sub := &subscriber{
		handler: handler,
		queue:   make(chan *Message, queueSize),
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
		queue:   make(chan *Message, queueSize),
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
		sub.handler(msg.Ctx, msg.Topic, msg.Msg)
	}
}

func (m *msgTopic) startWithRely(sub *subscriberWithReply) {
	for msg := range sub.queue {
		data, err := sub.handler(msg.Ctx, msg.Msg.([]interface{})...)
		m.bus.rl.Lock()
		m.bus.globalResult[msg.Id] <- result{
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
	sub2, ok2 := m.subsWithReply[subscribeId]
	if ok2 {
		close(sub2.queue)
		delete(m.subsWithReply, subscribeId)
	}
	m.mx.Unlock()
}

func (m *msgTopic) Close() {
	m.mx.Lock()
	for _, sub := range m.subs {
		close(sub.queue)
	}
	for _, sub := range m.subsWithReply {
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

func PublishWithContext(ctx context.Context, topic string, msg interface{}) {
	defaultMsgBus.PublishWithContext(ctx, topic, msg)
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
	return defaultMsgBus.SubscribeWithReply(topic, threadCount, handler)
}

func Call(funcName string, args ...interface{}) (interface{}, error) {
	return defaultMsgBus.Call(funcName, args...)
}

func CallWithContext(ctx context.Context, funcName string, args ...interface{}) (interface{}, error) {
	return defaultMsgBus.CallWithContext(ctx, funcName, args...)
}
func CallWithContextDirect(ctx context.Context, funcName string, args ...interface{}) (interface{}, error) {
	return defaultMsgBus.CallWithContextDirect(ctx, funcName, args...)
}

func RegistFunc(funcName string, threadCount int, handler HReply) (uint32, error) {
	return defaultMsgBus.RegistFunc(funcName, threadCount, handler)
}

func UnRegistFunc(funcName string, subscribeId uint32) {
	defaultMsgBus.Unsubscribe(funcName, subscribeId)
}
