package mq

import (
	"bufio"
	"bytes"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mymq/util"
)

type ClientV1 struct {
	net.Conn
	sync.Mutex
	frameBuf        bytes.Buffer
	Reader          *bufio.Reader
	State           int32
	ReadyCount      int64
	LastReadyCount  int64
	InFlightCount   int64
	MessageCount    uint64
	FinishCount     uint64
	RequeueCount    uint64
	ConnectTime     time.Time
	Channel         *Channel
	ReadyStateChan  chan int
	ExitChan        chan int
	ShortIdentifier string
	LongIdentifier  string
}

func NewClientV1(conn net.Conn) *ClientV1 {
	var identifier string
	if conn != nil {
		identifier, _, _ = net.SplitHostPort(conn.RemoteAddr().String())
	}
	return &ClientV1{
		Conn:            conn,
		ReadyStateChan:  make(chan int, 1),
		ExitChan:        make(chan int),
		ConnectTime:     time.Now(),
		ShortIdentifier: identifier,
		LongIdentifier:  identifier,
	}
}

func (c *ClientV1) String() string {
	return c.RemoteAddr().String()
}

func (c *ClientV1) Stats() ClientStats {
	return ClientStats{
		version:       "V1",
		address:       c.RemoteAddr().String(),
		name:          c.ShortIdentifier,
		state:         atomic.LoadInt32(&c.State),
		readyCount:    atomic.LoadInt64(&c.ReadyCount),
		inFlightCount: atomic.LoadInt64(&c.InFlightCount),
		messageCount:  atomic.LoadUint64(&c.MessageCount),
		finishCount:   atomic.LoadUint64(&c.FinishCount),
		requeueCount:  atomic.LoadUint64(&c.RequeueCount),
		connectTime:   c.ConnectTime,
	}
}

func (c *ClientV1) IsReadyForMessages() bool {
	if c.Channel.IsPaused() {
		return false
	}

	readyCount := atomic.LoadInt64(&c.ReadyCount)
	lastReadyCount := atomic.LoadInt64(&c.LastReadyCount)
	inFlightCount := atomic.LoadInt64(&c.InFlightCount)

	if inFlightCount >= lastReadyCount || readyCount <= 0 {
		return false
	}

	return true
}

func (c *ClientV1) SetReadyCount(count int64) {
	atomic.StoreInt64(&c.ReadyCount, count)
	atomic.StoreInt64(&c.LastReadyCount, count)
	c.tryUpdateReadyState()
}

func (c *ClientV1) tryUpdateReadyState() {
	// you can always *try* to write to ReadyStateChan because in the cases
	// where you cannot the message pump loop would have iterated anyway.
	// the atomic integer operations guarantee correctness of the value.
	select {
	case c.ReadyStateChan <- 1:
	default:
	}
}

func (c *ClientV1) FinishedMessage() {
	atomic.AddUint64(&c.FinishCount, 1)
	atomic.AddInt64(&c.InFlightCount, -1)
	c.tryUpdateReadyState()
}

func (c *ClientV1) SendingMessage() {
	atomic.AddInt64(&c.ReadyCount, -1)
	atomic.AddInt64(&c.InFlightCount, 1)
	atomic.AddUint64(&c.MessageCount, 1)
}

func (c *ClientV1) TimedOutMessage() {
	atomic.AddInt64(&c.InFlightCount, -1)
	c.tryUpdateReadyState()
}

func (c *ClientV1) RequeuedMessage() {
	atomic.AddUint64(&c.RequeueCount, 1)
	atomic.AddInt64(&c.InFlightCount, -1)
	c.tryUpdateReadyState()
}

func (c *ClientV1) StartClose() {
	// Force the client into ready 0
	c.SetReadyCount(0)
	// mark this client as closing
	atomic.StoreInt32(&c.State, util.StateClosing)
	// TODO: start a timer to actually close the channel (in case the client doesn't do it first)
}

func (c *ClientV1) Pause() {
	c.tryUpdateReadyState()
}

func (c *ClientV1) UnPause() {
	c.tryUpdateReadyState()
}
