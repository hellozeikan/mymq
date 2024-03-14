package mq

import (
	"bytes"
	"errors"
	"log"
	"sync"
	"sync/atomic"

	"github.com/mymq/util"
)

type Topic struct {
	sync.RWMutex
	name               string
	channelMap         map[string]*Channel
	backend            BackendQueue
	incomingMsgChan    chan *Message
	memoryMsgChan      chan *Message
	messagePumpStarter *sync.Once
	exitChan           chan int
	waitGroup          util.WaitGroupWrapper
	exitFlag           int32
	messageCount       uint64
	options            *MqOptions
}

// Deferred implements Queue.
func (t *Topic) Deferred() map[string]*util.Item {
	return nil
}

// InFlight implements Queue.
func (t *Topic) InFlight() map[string]*util.Item {
	return nil
}

func (t *Topic) MemoryChan() chan *Message {
	return t.memoryMsgChan
}

func (t *Topic) BackendQueue() BackendQueue {
	return t.backend
}

func NewTopic(topicName string, options *MqOptions) *Topic {
	topic := &Topic{
		name:       topicName,
		channelMap: make(map[string]*Channel),
		// backend:            NewDiskQueue(topicName, options.DataPath, options.MaxBytesPerFile, options.SyncEvery),
		incomingMsgChan:    make(chan *Message, 1),
		memoryMsgChan:      make(chan *Message, options.MemQueueSize),
		options:            options,
		exitChan:           make(chan int),
		messagePumpStarter: new(sync.Once),
	}

	topic.waitGroup.Wrap(func() { topic.router() })

	return topic
}

func (t *Topic) router() {
	var msgBuf bytes.Buffer
	for msg := range t.incomingMsgChan {
		select {
		case t.memoryMsgChan <- msg:
		default:
			err := WriteMessageToBackend(&msgBuf, msg, t)
			if err != nil {
				log.Printf("ERROR: failed to write message to backend - %s", err.Error())
			}
		}
	}

	log.Printf("TOPIC(%s): closing ... router", t.name)
}

func (t *Topic) GetChannel(channelName string) *Channel {
	t.Lock()
	defer t.Unlock()
	return t.getOrCreateChannel(channelName)
}
func (t *Topic) DeleteExistingChannel(channelName string) error {
	t.Lock()
	channel, ok := t.channelMap[channelName]
	if !ok {
		t.Unlock()
		return errors.New("channel does not exist")
	}
	delete(t.channelMap, channelName)
	// not defered so that we can continue while the channel async closes
	t.Unlock()

	log.Printf("TOPIC(%s): deleting channel %s", t.name, channel.name)
	channel.Delete()
	return nil
}

// this expects the caller to handle locking
func (t *Topic) getOrCreateChannel(channelName string) *Channel {
	channel, ok := t.channelMap[channelName]
	if !ok {
		deleteCallback := func(c *Channel) {
			t.DeleteExistingChannel(c.name)
		}
		channel = NewChannel(t.name, channelName, t.options, deleteCallback)
		t.channelMap[channelName] = channel
		log.Printf("TOPIC(%s): new channel(%s)", t.name, channel.name)
		// start the topic message pump lazily using a `once` on the first channel creation
		t.messagePumpStarter.Do(func() { t.waitGroup.Wrap(func() { t.messagePump() }) })
	}
	return channel
}

func (t *Topic) messagePump() {
	var msg *Message
	var buf []byte
	var err error

	for {
		// do an extra check for exit before we select on all the memory/backend/exitChan
		// this solves the case where we are closed and something else is writing into
		// backend. we don't want to reverse that
		if atomic.LoadInt32(&t.exitFlag) == 1 {
			goto exit
		}

		select {
		case msg = <-t.memoryMsgChan:
		case buf = <-t.backend.ReadChan():
			msg, err = DecodeMessage(buf)
			if err != nil {
				log.Printf("ERROR: failed to decode message - %s", err.Error())
				continue
			}
		case <-t.exitChan:
			goto exit
		}

		t.RLock()
		// check if all the channels have been deleted
		if len(t.channelMap) == 0 {
			// put this message back on the queue
			// we need to background because we currently hold the lock
			go func() {
				t.PutMessage(msg)
			}()

			// reset the sync.Once
			t.messagePumpStarter = new(sync.Once)

			t.RUnlock()
			goto exit
		}

		for _, channel := range t.channelMap {
			// copy the message because each channel
			// needs a unique instance
			chanMsg := NewMessage(msg.Id, msg.Body)
			chanMsg.Timestamp = msg.Timestamp
			err := channel.PutMessage(chanMsg)
			if err != nil {
				log.Printf("TOPIC(%s) ERROR: failed to put msg(%s) to channel(%s) - %s", t.name, msg.Id, channel.name, err.Error())
			}
		}
		t.RUnlock()
	}

exit:
	log.Printf("TOPIC(%s): closing ... messagePump", t.name)
}

func (t *Topic) PutMessage(msg *Message) error {
	t.RLock()
	defer t.RUnlock()
	if atomic.LoadInt32(&t.exitFlag) == 1 {
		return errors.New("exiting")
	}
	t.incomingMsgChan <- msg
	atomic.AddUint64(&t.messageCount, 1)
	return nil
}

func (t *Topic) Close() error {
	if atomic.LoadInt32(&t.exitFlag) == 1 {
		return errors.New("exiting")
	}

	log.Printf("TOPIC(%s): closing", t.name)

	// initiate exit
	atomic.StoreInt32(&t.exitFlag, 1)

	close(t.exitChan)
	t.Lock()
	close(t.incomingMsgChan)
	t.Unlock()

	// synchronize the close of router() and messagePump()
	t.waitGroup.Wait()

	// close all the channels
	for _, channel := range t.channelMap {
		err := channel.Close()
		if err != nil {
			// we need to continue regardless of error to close all the channels
			log.Printf("ERROR: channel(%s) close - %s", channel.name, err.Error())
		}
	}

	// write anything leftover to disk
	if len(t.memoryMsgChan) > 0 {
		log.Printf("TOPIC(%s): flushing %d memory messages to backend", t.name, len(t.memoryMsgChan))
	}
	FlushQueue(t)
	return t.backend.Close()
}

func (t *Topic) Delete() error {
	EmptyQueue(t)
	t.Lock()
	for _, channel := range t.channelMap {
		delete(t.channelMap, channel.name)
		channel.Delete()
	}
	t.Unlock()
	return t.Close()
}
