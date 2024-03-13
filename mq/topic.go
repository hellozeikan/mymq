package mq

import (
	"bytes"
	"log"
	"sync"

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
