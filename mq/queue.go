package mq

import (
	"bytes"
	"log"

	pqueue "github.com/mymq/util"
)

// BackendQueue represents the behavior for the secondary message
// storage system
type BackendQueue interface {
	Put([]byte) error
	ReadChan() chan []byte
	Close() error
	Depth() int64
	Empty() error
}

type Queue interface {
	MemoryChan() chan *Message
	BackendQueue() BackendQueue
	InFlight() map[string]*pqueue.Item
	Deferred() map[string]*pqueue.Item
}

func EmptyQueue(q Queue) error {
	for {
		select {
		case <-q.MemoryChan():
		default:
			return q.BackendQueue().Empty()
		}
	}
}

func FlushQueue(q Queue) error {
	var msgBuf bytes.Buffer

	for {
		select {
		case msg := <-q.MemoryChan():
			err := WriteMessageToBackend(&msgBuf, msg, q)
			if err != nil {
				log.Printf("ERROR: failed to write message to backend - %s", err.Error())
			}
		default:
			goto finish
		}
	}

finish:
	for _, item := range q.InFlight() {
		msg := item.Value.(*inFlightMessage).msg
		err := WriteMessageToBackend(&msgBuf, msg, q)
		if err != nil {
			log.Printf("ERROR: failed to write message to backend - %s", err.Error())
		}
	}

	for _, item := range q.Deferred() {
		msg := item.Value.(*Message)
		err := WriteMessageToBackend(&msgBuf, msg, q)
		if err != nil {
			log.Printf("ERROR: failed to write message to backend - %s", err.Error())
		}
	}

	return nil
}

func WriteMessageToBackend(buf *bytes.Buffer, msg *Message, q Queue) error {
	buf.Reset()
	err := msg.Encode(buf)
	if err != nil {
		return err
	}
	err = q.BackendQueue().Put(buf.Bytes())
	if err != nil {
		return err
	}
	return nil
}
