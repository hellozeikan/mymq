package mq

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"path"
	"runtime"
	"strings"
	"sync"
	"time"

	pro "github.com/mymq/protocol"
	"github.com/mymq/util"
)

type Mmq struct {
	sync.RWMutex
	Options     *MqOptions
	WorkerId    int64
	TopicMap    map[string]*Topic
	TcpAddr     *net.TCPAddr
	TcpListener net.Listener
	IdChan      chan []byte
	ExitChan    chan int
	WaitGroup   util.WaitGroupWrapper
	Ctx         context.Context
	CtxCancel   context.CancelFunc
}

type MqOptions struct {
	MemQueueSize    int64
	DataPath        string
	MaxBytesPerFile int64
	SyncEvery       int64
	MsgTimeout      time.Duration
	ClientTimeout   time.Duration
}

func NewMqOptions() *MqOptions {
	return &MqOptions{
		MemQueueSize:    10000,
		DataPath:        os.TempDir(),
		MaxBytesPerFile: 104857600,
		SyncEvery:       2500,
		MsgTimeout:      60 * time.Second,
		ClientTimeout:   util.DefaultClientTimeout,
	}
}

func NewMmq(workerId int64, options *MqOptions) *Mmq {
	n := &Mmq{
		WorkerId: workerId,
		Options:  options,
		TopicMap: make(map[string]*Topic),
		IdChan:   make(chan []byte, 4096),
		ExitChan: make(chan int),
	}
	n.Ctx, n.CtxCancel = context.WithCancel(context.Background())
	n.WaitGroup.Wrap(func() { n.idPump() })

	return n
}

func (n *Mmq) Main() {
	tcpListener, err := net.Listen("tcp", n.TcpAddr.String())
	if err != nil {
		log.Fatalf("FATAL: listen (%s) failed - %s", n.TcpAddr, err.Error())
	}
	n.TcpListener = tcpListener
	protocols := map[string]pro.Protocol{"V1": nil}
	n.WaitGroup.Wrap(func() { util.TcpServer(n.TcpListener, &TcpProtocol{protocols: protocols}) })
}

func (n *Mmq) LoadMetadata() {
	fn := fmt.Sprintf(path.Join(n.Options.DataPath, "Mmq.%d.dat"), n.WorkerId)
	data, err := os.ReadFile(fn)
	if err != nil {
		if !os.IsNotExist(err) {
			log.Printf("ERROR: failed to read channel metadata from %s - %s", fn, err.Error())
		}
		return
	}

	for _, line := range strings.Split(string(data), "\n") {
		if line != "" {
			parts := strings.SplitN(line, ":", 2)

			if !pro.IsValidTopicName(parts[0]) {
				log.Printf("WARNING: skipping creation of invalid topic %s", parts[0])
				continue
			}
			topic := n.GetTopic(parts[0])

			if len(parts) < 2 {
				continue
			}
			if !pro.IsValidChannelName(parts[1]) {
				log.Printf("WARNING: skipping creation of invalid channel %s", parts[1])
			}
			topic.GetChannel(parts[1])
		}
	}
}

func (n *Mmq) Exit() {
	if n.TcpListener != nil {
		n.TcpListener.Close()
	}
	// persist metadata about what topics/channels we have
	// so that upon restart we can get back to the same state
	fn := fmt.Sprintf(path.Join(n.Options.DataPath, "Mmq.%d.dat"), n.WorkerId)
	f, err := os.OpenFile(fn, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		log.Printf("ERROR: failed to open channel metadata file %s - %s", fn, err.Error())
	}

	log.Printf("NSQ: closing topics")
	n.Lock()
	for _, topic := range n.TopicMap {
		if f != nil {
			topic.Lock()
			fmt.Fprintf(f, "%s\n", topic.name)
			for _, channel := range topic.channelMap {
				if !channel.ephemeralChannel {
					fmt.Fprintf(f, "%s:%s\n", topic.name, channel.name)
				}
			}
			topic.Unlock()
		}
		topic.Close()
	}
	n.Unlock()

	if f != nil {
		f.Sync()
		f.Close()
	}

	// we want to do this last as it closes the idPump (if closed first it
	// could potentially starve items in process and deadlock)
	close(n.ExitChan)
	n.WaitGroup.Wait()
}

// GetTopic performs a thread safe operation
// to return a pointer to a Topic object (potentially new)
func (n *Mmq) GetTopic(topicName string) *Topic {
	n.Lock()
	t, ok := n.TopicMap[topicName]
	if ok {
		n.Unlock()
		return t
	} else {
		t = NewTopic(topicName, n.Options)
		n.TopicMap[topicName] = t
		log.Printf("TOPIC(%s): created", t.name)

		// release our global Mmq lock, and switch to a more granular topic lock while we init our
		// channels from lookupd. This blocks concurrent PutMessages to this topic.
		t.Lock()
		defer t.Unlock()
		n.Unlock()
		// if using lookupd, make a blocking call to get the topics, and immediately create them.
		// this makes sure that any message received is buffered to the right channels
		// if len(n.LookupPeers) > 0 {
		// 	channelNames, _ := util.GetChannelsForTopic(t.name, n.lookupHttpAddrs())
		// 	for _, channelName := range channelNames {
		// 		t.getOrCreateChannel(channelName)
		// 	}
		// }
	}
	return t
}

// GetExistingTopic gets a topic only if it exists
func (n *Mmq) GetExistingTopic(topicName string) (*Topic, error) {
	n.RLock()
	defer n.RUnlock()
	topic, ok := n.TopicMap[topicName]
	if !ok {
		return nil, errors.New("topic does not exist")
	}
	return topic, nil
}

// DeleteExistingTopic removes a topic only if it exists
func (n *Mmq) DeleteExistingTopic(topicName string) error {
	n.Lock()
	topic, ok := n.TopicMap[topicName]
	if !ok {
		n.Unlock()
		return errors.New("topic does not exist")
	}
	delete(n.TopicMap, topicName)
	// not defered so that we can continue while the topic async closes
	n.Unlock()

	log.Printf("TOPIC(%s): deleting", topic.name)

	// delete empties all channels and the topic itself before closing
	// (so that we dont leave any messages around)
	topic.Delete()

	return nil
}

func (n *Mmq) idPump() {
	lastError := time.Now()
	for {
		id, err := util.NewGUID(n.WorkerId)
		if err != nil {
			now := time.Now()
			if now.Sub(lastError) > time.Second {
				// only print the error once/second
				log.Printf("ERROR: %s", err.Error())
				lastError = now
			}
			runtime.Gosched()
			continue
		}
		select {
		case n.IdChan <- id.Hex():
		case <-n.ExitChan:
			goto exit
		}
	}

exit:
	log.Printf("ID: closing")
}
