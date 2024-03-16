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
	mmq = n
	return n
}

func (m *Mmq) Main() {
	tcpListener, err := net.Listen("tcp", m.TcpAddr.String())
	if err != nil {
		log.Fatalf("FATAL: listen (%s) failed - %s", m.TcpAddr, err.Error())
	}
	m.TcpListener = tcpListener
	protocols := map[string]pro.Protocol{"V1": nil}
	m.WaitGroup.Wrap(func() { util.TcpServer(m.TcpListener, &TcpProtocol{protocols: protocols}) })
}

func (m *Mmq) LoadMetadata() {
	fn := fmt.Sprintf(path.Join(m.Options.DataPath, "Mmq.%d.dat"), m.WorkerId)
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
			topic := m.GetTopic(parts[0])

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

func (m *Mmq) Exit() {
	if m.TcpListener != nil {
		m.TcpListener.Close()
	}
	// persist metadata about what topics/channels we have
	// so that upon restart we can get back to the same state
	fn := fmt.Sprintf(path.Join(m.Options.DataPath, "Mmq.%d.dat"), m.WorkerId)
	f, err := os.OpenFile(fn, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		log.Printf("ERROR: failed to open channel metadata file %s - %s", fn, err.Error())
	}

	m.Lock()
	for _, topic := range m.TopicMap {
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
	m.Unlock()

	if f != nil {
		f.Sync()
		f.Close()
	}

	// we want to do this last as it closes the idPump (if closed first it
	// could potentially starve items in process and deadlock)
	close(m.ExitChan)
	m.WaitGroup.Wait()
}

// GetTopic performs a thread safe operation
// to return a pointer to a Topic object (potentially new)
func (m *Mmq) GetTopic(topicName string) *Topic {
	m.Lock()
	t, ok := m.TopicMap[topicName]
	if ok {
		m.Unlock()
		return t
	} else {
		t = NewTopic(topicName, m.Options)
		m.TopicMap[topicName] = t
		log.Printf("TOPIC(%s): created", t.name)

		m.Unlock()
	}
	return t
}

// GetExistingTopic gets a topic only if it exists
func (m *Mmq) GetExistingTopic(topicName string) (*Topic, error) {
	m.RLock()
	defer m.RUnlock()
	topic, ok := m.TopicMap[topicName]
	if !ok {
		return nil, errors.New("topic does not exist")
	}
	return topic, nil
}

// DeleteExistingTopic removes a topic only if it exists
func (m *Mmq) DeleteExistingTopic(topicName string) error {
	m.Lock()
	topic, ok := m.TopicMap[topicName]
	if !ok {
		m.Unlock()
		return errors.New("topic does not exist")
	}
	delete(m.TopicMap, topicName)
	// not defered so that we can continue while the topic async closes
	m.Unlock()

	log.Printf("TOPIC(%s): deleting", topic.name)

	// delete empties all channels and the topic itself before closing
	// (so that we dont leave any messages around)
	topic.Delete()

	return nil
}

func (m *Mmq) idPump() {
	lastError := time.Now()
	for {
		id, err := util.NewGUID(m.WorkerId)
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
		case m.IdChan <- id.Hex():
		case <-m.ExitChan:
			goto exit
		}
	}

exit:
	log.Printf("ID: closing")
}
