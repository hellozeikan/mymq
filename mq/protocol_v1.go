package mq

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"log"
	"net"
	"strconv"
	"sync/atomic"
	"time"

	pro "github.com/mymq/protocol"
	"github.com/mymq/util"
)

type ProtocolV1 struct {
	pro.Protocol
}

func (p *ProtocolV1) IOLoop(conn net.Conn) error {
	var err error
	var line []byte

	client := NewClientV1(conn)
	atomic.StoreInt32(&client.State, util.StateInit)

	err = nil
	client.Reader = bufio.NewReader(client)
	for {
		fmt.Println(mmq.Options)
		client.Conn.SetReadDeadline(time.Now().Add(mmq.Options.ClientTimeout))
		// ReadSlice does not allocate new space for the data each request
		// ie. the returned slice is only valid until the next call to it
		line, err = client.Reader.ReadSlice('\n')
		if err != nil {
			break
		}
		// 'SUB' topic, ch, xxx
		// trim the '\n'
		line = line[:len(line)-1]
		// optionally trim the '\r'
		if line[len(line)-1] == '\r' {
			line = line[:len(line)-1]
		}
		params := bytes.Split(line, []byte(" "))

		fmt.Println("----->", string(params[0]))
		response, err := p.Exec(client, params)
		if err != nil {
			log.Printf("ERROR: CLIENT(%s) - %s", client, err.(*util.ClientErr).Description())
			err = p.Send(client, util.FrameTypeError, []byte(err.Error()))
			if err != nil {
				break
			}
			continue
		}

		if response != nil {
			err = p.Send(client, util.FrameTypeResponse, response)
			if err != nil {
				break
			}
		}
	}

	log.Printf("PROTOCOL(V2): [%s] exiting ioloop", client)
	// TODO: gracefully send clients the close signal
	conn.Close()
	close(client.ExitChan)

	return err
}

func (p *ProtocolV1) Send(client *ClientV1, frameType int32, data []byte) error {
	client.Lock()
	defer client.Unlock()

	client.frameBuf.Reset()
	err := pro.Frame(&client.frameBuf, frameType, data)
	if err != nil {
		return err
	}

	attempts := 0
	for {
		client.SetWriteDeadline(time.Now().Add(time.Second))
		_, err := pro.SendResponse(client, client.frameBuf.Bytes())
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				attempts++
				if attempts == 3 {
					return errors.New("timed out trying to send to client")
				}
				continue
			}
			return err
		}
		break
	}

	return nil
}

func (p *ProtocolV1) Exec(client *ClientV1, params [][]byte) ([]byte, error) {
	switch {
	case bytes.Equal(params[0], []byte("SUB")):
		return p.SUB(client, params)
	case bytes.Equal(params[0], []byte("RDY")):
		return p.RDY(client, params)
	case bytes.Equal(params[0], []byte("FIN")):
		return p.FIN(client, params)
	case bytes.Equal(params[0], []byte("REQ")):
		return p.REQ(client, params)
	case bytes.Equal(params[0], []byte("CLS")):
		return p.CLS(client, params)
	case bytes.Equal(params[0], []byte("PUB")):
		return p.PUB(client, params)
	}
	return nil, util.NewClientErr("INVALID", fmt.Sprintf("invalid command %s", params[0]))
}

func (p *ProtocolV1) sendHeartbeat(client *ClientV1) error {
	err := p.Send(client, pro.FrameTypeResponse, []byte("_heartbeat_"))
	if err != nil {
		return err
	}

	return nil
}

func (p *ProtocolV1) messagePump(client *ClientV1) {
	var err error
	var buf bytes.Buffer
	var c chan *Message

	heartbeat := time.NewTicker(mmq.Options.ClientTimeout / 2)

	// ReadyStateChan has a buffer of 1 to guarantee that in the event
	// there is a race the state update is not lost
	for {
		if client.IsReadyForMessages() {
			c = client.Channel.clientMsgChan
		} else {
			c = nil
			break
		}

		select {
		case <-client.ReadyStateChan:
		case <-heartbeat.C:
			err = p.sendHeartbeat(client)
			if err != nil {
				log.Printf("PROTOCOL(V1): error sending heartbeat - %s", err.Error())
			}
		case msg, ok := <-c:
			if !ok {
				goto exit
			}

			buf.Reset()
			err = msg.Encode(&buf)
			if err != nil {
				goto exit
			}

			client.Channel.StartInFlightTimeout(msg, client)
			client.SendingMessage()

			err = p.Send(client, pro.FrameTypeMessage, buf.Bytes())
			if err != nil {
				goto exit
			}
		case <-client.ExitChan:
			goto exit
		}
	}

exit:
	log.Printf("PROTOCOL(V2): [%s] exiting messagePump", client == nil)
	heartbeat.Stop()
	client.Channel.RemoveClient(client)
	if err != nil {
		log.Printf("PROTOCOL(V2): messagePump error - %s", err.Error())
	}
}

func (p *ProtocolV1) SUB(client *ClientV1, params [][]byte) ([]byte, error) {
	if atomic.LoadInt32(&client.State) != util.StateInit {
		return nil, util.NewClientErr("E_INVALID", "client not initialized")
	}

	if len(params) < 3 {
		return nil, util.NewClientErr("E_MISSING_PARAMS", "insufficient number of parameters")
	}

	topicName := string(params[1])
	if !pro.IsValidTopicName(topicName) {
		return nil, util.NewClientErr("E_BAD_TOPIC", fmt.Sprintf("topic name '%s' is not valid", topicName))
	}

	channelName := string(params[2])
	if !pro.IsValidChannelName(channelName) {
		return nil, util.NewClientErr("E_BAD_CHANNEL", fmt.Sprintf("channel name '%s' is not valid", channelName))
	}

	if len(params) == 5 {
		client.ShortIdentifier = string(params[3])
		client.LongIdentifier = string(params[4])
	}

	topic := mmq.GetTopic(topicName)
	channel := topic.GetChannel(channelName)
	channel.AddClient(client)

	client.Channel = channel
	atomic.StoreInt32(&client.State, util.StateSubscribed)

	go p.messagePump(client)

	return nil, nil
}

func (p *ProtocolV1) RDY(client *ClientV1, params [][]byte) ([]byte, error) {
	var err error

	state := atomic.LoadInt32(&client.State)

	if state == util.StateClosing {
		// just ignore ready changes on a closing channel
		log.Printf("PROTOCOL(V2): ignoring RDY after CLS in state ClientStateV2Closing")
		return nil, nil
	}

	if state != util.StateSubscribed {
		return nil, util.NewClientErr("E_INVALID", "client not subscribed")
	}

	count := 1
	if len(params) > 1 {
		count, err = strconv.Atoi(string(params[1]))
		if err != nil {
			return nil, util.NewClientErr("E_INVALID", fmt.Sprintf("could not parse RDY count %s", params[1]))
		}
	}

	if count > pro.MaxReadyCount {
		log.Printf("ERROR: client(%s) sent ready count %d > %d", client, count, pro.MaxReadyCount)
		count = pro.MaxReadyCount
	}

	client.SetReadyCount(int64(count))

	return nil, nil
}

func (p *ProtocolV1) FIN(client *ClientV1, params [][]byte) ([]byte, error) {
	state := atomic.LoadInt32(&client.State)
	if state != util.StateSubscribed && state != util.StateClosing {
		return nil, util.NewClientErr("E_INVALID", "cannot finish in current state")
	}

	if len(params) < 2 {
		return nil, util.NewClientErr("E_MISSING_PARAMS", "insufficient number of params")
	}

	idStr := params[1]
	err := client.Channel.FinishMessage(client, idStr)
	if err != nil {
		return nil, util.NewClientErr("E_FIN_FAILED", err.Error())
	}

	client.FinishedMessage()

	return nil, nil
}

func (p *ProtocolV1) REQ(client *ClientV1, params [][]byte) ([]byte, error) {
	state := atomic.LoadInt32(&client.State)
	if state != util.StateSubscribed && state != util.StateClosing {
		return nil, util.NewClientErr("E_INVALID", "cannot re-queue in current state")
	}

	if len(params) < 3 {
		return nil, util.NewClientErr("E_MISSING_PARAMS", "insufficient number of params")
	}

	idStr := params[1]
	timeoutMs, err := strconv.Atoi(string(params[2]))
	if err != nil {
		return nil, util.NewClientErr("E_INVALID", fmt.Sprintf("could not parse timeout %s", params[2]))
	}
	timeoutDuration := time.Duration(timeoutMs) * time.Millisecond

	if timeoutDuration < 0 || timeoutDuration > util.MaxTimeout {
		return nil, util.NewClientErr("E_INVALID", fmt.Sprintf("timeout %d out of range", timeoutDuration))
	}

	err = client.Channel.RequeueMessage(client, idStr, timeoutDuration)
	if err != nil {
		return nil, util.NewClientErr("E_REQ_FAILED", err.Error())
	}

	client.RequeuedMessage()

	return nil, nil
}

func (p *ProtocolV1) CLS(client *ClientV1, params [][]byte) ([]byte, error) {
	if atomic.LoadInt32(&client.State) != util.StateSubscribed {
		return nil, util.NewClientErr("E_INVALID", "client not subscribed")
	}

	if len(params) > 1 {
		return nil, util.NewClientErr("E_MISSING_PARAMS", "insufficient number of params")
	}

	client.StartClose()

	return []byte("CLOSE_WAIT"), nil
}

func (p *ProtocolV1) PUB(client *ClientV1, params [][]byte) ([]byte, error) {
	var err error

	if len(params) < 2 {
		return nil, util.NewClientErr("E_MISSING_PARAMS", "insufficient number of parameters")
	}

	topicName := string(params[1])
	if !pro.IsValidTopicName(topicName) {
		return nil, util.NewClientErr("E_BAD_TOPIC", fmt.Sprintf("topic name '%s' is not valid", topicName))
	}

	// var bodyLen int32
	// err = binary.Read(client.Reader, binary.BigEndian, &bodyLen)
	// if err != nil {
	// 	return nil, util.NewClientErr("E_BAD_BODY", err.Error())
	// }

	// messageBody := make([]byte, bodyLen)
	// _, err = io.ReadFull(client.Reader, messageBody)
	// if err != nil {
	// 	return nil, util.NewClientErr("E_BAD_BODY", err.Error())
	// }
	messageBody := params[2]
	topic := mmq.GetTopic(topicName)
	msg := NewMessage(<-mmq.IdChan, messageBody)
	err = topic.PutMessage(msg)
	if err != nil {
		return nil, util.NewClientErr("E_PUT_FAILED", err.Error())
	}
	return []byte("OK"), nil
}
