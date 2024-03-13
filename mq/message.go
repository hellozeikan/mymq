package mq

import (
	"bytes"
	"encoding/binary"
	"io"
	"io/ioutil"
	"time"
)

const MsgIdLength = 16

// Message is the fundamental data type containing
// the id, body, and meta-data
type Message struct {
	Id        []byte
	Body      []byte
	Timestamp int64
	Attempts  uint16
}

// NewMessage creates a Message, initializes some meta-data,
// and returns a pointer
func NewMessage(id []byte, body []byte) *Message {
	return &Message{
		Id:        id,
		Body:      body,
		Timestamp: time.Now().Unix(),
	}
}

func (m *Message) EncodeBytes() ([]byte, error) {
	var buf bytes.Buffer
	err := m.Encode(&buf)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (m *Message) Encode(w io.Writer) error {
	err := binary.Write(w, binary.BigEndian, &m.Timestamp)
	if err != nil {
		return err
	}

	err = binary.Write(w, binary.BigEndian, &m.Attempts)
	if err != nil {
		return err
	}

	_, err = w.Write(m.Id)
	if err != nil {
		return err
	}

	_, err = w.Write(m.Body)
	if err != nil {
		return err
	}

	return nil
}

func DecodeMessage(byteBuf []byte) (*Message, error) {
	var timestamp int64
	var attempts uint16

	buf := bytes.NewBuffer(byteBuf)

	err := binary.Read(buf, binary.BigEndian, &timestamp)
	if err != nil {
		return nil, err
	}

	err = binary.Read(buf, binary.BigEndian, &attempts)
	if err != nil {
		return nil, err
	}

	id := make([]byte, MsgIdLength)
	_, err = io.ReadFull(buf, id)
	if err != nil {
		return nil, err
	}

	body, err := ioutil.ReadAll(buf)
	if err != nil {
		return nil, err
	}

	msg := NewMessage(id, body)
	msg.Timestamp = timestamp
	msg.Attempts = attempts

	return msg, nil
}
