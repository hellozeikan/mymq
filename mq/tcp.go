package mq

import (
	"fmt"
	"io"
	"log"
	"net"

	pro "github.com/mymq/protocol"
	"github.com/mymq/util"
)

type TcpProtocol struct {
	util.TcpHandler
	protocols map[string]pro.Protocol
}

func (tcpP *TcpProtocol) Handle(conn net.Conn) {
	log.Printf("TCP: new client(%s)", conn.RemoteAddr())
	buf := make([]byte, 3) // The first two bytes are the version number
	_, err := io.ReadFull(conn, buf)
	if err != nil {
		conn.Close()
		return
	}
	protocolMagic := string(buf)
	if err != nil {
		log.Printf("ERROR: failed to read protocol version - %s", err.Error())
		return
	}
	log.Printf("CLIENT(%s): desired protocol %d", conn.RemoteAddr(), protocolMagic)
	switch protocolMagic {
	case "V1 ":
		tcpP.protocols["V1"] = protocol
	default:
		pro.SendResponse(conn, []byte("E_BAD_PROTOCOL"))
		log.Printf("ERROR: client(%s) bad protocol version %d", conn.RemoteAddr(), protocolMagic)
		return
	}
	if p, ok := tcpP.protocols["V1"]; ok {
		err = p.IOLoop(conn)
		if err != nil {
			fmt.Println("Error in IOLoop:", err)
			return
		}
	} else {
		log.Printf("ERROR: client(%s) - %s", conn.RemoteAddr(), err.Error())
		return
	}
}

/**
	protocols[string(protocolMagic)] = &ProtocolV1{}
	err = p.protocols["V1"].IOLoop(conn)
	出现panic: runtime error: invalid memory address or nil pointer dereference
**/
