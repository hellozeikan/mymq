package mq

import (
	"log"
	"net"

	pro "github.com/mymq/protocol"
	"github.com/mymq/util"
)

type TcpProtocol struct {
	util.TcpHandler
	protocols map[int32]pro.Protocol
}

func (p *TcpProtocol) Handle(clientConn net.Conn) {
	log.Printf("TCP: new client(%s)", clientConn.RemoteAddr())

	protocolMagic, err := pro.ReadMagic(clientConn)
	if err != nil {
		log.Printf("ERROR: failed to read protocol version - %s", err.Error())
		return
	}

	log.Printf("CLIENT(%s): desired protocol %d", clientConn.RemoteAddr(), protocolMagic)

	prot, ok := p.protocols[protocolMagic]
	if !ok {
		pro.SendResponse(clientConn, []byte("E_BAD_PROTOCOL"))
		log.Printf("ERROR: client(%s) bad protocol version %d", clientConn.RemoteAddr(), protocolMagic)
		return
	}

	err = prot.IOLoop(clientConn)
	if err != nil {
		log.Printf("ERROR: client(%s) - %s", clientConn.RemoteAddr(), err.Error())
		return
	}
}
