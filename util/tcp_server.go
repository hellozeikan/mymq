package util

import (
	"fmt"
	"log"
	"net"
	"runtime"
	"strings"
)

type TcpHandler interface {
	Handle(net.Conn)
}

func TcpServer(listener net.Listener, handler TcpHandler) {
	log.Printf("TCP: listening on %s", listener.Addr().String())

	for {
		clientConn, err := listener.Accept()
		if err != nil {
			if _, ok := err.(net.Error); ok {
				log.Printf("NOTICE: temporary Accept() failure - %s", err.Error())
				runtime.Gosched()
				break
				// continue
			}
			// theres no direct way to detect this error because it is not exposed
			if !strings.Contains(err.Error(), "use of closed network connection") {
				log.Printf("ERROR: listener.Accept() - %s", err.Error())
			}
			break
		}
		// do somethings
		// go handler.Handle(clientConn)
		fmt.Println(clientConn.LocalAddr(), clientConn.RemoteAddr())
	}

	log.Printf("TCP: closing %s", listener.Addr().String())
}
