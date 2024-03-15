package util

import (
	"fmt"
	"log"
	"net"
	"runtime"
	"strings"
	"sync"
)

type TcpHandler interface {
	Handle(net.Conn)
}

func TcpServer(listener net.Listener, handler TcpHandler) {
	log.Printf("TCP: listening on %s", listener.Addr().String())
	var wg sync.WaitGroup
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
		wg.Add(1)
		go func() {
			handler.Handle(clientConn)
			wg.Done()
		}()
		fmt.Println(clientConn.LocalAddr(), clientConn.RemoteAddr())
	}
	wg.Wait()
	log.Printf("TCP: closing %s", listener.Addr().String())
}
