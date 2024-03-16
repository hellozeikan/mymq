package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/mymq/mq"
)

var (
	showVersion     = flag.Bool("version", false, "print version string")
	tcpAddress      = flag.String("tcp-address", "0.0.0.0:3210", "<addr>:<port> to listen on for TCP clients")
	memQueueSize    = flag.Int64("mem-queue-size", 10000, "number of messages to keep in memory (per topic)")
	maxBytesPerFile = flag.Int64("max-bytes-per-file", 104857600, "number of bytes per diskqueue file before rolling")
	syncEvery       = flag.Int64("sync-every", 2500, "number of messages between diskqueue syncs")
	msgTimeoutMs    = flag.Int64("msg-timeout", 60000, "time (ms) to wait before auto-requeing a message")
	dataPath        = flag.String("data-path", "", "path to store disk-backed messages")
	workerId        = flag.Int64("worker-id", 0, "unique identifier (int) for this worker (will default to a hash of hostname)")
	verbose         = flag.Bool("verbose", false, "enable verbose logging")
)
var mmq *mq.Mmq

func main() {
	flag.Parse()
	fmt.Println(*showVersion)
	tcpAddr, err := net.ResolveTCPAddr("tcp", *tcpAddress)
	if err != nil {
		log.Fatal(err)
	}
	exitChan := make(chan int)
	signalChan := make(chan os.Signal, 1)
	go func() {
		<-signalChan
		exitChan <- 1
	}()
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)
	options := mq.NewMqOptions()
	options.MemQueueSize = *memQueueSize
	options.DataPath = *dataPath
	options.MaxBytesPerFile = *maxBytesPerFile
	options.SyncEvery = *syncEvery
	options.MsgTimeout = time.Duration(*msgTimeoutMs) * time.Millisecond
	mmq = mq.NewMmq(*workerId, options)
	mmq.TcpAddr = tcpAddr
	mmq.LoadMetadata()
	mmq.Main()
	<-exitChan
	mmq.Exit()
}
