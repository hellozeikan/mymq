package util

import (
	"time"
)

const (
	StateInit = iota
	StateDisconnected
	StateConnected
	StateSubscribed
	StateClosing // close has started. responses are ok, but no new messages will be sent
)

const (
	FrameTypeResponse int32 = 0
	FrameTypeError    int32 = 1
	FrameTypeMessage  int32 = 2
)
const MaxTimeout = time.Hour
const DefaultClientTimeout = 60 * time.Second
const BINARY_VERSION = "0.1.0"
