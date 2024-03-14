package main

import (
	"fmt"
	"net"
)

func main() {
	// 与服务器建立连接
	conn, err := net.Dial("tcp", "127.0.0.1:3210")
	if err != nil {
		fmt.Println("Error connecting:", err)
		return
	}
	defer conn.Close()

	// 准备要发送的数据
	message := []byte("Hello, TCP server!")

	// 发送数据
	_, err = conn.Write(message)
	if err != nil {
		fmt.Println("Error sending data:", err)
		return
	}

	fmt.Println("Data sent successfully!")
}
