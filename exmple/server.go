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
	message := []byte("V1 xxx qwer")

	// 发送数据
	_, err = conn.Write(message)
	if err != nil {
		fmt.Println("Error sending data:", err)
		return
	}
	// 接收响应
	buffer := make([]byte, 1024)
	n, err := conn.Read(buffer)
	if err != nil {
		fmt.Println("Error receiving response:", err)
		return
	}

	// 处理响应
	fmt.Println("Response from server:", n, string(buffer))

	_, _ = conn.Read(buffer)
	if err != nil {
		fmt.Println("Error receiving response:", err)
		return
	}

	// 处理响应
	fmt.Println("Response from server:", n, string(buffer))
}
