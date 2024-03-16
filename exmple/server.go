package main

import (
	"fmt"
	"net"
)

func main() {
	producter()
	consumer()
}

func consumer() {
	// 与服务器建立连接
	conn, err := net.Dial("tcp", "127.0.0.1:3210")
	if err != nil {
		fmt.Println("Error connecting:", err)
		return
	}
	defer conn.Close()
	/**

	  消息格式： 空格区分
	  p0: 版本号(默认V1)
	  p1: 操作标志 分为（SUB，RDY, FIN,REQ,CLS,PUB）
	  p2: topic
	  p3: channel

	  **/
	message := []byte("V1 SUB qwer chan1\n")

	_, err = conn.Write(message)
	if err != nil {
		fmt.Println("Error sending data:", err)
		return
	}
	buffer := make([]byte, 1024)
	n, err := conn.Read(buffer)
	if err != nil {
		fmt.Println("Error receiving response:", err)
		return
	}

	// 处理响应
	fmt.Println("Response from server:", n, string(buffer))
	_, err = conn.Read(buffer)
	if err != nil {
		fmt.Println("Error receiving response:", err)
		return
	}
	// 处理响应
	fmt.Println("Response from server:", string(buffer))
}

func producter() {
	// 与服务器建立连接
	conn, err := net.Dial("tcp", "127.0.0.1:3210")
	if err != nil {
		fmt.Println("Error connecting:", err)
		return
	}
	defer conn.Close()
	/**

	消息格式： 空格区分
	p0: 版本号(默认V1)
	p1: 操作标志 分为（SUB，RDY, FIN,REQ,CLS,PUB）
	p2: topic
	p3: body

	**/
	// 准备要发送的数据
	message := []byte("V1 PUB qwer qweqweqwe\n")

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
	_, err = conn.Read(buffer)
	if err != nil {
		fmt.Println("Error receiving response:", err)
		return
	}
	// 处理响应
	fmt.Println("Response from server:", string(buffer))
}
