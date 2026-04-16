package main

import (
	"bufio"
	"cache/internal/server"
	"fmt"
	"os"
	"strings"

	"cache/pkg/store"

	"github.com/sirupsen/logrus"
)

func main() {
	//读取端口
	fmt.Println("请输入端口:")
	var port string
	fmt.Scanln(&port)
	//启动服务
	srv := server.NewServer("localhost:"+port, "cache")
	group := server.NewGroup("cache", "localhost:"+port, store.NewOptions(), store.LRU)
	picker, err := server.NewClientPicker("localhost:"+port, "cache", nil)
	if err != nil {
		logrus.Errorf("failed to create client picker: %v", err)
	}
	server.RegisterGroupToServer(group, srv)
	server.RegisterPeersToServer(picker, srv)
	srv.Start()
	reader := bufio.NewReader(os.Stdin)
	//开始不断询问操作并打印结果
	for {
		fmt.Println("请输入操作:")
		operation, _ := reader.ReadString('\n')
		operation = strings.TrimRight(operation, "\r\n")
		switch operation {
		case "get":
			fmt.Println("请输入key:")
			key, _ := reader.ReadString('\n')
			key = strings.TrimRight(key, "\r\n")
			value, err := srv.GetFromCacheAndRedis(key)
			if err != nil {
				logrus.Errorf("failed to get: %v", err)
			}
			fmt.Println("get value:", value)
		case "set":
			fmt.Println("请输入key:")
			key, _ := reader.ReadString('\n')
			key = strings.TrimRight(key, "\r\n")
			fmt.Println("请输入value:")
			value, _ := reader.ReadString('\n')
			value = strings.TrimRight(value, "\r\n")
			srv.SetToCacheAndRedis(key, store.ByteView(value))
			fmt.Println("设置成功")
		case "delete":
			fmt.Println("请输入key:")
			key, _ := reader.ReadString('\n')
			key = strings.TrimRight(key, "\r\n")
			srv.DeleteCacheAndRedis(key)
			fmt.Println("删除成功")
		case "set_hot":
			fmt.Println("请输入key:")
			key, _ := reader.ReadString('\n')
			key = strings.TrimRight(key, "\r\n")
			fmt.Println("请输入value:")
			value, _ := reader.ReadString('\n')
			value = strings.TrimRight(value, "\r\n")
			srv.SetToCache(key, store.ByteView(value))
			fmt.Println("设置成功")
		case "exit":
			srv.Close()
			return
		}
	}
}
