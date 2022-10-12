package main

import (
	"fmt"
	"go-zk/watch"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

func main() {
	driver, err := watch.NewZKServer([]string{"127.0.0.1:2181"})
	if err != nil {
		log.Fatal(err)
	}
	defer driver.Close()

	// 获取节点列表, 如果节点不存在则新建
	nodeList, err := driver.GetListByPath("/server")
	if err != nil && strings.Contains(err.Error(), "not exist") {
		log.Fatal("首次启动请先启动注册端先把路径给创建了(持久节点)")
	} else if err != nil {
		log.Fatal(err)
	}
	fmt.Println("server node:", nodeList)

	// 监听节点变化
	chanNodes, chanNodesErr := driver.WatchHostsByPath("/server")
	go func() {
		for {
			select {
			case err := <-chanNodesErr:
				fmt.Println("chanNodes occur error, info: ", err.Error())
			case changeNodes := <-chanNodes:
				fmt.Println("node list changed, new list: ", changeNodes)
			default:
				time.Sleep(time.Millisecond * 200)
			}
		}
	}()
	quit := make(chan os.Signal)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	fmt.Println("----------------")
	<-quit
}
