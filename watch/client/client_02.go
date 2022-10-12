package main

import (
	"fmt"
	"go-zk/watch"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
)

func main() {
	driver, err := watch.NewZKServer([]string{"127.0.0.1:2181"})
	if err != nil {
		log.Fatal(err)
	}
	defer driver.Close()

	//获取节点内容
	zc, _, err := driver.GetDataByPath("/my.cnf")
	if err != nil && strings.Contains(err.Error(), "not exist") {
		log.Fatal("首次运行请先启动内容变更程序端")
	} else if err != nil {
		log.Fatal(err)
	}
	fmt.Println("get node data:")
	fmt.Println(string(zc))

	//动态监听节点内容
	dataChan, dataErrChan := driver.WatchDataByPath("/my.cnf")
	go func() {
		for {
			select {
			case changeErr := <-dataErrChan:
				fmt.Println("content change occur err, info: ", changeErr)
			case changedData := <-dataChan:
				fmt.Println("WatchGetData changed, info: ", string(changedData))
			}
		}
	}()
	quit := make(chan os.Signal)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
}
