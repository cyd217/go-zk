package main

import (
	"fmt"
	"go-zk/watch"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	driver, err := watch.NewZKServer([]string{"127.0.0.1:2181"})
	if err != nil {
		log.Fatal(err)
	}
	defer driver.Close()
	// 60秒10次变动
	for i := 0; i < 10; i++ {
		if err = driver.RegistHostOnPath("/server", fmt.Sprintf("192.168.1.1%d", i)); err != nil {
			fmt.Println("failed regist host at path")
		} else {
			fmt.Println("regist host successfully")
		}
		time.Sleep(10 * time.Second)
	}
	fmt.Println("节点变动已完成")
	quit := make(chan os.Signal)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	fmt.Println("-------------------")
	<-quit
}
