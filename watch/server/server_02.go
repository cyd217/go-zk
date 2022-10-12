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
	// 10次变动
	for i := 0; i < 10; i++ {
		conf := fmt.Sprintf("server-id=" + fmt.Sprint(i))
		if err := driver.UpdateDataByPath("/my.cnf", []byte(conf), int32(i)); err != nil {
			fmt.Println("update content occur err, info: ", err.Error())
		} else {
			fmt.Println("update content successfully, data: ", conf)
		}
		time.Sleep(10 * time.Second)
	}

	fmt.Println("节点/my.cnf 内容变动已完成")
	quit := make(chan os.Signal)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
}
