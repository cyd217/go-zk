package demo

import (
	"fmt"
	"github.com/samuel/go-zookeeper/zk"
	"testing"
	"time"
)

var (
	zkList = []string{"127.0.0.1:2181"}
	path   = "/root"
)

func TestCreate(t *testing.T) {
	conn := GetConnect(zkList)

	defer conn.Close()
	//创建节点
	val, err := Create(conn, path, []byte("root value"), 0, 31)
	if err != nil {
		fmt.Printf("创建失败: %v\n", err)
		return
	}
	fmt.Printf("创建: %s 成功", val)
}

func TestGet(t *testing.T) {
	conn := GetConnect(zkList)

	defer conn.Close()
	//查询节点
	val, _, err := Get(conn, path)
	if err != nil {
		fmt.Printf("查询%s失败, err: %v\n", path, err)
		return
	}
	fmt.Printf("%s 的值为 %s\n", path, val)
}

func TestExist(t *testing.T) {
	conn := GetConnect(zkList)

	defer conn.Close()
	//是否存在
	val, err := Exists(conn, path)
	if err != nil {
		fmt.Printf("查询%s失败, err: %v\n", path, err)
		return
	}
	if val {
		fmt.Printf("%s 存在\n", path)
	} else {
		fmt.Printf("%s 不存在\n", path)
	}
}

func TestDel(t *testing.T) {
	conn := GetConnect(zkList)

	defer conn.Close()
	// 删除
	err := Del(conn, path)
	if err != nil {
		fmt.Printf("数据删除失败: %v\n", err)
		return
	}
	fmt.Println("数据删除成功")
}

func TestModify(t *testing.T) {
	conn := GetConnect(zkList)

	defer conn.Close()
	newData := []byte("hello zookeeper")
	stat, err := Modify(conn, path, newData)
	if err != nil {
		fmt.Printf("数据修改失败: %v\n", err)
		return
	}
	fmt.Printf("数据修改成功,stat %v\n", stat)
}

func TestChildren(t *testing.T) {
	conn := GetConnect(zkList)

	defer conn.Close()
	data, err := Children(conn, "/")
	if err != nil {
		fmt.Printf("获取数据失败: %v\n", err)
		return
	}
	fmt.Printf("获取数据成功,data %v\n", data)
}

func callback(event zk.Event) {
	fmt.Println(">>>>>>>>>>>>>>>>>>>")
	fmt.Println("path:", event.Path)
	fmt.Println("type:", event.Type.String())
	fmt.Println("state:", event.State.String())
	fmt.Println("<<<<<<<<<<<<<<<<<<<")
}

func ZKOperateWatchTest() {
	fmt.Printf("ZKOperateWatchTest\n")

	option := zk.WithEventCallback(callback)
	var hosts = []string{"localhost:2181"}
	conn, _, err := zk.Connect(hosts, time.Second*5, option)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer conn.Close()

	var path1 = "/zk_test_go1"
	var data1 = []byte("zk_test_go1_data1")
	exist, s, _, err := conn.ExistsW(path1)
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Printf("path[%s] exist[%t]\n", path1, exist)
	fmt.Printf("state:\n")

	// try create
	var acls = zk.WorldACL(zk.PermAll)
	p, err_create := conn.Create(path1, data1, zk.FlagEphemeral, acls)
	if err_create != nil {
		fmt.Println(err_create)
		return
	}
	fmt.Printf("created path[%s]\n", p)
	time.Sleep(time.Second * 2)

	exist, s, _, err = conn.ExistsW(path1)
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Printf("path[%s] exist[%t] after create\n", path1, exist)
	fmt.Printf("state:\n")

	// delete
	conn.Delete(path1, s.Version)

	exist, s, _, err = conn.ExistsW(path1)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Printf("path[%s] exist[%t] after delete\n", path1, exist)
	fmt.Printf("state:\n")
}
