package demo

import (
	"fmt"
	"github.com/samuel/go-zookeeper/zk"
	"time"
)

func GetConnect(zkList []string) (conn *zk.Conn) {
	// 创建监听的option，用于初始化zk
	conn, _, err := zk.Connect(zkList, 10*time.Second)
	if err != nil {
		fmt.Println(err)
	}
	return
}

// 创建节点
func Create(conn *zk.Conn, path string, data []byte, flags int32, acl int32) (val string, err error) {
	//flags有4种取值：
	//0:永久，除非手动删除
	//zk.FlagEphemeral = 1:短暂，session断开则改节点也被删除
	//zk.FlagSequence  = 2:会自动在节点后面添加序号
	//3:Ephemeral和Sequence，即，短暂且自动添加序号
	// 获取访问控制权限
	var acls []zk.ACL
	if acl == 0 {
		/**
		PermRead = 1 << iota   1
		PermWrite              2
		PermCreate             4
		PermDelete             8
		PermAdmin             16
		PermAll = 0x1f        31
		*/
		acls = zk.WorldACL(zk.PermAll)
	} else {
		acls = zk.WorldACL(acl)
	}

	val, err = conn.Create(path, data, flags, acls)
	return
}

// 查询节点信息
func Get(conn *zk.Conn, path string) (dataStr string, stat *zk.Stat, err error) {
	data, stat, err := conn.Get(path)
	if err != nil {
		return "", nil, err
	}
	return string(data), stat, err
}

// 节点是否存在
func Exists(conn *zk.Conn, path string) (exist bool, err error) {
	exist, _, err = conn.Exists(path)
	return
}

// 删除 cas支持
func Del(conn *zk.Conn, path string) (err error) {
	_, sate, _ := Get(conn, path)
	fmt.Println(sate)
	err = conn.Delete(path, sate.Version)
	return err
}

// 改 CAS支持
// 可以通过此种方式保证原子性
func Modify(conn *zk.Conn, path string, newData []byte) (sate *zk.Stat, err error) {
	_, sate, _ = conn.Get(path)
	fmt.Println(sate)
	sate, err = conn.Set(path, newData, sate.Version)
	return
}

func Children(conn *zk.Conn, path string) (data []string, err error) {
	data, _, err = conn.Children(path)
	return
}
