package watch

import (
	"fmt"
	"time"

	"github.com/samuel/go-zookeeper/zk"
)

type ZKServer struct {
	hosts      []string
	conn       *zk.Conn
	pathPrefix string
}

// 创建结构体
func NewZKServer(hosts []string) (*ZKServer, error) {
	conn, _, err := zk.Connect(hosts, 5*time.Second)
	if err != nil {
		return nil, err
	}
	return &ZKServer{hosts: hosts, conn: conn, pathPrefix: "/zk_service_"}, nil
}

// Close 关闭服务
func (z *ZKServer) Close() {
	z.conn.Close()
}

// GetPathData 获取配置
func (z *ZKServer) GetDataByPath(path string) ([]byte, *zk.Stat, error) {
	return z.conn.Get(path)
}

// UpdateDataByPath 有则更新、无则新建配置
// UpdateDataByPath 有则更新、无则新建节点并配置内容
func (z *ZKServer) UpdateDataByPath(path string, data []byte, version int32) (err error) {
	ex, _, _ := z.conn.Exists(path)
	if !ex {
		z.conn.Create(path, data, 0, zk.WorldACL(zk.PermAll))
		return nil
	}
	// 需要版本才能更新
	_, stat, err := z.GetDataByPath(path)
	if err != nil {
		return
	}
	_, err = z.conn.Set(path, data, stat.Version)
	return
}

func (z *ZKServer) WatchHostsByPath(path string) (chan []string, chan error) {
	snapshots := make(chan []string) // 变动后的挂载目标列表
	errors := make(chan error)       // 变动错误信息
	go func() {
		for {
			snapshot, _, events, err := z.conn.ChildrenW(path)
			if err != nil {
				errors <- err
			}
			snapshots <- snapshot
			select {
			case evt := <-events:
				if evt.Err != nil {
					errors <- evt.Err
				}
				if evt.Type == zk.EventNodeCreated {
					fmt.Printf("has node[%s] detete\n", evt.Path)
				} else if evt.Type == zk.EventNodeDeleted {
					fmt.Printf("has new node[%d] create\n", evt.Path)
				} else if evt.Type == zk.EventNodeDataChanged {
					fmt.Printf("has node[%d] data changed", evt.Path)
				}
				fmt.Printf("ChildrenW Event Path:%v, Type:%v\n", evt.Path, evt.Type)
			}
		}

	}()
	return snapshots, errors
}

func (z *ZKServer) WatchDataByPath(path string) (chan []byte, chan error) {

	snapshots := make(chan []byte)
	errors := make(chan error)
	go func() {
		for {
			data, _, events, err := z.conn.GetW(path)
			if err != nil {
				errors <- err
			}
			snapshots <- data
			select {
			case evt := <-events:
				if evt.Err != nil {
					errors <- evt.Err
					return
				}
				fmt.Printf("GetW Event Path:%v, Type:%v\n", evt.Path, evt.Type)
			}
		}
	}()
	return snapshots, errors
}

// RegistHostOnPath 将主机挂载在路径上(节点上)
func (z *ZKServer) RegistHostOnPath(path string, host string) (err error) {
	// 1. 若路径不存在则新建
	ex, _, err := z.conn.Exists(path)
	if err != nil {
		return
	}
	if !ex {
		_, err = z.conn.Create(path, nil, 0, zk.WorldACL(zk.PermAll))
		if err != nil {
			return
		}
	}
	// 2. 将主机进行挂载(路径为永久、主机为临时); 临时会检测主机的存活并清理
	subNodePath := fmt.Sprintf("%s/%s", path, host)
	// 2.1 主机是否已挂载, 没挂则挂
	if ex, _ := z.Exists(subNodePath); !ex {
		_, err = z.conn.Create(subNodePath, nil, zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
	}
	return
}

// GetListByPath 获取节点下挂载的列表
func (z *ZKServer) GetListByPath(path string) (list []string, err error) {
	list, _, err = z.conn.Children(path)
	return list, err
}

func (z *ZKServer) Exists(path string) (exist bool, err error) {
	exist, _, err = z.conn.Exists(path)
	return
}

// 删除 cas支持
func (z *ZKServer) Del(path string) (err error) {
	_, sate, _ := z.GetDataByPath(path)
	fmt.Println(sate)
	err = z.conn.Delete(path, sate.Version)
	return err
}
