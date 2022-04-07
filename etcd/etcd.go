package etcd

import (
	"context"
	"encoding/json"
	"fmt"
	"logagent/model"
	"logagent/tailfile"
	"time"

	"github.com/sirupsen/logrus"

	clientv3 "go.etcd.io/etcd/client/v3"
)

//etcd相关操作
var (
	client *clientv3.Client
)

func Init(address []string) (err error) {
	client, err = clientv3.New(clientv3.Config{
		Endpoints:   address,
		DialTimeout: time.Second * 5,
	})
	if err != nil {
		fmt.Printf("connect to etcd failed,err:%v", err)
		return
	}
	return
}

// GetConf 拉取日志收集配置项的函数
func GetConf(key string) (collectEntryList []model.CollectEntry, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	resp, err := client.Get(ctx, key)
	if err != nil {
		logrus.Errorf("get conf from etcd by key:%s failed,err:%v", key, err)
		return
	}
	if len(resp.Kvs) == 0 {
		logrus.Warningf("get len=0 conf from etcd by key:%s", key)
		return
	}
	ret := resp.Kvs[0]
	fmt.Println(ret.Value)
	err = json.Unmarshal(ret.Value, &collectEntryList) //json格式字符串
	if err != nil {
		logrus.Errorf("json Unmarshal failed,err:%v", err)
		return
	}
	return
}

//WatchConf 监控etcd中日志收集项配置变化
func WatchConf(key string) {
	for {
		watchCh := client.Watch(context.Background(), key)
		for wresp := range watchCh {
			logrus.Info("get new conf from etcd...")
			for _, evt := range wresp.Events {
				fmt.Printf("type:%s key:%s value:%s\n", evt.Type, evt.Kv.Key, evt.Kv.Value)
				var newConf []model.CollectEntry
				if evt.Type == clientv3.EventTypeDelete { //如果是一个删除的操作
					//如果是删除
					logrus.Warning("etcd delete the key")
					tailfile.SendNewConf(newConf) //传一个空的newConf 无接受就会阻塞
					continue
				}
				//否则是创建的操作
				err := json.Unmarshal(evt.Kv.Value, &newConf)
				if err != nil {
					logrus.Errorf("json Unmarshal new conf failed,err:%v", err)
					continue
				}
				//告诉tailfile模块启用新配置
				tailfile.SendNewConf(newConf) //无接受就会阻塞
			}
		}
	}
}
