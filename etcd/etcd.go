package etcd

import (
	"context"
	"encoding/json"
	"fmt"
	"logagent/model"
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
