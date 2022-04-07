package main

import (
	"fmt"
	"logagent/etcd"
	"logagent/kafka"
	"logagent/model"
	"logagent/tailfile"

	"github.com/go-ini/ini"
	"github.com/sirupsen/logrus"
)

//日志收集客户端

type Config struct {
	KafkaConfig   `ini:"kafka"`
	CollectConfig `ini:"collect"`
	EtcdConfig    `ini:"etcd"`
}

type KafkaConfig struct {
	Address  string `ini:"address"`
	ChanSize int64  `ini:"chan_size"`
}

type CollectConfig struct {
	LogFilePath string `ini:"logfile_path"`
}

type EtcdConfig struct {
	Address    string `ini:"address"`
	CollectKey string `ini:"collect_key"`
}

func run() {
	select {}
}

func main() {
	//0.获取本机ip 为了后续去etcd取配置文件
	ip, err := model.GetOutBoundIP()
	if err != nil {
		logrus.Errorf("Get IP failed,err:%v", err)
		return
	}

	//1.读配置文件
	//初始化全局配置文件
	//加载全局文件配置项
	var configObj = new(Config)
	err = ini.MapTo(configObj, "./config/config.ini")
	if err != nil {
		logrus.Errorf("logic config failed,err:%v", err)
		return
	}

	//2.初始化 连接kafka
	//初始化msgChan
	//起后台goroutine去往kafka里发送msg
	err = kafka.Init([]string{configObj.KafkaConfig.Address}, configObj.KafkaConfig.ChanSize)
	if err != nil {
		logrus.Errorf("init kafka failed,err:%v", err)
		return
	}
	logrus.Debug("init kafka success!")

	//3.初始化 连接etcd
	//从etcd中拉去要收集的日志的配置项
	err = etcd.Init([]string{configObj.EtcdConfig.Address})
	if err != nil {
		logrus.Errorf("init etcd failed,err:%v", err)
		return
	}
	//将本机ip以fmt.Sprintf的形式插入到配置信息configObj.EtcdConfig.CollectKey中
	collectKey := fmt.Sprintf(configObj.EtcdConfig.CollectKey, ip)

	//4.从etcd中拉去要收集日志的配置项
	allConf, err := etcd.GetConf(collectKey)
	if err != nil {
		logrus.Errorf("etcd GetConf failed,err:%v", err)
		return
	}
	logrus.Info("init etcd success!")

	//5.监控etcd中 configObj.EtcdConfig.CollectKey对应值的变化
	go etcd.WatchConf(collectKey)

	//6.根据配置中的日志路径初始化tail
	//根据配置文件中的指定路径
	//创建一个对应的tailObj
	err = tailfile.Init(allConf) //把从etcd中加载获取的配置项撞到Init中
	if err != nil {
		logrus.Errorf("init tail failed,err:%v", err)
		return
	}
	logrus.Info("init tail success!")
	run()
}
