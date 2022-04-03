package main

import (
	"logagent/kafka"
	"logagent/tailfile"
	"time"

	"github.com/Shopify/sarama"

	"github.com/go-ini/ini"
	"github.com/sirupsen/logrus"
)

//日志收集客户端

type Config struct {
	KafkaConfig   `ini:"kafka"`
	CollectConfig `ini:"collect"`
}

type KafkaConfig struct {
	Address  string `ini:"address"`
	ChanSize int64  `ini:"chan_size"`
}

type CollectConfig struct {
	LogFilePath string `ini:"logfile_path"`
}

//run 业务逻辑
func run() (err error) {
	//tailObj ---> log ---> Client ---> kafka
	for {
		//循环读数据
		line, ok := <-tailfile.TailObj.Lines //chan tail.Line
		if !ok {
			logrus.Warn("tail file close reopen,filename：%s\n", tailfile.TailObj.Filename)
			time.Sleep(time.Second) //读取出错等一秒
			continue
		}
		//利用chan将同步代码改为异步的
		//把读出来的一行日志包装成卡夫卡、里面的msg类型
		msg := &sarama.ProducerMessage{
			Topic: "web_log",
			Value: sarama.StringEncoder(line.Text),
		}
		//放入通道中
		kafka.MsgChan <- msg
	}
}

func main() {
	//1.读配置文件
	var configObj = new(Config)
	err := ini.MapTo(configObj, "./config/config.ini")
	if err != nil {
		logrus.Error("logic config failed,err:%v", err)
		return
	}

	//2.初始化 连接kafka
	err = kafka.Init([]string{configObj.KafkaConfig.Address}, configObj.KafkaConfig.ChanSize)
	if err != nil {
		logrus.Error("init kafka failed,err:%v", err)
		return
	}
	logrus.Debug("init kafka success!")

	//3.根据配置中的日志路径初始化tail
	err = tailfile.Init(configObj.CollectConfig.LogFilePath)
	if err != nil {
		logrus.Error("init tail failed,err:%v", err)
		return
	}
	logrus.Debug("init tail success!")

	//4.把日志通过sarama发往kafka
	err = run()
	if err != nil {
		logrus.Error("run failed,err:%v", err)
		return
	}
}
