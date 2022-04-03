package kafka

import (
	"github.com/sirupsen/logrus"

	"github.com/Shopify/sarama"
)

var (
	client  sarama.SyncProducer
	MsgChan chan *sarama.ProducerMessage
)

//Init 初始化全局Kafka连接
func Init(address []string, chanSize int64) (err error) {
	//1.生产者配置
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll          // ACK 发送完数据需要leader和follow都确认
	config.Producer.Partitioner = sarama.NewRandomPartitioner // 分区 新选出一个partition
	config.Producer.Return.Successes = true                   // 成功交付的消息将在success channel返回

	//2.连接kafka
	client, err = sarama.NewSyncProducer(address, config)
	if err != nil {
		logrus.Error("producer closed, err:", err)
		return
	}
	MsgChan = make(chan *sarama.ProducerMessage, chanSize)
	//起一个goroutine从MsgChan中读数据
	go sendMsg()
	return
}

//sendMsg 从msgChan读取msg 发送给kafka
func sendMsg() {
	for {
		select {
		case msg := <-MsgChan:
			pid, offset, err := client.SendMessage(msg)
			if err != nil {
				logrus.Warning("send msg failed,err:", err)
			}
			logrus.Info("send msg to kafka success pid:%v offset:%v", pid, offset)
		}
	}
}
