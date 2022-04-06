package tailfile

import (
	"logagent/kafka"
	"logagent/model"
	"strings"
	"time"

	"github.com/Shopify/sarama"

	"github.com/hpcloud/tail"
	"github.com/sirupsen/logrus"
)

type tailTask struct {
	path    string
	topic   string
	tailObj *tail.Tail
}

func newTailTask(path string, topic string) *tailTask {
	return &tailTask{
		path:  path,
		topic: topic,
	}
}

// Init 结构体tailTask的初始化方法
func (t *tailTask) Init() (err error) {
	cfg := tail.Config{
		ReOpen: true,
		Follow: true,
		Location: &tail.SeekInfo{
			Offset: 0,
			Whence: 2,
		},
		MustExist: false,
		Poll:      true,
	}
	t.tailObj, err = tail.TailFile(t.path, cfg)
	return
}

//Run 读取日志往kafka里发送
func (t *tailTask) Run() {
	//读取日志发往kafka
	//tailObj ---> log ---> Client ---> kafka
	logrus.Infof("collect for path:%s is running...", t.path)
	for {
		//循环读数据
		line, ok := <-t.tailObj.Lines //chan tail.Line
		if !ok {
			logrus.Warn("tail file close reopen,path:%s\n", t.path)
			time.Sleep(time.Second) //读取出错等一秒
			continue
		}
		//如果是空行 就略过
		if len(strings.Trim(line.Text, "\r")) == 0 {
			logrus.Info("出现空行...跳过")
			continue
		}
		//利用chan将同步代码改为异步的
		//把读出来的一行日志包装成卡夫卡、里面的msg类型
		msg := &sarama.ProducerMessage{
			Topic: t.topic,
			Value: sarama.StringEncoder(line.Text),
		}
		//放入通道中
		kafka.SendMsgChan(msg)
	}
}

// Init tail的初始化方法
//为每一个日志文件造一个单独的tailTask
func Init(allConf []model.CollectEntry) (err error) {
	//allConf里存了若干日志收集项
	//针对每个日志收集项创建一个对应的tailObj
	for _, conf := range allConf {
		tt := newTailTask(conf.Path, conf.Topic) //创建一个日志收集的任务
		err = tt.Init()                          //创建一个日志收集的任务
		if err != nil {
			logrus.Errorf("create tailObj for path:%s failed,err:%v", conf.Path, err)
			continue //这个path没启动 启动下一个
		}
		logrus.Infof("create a tail task for path:%s success", conf.Path)
		//收集日志
		go tt.Run()
	}
	return
}
