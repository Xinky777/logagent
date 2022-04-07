package tailfile

import (
	"context"
	"logagent/kafka"
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
	//终止goroutine使用的context
	ctx    context.Context
	cancel context.CancelFunc
}

func newTailTask(path string, topic string) *tailTask {
	ctx, cancel := context.WithCancel(context.Background())
	return &tailTask{
		path:   path,
		topic:  topic,
		ctx:    ctx,
		cancel: cancel,
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
		select {
		case <-t.ctx.Done(): //只要调用了t.cancel() 表示一个goroutine结束了 t.ctx.Done()就有值 返回进行下一个循环
			logrus.Infof("path:%s is stopping...", t.path)
			return
			//循环读数据
		case line, ok := <-t.tailObj.Lines: //chan tail.Line
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
}
