package tailfile

import (
	"logagent/model"

	"github.com/sirupsen/logrus"
)

//管理tailTask 的管理者
type tailTaskMgr struct {
	tailTaskMap      map[string]*tailTask      //所有的tailTask任务
	collectEntryList []model.CollectEntry      //所有配置项
	confChan         chan []model.CollectEntry //传输新配置的通道
}

var (
	ttMgr *tailTaskMgr
)

// Init 全局tail的初始化方法 main函数调用
//为每一个日志文件造一个单独的tailTask
func Init(allConf []model.CollectEntry) (err error) {
	//allConf里存了若干日志收集项
	//针对每个日志收集项创建一个对应的tailObj
	ttMgr = &tailTaskMgr{
		tailTaskMap:      make(map[string]*tailTask, 20),  //所有的tailTask任务
		confChan:         make(chan []model.CollectEntry), //做一个无缓冲区阻塞的channel 等待新配制的通道
		collectEntryList: allConf,                         //所有配置项
	}

	for _, conf := range allConf {
		tt := newTailTask(conf.Path, conf.Topic) //创建一个日志收集的任务
		err = tt.Init()                          //创建一个日志收集的任务
		if err != nil {
			logrus.Errorf("create tailObj for path:%s failed,err:%v", conf.Path, err)
			continue //这个path没启动 启动下一个
		}
		logrus.Infof("create a tail task for path:%s success", conf.Path)
		ttMgr.tailTaskMap[tt.path] = tt //将创建的tailTask任务登记保存
		//收集日志
		go tt.Run()
	}
	go ttMgr.watch()
	return
}

//watch tailTaskMgr监控新配置并管理所有配置的方法
//一直在等confChan有值
//有值就开始管理之前的tailTack
func (t *tailTaskMgr) watch() {
	for {
		//等待新配置载入
		newConf := <-t.confChan //取到值 说明新的配置已载入
		logrus.Infof("get new conf from etcd,conf:%v,start manage tailTask...", newConf)
		//新配置载入后 管理之前启动的tailTask
		for _, conf := range newConf {
			//1.原来有的 现在还有 保持状态
			if t.isExist(conf) {
				continue
			}
			//2.原来没有的 新建tailTask任务
			tt := newTailTask(conf.Path, conf.Topic)
			err := tt.Init()
			if err != nil {
				logrus.Errorf("create tailObj for path:%s failed,err:%v", conf.Path, err)
				continue //这个path没启动 启动下一个
			}
			logrus.Infof("create a tail task for path:%s success", conf.Path)
			t.tailTaskMap[tt.path] = tt //将创建的tailTask任务登记保存
			//收集日志
			go tt.Run()
		}
		//3.原来有的 现在没有了 暂停该任务
		//找出tailTaskMap中存在（以前存在），但是newConf中不存在的那些tailTask 把他们暂停掉
		for key, task := range t.tailTaskMap {
			var found bool
			for _, conf := range newConf {
				if key == conf.Path { //之前的task在新的配置文件存在
					found = true
					break //跳过该task 遍历下一个task
				}
			}
			if !found { //之前的task在新的配置文件中不存在
				logrus.Infof("the task collect path:%s need to stop.", task.path)
				delete(t.tailTaskMap, key) //从管理类中删掉key
				task.cancel()              //停止该goroutine
			}
		}
	}
}

//isExist 判断tailTaskMap的配置中是否存在该收集项
func (t *tailTaskMgr) isExist(conf model.CollectEntry) bool {
	_, ok := t.tailTaskMap[conf.Path]
	return ok
}

//SendNewConf 把新的配置对象丢给管理对象的confChan中
func SendNewConf(newConf []model.CollectEntry) {
	ttMgr.confChan <- newConf
}
