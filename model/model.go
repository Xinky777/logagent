package model

import (
	"fmt"
	"net"
	"strings"
)

// CollectEntry 要收集日志配置项结构体
type CollectEntry struct {
	Path  string `json:"path"`  //去哪个路径读取日志文件
	Topic string `json:"topic"` //日志文件发往kafka中那个topic
}

//GetOutBoundIP 获取本机ip
func GetOutBoundIP() (ip string, err error) {
	conn, err := net.Dial("udp", "8.8.8.8:88")
	if err != nil {
		return
	}
	defer conn.Close()
	localAddr := conn.LocalAddr().(*net.UDPAddr)
	fmt.Println(localAddr.String())
	ip = strings.Split(localAddr.IP.String(), ":")[0]
	return ip, err
}
