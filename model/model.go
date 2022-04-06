package model

// CollectEntry 要收集日志配置项结构体
type CollectEntry struct {
	Path  string `json:"path"`
	Topic string `json:"topic"`
}
