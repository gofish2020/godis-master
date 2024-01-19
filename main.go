package main

import (
	"fmt"
	"os"

	"github.com/hdt3213/godis/config"
	"github.com/hdt3213/godis/lib/logger"
	"github.com/hdt3213/godis/lib/utils"
	RedisServer "github.com/hdt3213/godis/redis/server"
	"github.com/hdt3213/godis/tcp"
)

var banner = `
   _____	_          ___
  / ____/___  ____/ (_)____
 / / __/ __ \/ __  / / ___/
/ /_/ / /_/ / /_/ / (__  )
\____/\____/\__,_/_/____/
`

var defaultProperties = &config.ServerProperties{
	Bind:           "0.0.0.0",
	Port:           6399,
	AppendOnly:     false,
	AppendFilename: "",
	MaxClients:     1000,
	RunID:          utils.RandString(40),
}

func fileExists(filename string) bool {
	info, err := os.Stat(filename)
	return err == nil && !info.IsDir()
}

func main() {
	// 打印banner
	print(banner)

	// 初始化默认的日志打印器
	logger.Setup(&logger.Settings{ // logs/godis-2023-02-01.log
		Path:       "logs",
		Name:       "godis",
		Ext:        "log",
		TimeFormat: "2006-01-02",
	})
	// *读取环境变量
	configFilename := os.Getenv("CONFIG")
	// *解析配置 保存到 Properties 全局变量中
	if configFilename == "" {
		if fileExists("redis.conf") {
			config.SetupConfig("redis.conf")
		} else {
			config.Properties = defaultProperties
		}
	} else {
		config.SetupConfig(configFilename)
	}
	// *开始启动tcp服务
	err := tcp.ListenAndServeWithSignal(&tcp.Config{
		Address: fmt.Sprintf("%s:%d", config.Properties.Bind, config.Properties.Port),
	}, RedisServer.MakeHandler()) // RedisServer.MakeHandler() 这个相当于全局唯一的一个Handler
	if err != nil {
		logger.Error(err)
	}
}
