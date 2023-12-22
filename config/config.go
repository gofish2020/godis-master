package config

import (
	"bufio"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/hdt3213/godis/lib/utils"

	"github.com/hdt3213/godis/lib/logger"
)

var (
	ClusterMode    = "cluster"
	StandaloneMode = "standalone"
)

// ServerProperties defines global config properties
type ServerProperties struct {
	// for Public configuration
	RunID             string `cfg:"runid"` // runID always different at every exec.
	Bind              string `cfg:"bind"`
	Port              int    `cfg:"port"`
	Dir               string `cfg:"dir"`
	AnnounceHost      string `cfg:"announce-host"`
	AppendOnly        bool   `cfg:"appendonly"`
	AppendFilename    string `cfg:"appendfilename"`
	AppendFsync       string `cfg:"appendfsync"`
	AofUseRdbPreamble bool   `cfg:"aof-use-rdb-preamble"`
	MaxClients        int    `cfg:"maxclients"`
	RequirePass       string `cfg:"requirepass"`
	Databases         int    `cfg:"databases"`
	RDBFilename       string `cfg:"dbfilename"`
	MasterAuth        string `cfg:"masterauth"`
	SlaveAnnouncePort int    `cfg:"slave-announce-port"`
	SlaveAnnounceIP   string `cfg:"slave-announce-ip"`
	ReplTimeout       int    `cfg:"repl-timeout"`
	ClusterEnable     bool   `cfg:"cluster-enable"`
	ClusterAsSeed     bool   `cfg:"cluster-as-seed"`
	ClusterSeed       string `cfg:"cluster-seed"`
	ClusterConfigFile string `cfg:"cluster-config-file"`

	// for cluster mode configuration
	ClusterEnabled string   `cfg:"cluster-enabled"` // Not used at present.
	Peers          []string `cfg:"peers"`
	Self           string   `cfg:"self"`

	// config file path
	CfPath string `cfg:"cf,omitempty"`
}

type ServerInfo struct {
	StartUpTime time.Time
}

func (p *ServerProperties) AnnounceAddress() string {
	return p.AnnounceHost + ":" + strconv.Itoa(p.Port)
}

// Properties holds global config properties
var Properties *ServerProperties
var EachTimeServerInfo *ServerInfo

func init() {
	// A few stats we don't want to reset: server startup time, and peak mem.
	EachTimeServerInfo = &ServerInfo{
		StartUpTime: time.Now(),
	}

	// default config
	Properties = &ServerProperties{
		Bind:       "127.0.0.1",
		Port:       6379,
		AppendOnly: false,
		RunID:      utils.RandString(40),
	}
}

/*
bind 0.0.0.0
port 6399
maxclients 128

appendonly yes
appendfilename appendonly.aof
appendfsync everysec
aof-use-rdb-preamble yes

dbfilename test.rdb
*/
func parse(src io.Reader) *ServerProperties {
	config := &ServerProperties{}

	// read config file
	rawMap := make(map[string]string)
	// *按照行扫描文件
	scanner := bufio.NewScanner(src)
	for scanner.Scan() { // 每次返回一行
		// 读取一行
		line := scanner.Text()
		// 判断改行是否 已经#注释
		if len(line) > 0 && strings.TrimPrefix(line, " ")[0] == '#' {
			continue
		}
		// 找到第一个 空格：这里要结合*.conf的文件格式去看
		pivot := strings.IndexAny(line, " ")
		// 该索引位置有效
		if pivot > 0 && pivot < len(line)-1 { // separator found

			// 截取 key
			key := line[0:pivot]
			// 截取 value（同时去掉首尾的空格）
			value := strings.Trim(line[pivot+1:], " ")

			// key转成小写，保存到 rawMap中
			rawMap[strings.ToLower(key)] = value
		}
	}

	// 如果是正常扫描完配置文件，即使是(io.EOF),  scanner.Err() 函数也不会报错
	if err := scanner.Err(); err != nil {
		logger.Fatal(err)
	}

	// parse format
	t := reflect.TypeOf(config)
	v := reflect.ValueOf(config)

	// t 是结构体指针类型 t.Elem()获取原始结构体类型
	n := t.Elem().NumField()
	for i := 0; i < n; i++ {
		// 获取 字段类型
		field := t.Elem().Field(i)
		// 获取 字段值
		fieldVal := v.Elem().Field(i)

		// 获取结构体字段类型中的 Tag 标记
		key, ok := field.Tag.Lookup("cfg")
		if !ok || strings.TrimLeft(key, " ") == "" {
			key = field.Name // 默认用户定义的字段名字
		}

		// 通过结构体的字段名字，从 rawMap（也就是配置文件）读取value
		value, ok := rawMap[strings.ToLower(key)]
		if ok {

			// 字段类型
			switch field.Type.Kind() {
			case reflect.String:
				// 字段如果是字符串，直接赋值
				fieldVal.SetString(value)
			case reflect.Int:
				// 字段如果是整数：转换下
				intValue, err := strconv.ParseInt(value, 10, 64)
				if err == nil {
					fieldVal.SetInt(intValue)
				}
			case reflect.Bool:
				// 字段如果是bool，转下
				boolValue := "yes" == value // Yoda Condition
				fieldVal.SetBool(boolValue)
			case reflect.Slice:

				// 字段如果是slice，获取切片中的一个元素的类型
				if field.Type.Elem().Kind() == reflect.String {

					// 将字符串转成切片，保存到 结构体中的字段中
					slice := strings.Split(value, ",")
					fieldVal.Set(reflect.ValueOf(slice))
				}
			}
		}
	}
	return config
}

// SetupConfig read config file and store properties into Properties
func SetupConfig(configFilename string) {
	// 打开文件
	file, err := os.Open(configFilename)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	//
	Properties = parse(file)
	Properties.RunID = utils.RandString(40) // 随机 RunID
	configFilePath, err := filepath.Abs(configFilename)
	if err != nil {
		return
	}
	// 文件路径
	Properties.CfPath = configFilePath
	if Properties.Dir == "" {
		Properties.Dir = "."
	}
}

func GetTmpDir() string {
	return Properties.Dir + "/tmp"
}
