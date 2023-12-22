package logger

import (
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"sync"
	"time"
)

// Settings stores config for Logger
type Settings struct {
	Path       string `yaml:"path"`
	Name       string `yaml:"name"`
	Ext        string `yaml:"ext"`
	TimeFormat string `yaml:"time-format"`
}

type logLevel int

// Output levels
const (
	DEBUG logLevel = iota
	INFO
	WARNING
	ERROR
	FATAL
)

const (
	flags              = log.LstdFlags
	defaultCallerDepth = 2
	bufferSize         = 1e5
)

type logEntry struct {
	msg   string
	level logLevel
}

var (
	levelFlags = []string{"DEBUG", "INFO", "WARN", "ERROR", "FATAL"}
)

// Logger is Logger
type Logger struct {
	logFile   *os.File
	logger    *log.Logger
	entryChan chan *logEntry
	entryPool *sync.Pool
}

// *默认是打印到标注输出 os.Stdout中
var DefaultLogger = NewStdoutLogger()

// NewStdoutLogger creates a logger which print msg to stdout
func NewStdoutLogger() *Logger {
	logger := &Logger{
		logFile:   nil,
		logger:    log.New(os.Stdout, "", flags),    // *标准输出
		entryChan: make(chan *logEntry, bufferSize), // *这个设计思想可以优化日志写入效率（相当于先缓存在内存中）
		entryPool: &sync.Pool{ // *对象池
			New: func() interface{} {
				return &logEntry{}
			},
		},
	}
	// *从内存 entryChan 中读取，然后打印到标准输出中
	go func() {
		for e := range logger.entryChan {
			// *写入日志到标准输出
			_ = logger.logger.Output(0, e.msg) // msg includes call stack, no need for calldepth
			// *将对象放回对象池子（复用对象）
			logger.entryPool.Put(e)
		}
	}()
	return logger
}

// NewFileLogger creates a logger which print msg to stdout and log file
func NewFileLogger(settings *Settings) (*Logger, error) {
	fileName := fmt.Sprintf("%s-%s.%s",
		settings.Name,
		time.Now().Format(settings.TimeFormat),
		settings.Ext)
	// *其实就是为了打开文件（获得文件句柄fd)
	logFile, err := mustOpen(fileName, settings.Path)
	if err != nil {
		return nil, fmt.Errorf("logging.Join err: %s", err)
	}
	// *相当于一个 写入器集合（包括 os.Stdout 和 logFile),当写入的时候，会同时【向这两个写入】
	mw := io.MultiWriter(os.Stdout, logFile)

	// *定义logger对象
	logger := &Logger{
		logFile:   logFile,
		logger:    log.New(mw, "", flags),
		entryChan: make(chan *logEntry, bufferSize),
		entryPool: &sync.Pool{
			New: func() interface{} {
				return &logEntry{}
			},
		},
	}
	go func() {
		// *从缓存中读取日志
		for e := range logger.entryChan {
			logFilename := fmt.Sprintf("%s-%s.%s",
				settings.Name,
				time.Now().Format(settings.TimeFormat),
				settings.Ext)
			// *判断文件是否变了，（比如每天一个日志文件 godis-2023-12-01.log）
			if path.Join(settings.Path, logFilename) != logger.logFile.Name() {
				logFile, err := mustOpen(logFilename, settings.Path)
				if err != nil {
					panic("open log " + logFilename + " failed: " + err.Error())
				}
				// *重新打开新的文件
				logger.logFile = logFile
				logger.logger = log.New(io.MultiWriter(os.Stdout, logFile), "", flags)
			}
			// *写入【日志到磁盘和标准输出】
			_ = logger.logger.Output(0, e.msg) // msg includes call stack, no need for calldepth
			logger.entryPool.Put(e)
		}
	}()
	return logger, nil
}

// Setup initializes DefaultLogger
func Setup(settings *Settings) {
	// *构建一个日志写入对象（既可以写入文件/也可以写入到标准输出）
	logger, err := NewFileLogger(settings)
	if err != nil {
		panic(err)
	}
	// *替换默认
	DefaultLogger = logger
}

// Output sends a msg to logger
func (logger *Logger) Output(level logLevel, callerDepth int, msg string) {
	var formattedMsg string
	// *就是为了显示日志打印的文件名/文件行数/日志消息...(目的在于：错误的快速定位)
	_, file, line, ok := runtime.Caller(callerDepth)
	if ok {
		formattedMsg = fmt.Sprintf("[%s][%s:%d] %s", levelFlags[level], filepath.Base(file), line, msg)
	} else {
		formattedMsg = fmt.Sprintf("[%s] %s", levelFlags[level], msg)
	}
	// *从对象池取一个对象
	entry := logger.entryPool.Get().(*logEntry)
	// *将日志等级 + 日志消息 保存到 entryChan 通道中（其实就是缓存）（提升写入的效率）
	entry.msg = formattedMsg
	entry.level = level
	logger.entryChan <- entry
}

// Debug logs debug message through DefaultLogger
func Debug(v ...interface{}) {
	msg := fmt.Sprintln(v...)
	DefaultLogger.Output(DEBUG, defaultCallerDepth, msg)
}

// Debugf logs debug message through DefaultLogger
func Debugf(format string, v ...interface{}) {
	msg := fmt.Sprintf(format, v...)
	DefaultLogger.Output(DEBUG, defaultCallerDepth, msg)
}

// Info logs message through DefaultLogger
func Info(v ...interface{}) {
	msg := fmt.Sprintln(v...)
	DefaultLogger.Output(INFO, defaultCallerDepth, msg)
}

// Infof logs message through DefaultLogger
func Infof(format string, v ...interface{}) {
	msg := fmt.Sprintf(format, v...)
	DefaultLogger.Output(INFO, defaultCallerDepth, msg)
}

// Warn logs warning message through DefaultLogger
func Warn(v ...interface{}) {
	msg := fmt.Sprintln(v...)
	DefaultLogger.Output(WARNING, defaultCallerDepth, msg)
}

// Error logs error message through DefaultLogger
func Error(v ...interface{}) {
	msg := fmt.Sprintln(v...)
	DefaultLogger.Output(ERROR, defaultCallerDepth, msg)
}

// Errorf logs error message through DefaultLogger
func Errorf(format string, v ...interface{}) {
	msg := fmt.Sprintf(format, v...)
	DefaultLogger.Output(ERROR, defaultCallerDepth, msg)
}

// Fatal prints error message then stop the program
func Fatal(v ...interface{}) {
	msg := fmt.Sprintln(v...)
	DefaultLogger.Output(FATAL, defaultCallerDepth, msg)
}
