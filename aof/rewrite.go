package aof

import (
	"io"
	"os"
	"strconv"

	"github.com/hdt3213/godis/config"
	"github.com/hdt3213/godis/lib/logger"
	"github.com/hdt3213/godis/lib/utils"
	"github.com/hdt3213/godis/redis/protocol"
)

func (persister *Persister) newRewriteHandler() *Persister {
	h := &Persister{}
	h.aofFilename = persister.aofFilename
	h.db = persister.tmpDBMaker()
	return h
}

// RewriteCtx holds context of an AOF rewriting procedure
type RewriteCtx struct {
	tmpFile  *os.File // tmpFile is the file handler of aof tmpFile
	fileSize int64
	dbIdx    int // selected db index when startRewrite
}

// Rewrite carries out AOF rewrite
func (persister *Persister) Rewrite() error {

	// 当前的aof的文件大小， tmpFile 文件句柄
	ctx, err := persister.StartRewrite()
	if err != nil {
		return err
	}
	err = persister.DoRewrite(ctx)
	if err != nil {
		return err
	}

	persister.FinishRewrite(ctx)
	return nil
}

// DoRewrite actually rewrite aof file
// makes DoRewrite public for testing only, please use Rewrite instead
func (persister *Persister) DoRewrite(ctx *RewriteCtx) (err error) {
	// start rewrite
	if !config.Properties.AofUseRdbPreamble {
		logger.Info("generate aof preamble")

		// 读取当前aof文件中的内容，保存到【新】内存当中，然后再将内存中的数据保存到tmpFile中
		err = persister.generateAof(ctx)
	} else {
		logger.Info("generate rdb preamble")
		err = persister.generateRDB(ctx)
	}
	return err
}

// StartRewrite prepares rewrite procedure
func (persister *Persister) StartRewrite() (*RewriteCtx, error) {
	// pausing aof

	// 停止操作aof
	persister.pausingAof.Lock()
	defer persister.pausingAof.Unlock()

	// 将数据全部刷盘
	err := persister.aofFile.Sync()
	if err != nil {
		logger.Warn("fsync failed")
		return nil, err
	}

	// get current aof file size

	// 记录下当前文件的大小
	fileInfo, _ := os.Stat(persister.aofFilename)
	filesize := fileInfo.Size()

	// create tmp file
	file, err := os.CreateTemp(config.GetTmpDir(), "*.aof")
	if err != nil {
		logger.Warn("tmp file create failed")
		return nil, err
	}
	return &RewriteCtx{
		tmpFile:  file,
		fileSize: filesize,
		dbIdx:    persister.currentDB, /// 这个是必须要的；正常将内存数据，全部重写完到tmp aof以后，tmpFile尾部的选中的库一般是15号库
	}, nil
}

// FinishRewrite finish rewrite procedure
func (persister *Persister) FinishRewrite(ctx *RewriteCtx) {
	persister.pausingAof.Lock() // pausing aof
	defer persister.pausingAof.Unlock()
	tmpFile := ctx.tmpFile

	// copy commands executed during rewriting to tmpFile
	errOccurs := func() bool {
		/* read write commands executed during rewriting */

		// 只读打开当前的aof文件
		src, err := os.Open(persister.aofFilename)
		if err != nil {
			logger.Error("open aofFilename failed: " + err.Error())
			return true
		}
		defer func() {
			_ = src.Close()
			_ = tmpFile.Close()
		}()

		// 游标移动到 文件的fileSize位置，位置后面都是在重写的过程中，新加入的数据
		_, err = src.Seek(ctx.fileSize, 0)
		if err != nil {
			logger.Error("seek failed: " + err.Error())
			return true
		}

		// 增量的aof，写入到 tmpFile中，同时选中的数据库应该是 dbIdx
		// sync tmpFile's db index with online aofFile
		data := protocol.MakeMultiBulkReply(utils.ToCmdLine("SELECT", strconv.Itoa(ctx.dbIdx))).ToBytes()
		_, err = tmpFile.Write(data)
		if err != nil {
			logger.Error("tmp file rewrite failed: " + err.Error())
			return true
		}
		// copy data

		// 将当前aof中的新数据 复制到tmpFile中
		_, err = io.Copy(tmpFile, src)
		if err != nil {
			logger.Error("copy aof filed failed: " + err.Error())
			return true
		}
		return false
	}()
	if errOccurs {
		return
	}

	// replace current aof file by tmp file

	// 关闭当前的aof
	_ = persister.aofFile.Close()

	// 将 tmpAof文件重命名为 新的当前aof
	if err := os.Rename(tmpFile.Name(), persister.aofFilename); err != nil {
		logger.Warn(err)
	}

	// reopen aof file for further write
	// 重新打开新的aof，作为写入文件句柄
	aofFile, err := os.OpenFile(persister.aofFilename, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		panic(err)
	}
	persister.aofFile = aofFile

	// write select command again to resume aof file selected db
	// it should have the same db index with  persister.currentDB
	data := protocol.MakeMultiBulkReply(utils.ToCmdLine("SELECT", strconv.Itoa(persister.currentDB))).ToBytes()
	_, err = persister.aofFile.Write(data)
	if err != nil {
		panic(err)
	}
}
