package aof

import (
	"io"
	"os"
	"redigo/pkg/config"
	"redigo/pkg/interface/database"
	"redigo/pkg/redis"
	"redigo/pkg/util/log"
	"strconv"
	"time"
)

type rewriteContext struct {
	tmpFile     *os.File
	currentDB   int
	oldFileSize int64
}

func (h *Handler) makeRewriteHandler() *Handler {
	handler := &Handler{}
	// 创建临时数据库来保存rewrie时新增的data
	handler.db = h.dbMaker()
	handler.aofFile = h.aofFile
	handler.aofFileName = h.aofFileName
	handler.currentDB = h.currentDB
	return handler
}

func (h *Handler) StartRewrite() error {
	// cas避免同时rewrite
	if !h.RewriteStarted.CompareAndSwap(false, true) {
		return redis.AppendOnlyRewriteInProgressError
	}
	go func() {
		start := time.Now()
		if err := h.rewrite(); err != nil {
			log.Errorf("rewrite aof error: %v", err)
		}
		h.RewriteStarted.Store(false)
		log.Info("Rewrite AOF finished, time used: %d ms", time.Now().Sub(start).Milliseconds())
	}()
	return nil
}

func (h *Handler) rewrite() error {
	context, err := h.prepareRewrite()
	if err != nil {
		return err
	}

	if err := h.doRewrite(context); err != nil {
		return err
	}

	return h.finishRewrite(context)
}

func (h *Handler) doRewrite(ctx *rewriteContext) error {
	tempAof := h.makeRewriteHandler()

	if err := tempAof.loadAof(ctx.oldFileSize); err != nil {
		return err
	}
	for i := 0; i <= config.Properties.Databases; i++ {
		// 跳过空数据库
		if tempAof.db.Len(i) == 0 {
			continue
		}
		// 插入select命令切换数据库
		selectCmd := redis.NewStringArrayCommand([]string{"SELECT", strconv.Itoa(i)})
		_, err := ctx.tmpFile.Write(selectCmd.ToBytes())
		if err != nil {
			return err
		}
		// 保存数据库keys
		tempAof.db.ForEach(i, func(key string, entry *database.Entry, expire *time.Time) bool {
			command := EntryToCommand(key, entry)
			if command != nil {
				_, _ = ctx.tmpFile.Write(command.ToBytes())
				if expire != nil {
					expireCommand := makeExpireCommand(key, expire)
					_, _ = ctx.tmpFile.Write(expireCommand.ToBytes())
				}
			}
			return true
		})
	}
	return nil
}

// prepareRewrite 初始化重写：创建aof临时文件和重写上下文
func (h *Handler) prepareRewrite() (*rewriteContext, error) {
	h.aofLock.Lock()
	defer h.aofLock.Unlock()
	// fsync将未落盘的aof写入文件
	if err := h.aofFile.Sync(); err != nil {
		return nil, err
	}
	stat, err := os.Stat(h.aofFileName)
	if err != nil {
		return nil, err
	}
	size := stat.Size()
	file, err := os.CreateTemp("./", "*.aof")
	if err != nil {
		return nil, err
	}
	return &rewriteContext{
		tmpFile:     file,
		currentDB:   h.currentDB,
		oldFileSize: size,
	}, nil
}

// finishRewrite 重写结束，将旧aof文件中的新追加数据添加到新文件，然后将旧aof文件删除
func (h *Handler) finishRewrite(ctx *rewriteContext) error {
	h.aofLock.Lock()
	defer h.aofLock.Unlock()

	src, err := os.Open(h.aofFileName)
	defer func() {
		_ = src.Close()
	}()
	if err != nil {
		log.Errorf("Open old aof file failed: %v", err)
		return err
	}
	// seek到旧文件的末尾
	if _, err := src.Seek(ctx.oldFileSize, 0); err != nil {
		log.Errorf("Seek offset in old aof file failed %v", err)
		return err
	}
	// 切换到服务器正在使用的dbIdx，插入一条select
	selectDbCommand := redis.NewStringArrayCommand([]string{"SELECT", strconv.Itoa(ctx.currentDB)})
	_, _ = ctx.tmpFile.Write(selectDbCommand.ToBytes())
	// 将旧文件的数据转移到新文件末尾
	_, err = io.Copy(ctx.tmpFile, src)

	_ = src.Close()
	_ = h.aofFile.Close()
	_ = ctx.tmpFile.Close()
	_ = os.Rename(ctx.tmpFile.Name(), h.aofFileName)

	// 重新设置服务器的aof文件
	aofFile, err := os.OpenFile(h.aofFileName, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return err
	}
	h.aofFile = aofFile
	selectDbCommand = redis.NewStringArrayCommand([]string{"SELECT", strconv.Itoa(h.currentDB)})
	_, _ = ctx.tmpFile.Write(selectDbCommand.ToBytes())
	return nil
}
