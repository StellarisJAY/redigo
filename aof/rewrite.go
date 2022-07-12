package aof

import (
	"io"
	"io/ioutil"
	"log"
	"os"
	"redigo/config"
	"redigo/interface/database"
	"redigo/redis/protocol"
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
	// make a temporary database to hold rewrite data
	handler.db = h.dbMaker()
	handler.aofFile = h.aofFile
	handler.aofFileName = h.aofFileName
	handler.currentDB = h.currentDB
	return handler
}

func (h *Handler) StartRewrite() error {
	// CAS rewrite status
	if !h.RewriteStarted.CompareAndSwap(false, true) {
		return protocol.AppendOnlyRewriteInProgressError
	}
	go func() {
		start := time.Now()
		err := h.rewrite()
		if err != nil {
			log.Println(err)
		}
		h.RewriteStarted.Store(false)
		log.Println("Rewrite AOF finished, time used: ", time.Now().Sub(start).Milliseconds(), "ms")
	}()
	return nil
}

func (h *Handler) rewrite() error {
	// get current online aof handler's context of this moment
	context, err := h.prepareRewrite()
	if err != nil {
		return err
	}

	err = h.doRewrite(context)
	if err != nil {
		return err
	}
	// finish rewrite by removing old aof file
	err = h.finishRewrite(context)
	return err
}

func (h *Handler) doRewrite(ctx *rewriteContext) error {
	// create a temp database
	tempAof := h.makeRewriteHandler()
	// load aof file's data into temp database
	err := tempAof.loadAof(ctx.oldFileSize)
	if err != nil {
		return err
	}
	for i := 0; i <= config.Properties.Databases; i++ {
		// skip empty databases
		if tempAof.db.Len(i) == 0 {
			continue
		}
		//rewrite select database command
		selectCmd := protocol.NewStringArrayReply([]string{"SELECT", strconv.Itoa(i)})
		_, err := ctx.tmpFile.Write(selectCmd.ToBytes())
		if err != nil {
			return err
		}
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

// prepareRewrite creates temp aof file and make the context of online aof handler
func (h *Handler) prepareRewrite() (*rewriteContext, error) {
	h.aofLock.Lock()
	defer h.aofLock.Unlock()
	// sync the un-flushed data to persist storage
	err := h.aofFile.Sync()
	if err != nil {
		return nil, err
	}
	stat, err := os.Stat(h.aofFileName)
	if err != nil {
		return nil, err
	}
	size := stat.Size()
	// create a temp file to store rewrite data
	file, err := ioutil.TempFile("./", "*.aof")
	if err != nil {
		return nil, err
	}
	return &rewriteContext{
		tmpFile:     file,
		currentDB:   h.currentDB,
		oldFileSize: size,
	}, nil
}

// finishRewrite removes old aof file
func (h *Handler) finishRewrite(ctx *rewriteContext) error {
	h.aofLock.Lock()
	defer h.aofLock.Unlock()

	src, err := os.Open(h.aofFileName)
	defer func() {
		_ = src.Close()
	}()
	if err != nil {
		log.Println("Open old aof file failed ", err)
		return err
	}
	// seek the position of the original aof file before rewrite
	_, err = src.Seek(ctx.oldFileSize, 0)
	if err != nil {
		log.Println("Seek offset in old aof file failed ", err)
		return err
	}
	// change temp file's database to online aof file's database before rewrite
	selectDbCommand := protocol.NewStringArrayReply([]string{"SELECT", strconv.Itoa(ctx.currentDB)})
	_, _ = ctx.tmpFile.Write(selectDbCommand.ToBytes())
	// copy the newly written commands in old aof file to new aof file
	_, err = io.Copy(ctx.tmpFile, src)

	// Close old aof file and rename temp file
	_ = src.Close()
	_ = h.aofFile.Close()
	_ = ctx.tmpFile.Close()
	_ = os.Rename(ctx.tmpFile.Name(), h.aofFileName)

	aofFile, err := os.OpenFile(h.aofFileName, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return err
	}
	h.aofFile = aofFile
	// reset current database in new aof file
	selectDbCommand = protocol.NewStringArrayReply([]string{"SELECT", strconv.Itoa(h.currentDB)})
	_, _ = ctx.tmpFile.Write(selectDbCommand.ToBytes())
	return nil
}
