//go:build linux

package database

import (
	"redigo/pkg/rdb"
	"redigo/pkg/redis"
	"redigo/pkg/util/log"
	"syscall"
)

// BGSave 使用fork子进程CopyOnWrite来避免拷贝数据
func BGSave(db *MultiDB, command redis.Command) *redis.RespCommand {
	// 避免同时存在多个bgsave子进程
	if !db.aofHandler.RewriteStarted.CompareAndSwap(false, true) {
		return redis.NewErrorCommand(redis.BackgroundSaveInProgressError)
	}
	if r0, _, err := syscall.Syscall(syscall.SYS_FORK, 0, 0, 0); err != 0 {
		log.Errorf("fork child process for bgsave failed")
		return redis.NewSingleLineCommand([]byte("Background saving failed"))
	} else {
		if pid := int(r0); pid == 0 {
			// 子进程进行RDB
			err := rdb.Save(db)
			if err != nil {
				syscall.Exit(-1)
			}
			// 子进程主动exit，通知父进程save结束
			syscall.Exit(0)
			// unreachable
			return nil
		} else {
			// 父进程开启一个goroutine，等待子进程结束，并释放save锁
			go func() {
				var status *syscall.WaitStatus
				if _, err := syscall.Wait4(pid, status, 0, &syscall.Rusage{}); err != nil {
					log.Errorf("wait bgsave child process error: %w", err)
				} else {
					if status != nil {
						log.Info("bgsave child process exited with code: %d", (*status).ExitStatus())
					} else {
						log.Info("bgsave child process exited")
					}
				}
				db.aofHandler.RewriteStarted.Store(false)
			}()
			// 父进程返回
			return redis.NewSingleLineCommand([]byte("Background saving started"))
		}
	}
}
