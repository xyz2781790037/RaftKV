package kvraft

import (
	"RaftKV/tool"
	"context"
	"strconv"
)

type Mutex struct {
	ck      *Clerk
	lockKey string
	ownerID string // 锁的持有者标识（用 clientId 防止别人误删我的锁）
}

func (ck *Clerk) NewMutex(lockName string) *Mutex {
	return &Mutex{
		ck:      ck,
		lockKey: "/locks/" + lockName,
		ownerID: strconv.FormatInt(ck.clientId, 10),
	}
}
func (m *Mutex) Lock(ctx context.Context) bool {
	for {
		select {
		case <-ctx.Done():
			return false
		default:
		}
		success := m.ck.CAS(m.lockKey, "", m.ownerID)
		if success {
			return true
		}
		waitCh := make(chan struct{})
		watchCtx, cancelWatch := context.WithCancel(ctx)
		go m.ck.Watch(watchCtx, m.lockKey, false, func(op, key, val string, ts uint64) {
			if op == "Delete" || val == "" {
				select {
				case waitCh <- struct{}{}:
				default:
				}
				cancelWatch()
			}
		})
		select {
		case <-waitCh:
		case <-ctx.Done():
			cancelWatch()
			return false
		}
	}
}

func (m *Mutex) Unlock() {
	success := m.ck.CAS(m.lockKey, m.ownerID, "")
	if !success {
		tool.Log.Warn("释放锁失败：锁可能已过期或被其他人篡改", "lockKey", m.lockKey)
	}
}
