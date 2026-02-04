package raft

import (
	"RaftKV/service/raftapi"
	"RaftKV/tool"
	"context"
	"sync/atomic"
	"time"
)

func (rf *Raft) ticker() {
	for !rf.killed() {
		select {
		case <-rf.electionCh:
			rf.mu.Lock()
			isLeader := rf.state == Leader
			rf.mu.Unlock()
			if !isLeader {
				tool.Log.Info("Start to a new elecion", "id", rf.me)
				go rf.sendRequestVote()
			}
		case <-rf.heartbeatCh:
			rf.mu.Lock()
			isLeader := rf.state == Leader
			rf.mu.Unlock()
			if isLeader {
				go rf.sendAppendEntries()
			}
		case <-rf.shutdownCh:
			return
		}
	}
}

func (rf *Raft) electionTimer() {
	timer := time.NewTimer(RandomElectionTimeout())
	defer timer.Stop()

	for !rf.killed() {
		select {
		case <-timer.C:
			rf.mu.Lock()
			isLeader := rf.state == Leader
			rf.mu.Unlock()
			if !isLeader {
				select {
				case rf.electionCh <- struct{}{}:
				default:
				}
			}
			timer.Reset(RandomElectionTimeout())
		case <-rf.resetElectionTimerCh:
			if !timer.Stop() {
				<-timer.C
			}
			timer.Reset(RandomElectionTimeout())
		case <-rf.shutdownCh:
			if !timer.Stop() {
				<-timer.C
			}
			return
		}
	}
}
func (rf *Raft) heartbeatTimer() {
    // 1. 定义心跳定时器 (保底机制，没数据也要发)
    timer := time.NewTicker(StableHeartbeatTimeout())
    defer timer.Stop()

    // 2. 定义批处理等待时间 (缓冲机制，有数据攒一攒再发)
    // 15ms 是一个经验值，既能攒批，又不会让延迟太高
    const BatchDuration = 15 * time.Millisecond
    
    // 用于标记是否需要立即触发发送
    needSend := false

    for !rf.killed() {
        select {
        case <-timer.C:
            // 时间到了，必须发送心跳保活
            needSend = true

        case <-rf.sendHeartbeatAtOnceCh:
            // 收到 Propose 的信号，说明有新日志了
            // 【关键修改】不要立刻发！进入缓冲模式
            
            // 启动一个短定时器
            batchTimer := time.NewTimer(BatchDuration)
            
            // 在这 15ms 内，把后续涌进来的信号全部“吃掉”
        DrainLoop:
            for {
                select {
                case <-rf.sendHeartbeatAtOnceCh:
                    // 又有新 Propose？太好了，合并！继续等。
                case <-batchTimer.C:
                    // 15ms 到了，不再等了
                    break DrainLoop
                case <-rf.shutdownCh:
                    batchTimer.Stop()
                    return
                }
            }
            // 标记需要发送
            needSend = true
            
        case <-rf.shutdownCh:
            return
        }

        // 统一执行发送逻辑
        if needSend {
            rf.mu.Lock()
            isLeader := rf.state == Leader
            rf.mu.Unlock()
            
            if isLeader {
                // 注意：这里不要用 go rf.sendAppendEntries()
                // 直接调用，或者用一个单独的 worker pool
                // 为了防止阻塞 timer，这里用 go 是可以的，但最好限制并发
                go rf.sendAppendEntries()
            }
            needSend = false
        }
    }
}
func (rf *Raft) resetElectionTimer() {
	select {
	case rf.resetElectionTimerCh <- struct{}{}:
	default:
	}
}
func (rf *Raft) sendHeartbeatAtOnce() {
	select {
	case rf.sendHeartbeatAtOnceCh <- struct{}{}:
	default:
	}
}
func (rf *Raft) applier() {
	for !rf.killed() {
		rf.mu.Lock()
		for rf.commitIndex <= rf.lastApplied && rf.lastIncludedIndex <= rf.lastApplied {
			rf.applyCond.Wait()
		}
		if rf.killed() {
			rf.mu.Unlock()
			return
		}
		if rf.lastApplied < rf.lastIncludedIndex {
			_, snapshotData, ok := rf.store.Log.LoadSnapshot()
			if !ok {
				tool.Log.Error("Failed to load snapshot in applier")
				snapshotData = nil
			}
			snapshotMsg := raftapi.ApplyMsg{
				CommandValid:  false,
				SnapshotValid: true,
				Snapshot:      snapshotData,
				SnapshotTerm:  rf.lastIncludedTerm,
				SnapshotIndex: rf.lastIncludedIndex,
			}
			rf.lastApplied = rf.lastIncludedIndex

			rf.mu.Unlock()
			rf.applyCh <- snapshotMsg
			continue
		}
		if rf.lastApplied < rf.commitIndex {
			startIndex := rf.lastApplied + 1
			endIndex := rf.commitIndex
			count := endIndex - startIndex + 1
			applyMsgs := make([]raftapi.ApplyMsg, count)
			var i int64 = 0
			for ; i < count; i++ {
				logIndex := startIndex + i
				sliceIndex := logIndex - rf.lastIncludedIndex
				if sliceIndex < 0 || sliceIndex >= int64(len(rf.log)) {
					tool.Log.Error("Applier index out of bound", "index", sliceIndex, "log", int64(len(rf.log)))
					break
				}
				applyMsgs[i] = raftapi.ApplyMsg{
					CommandValid:  true,
					Command:       rf.log[sliceIndex].Command,
					CommandIndex:  logIndex,
					SnapshotValid: false,
				}
			}
			// 乐观更新 lastApplied
			rf.lastApplied = endIndex
			rf.mu.Unlock()

			for _, msg := range applyMsgs {
				rf.applyCh <- msg
			}
			continue
		}
		rf.mu.Unlock()
	}
}
func (rf *Raft) flushLoop(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			rf.doFlush()
			return

		case <-ticker.C:
			rf.doFlush()
		case <-rf.flushNotifyCh:
            rf.doFlush()
            ticker.Reset(10 * time.Millisecond)
		}
		
	}
}
func (rf *Raft) monitorLoop() {
	ticker := time.NewTicker(15 * time.Second)
    defer ticker.Stop()
	for !rf.killed() {
        <-ticker.C
        currentCount := atomic.SwapInt64(&rf.writeMetrics, 0)
        if currentCount > 0 {
            tool.Log.Info("🚀 性能监控 [TPS]", 
                "ops/s", float64(currentCount)/15,
                "数量", currentCount, // 估算吞吐量(假设每条1KB)
            )
        }
    }
}