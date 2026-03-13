package raft

import (
	"RaftKV/service/raftapi"
	"RaftKV/tool"
	"context"
	"sync/atomic"
	"time"
)


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
    timer := time.NewTicker(StableHeartbeatTimeout()) // 每 100ms 滴答一次
    defer timer.Stop()

    const BatchDuration = 15 * time.Millisecond
    needSendData := false

    for !rf.killed() {
        select {
        case <-timer.C:
            rf.mu.Lock()
            isLeader := rf.state == Leader
            rf.mu.Unlock()
            if isLeader {
                // 🚨 走纯心跳通道，0字节负荷，绝对不堵车
                go rf.sendAppendEntries(true)
            }

        case <-rf.sendHeartbeatAtOnceCh:
            batchTimer := time.NewTimer(BatchDuration)
        DrainLoop:
            for {
                select {
                case <-rf.sendHeartbeatAtOnceCh:
                case <-batchTimer.C:
                    break DrainLoop
                case <-rf.shutdownCh:
                    batchTimer.Stop()
                    return
                }
            }
            needSendData = true
            
        case <-rf.shutdownCh:
            return
        }

        if needSendData {
            rf.mu.Lock()
            isLeader := rf.state == Leader
            rf.mu.Unlock()
            if isLeader {
                // 🚨 走数据同步通道
                go rf.sendAppendEntries(false)
            }
            needSendData = false
        }
    }
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		select {
		case <-rf.electionCh:
			rf.mu.Lock()
			isStaleSignal := time.Since(rf.lastResetElectionTime) < 500*time.Millisecond
			isLeader := rf.state == Leader
			rf.mu.Unlock()
			
			if !isLeader && !isStaleSignal {
				tool.Log.Info("Start to a new elecion", "id", rf.me)
				go rf.sendRequestVote()
			}
		case <-rf.heartbeatCh: // 兼容你之前的信号
			rf.mu.Lock()
			isLeader := rf.state == Leader
			rf.mu.Unlock()
			if isLeader {
				go rf.sendAppendEntries(false)
			}
		case <-rf.shutdownCh:
			return
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