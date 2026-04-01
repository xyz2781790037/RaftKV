package kvraft

import (
	"RaftKV/proto/kvpb"
	"RaftKV/tool"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type WatchEvent struct {
	Op    string
	Key   string
	Value string
	Ts    uint64
}

type Watcher struct {
	id       int64
	key      string
	isPrefix bool
	ch       chan WatchEvent
}
type WatchManager struct {
	mu         sync.RWMutex
	watchers   map[int64]*Watcher
	nextId     atomic.Int64
	history    []WatchEvent
	maxHistory int
}

func NewWatchManager() *WatchManager {
	return &WatchManager{
		watchers:   make(map[int64]*Watcher),
		history:    make([]WatchEvent, 0, 10000),
		maxHistory: 10000,
	}
}
func (wm *WatchManager) AddWatcher(key string, isPrefix bool, startTs uint64) *Watcher {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	w := &Watcher{
		id:       wm.nextId.Add(1),
		key:      key,
		isPrefix: isPrefix,
		ch:       make(chan WatchEvent, 1000),
	}
	wm.watchers[w.id] = w
	if startTs > 0 {
		for _, ev := range wm.history {
			if ev.Ts > startTs && wm.isMatch(w, ev.Key) {
				select {
				case w.ch <- ev:
				default:
					tool.Log.Warn("历史事件过多，Watcher 通道已满，丢弃部分追赶数据", "watcherID", w.id)
				}
			}
		}
	}
	return w
}
func (wm *WatchManager) RemoveWatcher(w *Watcher) {
	wm.mu.Lock()
	defer wm.mu.Unlock()
	delete(wm.watchers, w.id)
	close(w.ch)
}
func (kv *KVServer) Watch(req *kvpb.WatchRequest, stream kvpb.RaftKV_WatchServer) error {
	tool.Log.Info("客户端开启 Watch", "key", req.Key, "isPrefix", req.IsPrefix, "startTs", req.StartTs)

	if !req.IsPrefix && !kv.checkGroup(req.Key) {
		return fmt.Errorf("ERR_WRONG_GROUP")
	}
	watcher := kv.watchManager.AddWatcher(req.Key, req.IsPrefix, req.StartTs)

	defer func() {
		kv.watchManager.RemoveWatcher(watcher)
		tool.Log.Info("客户端关闭 Watch，清理完成", "key", req.Key)
	}()

	for {
		select {
		case <-stream.Context().Done():
			return nil
		case event := <-watcher.ch:
			resp := &kvpb.WatchResponse{
				Op:    event.Op,
				Key:   event.Key,
				Value: event.Value,
				Ts:    event.Ts,
			}
			if err := stream.Send(resp); err != nil {
				tool.Log.Error("推送 Watch 事件失败", "err", err)
				return err
			}
		case <-time.After(1 * time.Second):
			if !req.IsPrefix && !kv.checkGroup(req.Key) {
				tool.Log.Warn("分片已迁移，主动踢出过期的 Watch 流", "key", req.Key)
				return fmt.Errorf("ERR_WRONG_GROUP_MIGRATED")
			}
		}
	}
}

// Dispatch 状态机通知中心，分发事件并记录历史
func (wm *WatchManager) Dispatch(event WatchEvent) {
	wm.mu.Lock()
	defer wm.mu.Unlock()
	if len(wm.history) >= wm.maxHistory {
		wm.history = wm.history[wm.maxHistory/2:]
	}
	wm.history = append(wm.history, event)
	for _, w := range wm.watchers {
		if wm.isMatch(w, event.Key) {
			select {
			case w.ch <- event:
			default:
				tool.Log.Warn("Watcher 通道满，事件丢弃", "watcherID", w.id)
			}
		}
	}
}
func (wm *WatchManager) isMatch(w *Watcher, eventKey string) bool {
	if w.isPrefix {
		return strings.HasPrefix(eventKey, w.key)
	}
	return eventKey == w.key
}
