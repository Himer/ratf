package raft

import (
	"fmt"
	"sync"
)

// LogCache wraps any LogStore implementation to provide an
// in-memory ring buffer. This is used to cache access to
// the recently written entries. For implementations that do not
// cache themselves, this can provide a substantial boost by
// avoiding disk I/O on recent entries.

//将LogStore(一般都是硬盘实现LogStore的接口)进行包装,增加一个环形buffer来缓存最近的添加的日志
//这个环形buffer 就是个数组   用index%cap来决定存在在数组的哪个位置  去除数据后 会对比index和数据中的index是否一样来判断冲突的情形
//因为index是一直增加的  而且一般都是顺序增加 空洞也不多  所以他们成为环形buffer
//所以如果key单点递增 则为环形buffer  如果不递增 就是个hash值冲突后覆盖的残缺map

type LogCache struct {
	store LogStore

	cache []*Log
	l     sync.RWMutex
}

// NewLogCache is used to create a new LogCache with the
// given capacity and backend store.
// 创建一个新的带有cache 的logstore
func NewLogCache(capacity int, store LogStore) (*LogCache, error) {
	if capacity <= 0 {
		return nil, fmt.Errorf("capacity must be positive")
	}
	c := &LogCache{
		store: store,
		cache: make([]*Log, capacity),
	}
	return c, nil
}

//返回特定index的log  先从cache中取再从logstore中取
func (c *LogCache) GetLog(idx uint64, log *Log) error {
	// Check the buffer for an entry
	c.l.RLock()
	cached := c.cache[idx%uint64(len(c.cache))]
	c.l.RUnlock()

	// Check if entry is valid
	if cached != nil && cached.Index == idx {
		*log = *cached
		return nil
	}

	// Forward request on cache miss
	return c.store.GetLog(idx, log)
}
//存储单个log
func (c *LogCache) StoreLog(log *Log) error {
	return c.StoreLogs([]*Log{log})
}
//存储log  先写到cache里边  在存储到logcache里边
func (c *LogCache) StoreLogs(logs []*Log) error {
	// Insert the logs into the ring buffer
	c.l.Lock()
	for _, l := range logs {
		c.cache[l.Index%uint64(len(c.cache))] = l
	}
	c.l.Unlock()

	return c.store.StoreLogs(logs)
}

//第一个日志的index
func (c *LogCache) FirstIndex() (uint64, error) {
	return c.store.FirstIndex()
}

//最新日志的index
func (c *LogCache) LastIndex() (uint64, error) {
	return c.store.LastIndex()
}

//删除从min到max的(包含min和max)日志, 清空了内存缓冲cahce, 然后从logstore中取数据
func (c *LogCache) DeleteRange(min, max uint64) error {
	// Invalidate the cache on deletes
	c.l.Lock()
	c.cache = make([]*Log, len(c.cache))
	c.l.Unlock()

	return c.store.DeleteRange(min, max)
}
