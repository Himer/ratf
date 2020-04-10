package raft

import (
	"errors"
	"sync"
)

// InmemStore implements the LogStore and StableStore interface.
// It should NOT EVER be used for production. It is used only for
// unit tests. Use the MDBStore implementation instead.
//InmemStore实现了LogStore和StableStore接口, 不能在线上环境使用,仅能在测试环境使用
//可以用来保存日志快照(LogStore) 也可以用来保存集群信息(StableStore)

type InmemStore struct {
	l         sync.RWMutex
	lowIndex  uint64    /*最小的日志的inex*/
	highIndex uint64    /*最高的日志的inex*/
	logs      map[uint64]*Log   /*日志存储的map*/
	kv        map[string][]byte
	kvInt     map[string]uint64
}

// NewInmemStore returns a new in-memory backend. Do not ever
// use for production. Only for testing.
func NewInmemStore() *InmemStore {
	i := &InmemStore{
		logs:  make(map[uint64]*Log),
		kv:    make(map[string][]byte),
		kvInt: make(map[string]uint64),
	}
	return i
}

// FirstIndex implements the LogStore interface.
//返回最小的日志的index编号
func (i *InmemStore) FirstIndex() (uint64, error) {
	i.l.RLock()
	defer i.l.RUnlock()
	return i.lowIndex, nil
}

// LastIndex implements the LogStore interface.
//返回最大的日志的index编号
func (i *InmemStore) LastIndex() (uint64, error) {
	i.l.RLock()
	defer i.l.RUnlock()
	return i.highIndex, nil
}

// GetLog implements the LogStore interface.
//根据日志的index编号 返回日志   如果没有返回"log not found"的错误
func (i *InmemStore) GetLog(index uint64, log *Log) error {
	i.l.RLock()
	defer i.l.RUnlock()
	l, ok := i.logs[index]
	if !ok {
		return ErrLogNotFound
	}
	*log = *l
	return nil
}

// StoreLog implements the LogStore interface.
//存储单个log 到自己的map
func (i *InmemStore) StoreLog(log *Log) error {
	return i.StoreLogs([]*Log{log})
}

// StoreLogs implements the LogStore interface.
//存储多个log 到自己的map
func (i *InmemStore) StoreLogs(logs []*Log) error {
	i.l.Lock()
	defer i.l.Unlock()
	for _, l := range logs {
		i.logs[l.Index] = l
		if i.lowIndex == 0 {
			i.lowIndex = l.Index
		}
		if l.Index > i.highIndex {
			i.highIndex = l.Index
		}
	}
	return nil
}

// DeleteRange implements the LogStore interface.
//删除从min到max的日志,包含min和max
func (i *InmemStore) DeleteRange(min, max uint64) error {
	i.l.Lock()
	defer i.l.Unlock()
	for j := min; j <= max; j++ {
		delete(i.logs, j)
	}
	if min <= i.lowIndex {
		i.lowIndex = max + 1
	}
	if max >= i.highIndex {
		i.highIndex = min - 1
	}
	if i.lowIndex > i.highIndex {
		i.lowIndex = 0
		i.highIndex = 0
	}
	return nil
}

// Set implements the StableStore interface.
//保存key value
func (i *InmemStore) Set(key []byte, val []byte) error {
	i.l.Lock()
	defer i.l.Unlock()
	i.kv[string(key)] = val
	return nil
}

// Get implements the StableStore interface.
//通过key得到value
func (i *InmemStore) Get(key []byte) ([]byte, error) {
	i.l.RLock()
	defer i.l.RUnlock()
	val := i.kv[string(key)]
	if val == nil {
		return nil, errors.New("not found")
	}
	return val, nil
}

// SetUint64 implements the StableStore interface.
//给key设置个uint64
func (i *InmemStore) SetUint64(key []byte, val uint64) error {
	i.l.Lock()
	defer i.l.Unlock()
	i.kvInt[string(key)] = val
	return nil
}

// GetUint64 implements the StableStore interface.
//通过key得到uint64
func (i *InmemStore) GetUint64(key []byte) (uint64, error) {
	i.l.RLock()
	defer i.l.RUnlock()
	return i.kvInt[string(key)], nil
}
