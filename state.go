package raft

import (
	"sync"
	"sync/atomic"
)


//RaftState是所有的Raft节点当前呃状态, 分为 Follower, Candidate, Leader 或者 Shutdown.
type RaftState uint32

const (
	//Follower状态是一个raft节点的初始状态
	Follower RaftState = iota

	//Candidate状态是一个raft节点选主时候备选状态
	Candidate

	//Leader状态是一个raft节点成为主节点的时候的状态
	Leader

	//Shutdown是一个raft节点关闭的状态
	Shutdown
)

func (s RaftState) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	case Shutdown:
		return "Shutdown"
	default:
		return "Unknown"
	}
}

//raftState 用来保存一个raft节点各种状态数据 并提供线程安全的set/get访问接口
type raftState struct {
	//currentTerm+commitIndex+lastApplied 涉及atomic操作  所以必须64位对取
	// The current term, cache of StableStore 当前的任期
	currentTerm uint64

	// Highest committed log entry 最高的日志提交编号
	commitIndex uint64

	// Last applied log to the FSM 上一个状态机使用的log
	lastApplied uint64

	// 保护下面4个字段
	lastLock sync.Mutex  //

	// Cache the latest snapshot index/term

	lastSnapshotIndex uint64//(稳定存储里边)保存快照的日志index
	lastSnapshotTerm  uint64//(稳定存储里边)保存快照的任期编号

	// Cache the latest log from LogStore
	lastLogIndex uint64   //(稳定存储里边)已经落地硬盘记录的日志编号
	lastLogTerm  uint64   //(稳定存储里边)已经落地硬盘记录的日志的

	// Tracks running goroutines 追踪此raft node执行的相关协程
	routinesGroup sync.WaitGroup

	// The current state  当前的raft节点状态    分为 Follower, Candidate, Leader 或者 Shutdown.
	state RaftState
}

func (r *raftState) getState() RaftState {
	stateAddr := (*uint32)(&r.state)
	return RaftState(atomic.LoadUint32(stateAddr))
}

func (r *raftState) setState(s RaftState) {
	stateAddr := (*uint32)(&r.state)
	atomic.StoreUint32(stateAddr, uint32(s))
}

func (r *raftState) getCurrentTerm() uint64 {
	return atomic.LoadUint64(&r.currentTerm)
}

func (r *raftState) setCurrentTerm(term uint64) {
	atomic.StoreUint64(&r.currentTerm, term)
}

func (r *raftState) getLastLog() (index, term uint64) {
	r.lastLock.Lock()
	index = r.lastLogIndex
	term = r.lastLogTerm
	r.lastLock.Unlock()
	return
}

func (r *raftState) setLastLog(index, term uint64) {
	r.lastLock.Lock()
	r.lastLogIndex = index
	r.lastLogTerm = term
	r.lastLock.Unlock()
}

func (r *raftState) getLastSnapshot() (index, term uint64) {
	r.lastLock.Lock()
	index = r.lastSnapshotIndex
	term = r.lastSnapshotTerm
	r.lastLock.Unlock()
	return
}

func (r *raftState) setLastSnapshot(index, term uint64) {
	r.lastLock.Lock()
	r.lastSnapshotIndex = index
	r.lastSnapshotTerm = term
	r.lastLock.Unlock()
}

func (r *raftState) getCommitIndex() uint64 {
	return atomic.LoadUint64(&r.commitIndex)
}

func (r *raftState) setCommitIndex(index uint64) {
	atomic.StoreUint64(&r.commitIndex, index)
}

func (r *raftState) getLastApplied() uint64 {
	return atomic.LoadUint64(&r.lastApplied)
}

func (r *raftState) setLastApplied(index uint64) {
	atomic.StoreUint64(&r.lastApplied, index)
}

// Start a goroutine and properly handle the race between a routine
// starting and incrementing, and exiting and decrementing.
//开始一个协程  并将此node的routinesGroup中的计数加一用于统一管理
func (r *raftState) goFunc(f func()) {
	r.routinesGroup.Add(1)
	go func() {
		defer r.routinesGroup.Done()
		f()
	}()
}

//等待此node的所有协程退出
func (r *raftState) waitShutdown() {
	r.routinesGroup.Wait()
}

// getLastIndex returns the last index in stable storage.
// Either from the last log or from the last snapshot.
//返回稳定存储里边(包括快照和落地日志)里边最后的log id
func (r *raftState) getLastIndex() uint64 {
	r.lastLock.Lock()
	defer r.lastLock.Unlock()

	return max(r.lastLogIndex, r.lastSnapshotIndex)
}

// getLastEntry returns the last index and term in stable storage.
// Either from the last log or from the last snapshot.
//返回稳定存储里边(包括快照和落地日志)里边最后的那个任期id
func (r *raftState) getLastEntry() (uint64, uint64) {
	r.lastLock.Lock()
	defer r.lastLock.Unlock()
	if r.lastLogIndex >= r.lastSnapshotIndex {
		return r.lastLogIndex, r.lastLogTerm
	}
	return r.lastSnapshotIndex, r.lastSnapshotTerm
}
