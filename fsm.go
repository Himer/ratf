package raft

import (
	"fmt"
	"io"
	"time"

	"github.com/armon/go-metrics"
)

// FSM provides an interface that can be implemented by
// clients to make use of the replicated log.
//leader第一个是Apply，当raft内部commit了一个log entry后，会记录在上面说过的logStore里面，
// 被commit的log entry需要被执行，就stcache来说，执行log entry就是把数据写入缓存，
// 即执行set操作。我们改造doSet方法，
// 这里不再直接写缓存，而是调用raft的Apply方式，为这次set操作生成一个log entry，
// 这里面会根据raft的内部协议，在各个节点之间进行通信协作，确保最后这条log 会在整个集群的节点里面提交或者失败。

//对follower节点来说，leader会通知它来commit log entry，
// 被commit的log entry需要调用应用层提供的Apply方法来执行日志，这里就是从logEntry拿到具体的数据，然后写入缓存里面即可。
type FSM interface {
	// Apply log is invoked once a log entry is committed.
	// It returns a value which will be made available in the
	// ApplyFuture returned by Raft.Apply method if that
	// method was called on the same Raft node as the FSM.
	//master的Apply在各个节点之间进行通信协作，确保最后这条log 会在整个集群的节点里面提交或者失败。
	//follow的Apply保存到本机的存储里边
	Apply(*Log) interface{}

	// Snapshot is used to support log compaction. This call should
	// return an FSMSnapshot which can be used to save a point-in-time
	// snapshot of the FSM. Apply and Snapshot are not called in multiple
	// threads, but Apply will be called concurrently with Persist. This means
	// the FSM should be implemented in a fashion that allows for concurrent
	// updates while a snapshot is happening.
	//返回FSMSnapshot用来保存快照, Apply和Snapshot不能并行执行,但是Apply会和Persist会并发执行
	//所有FSM必须处理更新和保存快照同时进行的问题
	Snapshot() (FSMSnapshot, error)

	// Restore is used to restore an FSM from a snapshot. It is not called
	// concurrently with any other command. The FSM must discard all previous
	// state.
	//从镜像读取数据到FSM
	Restore(io.ReadCloser) error
}

// BatchingFSM extends the FSM interface to add an ApplyBatch function. This can
// optionally be implemented by clients to enable multiple logs to be applied to
// the FSM in batches. Up to MaxAppendEntries could be sent in a batch.
//批量Apply日志到FSM
type BatchingFSM interface {
	// ApplyBatch is invoked once a batch of log entries has been committed and
	// are ready to be applied to the FSM. ApplyBatch will take in an array of
	// log entries. These log entries will be in the order they were committed,
	// will not have gaps, and could be of a few log types. Clients should check
	// the log type prior to attempting to decode the data attached. Presently
	// the LogCommand and LogConfiguration types will be sent.
	//
	// The returned slice must be the same length as the input and each response
	// should correlate to the log at the same index of the input. The returned
	// values will be made available in the ApplyFuture returned by Raft.Apply
	// method if that method was called on the same Raft node as the FSM.
	ApplyBatch([]*Log) []interface{}

	FSM
}

// FSMSnapshot is returned by an FSM in response to a Snapshot
// It must be safe to invoke FSMSnapshot methods with concurrent
// calls to Apply.
//FSM需要提供的另外两个方法是Snapshot()和Restore()，分
// 别用于生成一个快照结构和根据快照恢复数据。
// 首先我们需要定义快照，hashicorp/raft内部定义了快照的interface，
// 需要实现两个func，Persist用来生成快照数据，一般只需要实现它即可；
// Release则是快照处理完成后的回调，不需要的话可以实现为空函数。
type FSMSnapshot interface {
	// Persist should dump all necessary state to the WriteCloser 'sink',
	// and call sink.Close() when finished or call sink.Cancel() on error.
	//FSM所有的必须数据保存到镜像,如果成功了要调用sink.Close() 失败了调用sink.Cancel
	Persist(sink SnapshotSink) error

	// Release is invoked when we are finished with the snapshot.
	//当我们镜像保存完成后的调用
	Release()
}

// runFSM is a long running goroutine responsible for applying logs
// to the FSM. This is done async of other logs since we don't want
// the FSM to block our internal operations.
//异步将日志保存到FSM, 异步的原因是不要阻塞其他的内部操作
func (r *Raft) runFSM() {
	var lastIndex, lastTerm uint64

	batchingFSM, batchingEnabled := r.fsm.(BatchingFSM)
	configStore, configStoreEnabled := r.fsm.(ConfigurationStore)

	//commitTuple是log和logFuture的组合体(logFuture有个channel来判断任务完成 并且有个response来保存返回的结果)
	// 开源皆比较流行  request+channel的组合   channel可以用来判断这个request处理完成
	commitSingle := func(req *commitTuple) {
		// Apply the log if a command or config change
		var resp interface{}
		// Make sure we send a response
		defer func() {
			// Invoke the future if given
			if req.future != nil {
				req.future.response = resp /*设置结果*/
				req.future.respond(nil)/*关闭channel*/
			}
		}()

		switch req.log.Type {
		//如果是日志命令 ,fsm执行这条日志
		case LogCommand:
			start := time.Now()
			resp = r.fsm.Apply(req.log)
			metrics.MeasureSince([]string{"raft", "fsm", "apply"}, start)
        //如果是配置命令,而且支持配置保存,则保存到configStore
		case LogConfiguration:
			if !configStoreEnabled {
				// Return early to avoid incrementing the index and term for
				// an unimplemented operation.
				return
			}

			start := time.Now()
			configStore.StoreConfiguration(req.log.Index, DecodeConfiguration(req.log.Data))
			metrics.MeasureSince([]string{"raft", "fsm", "store_config"}, start)
		}

		// Update the indexes
		//更新最新的日志id和任期
		lastIndex = req.log.Index
		lastTerm = req.log.Term
	}

	//批量提交
	commitBatch := func(reqs []*commitTuple) {
		//如果不能批量提交 则用单个提交来模拟批量提交
		if !batchingEnabled {
			for _, ct := range reqs {
				commitSingle(ct)
			}
			return
		}

		// Only send LogCommand and LogConfiguration log types. LogBarrier types
		// will not be sent to the FSM.
		//用于过滤 LogCommand, LogConfiguration日志类型
		shouldSend := func(l *Log) bool {
			switch l.Type {
			case LogCommand, LogConfiguration:
				return true
			}
			return false
		}

		//得到需要提交的日志类型(LogCommand, LogConfiguration)
		var lastBatchIndex, lastBatchTerm uint64
		sendLogs := make([]*Log, 0, len(reqs))
		for _, req := range reqs {
			if shouldSend(req.log) {
				sendLogs = append(sendLogs, req.log)
			}
			lastBatchIndex = req.log.Index
			lastBatchTerm = req.log.Term
		}

		var responses []interface{}
		if len(sendLogs) > 0 {
			start := time.Now()
			//批量提交任务
			responses = batchingFSM.ApplyBatch(sendLogs)
			metrics.MeasureSince([]string{"raft", "fsm", "applyBatch"}, start)
			metrics.AddSample([]string{"raft", "fsm", "applyBatchNum"}, float32(len(reqs)))

			// Ensure we get the expected responses
			if len(sendLogs) != len(responses) {
				panic("invalid number of responses")
			}
		}

		// Update the indexes
		//更新日志的id 和任期id
		lastIndex = lastBatchIndex
		lastTerm = lastBatchTerm

		//等待所有的request都已经写执行完成
		var i int
		for _, req := range reqs {
			var resp interface{}
			// If the log was sent to the FSM, retrieve the response.
			//responses 的顺序和sendLogs是一样的
			if shouldSend(req.log) {
				resp = responses[i]
				i++
			}

			//将req的channel关闭
			if req.future != nil {
				req.future.response = resp
				req.future.respond(nil)
			}
		}
	}
	//读取镜像
	restore := func(req *restoreFuture) {
		// Open the snapshot
		meta, source, err := r.snapshots.Open(req.ID)
		if err != nil {
			req.respond(fmt.Errorf("failed to open snapshot %v: %v", req.ID, err))
			return
		}

		// Attempt to restore
		//读取镜像到fsm中
		start := time.Now()
		if err := r.fsm.Restore(source); err != nil {
			req.respond(fmt.Errorf("failed to restore snapshot %v: %v", req.ID, err))
			source.Close()
			return
		}
		source.Close()
		metrics.MeasureSince([]string{"raft", "fsm", "restore"}, start)

		// Update the last index and term
		//用镜像的日志index和任期更新
		lastIndex = meta.Index
		lastTerm = meta.Term
		req.respond(nil)
	}

	//返回用于保存镜像的FSMSnapshot结构
	snapshot := func(req *reqSnapshotFuture) {
		// Is there something to snapshot?
		if lastIndex == 0 {
			req.respond(ErrNothingNewToSnapshot)
			return
		}

		// Start a snapshot
		start := time.Now()
		snap, err := r.fsm.Snapshot()
		metrics.MeasureSince([]string{"raft", "fsm", "snapshot"}, start)

		// Respond to the request
		req.index = lastIndex
		req.term = lastTerm
		req.snapshot = snap
		req.respond(err)
	}

	for {
		select {
		//从fsmMutateCh取得命令 进行执行
		case ptr := <-r.fsmMutateCh:
			switch req := ptr.(type) {
			case []*commitTuple:
				commitBatch(req)

			case *restoreFuture:
				restore(req)

			default:
				panic(fmt.Errorf("bad type passed to fsmMutateCh: %#v", ptr))
			}

		case req := <-r.fsmSnapshotCh:
			snapshot(req)

		case <-r.shutdownCh:
			return
		}
	}
}
