package raft

// LogType describes various types of log entries.
//log的种类
type LogType uint8

const (
	// LogCommand is applied to a user FSM.
	//LogCommand 为一条条正常的日志  会发送到状态机
	LogCommand LogType = iota

	// LogNoop is used to assert leadership.
	//LogNoop 用来表明主从关系
	LogNoop

	// LogAddPeerDeprecated is used to add a new peer. This should only be used with
	// older protocol versions designed to be compatible with unversioned
	// Raft servers. See comments in config.go for details.
	LogAddPeerDeprecated

	// LogRemovePeerDeprecated is used to remove an existing peer. This should only be
	// used with older protocol versions designed to be compatible with
	// unversioned Raft servers. See comments in config.go for details.
	LogRemovePeerDeprecated

	// LogBarrier is used to ensure all preceding operations have been
	// applied to the FSM. It is similar to LogNoop, but instead of returning
	// once committed, it only returns once the FSM manager acks it. Otherwise
	// it is possible there are operations committed but not yet applied to
	// the FSM.
	//确认所有的log已经被状态机处理完成
	LogBarrier

	// LogConfiguration establishes a membership change configuration. It is
	// created when a server is added, removed, promoted, etc. Only used
	// when protocol version 1 or greater is in use.
	//LogConfiguration通知系统有一个node从集群增加或者删除
	LogConfiguration
)

// Log entries are replicated to all members of the Raft cluster
// and form the heart of the replicated state machine.
// raft里边传递的日志结构  整个系统就是一条条日志累计上来的
//master会将一条条log发送到所有的raft集群节点
//每个node从状态机出得到一条条日志进行处理
type Log struct {
	// Index holds the index of the log entry.
	//这个log的id
	Index uint64

	// Term holds the election term of the log entry.
	//这条日志是那个任期出来的
	Term uint64

	// Type holds the type of the log entry.
	//日志的种类
	Type LogType

	// Data holds the log entry's type-specific data.
	//日志的数据
	Data []byte

	// Extensions holds an opaque byte slice of information for middleware. It
	// is up to the client of the library to properly modify this as it adds
	// layers and remove those layers when appropriate. This value is a part of
	// the log, so very large values could cause timing issues.
	//
	// N.B. It is _up to the client_ to handle upgrade paths. For instance if
	// using this with go-raftchunking, the client should ensure that all Raft
	// peers are using a version that can handle that extension before ever
	// actually triggering chunking behavior. It is sometimes sufficient to
	// ensure that non-leaders are upgraded first, then the current leader is
	// upgraded, but a leader changeover during this process could lead to
	// trouble, so gating extension behavior via some flag in the client
	// program is also a good idea.
	//保存一些中间件的隐藏信息, 客户端可以自己决定怎么处理这些数据
	Extensions []byte
}

// LogStore is used to provide an interface for storing
// and retrieving logs in a durable fashion.
type LogStore interface {
	// FirstIndex returns the first index written. 0 for no entries.
	//返回日志的第一条的编号
	FirstIndex() (uint64, error)

	// LastIndex returns the last index written. 0 for no entries.
	//返回日志的最后一条的编号
	LastIndex() (uint64, error)

	// GetLog gets a log entry at a given index.
	//得到特别编号的日志
	GetLog(index uint64, log *Log) error

	// StoreLog stores a log entry.
	//保存日志
	StoreLog(log *Log) error

	// StoreLogs stores multiple log entries.
	//保存一坨日志
	StoreLogs(logs []*Log) error

	// DeleteRange deletes a range of log entries. The range is inclusive.
	//删除从min到max的日志(包含min和max)
	DeleteRange(min, max uint64) error
}
