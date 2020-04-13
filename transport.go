package raft

import (
	"io"
	"time"
)

// RPCResponse captures both a response and a potential error.
type RPCResponse struct {
	Response interface{}
	Error    error
}

// RPC has a command, and provides a response mechanism.
type RPC struct {
	Command  interface{}
	Reader   io.Reader // Set only for InstallSnapshot
	RespChan chan<- RPCResponse
}

// Respond is used to respond with a response, error or both
func (r *RPC) Respond(resp interface{}, err error) {
	r.RespChan <- RPCResponse{resp, err}
}

// Transport provides an interface for network transports
// to allow Raft to communicate with other nodes.
/*
Transport是raft集群内部节点之间的通信渠道，节点之间需要通过这个通道来进行日志同步、leader选举等。
hashicorp/raft内部提供了两种方式来实现，一种是通过TCPTransport，基于tcp，可以跨机器跨网络通信；
另一种是InmemTransport，不走网络，在内存里面通过channel来通信
*/
type Transport interface {
	// Consumer returns a channel that can be used to
	// consume and respond to RPC requests.
	//consumer提供一个消费rpc请求的channel
	Consumer() <-chan RPC

	// LocalAddr is used to return our local address to distinguish from our peers.
	//本node提供服务的地址
	LocalAddr() ServerAddress

	// AppendEntriesPipeline returns an interface that can be used to pipeline
	// AppendEntries requests.
	AppendEntriesPipeline(id ServerID, target ServerAddress) (AppendPipeline, error)

	// AppendEntries sends the appropriate RPC to the target node.
	//发送条目到对应的target机器
	AppendEntries(id ServerID, target ServerAddress, args *AppendEntriesRequest, resp *AppendEntriesResponse) error

	// RequestVote sends the appropriate RPC to the target node.
	//RequestVote 选举target
	RequestVote(id ServerID, target ServerAddress, args *RequestVoteRequest, resp *RequestVoteResponse) error

	// InstallSnapshot is used to push a snapshot down to a follower. The data is read from
	// the ReadCloser and streamed to the client.
	//想对应的节点发送一个镜像
	InstallSnapshot(id ServerID, target ServerAddress, args *InstallSnapshotRequest, resp *InstallSnapshotResponse, data io.Reader) error

	// EncodePeer is used to serialize a peer's address.
	//将一个节点地址进行序列化
	EncodePeer(id ServerID, addr ServerAddress) []byte

	// DecodePeer is used to deserialize a peer's address.
	//对网络地址进行反序列化
	DecodePeer([]byte) ServerAddress

	// SetHeartbeatHandler is used to setup a heartbeat handler
	// as a fast-pass. This is to avoid head-of-line blocking from
	// disk IO. If a Transport does not support this, it can simply
	// ignore the call, and push the heartbeat onto the Consumer channel.
	//心跳的请求快速处理(上了fast pass,快速通过 高速公路)
	SetHeartbeatHandler(cb func(rpc RPC))

	// TimeoutNow is used to start a leadership transfer to the target node.
	//对target发起leader选举
	TimeoutNow(id ServerID, target ServerAddress, args *TimeoutNowRequest, resp *TimeoutNowResponse) error
}

// WithClose is an interface that a transport may provide which
// allows a transport to be shut down cleanly when a Raft instance
// shuts down.
//
// It is defined separately from Transport as unfortunately it wasn't in the
// original interface specification.
//transport需要实现的关闭接口
type WithClose interface {
	// Close permanently closes a transport, stopping
	// any associated goroutines and freeing other resources.
	Close() error
}

// LoopbackTransport is an interface that provides a loopback transport suitable for testing
// e.g. InmemTransport. It's there so we don't have to rewrite tests.
type LoopbackTransport interface {
	Transport // Embedded transport reference
	WithPeers // Embedded peer management
	WithClose // with a close routine
}

// WithPeers is an interface that a transport may provide which allows for connection and
// disconnection. Unless the transport is a loopback transport, the transport specified to
// "Connect" is likely to be nil.
type WithPeers interface {
	Connect(peer ServerAddress, t Transport) // Connect a peer
	Disconnect(peer ServerAddress)           // Disconnect a given peer
	DisconnectAll()                          // Disconnect all peers, possibly to reconnect them later
}

// AppendPipeline is used for pipelining AppendEntries requests. It is used
// to increase the replication throughput by masking latency and better
// utilizing bandwidth.
type AppendPipeline interface {
	// AppendEntries is used to add another request to the pipeline.
	// The send may block which is an effective form of back-pressure.
	//发出去append 条目请求
	AppendEntries(args *AppendEntriesRequest, resp *AppendEntriesResponse) (AppendFuture, error)

	// Consumer returns a channel that can be used to consume
	// response futures when they are ready.
	//接收到到append请求
	Consumer() <-chan AppendFuture

	// Close closes the pipeline and cancels all inflight RPCs
	//关闭管道
	Close() error
}

// AppendFuture is used to return information about a pipelined AppendEntries request.
type AppendFuture interface {
	Future

	// Start returns the time that the append request was started.
	// It is always OK to call this method.
	Start() time.Time

	// Request holds the parameters of the AppendEntries call.
	// It is always OK to call this method.
	Request() *AppendEntriesRequest

	// Response holds the results of the AppendEntries call.
	// This method must only be called after the Error
	// method returns, and will only be valid on success.
	Response() *AppendEntriesResponse
}
