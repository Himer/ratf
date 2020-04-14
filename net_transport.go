
package raft

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"github.com/hashicorp/go-hclog"
	"io"
	"net"
	"os"
	"sync"
	"time"

	"github.com/hashicorp/go-msgpack/codec"
)

const (
	rpcAppendEntries uint8 = iota
	rpcRequestVote
	rpcInstallSnapshot
	rpcTimeoutNow

	// DefaultTimeoutScale is the default TimeoutScale in a NetworkTransport.
	DefaultTimeoutScale = 256 * 1024 // 256KB

	// rpcMaxPipeline controls the maximum number of outstanding
	// AppendEntries RPC calls.
	rpcMaxPipeline = 128
)

var (
	// ErrTransportShutdown is returned when operations on a transport are
	// invoked after it's been terminated.
	ErrTransportShutdown = errors.New("transport shutdown")

	// ErrPipelineShutdown is returned when the pipeline is closed.
	ErrPipelineShutdown = errors.New("append pipeline closed")
)

/*

NetworkTransport provides a network based transport that can be
used to communicate with Raft on remote machines. It requires
an underlying stream layer to provide a stream abstraction, which can
be simple TCP, TLS, etc.

This transport is very simple and lightweight. Each RPC request is
framed by sending a byte that indicates the message type, followed
by the MsgPack encoded request.

The response is an error string followed by the response object,
both are encoded using MsgPack.

InstallSnapshot is special, in that after the RPC request we stream
the entire state. That socket is not re-used as the connection state
is not known if there is an error.

*/
type NetworkTransport struct {
	connPool     map[ServerAddress][]*netConn /*缓存连接map  ServerAddress就是string的别名*/
	connPoolLock sync.Mutex  /*缓存池的锁*/

	consumeCh chan RPC  /*node之间交互消息处理单元*/

	heartbeatFn     func(RPC)  /*node之间心跳包单独的处理换上*/
	heartbeatFnLock sync.Mutex /*设置和读取heartbeatFn的锁*/

	logger hclog.Logger   /*日志*/

	maxPool int   /*最多多少个连接缓冲*/

	serverAddressProvider ServerAddressProvider

	shutdown     bool  /*关闭标志位*/
	shutdownCh   chan struct{} /*关闭通知chan*/
	shutdownLock sync.Mutex /*关闭标志位的锁*/

	stream StreamLayer   /*就是本机打开(刚开始还没有打开)的tcp 监听server*/

	// streamCtx is used to cancel existing connection handlers.
	streamCtx     context.Context /*streamCtx 用来取消现有的连接*/
	streamCancel  context.CancelFunc  /*streamCtx 结束函数*/
	streamCtxLock sync.RWMutex /*set和get streamCtx时候使用的锁*/

	timeout      time.Duration  /*网络IO的超时时间*/
	TimeoutScale int /*对应发送镜像的网络io, 网络IO超时时间为SnapshotSize / TimeoutScale*/
}

// NetworkTransportConfig encapsulates configuration for the network transport layer.
//创建NetworkTransport的配置项组成的结构体
type NetworkTransportConfig struct {
	// ServerAddressProvider is used to override the target address when establishing a connection to invoke an RPC
	ServerAddressProvider ServerAddressProvider

	Logger hclog.Logger

	// Dialer
	Stream StreamLayer

	// MaxPool controls how many connections we will pool
	MaxPool int

	// Timeout is used to apply I/O deadlines. For InstallSnapshot, we multiply
	// the timeout by (SnapshotSize / TimeoutScale).
	Timeout time.Duration
}

// ServerAddressProvider is a target address to which we invoke an RPC when establishing a connection
//根据服务node的id 得到node的rpc请求的地址
type ServerAddressProvider interface {
	ServerAddr(id ServerID) (ServerAddress, error)
}

// StreamLayer is used with the NetworkTransport to provide
// the low level stream abstraction.
type StreamLayer interface {
	net.Listener

	// Dial is used to create a new outgoing connection
	Dial(address ServerAddress, timeout time.Duration) (net.Conn, error)
}

type netConn struct {
	target ServerAddress
	conn   net.Conn
	r      *bufio.Reader
	w      *bufio.Writer
	dec    *codec.Decoder
	enc    *codec.Encoder
}

func (n *netConn) Release() error {
	return n.conn.Close()
}

type netPipeline struct {
	conn  *netConn
	trans *NetworkTransport

	doneCh       chan AppendFuture
	inprogressCh chan *appendFuture

	shutdown     bool
	shutdownCh   chan struct{}
	shutdownLock sync.Mutex
}

// NewNetworkTransportWithConfig creates a new network transport with the given config struct
//用配置初始化一个Transport
//所有的NewNetworkTransport 最后调用的都是这个方法
func NewNetworkTransportWithConfig(
	config *NetworkTransportConfig,
) *NetworkTransport {
	//配置打印log
	if config.Logger == nil {
		config.Logger = hclog.New(&hclog.LoggerOptions{
			Name:   "raft-net",//log的前缀
			Output: hclog.DefaultOutput,//打印到标准暑促
			Level:  hclog.DefaultLevel,//Info基本的日志
		})
	}
	trans := &NetworkTransport{
		connPool:              make(map[ServerAddress][]*netConn),
		consumeCh:             make(chan RPC),
		logger:                config.Logger,/*配置的log*/
		maxPool:               config.MaxPool,
		shutdownCh:            make(chan struct{}),
		stream:                config.Stream,
		timeout:               config.Timeout,
		TimeoutScale:          DefaultTimeoutScale,
		serverAddressProvider: config.ServerAddressProvider,
	}

	// Create the connection context and then start our listener.
	trans.setupStreamContext()
	go trans.listen()

	return trans
}

// NewNetworkTransport creates a new network transport with the given dialer
// and listener. The maxPool controls how many connections we will pool. The
// timeout is used to apply I/O deadlines. For InstallSnapshot, we multiply
// the timeout by (SnapshotSize / TimeoutScale).
func NewNetworkTransport(
	stream StreamLayer,
	maxPool int,
	timeout time.Duration,
	logOutput io.Writer,
) *NetworkTransport {
	if logOutput == nil {
		logOutput = os.Stderr
	}
	logger := hclog.New(&hclog.LoggerOptions{
		Name:   "raft-net",
		Output: logOutput,
		Level:  hclog.DefaultLevel,
	})
	config := &NetworkTransportConfig{Stream: stream, MaxPool: maxPool, Timeout: timeout, Logger: logger}
	return NewNetworkTransportWithConfig(config)
}

// NewNetworkTransportWithLogger creates a new network transport with the given logger, dialer
// and listener. The maxPool controls how many connections we will pool. The
// timeout is used to apply I/O deadlines. For InstallSnapshot, we multiply
// the timeout by (SnapshotSize / TimeoutScale).
////待用log的初始化一个Transport
func NewNetworkTransportWithLogger(
	stream StreamLayer,
	maxPool int,
	timeout time.Duration,
	logger hclog.Logger,
) *NetworkTransport {
	config := &NetworkTransportConfig{Stream: stream, MaxPool: maxPool, Timeout: timeout, Logger: logger}
	return NewNetworkTransportWithConfig(config)
}

// setupStreamContext is used to create a new stream context. This should be
// called with the stream lock held.
func (n *NetworkTransport) setupStreamContext() {
	ctx, cancel := context.WithCancel(context.Background())
	n.streamCtx = ctx
	n.streamCancel = cancel
}

// getStreamContext is used retrieve the current stream context.
func (n *NetworkTransport) getStreamContext() context.Context {
	n.streamCtxLock.RLock()
	defer n.streamCtxLock.RUnlock()
	return n.streamCtx
}

// SetHeartbeatHandler is used to setup a heartbeat handler
// as a fast-pass. This is to avoid head-of-line blocking from
// disk IO.
//设置心跳函数用于快传 , 减少硬盘操作导致的队头堵塞
func (n *NetworkTransport) SetHeartbeatHandler(cb func(rpc RPC)) {
	n.heartbeatFnLock.Lock()
	defer n.heartbeatFnLock.Unlock()
	n.heartbeatFn = cb
}

// CloseStreams closes the current streams.
//关闭transport的缓冲池的所有连接 而将context设置成done 并且新建一个context
func (n *NetworkTransport) CloseStreams() {
	n.connPoolLock.Lock()
	defer n.connPoolLock.Unlock()

	// Close all the connections in the connection pool and then remove their
	// entry.
	for k, e := range n.connPool {
		for _, conn := range e {
			conn.Release()
		}

		delete(n.connPool, k)
	}

	// Cancel the existing connections and create a new context. Both these
	// operations must always be done with the lock held otherwise we can create
	// connection handlers that are holding a context that will never be
	// cancelable.
	n.streamCtxLock.Lock()
	n.streamCancel()
	n.setupStreamContext()
	n.streamCtxLock.Unlock()
}

// Close is used to stop the network transport.
func (n *NetworkTransport) Close() error {
	n.shutdownLock.Lock()
	defer n.shutdownLock.Unlock()

	if !n.shutdown {
		close(n.shutdownCh)
		n.stream.Close()
		n.shutdown = true
	}
	return nil
}

// Consumer implements the Transport interface.
//返回rpc处理的channel,raft会调用这个得到消耗chan
func (n *NetworkTransport) Consumer() <-chan RPC {
	return n.consumeCh
}

// LocalAddr implements the Transport interface.
func (n *NetworkTransport) LocalAddr() ServerAddress {
	return ServerAddress(n.stream.Addr().String())
}

// IsShutdown is used to check if the transport is shutdown.
func (n *NetworkTransport) IsShutdown() bool {
	select {
	case <-n.shutdownCh:
		return true
	default:
		return false
	}
}

// getExistingConn is used to grab a pooled connection.
//从连接池中返回一个连接   如果没有连接返回nil
func (n *NetworkTransport) getPooledConn(target ServerAddress) *netConn {
	n.connPoolLock.Lock()
	defer n.connPoolLock.Unlock()

	//如果没有target地址的连接 返回nil
	conns, ok := n.connPool[target]
	if !ok || len(conns) == 0 {
		return nil
	}
	//这个target可能有很多连接 在连接池中可能是一个数组
	//返回最后那个(最新加入的那个)
	var conn *netConn
	num := len(conns)
	conn, conns[num-1] = conns[num-1], nil
	n.connPool[target] = conns[:num-1]
	return conn
}

// getConnFromAddressProvider returns a connection from the server address provider if available, or defaults to a connection using the target server address
//根据server的id或者server的地址(作为默认的参数target)翻译一个连接
func (n *NetworkTransport) getConnFromAddressProvider(id ServerID, target ServerAddress) (*netConn, error) {
	address := n.getProviderAddressOrFallback(id, target)
	return n.getConn(address)
}

//通过server的id得到server的address,如果不存在则返回target作为默认的
func (n *NetworkTransport) getProviderAddressOrFallback(id ServerID, target ServerAddress) ServerAddress {
	if n.serverAddressProvider != nil {
		serverAddressOverride, err := n.serverAddressProvider.ServerAddr(id)
		if err != nil {
			n.logger.Warn("unable to get address for server, using fallback address", "id", id, "fallback", target, "error", err)
		} else {
			return serverAddressOverride
		}
	}
	return target
}

// getConn is used to get a connection from the pool.
//根据server的rpc请求地址得到一个连接
//或者是从连接池里边得到一个连接
//或者是新建一个连接
func (n *NetworkTransport) getConn(target ServerAddress) (*netConn, error) {
	// Check for a pooled conn
	if conn := n.getPooledConn(target); conn != nil {
		return conn, nil
	}

	// Dial a new connection
	//新建一个新的连接
	conn, err := n.stream.Dial(target, n.timeout)
	if err != nil {
		return nil, err
	}

	// Wrap the conn
	//对net.conn进行封装  增加连接的地址和net.conn的读写buf
	netConn := &netConn{
		target: target,
		conn:   conn,
		r:      bufio.NewReader(conn),
		w:      bufio.NewWriter(conn),
	}

	// Setup encoder/decoders
	//在读写buf上创建编码和解码器
	netConn.dec = codec.NewDecoder(netConn.r, &codec.MsgpackHandle{})
	netConn.enc = codec.NewEncoder(netConn.w, &codec.MsgpackHandle{})

	// Done
	return netConn, nil
}

// returnConn returns a connection back to the pool.
//返还一个连接到缓冲池 他们这个连接池没有重试和超时逻辑????
//如果某个连接的缓存连接已经超过maxPool  则将这个连接释放掉
func (n *NetworkTransport) returnConn(conn *netConn) {
	n.connPoolLock.Lock()
	defer n.connPoolLock.Unlock()

	key := conn.target
	conns, _ := n.connPool[key]

	if !n.IsShutdown() && len(conns) < n.maxPool {
		n.connPool[key] = append(conns, conn)
	} else {
		conn.Release()
	}
}

// AppendEntriesPipeline returns an interface that can be used to pipeline
// AppendEntries requests.
//得到一个(服务id或者target地址的)连接管道
func (n *NetworkTransport) AppendEntriesPipeline(id ServerID, target ServerAddress) (AppendPipeline, error) {
	// Get a connection
	conn, err := n.getConnFromAddressProvider(id, target)
	if err != nil {
		return nil, err
	}

	// Create the pipeline
	return newNetPipeline(n, conn), nil
}

// AppendEntries implements the Transport interface.
//发送条目到对应的target机器
//会通过id这个server id得到连接地址,如果找不到则采用target作为连接地址
//发送完成后 id或者target对应的连接会进入到连接池里边等到下个请求使用

func (n *NetworkTransport) AppendEntries(id ServerID, target ServerAddress, args *AppendEntriesRequest, resp *AppendEntriesResponse) error {
	return n.genericRPC(id, target, rpcAppendEntries, args, resp)
}

// RequestVote implements the Transport interface.
//发起头条rpc
func (n *NetworkTransport) RequestVote(id ServerID, target ServerAddress, args *RequestVoteRequest, resp *RequestVoteResponse) error {
	return n.genericRPC(id, target, rpcRequestVote, args, resp)
}

// genericRPC handles a simple request/response RPC.
//通过id或者target得到连接 ,并发送rpc过去
func (n *NetworkTransport) genericRPC(id ServerID, target ServerAddress, rpcType uint8, args interface{}, resp interface{}) error {
	// Get a conn
	//得到id或者target的连接
	conn, err := n.getConnFromAddressProvider(id, target)
	if err != nil {
		return err
	}

	// Set a deadline
	//设置读写超时时间
	if n.timeout > 0 {
		conn.conn.SetDeadline(time.Now().Add(n.timeout))
	}

	// Send the RPC
	//编码rpc数据 并写数据到连接
	if err = sendRPC(conn, rpcType, args); err != nil {
		return err
	}

	// Decode the response
	//读取数据 并进行编码  还会return这个连接时候可以复用放回到连接池中
	canReturn, err := decodeResponse(conn, resp)
	if canReturn {
		n.returnConn(conn)
	}
	return err
}

// InstallSnapshot implements the Transport interface.
//安装镜像
func (n *NetworkTransport) InstallSnapshot(id ServerID, target ServerAddress, args *InstallSnapshotRequest, resp *InstallSnapshotResponse, data io.Reader) error {
	// Get a conn, always close for InstallSnapshot
	//得到镜像所在服务器的连接
	conn, err := n.getConnFromAddressProvider(id, target)
	if err != nil {
		return err
	}
	defer conn.Release()

	// Set a deadline, scaled by request size
	if n.timeout > 0 {
		timeout := n.timeout * time.Duration(args.Size/int64(n.TimeoutScale))
		if timeout < n.timeout {
			timeout = n.timeout
		}
		conn.conn.SetDeadline(time.Now().Add(timeout))
	}

	// Send the RPC
	if err = sendRPC(conn, rpcInstallSnapshot, args); err != nil {
		return err
	}

	// Stream the state
	//复制数据到我们的write
	if _, err = io.Copy(conn.w, data); err != nil {
		return err
	}

	// Flush
	//然后清空缓存区
	if err = conn.w.Flush(); err != nil {
		return err
	}

	// Decode the response, do not return conn
	//然后解析结果(args.Size个字节后 就是接口的结果)
	_, err = decodeResponse(conn, resp)
	return err
}

// EncodePeer implements the Transport interface.
//对server id或者p地址解析序列化
func (n *NetworkTransport) EncodePeer(id ServerID, p ServerAddress) []byte {
	address := n.getProviderAddressOrFallback(id, p)
	return []byte(address)
}

// DecodePeer implements the Transport interface.
//反序列化出地址
func (n *NetworkTransport) DecodePeer(buf []byte) ServerAddress {
	return ServerAddress(buf)
}

// TimeoutNow implements the Transport interface.
//发起选举请求
func (n *NetworkTransport) TimeoutNow(id ServerID, target ServerAddress, args *TimeoutNowRequest, resp *TimeoutNowResponse) error {
	return n.genericRPC(id, target, rpcTimeoutNow, args, resp)
}

// listen is used to handling incoming connections.
//监听
func (n *NetworkTransport) listen() {
	const baseDelay = 5 * time.Millisecond
	const maxDelay = 1 * time.Second

	var loopDelay time.Duration
	for {
		// Accept incoming connections
		conn, err := n.stream.Accept()
		if err != nil {

			//如果accept出错,再次accept的睡眠时间从[5ms, 10ms,20ms,40ms,.......,1s]递增
			//最高1秒
			if loopDelay == 0 {
				loopDelay = baseDelay
			} else {
				loopDelay *= 2
			}

			if loopDelay > maxDelay {
				loopDelay = maxDelay
			}

			//如果Transport如果没有关闭, 打印错误failed to accept connection
			if !n.IsShutdown() {
				n.logger.Error("failed to accept connection", "error", err)
			}

			//如果已经关闭了 则退出accept 循环
			//如果超时delay则进行下次的accept
			select {
			case <-n.shutdownCh:
				return
			case <-time.After(loopDelay):
				continue
			}
		}
		// No error, reset loop delay
		//如果没有出错 则充值delay
		loopDelay = 0

		n.logger.Debug("accepted connection", "local-address", n.LocalAddr(), "remote-address", conn.RemoteAddr().String())

		// Handle the connection in dedicated routine
		//创建协程处理这个连接 并把context传递下处理函数
		//我觉得此处应该设置个waitgroup更好
		go n.handleConn(n.getStreamContext(), conn)
	}
}

// handleConn is used to handle an inbound connection for its lifespan. The
// handler will exit when the passed context is cancelled or the connection is
// closed.
//处理接收到的连接  在协程里边执行  当ctx取消或者连接关闭的时候这个函数退出 协程结束
func (n *NetworkTransport) handleConn(connCtx context.Context, conn net.Conn) {
	defer conn.Close()

	//net.Conn实现了 read write close接口 此处封装成buf read+buf write
	r := bufio.NewReader(conn)
	w := bufio.NewWriter(conn)
	//选择解码和编码器
	dec := codec.NewDecoder(r, &codec.MsgpackHandle{})
	enc := codec.NewEncoder(w, &codec.MsgpackHandle{})

	for {
		select {
		case <-connCtx.Done():
			n.logger.Debug("stream layer is closed")
			return
		default:
		}

		if err := n.handleCommand(r, dec, enc); err != nil {
			if err != io.EOF {
				n.logger.Error("failed to decode incoming command", "error", err)
			}
			return
		}
		//处理完请求 将数据发送出去
		if err := w.Flush(); err != nil {
			n.logger.Error("failed to flush response", "error", err)
			return
		}
	}
}

// handleCommand is used to decode and dispatch a single command.
//handleCommand用来解析命令+分发命令进行执行
func (n *NetworkTransport) handleCommand(r *bufio.Reader, dec *codec.Decoder, enc *codec.Encoder) error {
	// Get the rpc type
	//读取一个byte 用来判断rpc的类型
	rpcType, err := r.ReadByte()
	if err != nil {
		return err
	}

	// Create the RPC object
	//创建一个rpc请求
	respCh := make(chan RPCResponse, 1)
	rpc := RPC{
		RespChan: respCh,//发送response的channel
	}

	// Decode the command
	isHeartbeat := false
	switch rpcType {
	case rpcAppendEntries: //增加日志
		var req AppendEntriesRequest //append日志的请求包
		if err := dec.Decode(&req); err != nil {
			return err
		}
		rpc.Command = &req

		// Check if this is a heartbeat
		//检查这儿rpc请求是否是心跳请求
		if req.Term != 0 && req.Leader != nil &&
			req.PrevLogEntry == 0 && req.PrevLogTerm == 0 &&
			len(req.Entries) == 0 && req.LeaderCommitIndex == 0 {
			isHeartbeat = true
		}

		//投票给本机
	case rpcRequestVote:
		var req RequestVoteRequest
		if err := dec.Decode(&req); err != nil {
			return err
		}
		rpc.Command = &req
	//传递已有镜像过来
	case rpcInstallSnapshot:
		var req InstallSnapshotRequest
		if err := dec.Decode(&req); err != nil {
			return err
		}
		rpc.Command = &req
		//io.LimitReader只都去req.Size的字节,读取超过此字节就返回EOF错误
		rpc.Reader = io.LimitReader(r, req.Size)
     //发起投票的请求
	case rpcTimeoutNow:
		var req TimeoutNowRequest
		if err := dec.Decode(&req); err != nil {
			return err
		}
		rpc.Command = &req

	default:
		return fmt.Errorf("unknown rpc type %d", rpcType)
	}

	// Check for heartbeat fast-path
	//如果是心跳包 则调用心跳函数处理
	if isHeartbeat {
		n.heartbeatFnLock.Lock()
		fn := n.heartbeatFn
		n.heartbeatFnLock.Unlock()
		if fn != nil {
			fn(rpc)
			goto RESP
		}
	}

	// Dispatch the RPC
	//将rpc放到consumeCh chanel, 并自己协程被调度出去,等到consumeCh数据被拿走在继续执行
	//consumeCh 为无缓存的队列
	select {
	case n.consumeCh <- rpc:
	case <-n.shutdownCh:
		return ErrTransportShutdown
	}

	// Wait for response
	//等待consume处理完成后 填充我们的respCh
	//我们的respCh是带有1个缓冲的队列(我们也只有一个request),consume处理完发送到我们的respCh后可以出现其他的请求
RESP:
	select {
	case resp := <-respCh:
		// Send the error first
		//如果处理出错  返回错误
		respErr := ""
		if resp.Error != nil {
			respErr = resp.Error.Error()
		}
		if err := enc.Encode(respErr); err != nil {
			return err
		}

		// Send the response
		//返回数据  enc中已经存储了net.conn enc.Encode直接调用net.conn的write函数
		if err := enc.Encode(resp.Response); err != nil {
			return err
		}
		//关闭 则退出
	case <-n.shutdownCh:
		return ErrTransportShutdown
	}
	return nil
}

// decodeResponse is used to decode an RPC response and reports whether
// the connection can be reused.
//解析返回的数据  并判断连接是否可以重用(如果是可以解析的错误+没有返回错误就可以重用)
func decodeResponse(conn *netConn, resp interface{}) (bool, error) {
	// Decode the error if any
	//如果请求返回错误
	//组包 rpcError+resp
	//如果是执行正确 返回""+resp
	//如果执行错误 返回"eeeeeeeeeeeeee"+resp
	var rpcError string
	if err := conn.dec.Decode(&rpcError); err != nil {
		conn.Release()
		return false, err
	}

	// Decode the response
	//解析返回数据
	if err := conn.dec.Decode(resp); err != nil {
		conn.Release()
		return false, err
	}

	// Format an error if any
	if rpcError != "" {
		return true, fmt.Errorf(rpcError)
	}
	return true, nil
}

// sendRPC is used to encode and send the RPC.
//编码rpc数据 并写数据到连接
func sendRPC(conn *netConn, rpcType uint8, args interface{}) error {
	// Write the request type
	//先写一个byte的rpc类型
	if err := conn.w.WriteByte(rpcType); err != nil {
		conn.Release()
		return err
	}

	// Send the request
	//在编码后发送数据
	if err := conn.enc.Encode(args); err != nil {
		conn.Release()
		return err
	}

	// Flush
	//在清空缓存
	if err := conn.w.Flush(); err != nil {
		conn.Release()
		return err
	}
	return nil
}

// newNetPipeline is used to construct a netPipeline from a given
// transport and connection.
//创建一个网络管道       请求发出后将请求结构体放到inprogressCh  服务端返回后将结果放到doneCh
//但是此处实现貌似有个问题  response和request的对应关系不对
//除非一个请求完成后在进行下一个请求 不会有并发请求在flight
func newNetPipeline(trans *NetworkTransport, conn *netConn) *netPipeline {
	n := &netPipeline{
		conn:         conn,
		trans:        trans,
		doneCh:       make(chan AppendFuture, rpcMaxPipeline),
		inprogressCh: make(chan *appendFuture, rpcMaxPipeline),
		shutdownCh:   make(chan struct{}),
	}
	go n.decodeResponses()
	return n
}

// decodeResponses is a long running routine that decodes the responses
// sent on the connection.
//解析这个conn返回的数据
func (n *netPipeline) decodeResponses() {
	timeout := n.trans.timeout
	for {
		select {
		case future := <-n.inprogressCh:
			if timeout > 0 {
				n.conn.conn.SetReadDeadline(time.Now().Add(timeout))
			}
			//decodeResponses接收到的future的结果 可是不是自己的结果
			_, err := decodeResponse(n.conn, future.resp)
			future.respond(err)
			select {
			case n.doneCh <- future:
			case <-n.shutdownCh:
				return
			}
		case <-n.shutdownCh:
			return
		}
	}
}

// AppendEntries is used to pipeline a new append entries request.
//append一个日志条目
func (n *netPipeline) AppendEntries(args *AppendEntriesRequest, resp *AppendEntriesResponse) (AppendFuture, error) {
	// Create a new future
	future := &appendFuture{
		start: time.Now(),
		args:  args,
		resp:  resp,
	}
	future.init()

	// Add a send timeout
	if timeout := n.trans.timeout; timeout > 0 {
		n.conn.conn.SetWriteDeadline(time.Now().Add(timeout))
	}

	// Send the RPC
	if err := sendRPC(n.conn, rpcAppendEntries, future.args); err != nil {
		return nil, err
	}

	// Hand-off for decoding, this can also cause back-pressure
	// to prevent too many inflight requests
	//将这个future放到inprogressCh中,可以让decodeResponses读取到一个数据执行下去的decodeResponse
	//但是此处有个问题  服务端的请求处理部署顺序来的  我们请求 1,2,3  可能返回2,1,3
	//decodeResponses接收到的future的结果 可是不是自己的结果
	select {
	case n.inprogressCh <- future:
		return future, nil
	case <-n.shutdownCh:
		return nil, ErrPipelineShutdown
	}
}

// Consumer returns a channel that can be used to consume complete futures.
//返回append的结果
func (n *netPipeline) Consumer() <-chan AppendFuture {
	return n.doneCh
}

// Closed is used to shutdown the pipeline connection.
//关闭netPipeline的连接
func (n *netPipeline) Close() error {
	n.shutdownLock.Lock()
	defer n.shutdownLock.Unlock()
	if n.shutdown {
		return nil
	}

	// Release the connection
	n.conn.Release()

	n.shutdown = true
	close(n.shutdownCh)
	return nil
}
