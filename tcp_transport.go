
package raft

import (
	"errors"
	"github.com/hashicorp/go-hclog"
	"io"
	"net"
	"time"
)


var (
	errNotAdvertisable = errors.New("local bind address is not advertisable")
	errNotTCP          = errors.New("local address is not a TCP address")
)

// TCPStreamLayer implements StreamLayer interface for plain TCP.
type TCPStreamLayer struct {
	advertise net.Addr   /*广播的地址*/
	listener  *net.TCPListener  /*监控的地址*/
}

// NewTCPTransport returns a NetworkTransport that is built on top of
// a TCP streaming transport layer.
func NewTCPTransport(
	bindAddr string,
	advertise net.Addr,
	maxPool int,
	timeout time.Duration,
	logOutput io.Writer,
) (*NetworkTransport, error) {
	return newTCPTransport(bindAddr, advertise, func(stream StreamLayer) *NetworkTransport {
		return NewNetworkTransport(stream, maxPool, timeout, logOutput)
	})
}

// NewTCPTransportWithLogger returns a NetworkTransport that is built on top of
// a TCP streaming transport layer, with log output going to the supplied Logger
func NewTCPTransportWithLogger(
	bindAddr string,
	advertise net.Addr,
	maxPool int,
	timeout time.Duration,
	logger hclog.Logger,
) (*NetworkTransport, error) {
	return newTCPTransport(bindAddr, advertise, func(stream StreamLayer) *NetworkTransport {
		return NewNetworkTransportWithLogger(stream, maxPool, timeout, logger)
	})
}

// NewTCPTransportWithConfig returns a NetworkTransport that is built on top of
// a TCP streaming transport layer, using the given config struct.
func NewTCPTransportWithConfig(
	bindAddr string,
	advertise net.Addr,
	config *NetworkTransportConfig,
) (*NetworkTransport, error) {
	return newTCPTransport(bindAddr, advertise, func(stream StreamLayer) *NetworkTransport {
		config.Stream = stream
		return NewNetworkTransportWithConfig(config)
	})
}

func newTCPTransport(bindAddr string,
	advertise net.Addr, /* net.Addr 是个接口 又协议名(tcp,udp)和地址(127.0.0.1:80)组成, *net.TCPAddr和*net.UDPAddr实现了这个接口 */
	transportCreator func(stream StreamLayer) *NetworkTransport) (*NetworkTransport, error) {
	// Try to bind
	list, err := net.Listen("tcp", bindAddr)
	if err != nil {
		return nil, err
	}

	// Create stream
	// stream 感觉就像是个打开了(还没有)个tcp socket
	stream := &TCPStreamLayer{
		advertise: advertise,
		listener:  list.(*net.TCPListener),
	}
	// Verify that we have a usable advertise address
	//net.TCPAddr 可以认为是有ip(127.0.0.1)和端口(80)组成
	//*net.TCPAddr
	addr, ok := stream.Addr().(*net.TCPAddr)
	if !ok {
		list.Close()
		return nil, errNotTCP
	}
	//IsUnspecified判断ip是否是 0.0.0.0 这样的地址
	//此处 如果我们监听的地址是 0.0.0.0:8080  而没有提供广播地址则会在此出错
	if addr.IP.IsUnspecified() {
		list.Close()
		return nil, errNotAdvertisable
	}

	// Create the network transport
	trans := transportCreator(stream)
	return trans, nil
}

// Dial implements the StreamLayer interface.
//发起tcp连接
func (t *TCPStreamLayer) Dial(address ServerAddress, timeout time.Duration) (net.Conn, error) {
	return net.DialTimeout("tcp", string(address), timeout)
}

// Accept implements the net.Listener interface.
//接收tcp请求
func (t *TCPStreamLayer) Accept() (c net.Conn, err error) {
	return t.listener.Accept()
}

// Close implements the net.Listener interface.
//关闭tcp请求
func (t *TCPStreamLayer) Close() (err error) {
	return t.listener.Close()
}

// Addr implements the net.Listener interface.
func (t *TCPStreamLayer) Addr() net.Addr {
	// Use an advertise addr if provided
	//如果提供了 建议广播的地址  则使用这个
	if t.advertise != nil {
		return t.advertise
	}
	//如果没有提供广播地址  则使用bind tcp的服务地址
	return t.listener.Addr()
}
