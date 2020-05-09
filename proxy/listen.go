package proxy

import (
	"net"
	"os"

	"github.com/pkg/errors"
)

//listen.go # listen相关工具方法

// Listen listen.
func Listen(proto string, addr string) (net.Listener, error) {
	switch proto {
	case "tcp":
		return listenTCP(addr)
	case "unix":
		return listenUnix(addr)
	}
	return nil, errors.New("no support proto")
}

func listenTCP(addr string) (net.Listener, error) {
	tcpAddr, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		return nil, errors.Wrap(err, "Proxy Listen tcp ResolveTCPAddr")
	}
	return net.ListenTCP("tcp", tcpAddr)
}

func listenUnix(addr string) (net.Listener, error) {
	err := os.Remove(addr)
	if err != nil && !os.IsNotExist(err) {
		return nil, errors.Wrap(err, "Proxy Listen unix sock but path exist and can't remove")
	}
	unixAddr, err := net.ResolveUnixAddr("unix", addr)
	if err != nil {
		return nil, errors.Wrap(err, "Proxy Listen unix ResolveUnixAddr")
	}
	return net.ListenUnix("unix", unixAddr)
}
