package modbus

import (
	"net"
	"time"
)

// udpSockWrapper wraps a net.UDPConn (UDP socket) to
// allow transports to consume data off the network socket on
// a byte per byte basis rather than datagram by datagram.
type udpSockWrapper struct {
	leftoverCount int
	rxbuf         []byte
	sock          *net.UDPConn
}

func newUDPSockWrapper(sock net.Conn) *udpSockWrapper {
	return &udpSockWrapper{
		rxbuf: make([]byte, maxTCPFrameLength),
		sock:  sock.(*net.UDPConn),
	}
}

func (usw *udpSockWrapper) Read(buf []byte) (int, error) {
	var (
		copied int
		rlen   int
	)
	if usw.leftoverCount > 0 {
		// if we're holding onto any bytes from a previous datagram,
		// use them to satisfy the read (potentially partially)
		copied = copy(buf, usw.rxbuf[0:usw.leftoverCount])

		if usw.leftoverCount > copied {
			// move any leftover bytes to the beginning of the buffer
			copy(usw.rxbuf, usw.rxbuf[copied:usw.leftoverCount])
		}
		// make a note of how many leftover bytes we have in the buffer
		usw.leftoverCount -= copied
	} else {
		// read up to maxTCPFrameLength bytes from the socket
		rlen, err := usw.sock.Read(usw.rxbuf)
		if err != nil {
			return 0, err
		}
		// copy as many bytes as possible to satisfy the read
		copied = copy(buf, usw.rxbuf[0:rlen])

		if rlen > copied {
			// move any leftover bytes to the beginning of the buffer
			copy(usw.rxbuf, usw.rxbuf[copied:rlen])
		}
		// make a note of how many leftover bytes we have in the buffer
		usw.leftoverCount = rlen - copied
	}
	rlen = copied
	return rlen, nil
}

func (usw *udpSockWrapper) Close() error {
	return usw.sock.Close()
}

func (usw *udpSockWrapper) Write(buf []byte) (int, error) {
	return usw.sock.Write(buf)
}

func (usw *udpSockWrapper) SetDeadline(deadline time.Time) error {
	return usw.sock.SetDeadline(deadline)
}

func (usw *udpSockWrapper) SetReadDeadline(deadline time.Time) error {
	return usw.sock.SetReadDeadline(deadline)
}

func (usw *udpSockWrapper) SetWriteDeadline(deadline time.Time) error {
	return usw.sock.SetWriteDeadline(deadline)
}

func (usw *udpSockWrapper) LocalAddr() net.Addr {
	return usw.sock.LocalAddr()
}

func (usw *udpSockWrapper) RemoteAddr() net.Addr {
	return usw.sock.RemoteAddr()
}
