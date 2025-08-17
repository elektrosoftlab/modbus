package modbus

import (
	"crypto/x509"
	"fmt"
	"net"
	"os"
	"time"
)

// LoadCertPool loads a certificate store from a file into a CertPool object.
func LoadCertPool(filePath string) (*x509.CertPool, error) {
	// read the entire cert store, which may contain zero, one
	// or more certificates
	buf, err := os.ReadFile(filePath)
	if err != nil {
		return nil, err
	}

	if len(buf) == 0 {
		return nil, fmt.Errorf("%v: empty file", filePath)
	}

	// add these certs to the pool
	cp := x509.NewCertPool()
	cp.AppendCertsFromPEM(buf)

	// let the caller know if no usable certificate was found
	if len(cp.Subjects()) == 0 {
		return nil, fmt.Errorf("%v: no certificate found", filePath)
	}
	return cp, nil
}

// tlsSockWrapper wraps a TLS socket to work around odd error handling in
// TLSConn on internal connection state corruption.
// tlsSockWrapper implements the net.Conn interface to allow its
// use by the modbus TCP transport.
type tlsSockWrapper struct {
	sock net.Conn
}

func newTLSSockWrapper(sock net.Conn) (tsw *tlsSockWrapper) {
	return &tlsSockWrapper{
		sock: sock,
	}
}

func (tsw *tlsSockWrapper) Read(buf []byte) (int, error) {
	return tsw.sock.Read(buf)
}

func (tsw *tlsSockWrapper) Write(buf []byte) (wlen int, err error) {
	wlen, err = tsw.sock.Write(buf)

	// since write timeouts corrupt the internal state of TLS sockets,
	// any subsequent read/write operation will fail and return the same write
	// timeout error (see https://pkg.go.dev/crypto/tls#Conn.SetWriteDeadline).
	// this isn't all that helpful to clients, which may be tricked into
	// retrying forever, treating timeout errors as transient.
	// to avoid this, close the TLS socket after the first write timeout.
	// this ensures that clients 1) get a timeout error on the first write timeout
	// and 2) get an ErrNetClosing "use of closed network connection" on subsequent
	// operations.
	if err != nil && os.IsTimeout(err) {
		tsw.sock.Close()
	}
	return
}

func (tsw *tlsSockWrapper) Close() error {
	return tsw.sock.Close()
}

func (tsw *tlsSockWrapper) SetDeadline(deadline time.Time) error {
	return tsw.sock.SetDeadline(deadline)
}

func (tsw *tlsSockWrapper) SetReadDeadline(deadline time.Time) error {
	return tsw.sock.SetReadDeadline(deadline)
}

func (tsw *tlsSockWrapper) SetWriteDeadline(deadline time.Time) error {
	return tsw.sock.SetWriteDeadline(deadline)
}

func (tsw *tlsSockWrapper) LocalAddr() net.Addr {
	return tsw.sock.LocalAddr()
}

func (tsw *tlsSockWrapper) RemoteAddr() net.Addr {
	return tsw.sock.RemoteAddr()
}
