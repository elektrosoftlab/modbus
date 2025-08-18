package modbus

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"time"
)

const (
	maxTCPFrameLength int = 260
	mbapHeaderLength  int = 7
)

type tcpTransport struct {
	logger    *logger
	socket    net.Conn
	timeout   time.Duration
	lastTxnId uint16
}

// Returns a new TCP transport.
func newTCPTransport(socket net.Conn, timeout time.Duration, customLogger *log.Logger) *tcpTransport {
	return &tcpTransport{
		socket:  socket,
		timeout: timeout,
		logger:  newLogger(fmt.Sprintf("tcp-transport(%s)", socket.RemoteAddr()), customLogger),
	}
}

// Closes the underlying tcp socket.
func (tt *tcpTransport) Close() error {
	return tt.socket.Close()
}

// Runs a request across the socket and returns a response.
func (tt *tcpTransport) ExecuteRequest(req *pdu) (*pdu, error) {
	// set an i/o deadline on the socket (read and write)
	err := tt.socket.SetDeadline(time.Now().Add(tt.timeout))
	if err != nil {
		return nil, err
	}
	// increase the transaction ID counter
	tt.lastTxnId++
	_, err = tt.socket.Write(tt.assembleMBAPFrame(tt.lastTxnId, req))
	if err != nil {
		return nil, err
	}
	return tt.readResponse()
}

// Reads a request from the socket.
func (tt *tcpTransport) ReadRequest() (*pdu, error) {
	var txnId uint16

	// set an i/o deadline on the socket (read and write)
	err := tt.socket.SetDeadline(time.Now().Add(tt.timeout))
	if err != nil {
		return nil, err
	}

	req, txnId, err := tt.readMBAPFrame()
	if err != nil {
		return nil, err
	}

	// store the incoming transaction id
	tt.lastTxnId = txnId
	return req, nil
}

// Writes a response to the socket.
func (tt *tcpTransport) WriteResponse(res *pdu) error {
	_, err := tt.socket.Write(tt.assembleMBAPFrame(tt.lastTxnId, res))
	return err
}

// Reads as many MBAP+modbus frames as necessary until either the response
// matching tt.lastTxnId is received or an error occurs.
func (tt *tcpTransport) readResponse() (*pdu, error) {
	var (
		res   *pdu
		txnId uint16
		err   error
	)

	for {
		// grab a frame
		res, txnId, err = tt.readMBAPFrame()
		// ignore unknown protocol identifiers
		if errors.Is(err, ErrUnknownProtocolId) {
			continue
		}
		// abort on any other erorr
		if err != nil {
			return nil, err
		}
		// ignore unknown transaction identifiers
		if tt.lastTxnId != txnId {
			tt.logger.Warningf("received unexpected transaction id "+
				"(expected 0x%04x, received 0x%04x)",
				tt.lastTxnId, txnId)
			continue
		}
		break
	}
	return res, nil
}

// Reads an entire frame (MBAP header + modbus PDU) from the socket.
func (tt *tcpTransport) readMBAPFrame() (*pdu, uint16, error) {
	var (
		bytesNeeded int
		protocolId  uint16
		unitId      uint8
	)
	// read the MBAP header
	rxbuf := make([]byte, mbapHeaderLength)
	_, err := io.ReadFull(tt.socket, rxbuf)
	if err != nil {
		return nil, 0, err
	}

	// decode the transaction identifier
	txnId := bytesToUint16(BIG_ENDIAN, rxbuf[0:2])
	// decode the protocol identifier
	protocolId = bytesToUint16(BIG_ENDIAN, rxbuf[2:4])
	// store the source unit id
	unitId = rxbuf[6]

	// determine how many more bytes we need to read
	bytesNeeded = int(bytesToUint16(BIG_ENDIAN, rxbuf[4:6]))

	// the byte count includes the unit ID field, which we already have
	bytesNeeded--

	// never read more than the max allowed frame length
	if bytesNeeded+mbapHeaderLength > maxTCPFrameLength {
		return nil, 0, ErrProtocol
	}

	// an MBAP length of 0 is illegal
	if bytesNeeded <= 0 {
		return nil, 0, ErrProtocol
	}

	// read the PDU
	rxbuf = make([]byte, bytesNeeded)
	_, err = io.ReadFull(tt.socket, rxbuf)
	if err != nil {
		return nil, 0, err
	}

	// validate the protocol identifier
	if protocolId != 0x0000 {
		tt.logger.Warningf("received unexpected protocol id 0x%04x", protocolId)
		return nil, 0, ErrUnknownProtocolId
	}

	// store unit id, function code and payload in the PDU object
	return &pdu{
		unitId:       unitId,
		functionCode: rxbuf[0],
		payload:      rxbuf[1:],
	}, txnId, nil
}

// Turns a PDU into an MBAP frame (MBAP header + PDU) and returns it as bytes.
func (tt *tcpTransport) assembleMBAPFrame(txnId uint16, p *pdu) []byte {
	// transaction identifier
	payload := uint16ToBytes(BIG_ENDIAN, txnId)
	// protocol identifier (always 0x0000)
	payload = append(payload, 0x00, 0x00)
	// length (covers unit identifier + function code + payload fields)
	payload = append(payload, uint16ToBytes(BIG_ENDIAN, uint16(2+len(p.payload)))...)
	// unit identifier
	payload = append(payload, p.unitId)
	// function code
	payload = append(payload, p.functionCode)
	// payload
	return append(payload, p.payload...)
}
