package transport

import (
	"bufio"
	"encoding/binary"
	"io"
	"net"
	"time"

	"github.com/laststate/simulator/internal/lep"
)

// fakeRelay accepts one COBS-framed LEP envelope per connection and answers
// with a proper LSAK stored frame — a minimal stand-in for the real Relay
// TCP source, used by the transport loop test.
func newFakeRelay() (net.Listener, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go serveFakeRelay(conn)
		}
	}()
	return listener, nil
}

func serveFakeRelay(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)
	for {
		_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		block, err := reader.ReadBytes(0)
		if err != nil {
			return
		}
		frame, err := COBSDecode(block)
		if err != nil {
			continue
		}
		if err := lep.Validate(frame); err != nil {
			continue
		}
		eventID, err := lep.DecodeEventID(frame)
		if err != nil {
			continue
		}
		ack := []byte{'L', 'S', 'A', 'K', 1, lep.AckStored, 0, 0, 0, 0, 0, 0}
		binary.LittleEndian.PutUint32(ack[8:], eventID)
		_ = conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
		if _, err := conn.Write(ack); err != nil {
			return
		}
	}
}

var _ = io.Discard // keep io imported for future use
