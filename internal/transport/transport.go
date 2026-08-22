// Package transport sends LEP envelopes to the Relay gateway over every
// transport the platform supports in demos: HTTP (single + binary batch),
// persistent TCP with COBS framing and LSAK acknowledgements, and
// fire-and-forget UDP datagrams.
package transport

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/laststate/simulator/internal/lep"
)

// Sender is one transport able to deliver one envelope.
type Sender interface {
	Name() string
	Send(ctx context.Context, env *lep.Envelope) error
	Close() error
}

// BatchSender can deliver several envelopes in one binary batch body
// (application/vnd.laststate.batch.v1).
type BatchSender interface {
	SendBatch(ctx context.Context, envelopes []*lep.Envelope) error
}

// ─── HTTP ──────────────────────────────────────────────────────────────────

// HTTP posts single envelopes and binary batches to the Relay HTTP source.
type HTTP struct {
	BaseURL string
	Token   string
	Client  *http.Client
}

func NewHTTP(baseURL, token string) *HTTP {
	return &HTTP{
		BaseURL: baseURL,
		Token:   token,
		Client:  &http.Client{Timeout: 10 * time.Second},
	}
}

func (h *HTTP) Name() string { return "http" }

func (h *HTTP) do(ctx context.Context, method, path, contentType, idemKey string, body io.Reader) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, h.BaseURL+path, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", contentType)
	req.Header.Set("Authorization", "Bearer "+h.Token)
	if idemKey != "" {
		req.Header.Set("Idempotency-Key", idemKey)
	}
	return h.Client.Do(req)
}

func (h *HTTP) Send(ctx context.Context, env *lep.Envelope) error {
	resp, err := h.do(ctx, http.MethodPost, "/v1/ingest", "application/octet-stream",
		fmt.Sprintf("evt-%08x", env.EventID), bytes.NewReader(env.Raw))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("http %d", resp.StatusCode)
	}
	return nil
}

func (h *HTTP) SendBatch(ctx context.Context, envelopes []*lep.Envelope) error {
	var buf bytes.Buffer
	var count [4]byte
	binary.LittleEndian.PutUint32(count[:], uint32(len(envelopes)))
	buf.Write(count[:])
	for _, env := range envelopes {
		var length [4]byte
		binary.LittleEndian.PutUint32(length[:], uint32(len(env.Raw)))
		buf.Write(length[:])
		buf.Write(env.Raw)
	}

	resp, err := h.do(ctx, http.MethodPost, "/v1/events:batch", "application/vnd.laststate.batch.v1",
		fmt.Sprintf("batch-%d", time.Now().UnixNano()), &buf)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("http %d", resp.StatusCode)
	}
	return nil
}

func (h *HTTP) Capabilities(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, h.BaseURL+"/v1/ingest/capabilities", nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+h.Token)
	resp, err := h.Client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("http %d", resp.StatusCode)
	}
	return nil
}

func (h *HTTP) Close() error { return nil }

// ─── COBS ──────────────────────────────────────────────────────────────────

// COBSEncode applies Consistent Overhead Byte Stuffing and appends the
// trailing zero delimiter: [encoded frame] 0x00. The Relay COBS source
// decoder splits stream bytes on the same delimiter.
func COBSEncode(frame []byte) []byte {
	out := make([]byte, 0, len(frame)+len(frame)/254+2)
	codeIdx := 0
	code := byte(1)
	out = append(out, 0) // placeholder for the first code
	for _, b := range frame {
		if b == 0 {
			out[codeIdx] = code
			codeIdx = len(out)
			code = 1
			out = append(out, 0)
			continue
		}
		out = append(out, b)
		code++
		if code == 0xFF {
			out[codeIdx] = code
			codeIdx = len(out)
			code = 1
			out = append(out, 0)
		}
	}
	out[codeIdx] = code
	return append(out, 0)
}

// COBSDecode strips COBS overhead from a delimiter-terminated block. Used by
// tests to prove encoder compatibility with the classic algorithm.
func COBSDecode(block []byte) ([]byte, error) {
	if len(block) < 2 {
		return nil, fmt.Errorf("cobs: short block")
	}
	if block[len(block)-1] != 0 {
		return nil, fmt.Errorf("cobs: missing delimiter")
	}
	data := block[:len(block)-1]
	out := make([]byte, 0, len(data))
	for i := 0; i < len(data); {
		code := data[i]
		if code == 0 || i+int(code) > len(data) {
			return nil, fmt.Errorf("cobs: bad code %d at %d", code, i)
		}
		i++
		out = append(out, data[i:i+int(code)-1]...)
		i += int(code) - 1
		if code < 0xFF && i < len(data) {
			out = append(out, 0)
		}
	}
	return out, nil
}

// ─── TCP + LSAK ────────────────────────────────────────────────────────────

// AckHandler is called once per LSAK frame received from the Relay.
type AckHandler func(eventID uint32, status uint8)

// TCP maintains one persistent connection to the Relay TCP source, frames
// envelopes with COBS, and reads LSAK acknowledgements asynchronously.
type TCP struct {
	Addr    string
	Handler AckHandler

	mu      sync.Mutex
	conn    net.Conn
	pending map[uint32]struct{}
	closed  bool
}

func NewTCP(addr string, handler AckHandler) *TCP {
	return &TCP{Addr: addr, Handler: handler, pending: make(map[uint32]struct{})}
}

func (t *TCP) Name() string { return "tcp" }

func (t *TCP) ensureConn(ctx context.Context) (net.Conn, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return nil, fmt.Errorf("tcp: closed")
	}
	if t.conn != nil {
		return t.conn, nil
	}
	d := net.Dialer{Timeout: 5 * time.Second}
	conn, err := d.DialContext(ctx, "tcp", t.Addr)
	if err != nil {
		return nil, err
	}
	t.conn = conn
	go t.readAcks(conn)
	return conn, nil
}

func (t *TCP) readAcks(conn net.Conn) {
	buf := make([]byte, 4096)
	var carry []byte
	for {
		_ = conn.SetReadDeadline(time.Now().Add(2 * time.Minute))
		n, err := conn.Read(buf)
		if n > 0 {
			carry = append(carry, buf[:n]...)
			for len(carry) >= lep.LSAKSize {
				frame := carry[:lep.LSAKSize]
				id, status, err := lep.ParseLSAK(frame)
				if err != nil {
					// Not a clean LSAK boundary: drop one byte and resync.
					carry = carry[1:]
					continue
				}
				carry = carry[lep.LSAKSize:]
				t.mu.Lock()
				delete(t.pending, id)
				t.mu.Unlock()
				if t.Handler != nil {
					t.Handler(id, status)
				}
			}
		}
		if err != nil {
			t.mu.Lock()
			if t.conn == conn {
				t.conn = nil // force reconnect on next send
			}
			t.mu.Unlock()
			return
		}
	}
}

func (t *TCP) Send(ctx context.Context, env *lep.Envelope) error {
	conn, err := t.ensureConn(ctx)
	if err != nil {
		return err
	}
	t.mu.Lock()
	t.pending[env.EventID] = struct{}{}
	t.mu.Unlock()

	_ = conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	if _, err := conn.Write(COBSEncode(env.Raw)); err != nil {
		t.mu.Lock()
		if t.conn == conn {
			t.conn = nil
		}
		t.mu.Unlock()
		return err
	}
	return nil
}

func (t *TCP) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.closed = true
	if t.conn != nil {
		_ = t.conn.Close()
		t.conn = nil
	}
	return nil
}

// ─── UDP ───────────────────────────────────────────────────────────────────

// UDP sends each envelope as one raw datagram, like a low-power radio
// uplink: no connection, no acknowledgement, best effort.
type UDP struct {
	Addr string
	conn *net.UDPConn
}

func NewUDP(addr string) (*UDP, error) {
	raddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return nil, err
	}
	conn, err := net.DialUDP("udp", nil, raddr)
	if err != nil {
		return nil, err
	}
	return &UDP{Addr: addr, conn: conn}, nil
}

func (u *UDP) Name() string { return "udp" }

func (u *UDP) Send(_ context.Context, env *lep.Envelope) error {
	_, err := u.conn.Write(env.Raw)
	return err
}

func (u *UDP) Close() error { return u.conn.Close() }
