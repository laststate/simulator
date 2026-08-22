package transport

import (
	"bytes"
	"math/rand"
	"testing"
	"time"

	"github.com/laststate/simulator/internal/lep"
)

func TestCOBSRoundTrip(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	for i := 0; i < 500; i++ {
		orig := make([]byte, rng.Intn(600))
		for j := range orig {
			// Heavy zero content: worst case for COBS.
			if rng.Intn(3) == 0 {
				orig[j] = 0
			} else {
				orig[j] = byte(rng.Intn(256))
			}
		}
		enc := COBSEncode(orig)
		for _, b := range enc[:len(enc)-1] {
			if b == 0 {
				t.Fatal("zero byte inside encoded frame")
			}
		}
		dec, err := COBSDecode(enc)
		if err != nil {
			t.Fatalf("COBSDecode: %v", err)
		}
		if !bytes.Equal(dec, orig) {
			t.Fatalf("round trip mismatch at iteration %d (len %d)", i, len(orig))
		}
	}
}

func TestCOBSEmptyAndAllZeros(t *testing.T) {
	enc := COBSEncode(nil)
	if dec, err := COBSDecode(enc); err != nil || len(dec) != 0 {
		t.Fatalf("empty: %v %v", dec, err)
	}
	zeros := make([]byte, 700)
	dec, err := COBSDecode(COBSEncode(zeros))
	if err != nil || !bytes.Equal(dec, zeros) {
		t.Fatalf("all zeros: err=%v match=%v", err, bytes.Equal(dec, zeros))
	}
}

// TestTCPLSAKLoop proves the full TCP transport contract against an in-process
// fake relay: COBS framing on the wire, LSAK replies, and ACK dispatching.
func TestTCPLSAKLoop(t *testing.T) {
	listener, err := newFakeRelay()
	if err != nil {
		t.Fatalf("fake relay: %v", err)
	}
	defer listener.Close()

	acks := make(chan uint32, 4)
	tcp := NewTCP(listener.Addr().String(), func(id uint32, status uint8) {
		if status != lep.AckStored {
			t.Errorf("status = %d, want AckStored", status)
		}
		acks <- id
	})
	defer tcp.Close()

	env := &lep.Envelope{
		Raw:     lep.Encode(lep.Version2, lep.TypeCrash, lep.ArchCortexM, 0, 7, 0xCAFEBABE, nil),
		EventID: 0xCAFEBABE,
	}
	if err := tcp.Send(t.Context(), env); err != nil {
		t.Fatalf("Send: %v", err)
	}
	select {
	case id := <-acks:
		if id != 0xCAFEBABE {
			t.Fatalf("ack id = %x", id)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("no LSAK received")
	}
}
