package device

import (
	"testing"

	"github.com/laststate/simulator/internal/lep"
)

func TestGenerateProducesValidEnvelopes(t *testing.T) {
	fleet := DefaultFleet(5, 32)
	kinds := []FaultKind{FaultNone, FaultHardFault, FaultAssert, FaultWatchdog, FaultPeripheral, FaultBrownout}

	for _, dev := range fleet {
		prevSeq := uint32(0)
		for _, kind := range kinds {
			env := dev.Generate(kind, false)
			if err := lep.Validate(env.Raw); err != nil {
				t.Fatalf("%s %v: %v", dev.ID(), kind, err)
			}
			if env.Seq <= prevSeq {
				t.Fatalf("%s: sequence not monotonic (%d after %d)", dev.ID(), env.Seq, prevSeq)
			}
			prevSeq = env.Seq
			if env.Summary == "" {
				t.Fatalf("%s: empty summary", dev.ID())
			}
		}
	}
}

func TestGenerateCoversAllTransports(t *testing.T) {
	fleet := DefaultFleet(5, 32)
	seen := map[TransportKind]bool{}
	for _, dev := range fleet {
		seen[dev.Transport] = true
	}
	for _, want := range []TransportKind{TransportHTTP, TransportTCP, TransportUDP} {
		if !seen[want] {
			t.Fatalf("default fleet missing transport %s", want)
		}
	}
}

func TestOfflineBufferFIFOAndOverflow(t *testing.T) {
	dev := New(Profile{DeviceID: "buf-test", Architecture: lep.ArchCortexM}, TransportHTTP, 4)

	for i := 0; i < 6; i++ {
		dev.Park(&lep.Envelope{EventID: uint32(i)})
	}
	if got := dev.Stats.Dropped.Load(); got != 2 {
		t.Fatalf("dropped = %d, want 2 (cap 4, parked 6)", got)
	}
	drained := dev.Drain()
	if len(drained) != 4 {
		t.Fatalf("drained = %d, want 4", len(drained))
	}
	if drained[0].EventID != 2 || drained[3].EventID != 5 {
		t.Fatalf("FIFO order broken: %v", []uint32{drained[0].EventID, drained[3].EventID})
	}
	if dev.BufferDepth() != 0 {
		t.Fatal("buffer not cleared after drain")
	}
}

func TestForceBootIncrementsBootCountAndRecharges(t *testing.T) {
	dev := New(Profile{DeviceID: "boot-test", Architecture: lep.ArchRISCV}, TransportUDP, 8)
	before := dev.BootCount
	dev.BatteryMV = 2950

	env := dev.Generate(FaultNone, true)
	if err := lep.Validate(env.Raw); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if env.Type != lep.TypeReset {
		t.Fatalf("boot event type = %d, want reset", env.Type)
	}
	if dev.BootCount != before+1 {
		t.Fatalf("boot count %d -> %d", before, dev.BootCount)
	}
	if dev.BatteryMV < 3300 {
		t.Fatalf("battery not recharged on boot: %dmV", dev.BatteryMV)
	}
}

func TestConcurrentGenerateAndPark(t *testing.T) {
	dev := New(Profile{DeviceID: "race-test", Architecture: lep.ArchXtensa}, TransportTCP, 128)
	done := make(chan struct{})
	go func() {
		for i := 0; i < 200; i++ {
			dev.Park(dev.Generate(FaultNone, false))
		}
		close(done)
	}()
	for i := 0; i < 200; i++ {
		dev.Drain()
		_ = dev.Generate(FaultHardFault, false)
	}
	<-done
}
